# Delays across pipelines connected by tables

## Resolve the starting identifier

Start with whatever the user has. Extract the pipeline ID and workspace from a pipeline URL if present, then verify them. A pipeline ID needs no name lookup. A fully qualified table name goes directly to the [table lineage lookup](#find-pipelines-around-one-table). For an exact pipeline name, use `system.lakeflow.pipelines` (latest SCD2 row by `change_time`) or the event-table lookup below. The event table is also useful for historical names; return every match and check the current name with the Pipelines API if it matters. A flow name can be looked up by replacing `origin.pipeline_name` with `origin.flow_name` below; it may occur in several pipelines.

```sql
SELECT workspace_id, pipeline_id, origin.pipeline_name AS matched_name,
       max(event_time) AS last_seen
FROM system.lakeflow_pipeline_events_preview.pipeline_events
WHERE workspace_id = :workspace_id
  AND event_time >= :start_time AND event_time < :end_time
  AND origin.pipeline_name = :pipeline_name
GROUP BY workspace_id, pipeline_id, origin.pipeline_name
ORDER BY last_seen DESC;
```

A pipeline update ID maps directly through top-level `update_id`. `system.lakeflow.pipeline_update_timeline` or `system.access.table_lineage.entity_run_id` can help when event-table retention or delivery leaves a gap, but check their coverage first.

```sql
SELECT workspace_id, pipeline_id, update_id,
       max_by(origin.pipeline_name, event_time) AS observed_name,
       min(event_time) AS first_seen_in_window,
       max(event_time) AS last_seen_in_window
FROM system.lakeflow_pipeline_events_preview.pipeline_events
WHERE workspace_id = :workspace_id AND update_id = :update_id
  AND event_time >= :start_time AND event_time < :end_time
GROUP BY workspace_id, pipeline_id, update_id;
```

If “run ID” means Spark streaming `progress_json.runId`, use the bounded lookup below. It resolves a flow and update; it is **not** the pipeline update ID. A Jobs task run ID instead belongs to `system.lakeflow.pipeline_update_timeline.trigger_details.job_task.job_task_run_id` when that table has the update; a parent Jobs run ID first needs its task run via `system.lakeflow.job_task_run_timeline` or the Jobs API. Keep these identifiers separate and report when a mapping is unavailable.

```sql
SELECT workspace_id, pipeline_id, update_id, origin.flow_name AS flow_name,
       min(event_time) AS first_seen, max(event_time) AS last_seen
FROM system.lakeflow_pipeline_events_preview.pipeline_events
WHERE workspace_id = :workspace_id AND event_type = 'stream_progress'
  AND event_time >= :start_time AND event_time < :end_time
  AND get_json_object(
        variant_get(details, '$.stream_progress.progress_json', 'STRING'),
        '$.runId') = :spark_run_id
GROUP BY workspace_id, pipeline_id, update_id, origin.flow_name
ORDER BY last_seen DESC;
```

Once the pipeline is known, discover its table edges from time-bounded lineage. Inspect the candidates and exclude event-log tables and self-edges; confirm the flow that reads or writes the chosen shared table. If a pipeline has several outputs, retain all plausible paths until the user’s question or the event evidence selects one.

```sql
SELECT source_table_full_name, target_table_full_name,
       max_by(entity_run_id, event_time) AS latest_update_id,
       max(event_time) AS last_seen
FROM system.access.table_lineage
WHERE workspace_id = :workspace_id AND entity_type = 'PIPELINE'
  AND entity_id = :pipeline_id
  AND event_date >= :start_date_utc AND event_date < :end_date_utc
  AND event_time >= :start_time AND event_time < :end_time
  AND source_table_full_name IS DISTINCT FROM target_table_full_name
GROUP BY source_table_full_name, target_table_full_name
ORDER BY last_seen DESC;
```

For one shared table, `GET /api/2.0/lineage-tracking/table-lineage?table_name=<catalog.schema.table>&include_entity_lineage=true` gives immediate upstream and downstream tables plus `pipelineInfos` with pipeline and update IDs. Use `system.access.table_lineage` when you need a time-bounded history or many edges. For `entity_type = 'PIPELINE'`, `entity_id` is the pipeline ID and `entity_run_id` is the update ID. Lineage `event_time` records observed access; it is not a table commit or data-arrival time.

The identifier, stage, and offset queries below were tested with literal parameters in an Azure Databricks workspace. The arrival-versus-advance calculation was also backtested on one append-only Delta handoff with a single, stable source. Bind UTC time bounds and the corresponding UTC date partitions. Confirm current table paths and permissions in the target workspace.

## Find pipelines around one table

```sql
WITH matches AS (
  SELECT entity_id AS pipeline_id, entity_run_id AS update_id, event_time,
         CASE
           WHEN target_table_full_name = :shared_table
             AND source_table_full_name IS DISTINCT FROM :shared_table THEN 'writes'
           WHEN source_table_full_name = :shared_table
             AND target_table_full_name IS DISTINCT FROM :shared_table THEN 'reads'
         END AS relationship
  FROM system.access.table_lineage
  WHERE workspace_id = :workspace_id AND entity_type = 'PIPELINE'
    AND event_date >= :start_date_utc AND event_date < :end_date_utc
    AND event_time >= :start_time AND event_time < :end_time
    AND (source_table_full_name = :shared_table
      OR target_table_full_name = :shared_table)
)
SELECT relationship, pipeline_id,
       max_by(update_id, event_time) AS latest_update_id,
       max(event_time) AS last_lineage_seen_at
FROM matches
WHERE relationship IS NOT NULL
GROUP BY relationship, pipeline_id
ORDER BY relationship, pipeline_id;
```

## Compare the two flows in one window

Choose the flow actually writing the shared table and the flow reading it. The downstream flow's `backlog_bytes` describes its own source, not the producer's backlog. For a flow with several sources, inspect the `details:flow_progress.metrics.source_metrics` array and match its `source_name` before attributing backlog to this edge. For a Delta source, match its path or `sources[*].endOffset.reservoirId` to `DESCRIBE DETAIL <shared_table>`; do not infer the source from the flow name alone. Do not add byte backlogs across stages.

```sql
WITH scoped AS (
  SELECT pipeline_id, update_id, origin.flow_name AS flow_name,
         event_type, event_time,
         variant_get(details, '$.flow_progress.metrics.backlog_bytes', 'DOUBLE') AS backlog_bytes,
         try_cast(get_json_object(
           variant_get(details, '$.stream_progress.progress_json', 'STRING'),
           '$.batchDuration') AS BIGINT) AS batch_duration_ms,
         get_json_object(
           variant_get(details, '$.stream_progress.progress_json', 'STRING'),
           '$.eventTime.watermark') AS watermark
  FROM system.lakeflow_pipeline_events_preview.pipeline_events
  WHERE workspace_id = :workspace_id
    AND event_time >= :start_time AND event_time < :end_time
    AND event_type IN ('flow_progress', 'stream_progress')
    AND pipeline_id IN (:producer_pipeline_id, :consumer_pipeline_id)
    AND ((pipeline_id = :producer_pipeline_id AND origin.flow_name = :producer_flow)
      OR (pipeline_id = :consumer_pipeline_id AND origin.flow_name = :consumer_flow))
)
SELECT pipeline_id, update_id, flow_name,
       count_if(event_type = 'stream_progress') AS completed_batches,
       max(event_time) FILTER (WHERE event_type = 'stream_progress') AS latest_batch_at,
       max_by(batch_duration_ms, event_time) FILTER (
         WHERE event_type = 'stream_progress' AND batch_duration_ms IS NOT NULL
       ) AS latest_batch_duration_ms,
       percentile_approx(batch_duration_ms, 0.95) FILTER (
         WHERE event_type = 'stream_progress'
       ) AS p95_batch_duration_ms,
       max(event_time) FILTER (WHERE backlog_bytes IS NOT NULL) AS latest_backlog_at,
       max_by(backlog_bytes, event_time) FILTER (
         WHERE backlog_bytes IS NOT NULL
       ) AS latest_backlog_bytes,
       count_if(watermark IS NOT NULL) AS batches_with_watermark
FROM scoped
GROUP BY pipeline_id, update_id, flow_name
ORDER BY pipeline_id, update_id;
```

`stream_progress.progress_json.timestamp` plus `batchDuration` was approximately `event_time` in the sampled flows. Use these to describe batch processing time and completion; batches can overlap, so do not sum durations into a critical path.

## Measure a Delta table handoff

For a consumer reading one Delta table, inspect its source offsets. Match `source_table_id` to `DESCRIBE DETAIL <shared_table>` before interpreting the versions. If there are several sources, inspect every `sources` entry and the per-source backlog instead of assuming index zero is the shared table.

```sql
WITH batches AS (
  SELECT event_time, update_id,
         variant_get(details, '$.stream_progress.progress_json', 'STRING') AS p
  FROM system.lakeflow_pipeline_events_preview.pipeline_events
  WHERE workspace_id = :workspace_id AND pipeline_id = :consumer_pipeline_id
    AND origin.flow_name = :consumer_flow AND event_type = 'stream_progress'
    AND event_time >= :start_time AND event_time < :end_time
)
SELECT event_time, update_id,
       try_cast(get_json_object(p, '$.batchId') AS BIGINT) AS batch_id,
       get_json_object(p, '$.sources[0].endOffset.reservoirId') AS source_table_id,
       try_cast(get_json_object(p, '$.sources[0].endOffset.sourceVersion') AS INT) AS offset_format_version,
       try_cast(get_json_object(p, '$.sources[0].startOffset.reservoirVersion') AS BIGINT) AS start_version,
       try_cast(get_json_object(p, '$.sources[0].startOffset.index') AS BIGINT) AS start_index,
       try_cast(get_json_object(p, '$.sources[0].endOffset.reservoirVersion') AS BIGINT) AS end_version,
       try_cast(get_json_object(p, '$.sources[0].endOffset.index') AS BIGINT) AS end_index
FROM batches
ORDER BY event_time DESC LIMIT 20;
```

For the sampled Delta source, `sourceVersion = 1` and `index = -1` mark the position before all changes in that version ([Delta source offset definition](https://github.com/delta-io/delta/blob/master/spark/src/main/scala/org/apache/spark/sql/delta/sources/DeltaSourceOffset.scala)). A batch with both indices at `-1` covers source versions `[start_version, end_version)`. Verify this boundary on the target runtime; do not use the shortcut for partial offsets or another source type.

Use `DESCRIBE HISTORY <shared_table> LIMIT <n>` for source commit times and `DESCRIBE HISTORY <consumer_output_table> LIMIT <n>` for output commit times. Check that an output `STREAMING UPDATE` has `operationParameters.epochId = batch_id`. For each data-writing source commit in the version interval, subtract its commit time from that batch's output commit time. Exclude maintenance commits and batches with no output. This measures **table commit to output commit**, not source-event creation to final availability. Weighting each source commit equally does not yield a per-record latency distribution.

### Compare source arrivals with consumer advance

For an append-only shared Delta table, reuse the verified consumer version ranges above and `DESCRIBE HISTORY <shared_table>`. In each complete, equal-length window, calculate:

| Measure | Calculation using the shared table's data-writing commits |
| --- | --- |
| Arrival bytes | Sum `operationMetrics.numOutputBytes` for commits made during the window. |
| Advanced bytes | Sum the **same commits'** `numOutputBytes` for versions consumed by batches completed during the window. Count each version once. |
| Observed rates | Divide each sum by the window's elapsed seconds; also report `arrival_rate - advance_rate` and `advance_bytes / arrival_bytes` when arrivals are nonzero. |

This compares bytes in the same compressed Delta files. Arrival rate above advance rate suggests accumulation; advance rate above arrival rate suggests recovery. Similar rates can sustain a nonzero backlog. Show the first and last source-specific backlog readings beside the rates, and investigate if their direction disagrees. The rate difference need not numerically equal the backlog change: backlog readings are snapshots inside the window, while history records file writes. Confirm that history covers every commit in the arrival window and every consumed version, and that each data-writing commit has `numOutputBytes`; otherwise report the affected rate as unknown. Exclude `OPTIMIZE`, metadata-only and zero-byte commits. Rewrites, deletes, partial offsets, or a changed source set need a different accounting method; do not call these figures bytes actually read or maximum processing capacity.

### Compare bytes written across the handoff

For the same matched batches, check `operationMetrics.numOutputBytes` and `numOutputRows` in both `DESCRIBE HISTORY` results. For each consumer batch, sum the producer's data-writing commit bytes for the source versions it consumed, then divide that batch's output-commit bytes by the sum. Across several batches, use **sum of output bytes / sum of matched input-table bytes**, rather than averaging batch ratios. Also report the row-count ratio and bytes per output row when the metrics are present; these help distinguish filtering from wider output rows. Omit batches with zero input bytes or missing metrics, and report how many batches qualified.

Call this a **Delta write-byte ratio**: the producer bytes are compressed files written to the shared table, not bytes the consumer actually read. Exclude `OPTIMIZE` and other maintenance writes. Rewrites, file layout, compression, joins, and multiple inputs can change the ratio; do not attribute all output bytes to one source when the flow reads several tables. This ratio describes the matched handoff's storage footprint, not record-level latency.

## Choose the delay you mean

| Question | Evidence and limit |
| --- | --- |
| Which stage is falling behind? | Compare each flow's backlog trend with its own healthy band, and compare batch-duration distributions over the same window. A nonzero end-of-batch backlog can be normal. Backlog units and sources can differ. |
| When was the shared table published? | `DESCRIBE HISTORY <shared_table>` gives Delta commit versions and timestamps. Filter to data-writing operations; maintenance such as `OPTIMIZE` also creates versions. A recent commit alone does not prove the consumer has read it. |
| Has the consumer reached a producer version? | Use the Delta handoff method above when the source ID and offset boundary are verified. A version gap alone is not elapsed time. |
| How old is the newest event in the final table? | If an original event timestamp survives every stage, query its maximum at each table and calculate `current_timestamp() - max(event_timestamp)` with an appropriate data window. Validate timestamp meaning and clock skew. This is a freshness indicator, not the distribution of record delays. |
| What is end-to-end latency for records? | Carry an original event ID and timestamp plus a final processing or publication timestamp; measure their difference on matched records or a canary. Lineage, backlog, and batch durations alone cannot produce this value. |

Check for `progress_json.eventTime.max` and `.watermark` only after inspecting the flow payload. A Spark watermark is a late-data threshold driven by event-time state and allowed lateness; it is not the latest event timestamp or an end-to-end latency measurement. In sampled producer and consumer flows, neither `eventTime` nor a watermark was present.

Before querying records for event-time freshness, check `DESCRIBE DETAIL` and bound a partition or use an existing latency aggregate. A full scan can be prohibitive for a large streaming table. Confirm what each candidate timestamp means; an ingestion timestamp copied from raw to final does not mark final publication.
