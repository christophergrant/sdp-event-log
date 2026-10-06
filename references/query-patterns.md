# Event-log patterns beyond the official examples

Use the [current table reference](https://docs.databricks.com/aws/en/admin/system-tables/pipeline-events) to confirm the path. Bind the scope parameters in Databricks SQL. The windowed backlog query was live tested on a single-source Delta flow in an Azure Databricks workspace. Inspect payloads and metric meaning in the target pipeline before using them in an alert.

## Time since the last completed batch

Use the [previous-updates example](https://docs.databricks.com/aws/en/ldp/monitor-event-logs#monitor-pipeline-updates-by-querying-previous-updates) to identify `:update_id`. Include its start in the time window. This returns flows with a `flow_definition` or `stream_progress` event in that window. A `NULL` last batch means none was observed in the window, not that the pipeline is stuck. Check event delivery, expected cadence, and whether input or backlog exists before alerting. Use the system table for this check: a Pipeline Events API listing may omit recent progress rows that the system table contains.

Compare the last completed batch with the last explicit backlog snapshot. A zero `backlog_bytes` means no reported backlog **at that snapshot**, even if the batch wrote rows. A missing or old backlog reading means current backlog is unknown. Spark's [`sink.numOutputRows = -1`](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/streaming/SinkProgress.html) means the sink did not report an output row count; it says nothing about backlog. When a flow has multiple sources, use the matching entry in `flow_progress.metrics.source_metrics`.

```sql
WITH scoped AS (
  SELECT event_time, pipeline_event_id, event_type, origin.flow_name,
         variant_get(details, '$.update_progress.state', 'STRING') AS update_state,
         variant_get(details, '$.flow_progress.metrics.backlog_bytes', 'BIGINT') AS backlog_bytes
  FROM system.lakeflow_pipeline_events_preview.pipeline_events
  WHERE workspace_id = :workspace_id AND pipeline_id = :pipeline_id
    AND update_id = :update_id
    AND event_time >= :start_time AND event_time < :end_time
), latest_state AS (
  SELECT update_state FROM scoped WHERE event_type = 'update_progress'
  ORDER BY event_time DESC, pipeline_event_id DESC LIMIT 1
), bounds AS (
  SELECT min(CASE WHEN event_type = 'create_update' THEN event_time END) AS update_started_at
  FROM scoped
), flows AS (
  SELECT flow_name,
         max(CASE WHEN event_type = 'stream_progress' THEN event_time END) AS last_completed_batch_at
  FROM scoped
  WHERE flow_name IS NOT NULL
    AND event_type IN ('flow_definition', 'stream_progress')
  GROUP BY flow_name
), backlog AS (
  SELECT flow_name,
         max(event_time) AS last_backlog_at,
         max_by(backlog_bytes, event_time) AS last_backlog_bytes
  FROM scoped
  WHERE event_type = 'flow_progress' AND backlog_bytes IS NOT NULL
    AND flow_name IS NOT NULL
  GROUP BY flow_name
)
SELECT flows.flow_name, latest_state.update_state, bounds.update_started_at,
       flows.last_completed_batch_at, backlog.last_backlog_at,
       backlog.last_backlog_bytes,
       CASE WHEN latest_state.update_state = 'RUNNING'
             AND bounds.update_started_at IS NOT NULL
            THEN timestampdiff(SECOND,
                   coalesce(flows.last_completed_batch_at, bounds.update_started_at),
                   current_timestamp())
       END AS seconds_since_batch_or_start
FROM flows CROSS JOIN bounds LEFT JOIN latest_state ON true
LEFT JOIN backlog ON backlog.flow_name = flows.flow_name
ORDER BY flows.flow_name;
```

## Backlog trend and return to baseline

`flow_progress` backlog is a snapshot after processing, so a busy continuous source can have a nonzero reading after every batch. Bind a positive `:window_seconds` long enough to include several readings and a normal burst cycle. Set `:start_time` and `:end_time` on window boundaries, then use three consecutive, complete windows from the same update and source set. This measures **net** backlog change, not input throughput. The SQL below uses the flow's total backlog; when it has multiple sources, inspect the `metrics.source_metrics` array and select the matching `source_name` before assigning a trend to one table.

```sql
WITH samples AS (
  SELECT workspace_id, pipeline_id, update_id, origin.flow_id AS flow_id,
         origin.flow_name AS flow_name,
         event_time,
         CAST(floor(unix_timestamp(event_time) / :window_seconds) AS BIGINT) AS window_id,
         variant_get(details, '$.flow_progress.metrics.backlog_bytes', 'DOUBLE') AS backlog_bytes
  FROM system.lakeflow_pipeline_events_preview.pipeline_events
  WHERE workspace_id = :workspace_id AND pipeline_id = :pipeline_id
    AND update_id = :update_id AND origin.flow_name = :flow_name
    AND event_type = 'flow_progress'
    AND event_time >= :start_time AND event_time < :end_time
), windowed AS (
  SELECT workspace_id, pipeline_id, update_id, flow_id, flow_name, window_id,
         count(*) AS samples, min(event_time) AS first_at, max(event_time) AS last_at,
         min_by(backlog_bytes, event_time) AS first_backlog_bytes,
         max_by(backlog_bytes, event_time) AS last_backlog_bytes,
         percentile_approx(backlog_bytes, 0.5) AS median_backlog_bytes,
         min(backlog_bytes) AS min_backlog_bytes,
         max(backlog_bytes) AS max_backlog_bytes
  FROM samples
  WHERE backlog_bytes IS NOT NULL
  GROUP BY workspace_id, pipeline_id, update_id, flow_id, flow_name, window_id
), rates AS (
  SELECT *, timestampdiff(SECOND, first_at, last_at) AS elapsed_seconds,
         last_backlog_bytes - first_backlog_bytes AS net_change_bytes
  FROM windowed
)
SELECT workspace_id, pipeline_id, update_id, flow_id, flow_name, window_id, samples,
       first_at, last_at, first_backlog_bytes, last_backlog_bytes,
       median_backlog_bytes, min_backlog_bytes, max_backlog_bytes,
       CASE WHEN samples >= 2 AND elapsed_seconds > 0
            THEN net_change_bytes / elapsed_seconds
       END AS net_backlog_change_bytes_per_second
FROM rates
ORDER BY window_id;
```

Build a baseline from comparable load and time-of-day windows when the flow met its freshness target. Let `U` be the p95 of their window medians, and `E` the p95 of their absolute changes between truly consecutive healthy windows in the same update. As a default, require at least 20 such baseline windows. For the last three complete, consecutive windows, let `M0`, `M1`, and `M2` be their median backlogs. Require at least five readings per window and use the same source set throughout; increase the window length if the flow has fewer readings.

| Rule | Classification |
| --- | --- |
| `M2 > U` and both `M1 - M0 > E` and `M2 - M1 > E` | Accumulating |
| `M2 > U` and both `M1 - M0 < -E` and `M2 - M1 < -E` | Recovering |
| `M2 > U` and neither sustained direction holds | Elevated or variable; check data age |
| `M2 <= U` | Within the healthy band; mention a rising trend if both increases exceed `E` |

If individual readings spike but window medians return to the band, describe them as bursty. Without representative healthy windows, report only the observed direction, not a health label. Report **unknown** for sparse or nonconsecutive windows, resets, a changed source set, or missing readings. For an append-only Delta source, compare source arrival and consumer advance rates over these same windows using [cross-pipeline handoff history](cross-pipeline.md#compare-source-arrivals-with-consumer-advance). Sustained disagreement between those rates and the backlog trend calls for investigating metric semantics or incomplete history, not forcing a label. Zero is a target only when the source is expected to go idle.

If backlog is recovering, estimate time to the healthy baseline as `(M2 - baseline_bytes) / sustained_net_drain_bytes_per_second`, with the positive drain rate estimated from the decline of window medians over elapsed time. Report it as conditional, and only when adjacent windows show a similar drain rate. Recompute after restarts or load changes. Backlog bytes alone cannot give record age: for a Delta source, use its offsets and source commit timestamps to check the age of unconsumed versions; use event timestamps carried with records for true data freshness.

## Classic autoscaling evidence

The documented schema nests slot metrics in `cluster_resources.task_slot_metrics`. Check the observed payload if an older example uses flattened paths. Slot utilization and queued tasks explain workload pressure better than CPU alone. `autoscale` rows show what was requested; they do not prove the resize succeeded unless their status says so. Serverless returns no classic resource signal.

```sql
SELECT event_time, update_id, event_type,
       variant_get(details, '$.cluster_resources.task_slot_metrics.num_executors', 'BIGINT') AS executors,
       variant_get(details, '$.cluster_resources.task_slot_metrics.avg_task_slot_utilization', 'DOUBLE') AS slot_utilization,
       variant_get(details, '$.cluster_resources.task_slot_metrics.avg_num_queued_tasks', 'DOUBLE') AS queued_tasks,
       variant_get(details, '$.cluster_resources.autoscale_info.state', 'STRING') AS scaler_state,
       variant_get(details, '$.cluster_resources.autoscale_info.optimal_num_executors', 'BIGINT') AS optimal_executors,
       variant_get(details, '$.autoscale.status', 'STRING') AS resize_status,
       variant_get(details, '$.autoscale.requested_num_executors', 'BIGINT') AS requested_executors
FROM system.lakeflow_pipeline_events_preview.pipeline_events
WHERE workspace_id = :workspace_id AND pipeline_id = :pipeline_id
  AND update_id = :update_id
  AND event_time >= :start_time AND event_time < :end_time
  AND event_type IN ('cluster_resources', 'autoscale')
ORDER BY event_time, pipeline_event_id;
```

## Correlate flow backlog with classic executors

Carry executor readings within an update. Backlog stays on the flow that reported it. A missing executor reading, including on serverless, remains `NULL`. The first backlog points can lack an executor value when the last resource event preceded `:start_time`; widen the lookback if those points matter.

```sql
WITH scoped AS (
  SELECT workspace_id, pipeline_id, update_id, pipeline_event_id,
         event_time, event_type, origin.flow_name,
         variant_get(details, '$.cluster_resources.task_slot_metrics.num_executors', 'BIGINT') AS num_executors,
         variant_get(details, '$.flow_progress.metrics.backlog_bytes', 'BIGINT') AS backlog_bytes
  FROM system.lakeflow_pipeline_events_preview.pipeline_events
  WHERE workspace_id = :workspace_id AND pipeline_id = :pipeline_id
    AND update_id IS NOT NULL
    AND event_time >= :start_time AND event_time < :end_time
    AND event_type IN ('cluster_resources', 'flow_progress')
), annotated AS (
  SELECT *, last(num_executors, true) OVER (
           PARTITION BY workspace_id, pipeline_id, update_id
           ORDER BY event_time, pipeline_event_id
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
         ) AS executors_at_event
  FROM scoped
)
SELECT event_time, update_id, flow_name, backlog_bytes, executors_at_event
FROM annotated
WHERE event_type = 'flow_progress' AND backlog_bytes IS NOT NULL
ORDER BY event_time DESC, pipeline_event_id DESC;
```

## Rate from a named observed metric

Inspect `details:stream_progress.progress_json` first. Use this only if `:metric_name` names a per-batch row count with a `total` field. The result is not automatically input or output throughput. An absent metric or duration stays `NULL`. For a slow-batch question, project `p.durationMs` from the same parsed record and compare its available phase keys across batches; use the Spark UI or profile to explain a slow task inside a phase.

```sql
WITH parsed AS (
  SELECT event_time, update_id, origin.flow_name,
         from_json(
           variant_get(details, '$.stream_progress.progress_json', 'STRING'),
           'batchId BIGINT, batchDuration BIGINT, durationMs MAP<STRING,BIGINT>, observedMetrics MAP<STRING,STRUCT<total:BIGINT>>'
         ) AS p
  FROM system.lakeflow_pipeline_events_preview.pipeline_events
  WHERE workspace_id = :workspace_id AND pipeline_id = :pipeline_id
    AND event_type = 'stream_progress'
    AND event_time >= :start_time AND event_time < :end_time
), measured AS (
  SELECT event_time, update_id, flow_name, p.batchId AS batch_id,
         element_at(p.observedMetrics, :metric_name).total AS metric_total,
         coalesce(p.batchDuration, element_at(p.durationMs, 'triggerExecution')) AS duration_ms
  FROM parsed
)
SELECT *, CASE WHEN duration_ms > 0 AND metric_total IS NOT NULL
               THEN round(metric_total * 1000.0 / duration_ms, 2)
          END AS metric_total_per_second
FROM measured
ORDER BY event_time DESC;
```

## Compare two update windows

Choose a baseline known to have been healthy and two equal-length windows for the same flow and comparable load. The summary below describes differences; it does not identify a cause. Inspect `create_update` configuration in the per-pipeline log or raw API, since the system table omits its configuration map. Check `user_action`, source changes, and individual resource events for timing, especially when the summary hides a short spike.

```sql
WITH windows AS (
  SELECT 'healthy' AS period, :healthy_update_id AS update_id,
         CAST(:healthy_start_time AS TIMESTAMP) AS start_at,
         CAST(:healthy_end_time AS TIMESTAMP) AS end_at
  UNION ALL
  SELECT 'current', :current_update_id,
         CAST(:current_start_time AS TIMESTAMP),
         CAST(:current_end_time AS TIMESTAMP)
), scoped AS (
  SELECT w.period, e.update_id, e.event_time, e.event_type,
         e.origin.flow_name AS flow_name,
         try_cast(get_json_object(
           variant_get(e.details, '$.stream_progress.progress_json', 'STRING'),
           '$.batchDuration') AS BIGINT) AS batch_duration_ms,
         variant_get(e.details, '$.flow_progress.metrics.backlog_bytes', 'DOUBLE') AS backlog_bytes,
         variant_get(e.details, '$.cluster_resources.task_slot_metrics.num_executors', 'BIGINT') AS executors,
         variant_get(e.details, '$.runtime_details.runtime_version.dbr_version', 'STRING') AS dbr_version
  FROM system.lakeflow_pipeline_events_preview.pipeline_events e
  JOIN windows w ON e.update_id = w.update_id
    AND e.event_time >= w.start_at AND e.event_time < w.end_at
  WHERE e.workspace_id = :workspace_id AND e.pipeline_id = :pipeline_id
    AND e.event_time >= LEAST(CAST(:healthy_start_time AS TIMESTAMP),
                              CAST(:current_start_time AS TIMESTAMP))
    AND e.event_time < GREATEST(CAST(:healthy_end_time AS TIMESTAMP),
                                CAST(:current_end_time AS TIMESTAMP))
    AND e.event_type IN ('stream_progress', 'flow_progress',
                         'cluster_resources', 'runtime_details')
)
SELECT period, update_id,
       max_by(dbr_version, event_time) FILTER (WHERE dbr_version IS NOT NULL) AS dbr_version,
       count_if(event_type = 'stream_progress' AND flow_name = :flow_name) AS completed_batches,
       percentile_approx(CASE WHEN event_type = 'stream_progress' AND flow_name = :flow_name
                              THEN batch_duration_ms END, 0.95) AS p95_batch_ms,
       min_by(backlog_bytes, event_time) FILTER (
         WHERE flow_name = :flow_name AND backlog_bytes IS NOT NULL) AS first_backlog_bytes,
       max_by(backlog_bytes, event_time) FILTER (
         WHERE flow_name = :flow_name AND backlog_bytes IS NOT NULL) AS last_backlog_bytes,
       min(executors) AS min_executors, max(executors) AS max_executors
FROM scoped
GROUP BY period, update_id
ORDER BY period;
```
