# Event-log patterns beyond the official examples

Use the [current table reference](https://docs.databricks.com/aws/en/admin/system-tables/pipeline-events) to confirm the path. Bind the scope parameters in Databricks SQL. These examples were tested with representative inputs in an Azure Databricks workspace; inspect payloads and metric meaning in the target pipeline before using them in an alert.

## Time since the last completed batch

Use the [previous-updates example](https://docs.databricks.com/aws/en/ldp/monitor-event-logs#monitor-pipeline-updates-by-querying-previous-updates) to identify `:update_id`. Include its start in the time window. This returns flows with a `flow_definition` or `stream_progress` event in that window. A `NULL` last batch means none was observed in the window, not that the pipeline is stuck. Check event delivery, expected cadence, and whether input or backlog exists before alerting.

```sql
WITH scoped AS (
  SELECT event_time, pipeline_event_id, event_type, origin.flow_name,
         variant_get(details, '$.update_progress.state', 'STRING') AS update_state
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
)
SELECT flows.flow_name, latest_state.update_state, bounds.update_started_at,
       flows.last_completed_batch_at,
       CASE WHEN latest_state.update_state = 'RUNNING'
             AND bounds.update_started_at IS NOT NULL
            THEN timestampdiff(SECOND,
                   coalesce(flows.last_completed_batch_at, bounds.update_started_at),
                   current_timestamp())
       END AS seconds_since_batch_or_start
FROM flows CROSS JOIN bounds LEFT JOIN latest_state ON true
ORDER BY flows.flow_name;
```

## Backlog drain and catch-up

Choose a recent window long enough to smooth bursts. This is **net** backlog change, not input throughput. Compare only like-for-like sources and flows; a missing backlog, reset, growing backlog, or nonpositive drain yields no catch-up estimate. Re-run after restarts rather than carrying a forecast across an update boundary.

```sql
WITH samples AS (
  SELECT workspace_id, pipeline_id, update_id, origin.flow_id AS flow_id,
         origin.flow_name AS flow_name,
         event_time,
         variant_get(details, '$.flow_progress.metrics.backlog_bytes', 'DOUBLE') AS backlog_bytes
  FROM system.lakeflow_pipeline_events_preview.pipeline_events
  WHERE workspace_id = :workspace_id AND pipeline_id = :pipeline_id
    AND event_type = 'flow_progress'
    AND event_time >= :start_time AND event_time < :end_time
), windowed AS (
  SELECT workspace_id, pipeline_id, update_id, flow_id, flow_name,
         count(*) AS samples, min(event_time) AS first_at, max(event_time) AS last_at,
         min_by(backlog_bytes, event_time) AS first_backlog_bytes,
         max_by(backlog_bytes, event_time) AS last_backlog_bytes
  FROM samples
  WHERE flow_name = :flow_name AND backlog_bytes IS NOT NULL
  GROUP BY workspace_id, pipeline_id, update_id, flow_id, flow_name
), rates AS (
  SELECT *, timestampdiff(SECOND, first_at, last_at) AS elapsed_seconds,
         first_backlog_bytes - last_backlog_bytes AS bytes_cleared
  FROM windowed
)
SELECT workspace_id, pipeline_id, update_id, flow_id, flow_name, samples,
       first_at, last_at, first_backlog_bytes, last_backlog_bytes,
       CASE WHEN samples >= 2 AND elapsed_seconds > 0
            THEN bytes_cleared / elapsed_seconds END AS net_drain_bytes_per_second,
       CASE WHEN samples >= 2 AND elapsed_seconds > 0 AND bytes_cleared > 0
            THEN last_backlog_bytes * elapsed_seconds / bytes_cleared
       END AS estimated_seconds_to_zero
FROM rates;
```

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
