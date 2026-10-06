# SDP event log skill

A Codex skill for investigating Lakeflow Spark Declarative Pipelines from a pipeline ID or name, update ID, Spark run ID, flow, or table. It uses tested SQL patterns and tells the agent when the event log cannot establish an answer.

## Questions it answers

1. Is a pipeline running, and when did each flow last finish a batch?
2. Is a flow's backlog accumulating, steady, or recovering relative to its normal level?
3. Which settings actually started a pipeline update?
4. What is the end-to-end latency of a flow, or the narrower table handoff latency when record timestamps are unavailable?
5. What changed between the last healthy update and this one?

## Install

Place this repository's `SKILL.md` and `references/` directory in `~/.codex/skills/sdp-event-log/`. Then ask Codex one of the questions above with a pipeline, update, flow, or table identifier.

## Data access

The primary SQL source is the Databricks [pipeline events system table](https://docs.databricks.com/aws/en/admin/system-tables/pipeline-events). Cross-pipeline questions also use Unity Catalog table lineage and Delta history. For an append-only Delta handoff, source commit bytes and consumer offsets give comparable arrival and advance rates. Exact start configuration may require the pipeline owner's [`event_log()` table-valued function](https://docs.databricks.com/aws/en/ldp/monitor-event-logs) or the [Pipeline Events API](https://docs.databricks.com/api/pipelines/v2/events), because the preview system table omits the configuration map. Record-level end-to-end latency requires source event timestamps carried through to the output; batch duration and lineage time are different measures.

The skill reports the last completed batch separately from the latest explicit backlog reading. Spark sink `numOutputRows = -1` means its row count is unavailable; it does not mean zero backlog.

The identifier, stage, offset, windowed backlog, and arrival-versus-advance patterns were tested in one Azure Databricks workspace. Healthy-band classification needs representative healthy windows, which were unavailable in that backtest. Confirm table paths, permissions, source metrics, and runtime payloads in your workspace.
