# Collector

Python process (python-oracledb, thin mode) that polls the queries in `queries/` and publishes events to Kafka, validated against `../schemas/`.

First task (M0): read `queries/sysmetric_60s.sql` for one database every 60 s, filter by `config/metrics.yaml`, publish `metric_sample.v1` to `ora.metric.raw.v1`.

Rules:
- Monitoring account with SELECT on named views only.
- Keep a per-instance watermark and persist it, so restarts don't skip or duplicate data.
- `base` mode never touches ASH or AWR. `diag-pack` mode needs the Diagnostics Pack license.
- Never publish SQL text. Publish `sql_id` only.
