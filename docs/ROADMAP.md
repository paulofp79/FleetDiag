# Roadmap

Each milestone ends with a check you can run, not a feature list.

## M0: Walking skeleton
One source database, metrics only, end to end.
- Oracle Free container as source, Kafka and Flink via `deploy/docker-compose.yml`.
- Collector publishes 60 s metrics, sink MERGEs rollups into ADB, a notebook plots them.
- Test the SQL in `sql/` in a scratch schema and fix whatever breaks.
- **Exit:** one metric flows for 24 hours with no gaps and no duplicates, including after restarting each component.

## M1: Detection on metrics
- Flink rollup and baseline scoring with the EWMA fallback. Spark baseline job and the baseline topic.
- Anomalies written to ADB.
- **Exit:** an injected load spike is flagged within 3 minutes, and false positives are counted over a week on a quiet database.

## M2: Wait profile and RAC rules
This is the part nobody else will have.
- ASH deltas, wait profile, global cache rules, incident correlator.
- DBStress provides the ground truth.
- **Exit:** DBStress-induced global cache contention produces one incident that names the right event and the right instances.

## M3: Knowledge and agent
- Incident and runbook embeddings, vector search, MCP server, hypothesis write-back, DBA confirmation loop.
- **Exit:** replay 10 labeled past incidents and check that the labeled match appears in the top 3 similar incidents. Set the pass mark after the first run.

## M4: Fleet scale and packaging
- Synthetic generator for 3,500 databases and a load test against the sizing assumptions.
- Per-fleet visibility, deployment scripts for OCI, documentation, first public release.
- **Exit:** sustained ingest at the target rate with sink lag under one minute.

## M0 progress

- [x] Kafka, local Oracle, schema loaded
- [x] Collector (PDB metrics, 24 metrics per interval) publishing to Kafka
- [x] Sink with idempotent MERGE, manual offset commits
- [x] Replay test passed (936 messages replayed, row count unchanged)
- [x] Component restart tests (sink, collector, Kafka, Oracle)
- [ ] 24 h run with zero gaps and zero duplicates

## M1 progress

- [x] Flink containers (Apple Silicon fix) and 5 minute rollup job, verified against SQL
- [x] Rollup sink into fd_metric_rollup (bucket_sec = 300)
- [x] Collector stall fix and Oracle restart test
- [ ] Make the Flink job restartable (checkpoints, resubmit script)
- [ ] 1 hour rollup
- [ ] Spark seasonal baseline job (fd_baseline)
- [ ] Flink scoring against baselines (fd_anomaly)
