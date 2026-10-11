# Flink jobs

Java, DataStream API. Planned jobs: `rollup`, `baseline-scoring`, `wait-profile`, `rac-rules`, `incident-correlator` (see `../docs/ARCHITECTURE.md`).

First task (M0/M1): `rollup` job from `ora.metric.raw.v1` to `ora.metric.rollup.v1` with 1 and 5 minute event-time windows and a bounded out-of-orderness watermark.

Flink writes to Kafka only. Anomaly and incident keys must be deterministic so replays are harmless.
