# Flink jobs

Java 17, DataStream API, Flink 1.20, one Maven project (`pom.xml`), one shaded jar
`target/fleetdiag-flink-jobs.jar` (Kafka connector and Jackson bundled, Flink provided).
Builds with any JDK 17+; tested with Homebrew OpenJDK 23 and Maven 3.9.

~~~bash
cd flink-jobs && mvn -q package      # runs the unit tests (ScorerTest)
cd .. && ./scripts/flink-submit.sh scoring
~~~

Jobs:
- `rollup`: still Flink SQL (`../flink/sql/rollup_5m.sql`), not in this project yet.
- `ScoringJob` (`fleetdiag-scoring`): `ora.metric.raw.v1` scored against the seasonal baseline
  from `ora.baseline.v1` (broadcast state), EWMA fallback, writes `ora.anomaly.v1`.
  Rules live in `Scorer` (no Flink dependency, unit tested).
- Planned: `wait-profile`, `rac-rules`, `incident-correlator` (see `../docs/ARCHITECTURE.md`).

Flink writes to Kafka only. Anomaly and incident keys are deterministic so replays are harmless.
