# FleetDiag

Fleet-level observability and AI-assisted diagnostics for Oracle databases (RAC, Exadata, Autonomous), built on Kafka, Apache Flink, Apache Spark and Oracle Autonomous Database.

**Status:** design stage. Nothing in this repo has been run yet. The SQL, compose file and collector queries are drafts to be tested in milestone M0 (see `docs/ROADMAP.md`).

## Why

Oracle gives you very good single-database diagnostics (AWR, ASH, ADDM). What is missing is the fleet view: a baseline per database and per hour of the week, detection in minutes instead of at the next report, and an explanation that points to what happened the last time.

## How it fits together

- **Collector** pulls metrics and wait data from each database and publishes events to Kafka.
- **Flink** rolls events up, scores them against seasonal baselines and groups related anomalies into incidents.
- **Spark** computes the baselines nightly and runs the heavy batch work: period comparisons, archiving, embeddings.
- **Autonomous Database** stores rollups, incidents and embeddings, and serves vector search plus the curated views used by Select AI and the MCP server.
- **An LLM agent** explains incidents from the stored evidence. It does not decide what counts as an incident; the statistics do.

Details in `docs/ARCHITECTURE.md`.

## Layout

| Path | What it holds |
|---|---|
| `docs/` | Architecture, open decisions, roadmap |
| `sql/` | ADB schema and curated views |
| `schemas/` | JSON Schemas for the Kafka events |
| `collector/` | Python collector and the exact SQL it runs |
| `sink/` | Kafka-to-ADB writer (idempotent MERGE) |
| `flink-jobs/` | Streaming jobs (Java) |
| `spark-jobs/` | Batch jobs (PySpark) |
| `mcp-server/` | MCP server over the curated views |
| `deploy/` | Local stack (Kafka, Flink, Oracle Free) |

## License

Not chosen yet. Apache-2.0 is the suggestion, see `docs/DECISIONS.md` (D8).
