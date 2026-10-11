# Decisions

Status is **default** (chosen for now, revisit with evidence) or **open** (needs an answer before it blocks work).

| # | Decision | Default | Alternatives | Status |
|---|---|---|---|---|
| D1 | Transport | Kafka API: Apache Kafka locally, OCI Streaming with Apache Kafka in the cloud | TxEventQ via OKafka (no broker, transactional; ADB availability unconfirmed) | default |
| D2 | Flink to ADB write path | Flink writes to Kafka, a sink service MERGEs into ADB | Flink JDBC sink directly (needs Oracle dialect support in the connector version) | default |
| D3 | Flink language | Java DataStream API (best control over state and timers) | PyFlink | default |
| D4 | Baseline method | Seasonal median/MAD by hour of week | ETS/Prophet-style models, only if the simple method misses real incidents | default |
| D5 | License mode | Collector supports `base` and `diag-pack`, docs flag the pack requirement | base only | default |
| D6 | Long-term storage | ADB for 5 minute and 1 hour rollups, Parquet/Iceberg for raw data | everything in ADB | open (depends on Iceberg write path from Spark) |
| D7 | Where the LLM runs | External agent through MCP over curated views. Select AI for ad hoc questions from humans | Select AI Agent inside ADB | default |
| D8 | Repo license | Apache-2.0 | MPL-2.0, GPL-3.0 | open (decide before the first public commit) |
| D9 | Embedding model and dimension | `VECTOR(1024, FLOAT32)` is a placeholder in the schema | any other dimension | open (pick the model first, then fix the column) |
| D10 | Name | FleetDiag | Farol, Atalaia, WaitWatch (considered) | decided 2026-10-11 |
