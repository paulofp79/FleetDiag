# FleetDiag

Open-source fleet observability and AI-assisted diagnostics for Oracle databases
(RAC, Exadata, ADB). Pipeline: collector -> Kafka -> Flink -> sink -> Oracle (ADB later);
Spark for baselines; MCP server + LLM to explain incidents. Statistics detect, the LLM
only explains. Design docs: docs/ARCHITECTURE.md, docs/DECISIONS.md (D1-D10),
docs/ROADMAP.md, docs/INSTALL.md. Read them before changing design.

## Working style
- Owner is Paulo (Oracle RAC/Exadata expert, macOS Apple Silicon, zsh). Keep answers
  concise and practical, natural language.
- Paulo prefers to run commands himself. Give him commands and wait for output unless he
  asks you to run them.
- Never commit or print secrets: deploy/.env, Wallet_*, wallet/, cwallet.sso, ewallet.*.
  DB password only via:
  export ORACLE_PASSWORD="$(grep ORACLE_SOURCE_PWD deploy/.env | cut -d= -f2)"
- GitHub repo (git@github.com:paulofp79/FleetDiag.git) stays private until license (D8) is decided.
- zsh does not word-split variables: never put a command in a variable ($DC); use shell
  functions instead.
- Python venv: collector/.venv (Python 3.13). Activate before running collector or sinks.
- Heredocs in docs: use ~~~ fences inside docs so chat code blocks do not break.

## Layout
- collector/collector.py  Oracle -> Kafka (start|stop|status|run), queries in collector/queries/, metric allow-list in collector/config/metrics.yaml
- sink/sink.py            Kafka ora.metric.raw.v1 -> fd_metric_rollup (bucket_sec=60, exact end_time), idempotent MERGE
- sink/rollup_sink.py     Kafka ora.metric.rollup.v1 -> fd_metric_rollup (any bucket_sec), idempotent MERGE
- flink/sql/rollup_5m.sql 5-minute tumbling rollup job (Flink SQL), jars in flink/extra
- deploy/                 docker-compose.yml (kafka, flink, oracle-source), docker-compose.flink.yml (UseSVE fix + mounts), create-topics.sh
- sql/001_schema.sql, 002_views.sql, schemas/*.json, scripts/m0_check.sh

## Environment facts
- Kafka apache/kafka:3.9.0 KRaft, internal kafka:19092, host localhost:9092.
- Apple Silicon JVM crash fixed with JAVA_TOOL_OPTIONS=-XX:UseSVE=0 (set for Kafka and Flink).
- Oracle: container oracle-source (gvenzl/oracle-free:23-faststart), PDB FREEPDB1, user fleetdiag. In a PDB, v$sysmetric is empty: 60 s metrics are in gv$con_sysmetric group_id 18 (root: gv$sysmetric group 2).
- Oracle intervals are slightly longer than 60 s; key rows on exact end_time, never truncate to the minute.
- Flink: use `dcf` function: docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.flink.yml "$@". UI http://localhost:8081. Submit: dcf exec -T flink-jobmanager ./bin/sql-client.sh -l /opt/flink/extra -f /opt/flink/sql/rollup_5m.sql (submitting twice creates a duplicate job; check first).
- Local timezone America/Sao_Paulo (UTC-3); all data timestamps are UTC.

## Status (2026-10-11)
- M0 walking skeleton: collector, Kafka, sink work. Replay test passed (936 messages replayed, 912 rows, no duplicates). Restart tests passed for sink, Kafka, collector, Oracle (collector/Oracle restarts leave expected gaps; collector does not backfill).
- BUG (fixed 2026-10-11: collector re-picks view on empty poll and logs STALL): the collector picks its query view once at startup. After an Oracle restart the PDB view was still empty, so it fell back to the empty root view and published nothing for ~40 min while still "running". Fix needed: re-run pick_query when a poll returns no rows (try/except oracledb.Error), and log ERROR / exit after N consecutive empty polls so a silent stall cannot happen.
- The 24 h M0 run was restarted after the fix; start time is the last line of sink/run/m0_start.txt. Restart it after the collector fix: write start time with `date -u +"%Y-%m-%d %H:%M:%S" | tee sink/run/m0_start.txt`, keep the Mac awake (nohup caffeinate -dimsu), then run scripts/m0_check.sh (filters bucket_sec = 60). Pass: no gaps over 90 s, no near-duplicates under 30 s, equal rows per metric.
- M1 step 1 done: Flink 5-min rollup job verified (Flink avg equals SQL average of 60 s rows). rollup_sink.py writes bucket_sec=300 rows.

## Next
1. Fix collector stall (above), restart the 24 h run.
2. M1: Spark seasonal baselines (median/MAD per series per hour-of-week, 168 slots), robust z-score, EWMA fallback, then Flink scoring and RAC rules, then incident correlator.
3. Later: collector backfill from gv$con_sysmetric_history, ADB (cloud) testing (open: ATP vs ADW, wallet vs TLS, whether ADB exposes group 18).
4. Open decisions: D6 Iceberg write path, D8 license, D9 embedding dimension.
