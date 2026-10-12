# Installation (local development stack)

Written on 2026-10-11 on macOS with Apple Silicon (arm64) and Docker Desktop. Commands run from the repo root unless noted.

| Step | Status |
|---|---|
| Git repo and GitHub remote | done |
| Kafka container | verified: broker starts, port 9092 reachable from the host |
| Kafka topics and smoke test | commands below, not yet confirmed |
| Oracle Free container | verified: starts, version banner returned |
| Metric source | verified: in a PDB use `gv$con_sysmetric` group 18 (section 6) |
| Python environment | verified (Python 3.13.3) |
| Schema in the local Oracle | verified: 7 tables, 4 views, no invalid objects |
| Collector | verified: runs in the background and publishes to Kafka |

## Prerequisites

- Docker Desktop (or OrbStack/Colima), running
- Python 3.11 or newer. Tested with Python 3.13.3 (`python3 --version`). Use whichever version you have, and adjust the commands below if your executable has a different name (for example `python3.12` from Homebrew or pyenv)
- git, and an SSH key registered with GitHub (`ssh -T git@github.com` greets you by username)

## 1. Git repository and GitHub

Keep a single repo at the project root. Do not run `git init` in subfolders.

```bash
cd "$HOME/Documents/ClaudeCode/FleetDiag"
git init -b main            # skip if the repo already exists

cat >> .gitignore <<'EOF'
.env
.DS_Store
.venv/
__pycache__/
*.pyc
.ipynb_checkpoints/
*.log
# ADB wallet files, never commit these
Wallet_*
wallet/
cwallet.sso
ewallet.p12
ewallet.pem
EOF

git add -A
git status --short
git diff --cached --name-only | grep -Ei '(^|/)\.env$|wallet' && echo "STOP: secret-looking file staged"

git commit -m "Add FleetDiag design docs and M0 skeleton"
git remote add origin git@github.com:paulofp79/FleetDiag.git
git push -u origin main
```

`deploy/.env` must not be staged. `deploy/.env.example` should be.

### Git troubleshooting

- **`origin` missing, or commits that don't contain the project files.** A nested `deploy/.git` made git commands run inside `deploy/` report a different repo than the project root. Find nested repos with `find . -maxdepth 3 -name .git -not -path "./.venv/*"`, and remove the nested one (`rm -rf deploy/.git`).
- **Junk in the first commit** (for example `.ipynb_checkpoints/`). If nothing has been pushed, untrack it and squash:
```bash
  git rm -r -q --cached .ipynb_checkpoints
  git checkout --orphan tmp
  git commit -m "Add FleetDiag design docs and M0 skeleton"
  git branch -D main && git branch -m main
```
- **Push rejected ("fetch first").** The GitHub repo was created with a README or license. Run `git pull --rebase origin main`, then push again.
- **HTTPS asks for a password.** GitHub no longer accepts passwords. Use the SSH remote, or `gh auth login`.

## 2. Kafka (single-node KRaft)

```bash
cd deploy
cp .env.example .env        # compose validates the whole file, so the .env must exist
docker compose up -d kafka
sleep 10
docker compose ps
docker compose logs kafka | grep -i started | tail -3
docker port kafka           # expect: 9092/tcp -> 0.0.0.0:9092
nc -vz localhost 9092       # expect: succeeded
```

A healthy start logs `Kafka Server started`. `docker compose ps` may show only `9092/tcp` in the PORTS column. That is a display quirk, and `docker port` is the real check.

Create the topics and run a smoke test on a throwaway topic:

```bash
./create-topics.sh
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --list

KT="docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092"
$KT --create --topic fd.smoketest --partitions 1 --replication-factor 1
echo '{"hello":"fleetdiag"}' | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:19092 --topic fd.smoketest
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic fd.smoketest --from-beginning --max-messages 1 --timeout-ms 15000
$KT --delete --topic fd.smoketest
```

Expected topics: `ora.anomaly.v1`, `ora.ash.raw.v1`, `ora.baseline.v1`, `ora.incident.v1`, `ora.metric.raw.v1`, `ora.metric.rollup.v1`, `ora.wait.rollup.v1`.

### Kafka troubleshooting: JVM crash on Apple Silicon

**Symptom.** The container exits at startup. The log shows `SIGILL (0x4)` with problematic frame `java.lang.System.registerNatives()` on `linux-aarch64`.

**Cause.** Reported for newer Apple chips (M4) under Docker Desktop. The reports I found do not confirm a root cause.

**Fix that worked here.** Pass `-XX:UseSVE=0` to the JVM through `JAVA_TOOL_OPTIONS` in the `kafka` service of `deploy/docker-compose.yml`:

```yaml
    environment:
      JAVA_TOOL_OPTIONS: "-XX:UseSVE=0"
      KAFKA_NODE_ID: 1
      # ... rest unchanged
```

Then recreate the container: `docker compose up -d --force-recreate kafka`.

**Other options reported by others:** `_JAVA_OPTIONS` instead of `JAVA_TOOL_OPTIONS`, updating Docker Desktop, or running the same images under OrbStack. The Flink containers will probably need the same variable.

References: https://forums.docker.com/t/kafka-fails-to-start/147141 and https://forum.keycloak.org/t/mac-m4-java-tool-options-xx-usesve-0/29899

## 3. Oracle Database Free container

The compose file uses `gvenzl/oracle-free:23-faststart`. It is multi-arch, so it runs on Apple Silicon, and it creates an application user (`fleetdiag`) on first start. It listens on port 1521, service `FREEPDB1`.

```bash
cd deploy
# set ORACLE_SOURCE_PWD in deploy/.env with an editor (letters and digits only; never commit it)
docker compose up -d oracle-source
docker compose logs -f oracle-source     # wait for "DATABASE IS READY TO USE!", then Ctrl+C
```

The first start pulls a large image and takes a few minutes. Data is kept in the `oracle-data` volume.

Verify (note `-T`, which is required when input comes from a heredoc):

```bash
docker compose exec -T oracle-source sqlplus -s system/"$(grep ORACLE_SOURCE_PWD .env | cut -d= -f2)"@//localhost/FREEPDB1 <<'SQL'
SELECT banner FROM v$version;
SET LINESIZE 200 PAGESIZE 50
SELECT group_id, COUNT(*) AS metrics FROM gv$sysmetric GROUP BY group_id ORDER BY 1;
SELECT group_id, inst_id, metric_name, ROUND(value,3) AS value FROM gv$sysmetric WHERE metric_name = 'Average Active Sessions';
SQL
```

Seen: `Oracle AI Database 26ai Free Release 23.26.3.0.0`. In `FREEPDB1` the two `gv$sysmetric` queries above return no rows. That is expected in a pluggable database; the metrics are in `gv$con_sysmetric` (see section 6).

### Oracle troubleshooting

- **`cannot attach stdin to a TTY-enabled container because stdin is not a terminal`.** Use `docker compose exec -T` when piping a heredoc.
- **ORA errors right after start.** The database is still opening. Wait for `DATABASE IS READY TO USE!` in the logs.
- **Start over.** `docker compose down -v` removes the containers and the `oracle-data` volume.

## 4. Python environment (collector)

The commands use the `python3` on your PATH. This was written with Python 3.13.3. If your version differs, use your own executable in the `venv` line. Packages are listed in `collector/requirements.txt`, so a very new or very old Python may need different package versions. Check the `pip install` output.

```bash
cd "$HOME/Documents/ClaudeCode/FleetDiag"
python3 -m venv collector/.venv
source collector/.venv/bin/activate
pip install --upgrade pip
pip install -r collector/requirements.txt
python -c "import oracledb, confluent_kafka; print('oracledb', oracledb.__version__, '| confluent-kafka', confluent_kafka.version()[0])"
```

## 5. Load the schema into the local database

Grants first. `SYSTEM` can grant the general privileges but not select on `SYS` views, so the second step uses `SYSDBA`:

```bash
cd deploy
PW="$(grep ORACLE_SOURCE_PWD .env | cut -d= -f2)"

docker compose exec -T oracle-source sqlplus -s system/"$PW"@//localhost/FREEPDB1 <<'SQL'
GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE SEQUENCE, CREATE ROLE, UNLIMITED TABLESPACE TO fleetdiag;
SQL

docker compose exec -T oracle-source sqlplus -s / as sysdba <<'SQL'
ALTER SESSION SET CONTAINER = FREEPDB1;
GRANT SELECT ON sys.gv_$sysmetric TO fleetdiag;
GRANT SELECT ON sys.gv_$system_event TO fleetdiag;
SQL
```

Then the schema and views, run as `fleetdiag`:

```bash
docker compose exec -T oracle-source sqlplus -s fleetdiag/"$PW"@//localhost/FREEPDB1 < ../sql/001_schema.sql
docker compose exec -T oracle-source sqlplus -s fleetdiag/"$PW"@//localhost/FREEPDB1 < ../sql/002_views.sql
docker compose exec -T oracle-source sqlplus -s fleetdiag/"$PW"@//localhost/FREEPDB1 <<'SQL'
SELECT object_type, COUNT(*) FROM user_objects GROUP BY object_type ORDER BY 1;
SELECT object_name, object_type FROM user_objects WHERE status <> 'VALID';
SQL
```

Seen on 2026-10-11: INDEX 34, INDEX PARTITION 4, LOB 15, LOB PARTITION 2, SEQUENCE 4, TABLE 11, TABLE PARTITION 4, VIEW 4, and no invalid objects. The 11 tables are the 7 FleetDiag tables plus, most likely, internal tables behind the two vector indexes. The DDL ran without errors on Oracle AI Database 26ai Free 23.26.3.

### Schema troubleshooting

- **`ORA-01031: insufficient privileges` on `GRANT SELECT ON sys.gv_$...` as SYSTEM.** SYSTEM does not hold those views with grant option. Run the grants as `SYSDBA`, as above.
- **Start over.** Run `DROP USER fleetdiag CASCADE;` as SYSTEM, then recreate the container with `docker compose down -v` and `docker compose up -d oracle-source`. The `down -v` removes the data volume too.

## 6. Collector (M0)

`collector/collector.py` reads the 60-second system metrics every 15 seconds and publishes one JSON event per metric per interval to `ora.metric.raw.v1`.

- Only metrics in `collector/config/metrics.yaml` are published.
- Events are validated against `schemas/metric_sample.v1.json` before sending.
- A value already published for the same interval is skipped. The state is in memory, so a restart can resend one interval. The sink is idempotent.
- Database times are converted to UTC using the offset between `SYSDATE` and the UTC time.

### Metric source: root container vs pluggable database

On startup the collector picks the source that has data:

| Where | View | Group |
|---|---|---|
| Non-CDB or root container | `gv$sysmetric` | 2, "System Metrics Long Duration" (60 s) |
| Pluggable database (`FREEPDB1`, and Autonomous Database is also a PDB) | `gv$con_sysmetric` | 18, "PDB System Metrics Long Duration" (60 s) |

In `FREEPDB1`, `v$sysmetric` is empty and `v$con_sysmetric` has 150 rows in group 18. The root container has groups 2 and 3 in `v$sysmetric`. Autonomous Database has not been tested yet.

Grant needed for the PDB source (run as `SYSDBA`):

```bash
cd deploy
docker compose exec -T oracle-source sqlplus -s / as sysdba <<'SQL'
ALTER SESSION SET CONTAINER = FREEPDB1;
GRANT SELECT ON sys.gv_$con_sysmetric TO fleetdiag;
SQL
```

Two allow-listed metrics do not exist at PDB level and were removed from `metrics.yaml`: `Average Synchronous Single-Block Read Latency` and `Host CPU Utilization (%)`. That leaves 24 metrics per interval.

### Settings

Environment variables:

| Variable | Default |
|---|---|
| `ORACLE_PASSWORD` | required |
| `ORACLE_USER` | `fleetdiag` |
| `ORACLE_DSN` | `localhost:1521/FREEPDB1` |
| `KAFKA_BOOTSTRAP` | `localhost:9092` |
| `KAFKA_TOPIC` | `ora.metric.raw.v1` |
| `FLEET` / `DB_UNIQUE_NAME` | `local` / `FREE` |
| `POLL_SEC` | `15` |

### Running it

The venv from section 4 must exist. The `export` reads the password from `deploy/.env` into the current terminal only; the background process inherits it.

```bash
cd "$HOME/Documents/ClaudeCode/FleetDiag"
source collector/.venv/bin/activate
export ORACLE_PASSWORD="$(grep ORACLE_SOURCE_PWD deploy/.env | cut -d= -f2)"

python collector/collector.py start     # background; prints the tail command
tail -f collector/run/collector.log     # follow the log; Ctrl+C leaves the collector running
python collector/collector.py status
python collector/collector.py stop
python collector/collector.py run       # foreground, stop with Ctrl+C
```

The log is `collector/run/collector.log` and the process ID is in `collector/run/collector.pid`. The log grows without limit for now. Do not start a second collector while one is running, or events will be published twice.

### Example output

Collector log, first minutes after `start` (2026-10-11):

```
2026-10-11 14:55:02,435 INFO connected to localhost:1521/FREEPDB1 as fleetdiag
2026-10-11 14:55:02,437 INFO metric source: gv$con_sysmetric group 18 (pluggable database)
2026-10-11 14:55:02,439 WARNING allow-listed metrics not found in this database: ['Average Synchronous Single-Block Read Latency', 'Host CPU Utilization (%)']
2026-10-11 14:55:02,474 INFO rows=150 published=24
2026-10-11 14:55:17,449 INFO rows=150 published=0
2026-10-11 14:55:32,453 INFO rows=150 published=0
2026-10-11 14:55:47,466 INFO rows=150 published=24
```

How to read it: the view returns 150 rows each poll. `published=24` appears about once a minute, when a new 60-second interval is available, and `published=0` means those values were already sent. The WARNING is from before the two metrics were removed from `metrics.yaml`.

Three messages from Kafka:

```bash
cd deploy
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic ora.metric.raw.v1 --from-beginning --max-messages 3 --timeout-ms 15000
```

```
Picked up JAVA_TOOL_OPTIONS: -XX:UseSVE=0
{"schema": "metric_sample.v1", "fleet": "local", "db_unique_name": "FREE", "inst_id": 1, "metric_name": "Session Count", "metric_unit": "Sessions", "value": 1.0, "begin_time": "2026-10-11T17:52:31Z", "end_time": "2026-10-11T17:53:32Z", "collected_at": "2026-10-11T17:54:29Z", "collector_id": "collector-1"}
{"schema": "metric_sample.v1", "fleet": "local", "db_unique_name": "FREE", "inst_id": 1, "metric_name": "Average Active Sessions", "metric_unit": "Active Sessions", "value": 5.41334661354582e-05, "begin_time": "2026-10-11T17:52:31Z", "end_time": "2026-10-11T17:53:32Z", "collected_at": "2026-10-11T17:54:29Z", "collector_id": "collector-1"}
{"schema": "metric_sample.v1", "fleet": "local", "db_unique_name": "FREE", "inst_id": 1, "metric_name": "CPU Usage Per Sec", "metric_unit": "CentiSeconds Per Second", "value": 0.00593127490039841, "begin_time": "2026-10-11T17:52:31Z", "end_time": "2026-10-11T17:53:32Z", "collected_at": "2026-10-11T17:54:29Z", "collector_id": "collector-1"}
Processed a total of 3 messages
```

The first line comes from the Kafka tool inside the container and is harmless. All times are UTC. 14:55 in Rio is 17:55Z, so the conversion is right. An interval runs about 61 seconds, for example 17:52:31Z to 17:53:32Z.

Total messages on the topic:

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --bootstrap-server kafka:19092 --topic ora.metric.raw.v1
```

### Collector troubleshooting

- **`source: no such file or directory: collector/.venv/bin/activate`.** The venv was never created. Do section 4 first.
- **"query returned no rows".** Neither source has data. Run the diagnostics as `SYSDBA`: `SELECT group_id, COUNT(*) FROM v$sysmetric GROUP BY group_id;` and the same on `v$con_sysmetric`, inside the PDB (`ALTER SESSION SET CONTAINER = FREEPDB1;`). `ORA-00942` on `gv$con_sysmetric` means the grant above is missing.
- **"allow-listed metrics not found".** Metric names vary by release and by container level. Remove the listed names from `config/metrics.yaml`.
- **`start` says the collector exited right away.** Read the last lines of `collector/run/collector.log`.

## 7. Generating test load

An idle database gives flat metrics, which cannot tell a working pipeline from a stuck one. Run a short CPU load while the collector is running:

```bash
cd deploy
PW="$(grep ORACLE_SOURCE_PWD .env | cut -d= -f2)"
docker compose exec -T oracle-source sqlplus -s fleetdiag/"$PW"@//localhost/FREEPDB1 <<'SQL'
DECLARE
  t0 TIMESTAMP := SYSTIMESTAMP;
  n  NUMBER := 1;
BEGIN
  WHILE SYSTIMESTAMP - t0 < INTERVAL '120' SECOND LOOP
    FOR i IN 1..200000 LOOP
      n := MOD(n * i + SQRT(i), 1000003);
    END LOOP;
  END LOOP;
END;
/
SQL
```

This burns one CPU for about 2 minutes. `Average Active Sessions` and the CPU metrics should rise during that time and fall afterwards.

Swingbench is a good next step for a realistic OLTP load. Limits of the Free container: 2 CPU threads, 2 GB RAM, and a single instance, so global cache contention cannot be tested there. Use DBStress on a RAC for that.

## Next

1. Sink: `sink/sink.py` consumes `ora.metric.raw.v1` and MERGEs into `fd_metric_rollup` (M0 only; Flink takes over the rollup in M1). Written, first run pending.
2. Replay test: reset the sink's consumer group to the earliest offset and check that the row count does not change.
3. Run the load test from section 7 and check that the values show up in `fd_metric_rollup`.
4. M0 exit check: one metric flowing for 24 hours with no gaps and no duplicates, including after restarting each component.
5. Add Flink containers (expect to need `JAVA_TOOL_OPTIONS` there too) for M1.

## Sink (Kafka to Oracle)

The sink reads `ora.metric.raw.v1` and writes to `fd_metric_rollup` with an idempotent MERGE.
Kafka offsets are committed only after the database commit, so a crash or restart replays
messages but never creates duplicates. If the database fails, the sink rewinds to the first
offset of the batch and reconnects.

Design notes:
- Consumer group `fleetdiag-adb-sink`, manual commits, `auto.offset.reset=earliest`.
- Rows are keyed on the exact `end_time` of the collection interval. Oracle intervals are
  slightly longer than 60 s (for example 17:52:31 to 17:53:32), so truncating to the minute
  would create false gaps and collisions.
- Databases are registered in `fd_database` on first sight (platform from env `PLATFORM`,
  default `OTHER`).
- Invalid messages are logged and counted as `bad`; they never block the batch.

### Install and run

~~~bash
cd FleetDiag
source collector/.venv/bin/activate        # same venv as the collector
export ORACLE_PASSWORD="$(grep ORACLE_SOURCE_PWD deploy/.env | cut -d= -f2)"
python sink/sink.py start                  # runs in the background
tail -f sink/run/sink.log
python sink/sink.py status
python sink/sink.py stop
~~~

Runtime files are in `sink/run/` (`sink.pid`, `sink.log`, `sink.err`) and are gitignored.

Example log (first start drains the backlog, then one batch of 24 per interval):

~~~
INFO sink started topic=ora.metric.raw.v1 group=fleetdiag-adb-sink dsn=localhost:1521/FREEPDB1
INFO messages=500 merged=500 bad=0
INFO messages=388 merged=388 bad=0
INFO messages=24 merged=24 bad=0
~~~

### Verify the data

~~~bash
q() { docker exec -i oracle-source sqlplus -s fleetdiag/"$ORACLE_PASSWORD"@localhost:1521/FREEPDB1; }
q <<'SQL'
select count(*) rows_total, count(distinct metric_name) metrics,
       min(bucket_ts) first_ts, max(bucket_ts) last_ts from fd_metric_rollup;
SQL
~~~

Expect 24 metrics and `last_ts` moving forward every minute.

## M0 tests

### Replay test (idempotency)

Stop the collector, let the sink reach lag 0, count rows, rewind the consumer group to the
beginning, restart the sink, and count again. The counts must be identical.

~~~bash
kcg() { docker compose -f deploy/docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 --group fleetdiag-adb-sink "$@"; }

python collector/collector.py stop
kcg --describe                                   # LAG must be 0
q <<'SQL' | tee /tmp/before.txt
select count(*) from fd_metric_rollup;
SQL
python sink/sink.py stop
kcg --topic ora.metric.raw.v1 --reset-offsets --to-earliest --execute
python sink/sink.py start; sleep 30
tail -6 sink/run/sink.log                        # the whole topic is merged again
q <<'SQL' | tee /tmp/after.txt
select count(*) from fd_metric_rollup;
SQL
diff /tmp/before.txt /tmp/after.txt && echo "REPLAY OK"
python collector/collector.py start
~~~

Result on the reference run: 936 messages replayed (500 + 436), table stayed at 912 rows.
The 24-message difference is one interval that the collector published twice around a
restart; the MERGE collapsed it.

Warning for zsh (macOS default shell): do not store the command in a variable such as
`DC="docker compose ..."` and call `$DC`. zsh does not split it into words and the command
fails with "no such file or directory", which makes the test look like it passed without
running. Use a shell function (`kcg` above).

### Component restart tests

Wait about 3 minutes between steps and check that the sink log resumes and the row count
grows by 24 per minute.

~~~bash
python sink/sink.py stop; sleep 90; python sink/sink.py start
python collector/collector.py stop; sleep 90; python collector/collector.py start
docker compose -f deploy/docker-compose.yml restart kafka
docker restart oracle-source        # wait for "healthy" in docker ps, then restart sink and collector
~~~

A 90 s collector stop can leave one missing interval; this is expected.

### 24 h run and exit check

~~~bash
caffeinate -dimsu &                 # keep the Mac awake
~~~

After 24 h:

~~~bash
q <<'SQL'
set linesize 200 pagesize 100
select metric_name, count(*) gaps from (
  select metric_name, bucket_ts - lag(bucket_ts) over (partition by db_id,inst_id,metric_name order by bucket_ts) d
  from fd_metric_rollup) where d > interval '90' second group by metric_name;
select metric_name, count(*) dups from (
  select metric_name, bucket_ts - lag(bucket_ts) over (partition by db_id,inst_id,metric_name order by bucket_ts) d
  from fd_metric_rollup) where d < interval '30' second group by metric_name;
select metric_name, count(*) n from fd_metric_rollup group by metric_name order by n;
SQL
~~~

Pass criteria: no near-duplicates, equal row counts per metric, and gaps only where a
component was deliberately stopped.

## Troubleshooting (sink and collector)

| Symptom | Cause and fix |
|---|---|
| Collector "no rows" in a pluggable DB | `v$sysmetric` is empty in a PDB. Use `gv$con_sysmetric` group 18 (`grant select on sys.gv_$con_sysmetric to fleetdiag`). The collector picks the right view automatically. |
| Sink log shows no new "sink started" | Check `sink/run/sink.err` and that `ORACLE_PASSWORD` is exported in the same shell. |
| Replay test "passes" instantly | Kafka commands failed silently (zsh word splitting, see above). Check that `--describe` printed a table. |
| `ORA-01031` granting SYS views | Connect as SYSDBA inside the PDB (`alter session set container = FREEPDB1`). |

### Restart test results

Each component was stopped and restarted on the reference run, one at a time:

| Component | Effect in the data | Result |
|---|---|---|
| Sink | none; it caught up with `messages=48` (two buffered intervals) | no gap, no duplicate |
| Kafka | none | no gap, no duplicate |
| Collector | gap of about 3 min (18:30:32 to 18:33:32 UTC) | expected |
| Oracle | gap of about 2 min (18:35:32 to 18:37:31 UTC) | expected |

Row count after the tests was 1008, which is 42 intervals x 24 metrics, so no interval was
partial or duplicated.

Known limitation: the collector reads only the current 60 s interval. While it is stopped
(or Oracle is down) those intervals are lost. Backfill from `gv$con_sysmetric_history` is
planned for M1.

### Why a 24 h run

The short tests prove replay and restart behaviour. The 24 h run covers what they cannot:
slow failures (leaks, idle connection timeouts, a stalled consumer), the daily partition
rollover of `fd_metric_rollup` at 00:00 UTC, and one continuous day of history, which the
M1 seasonal baselines need. The expected result is 1440 intervals x 24 metrics = 34,560
rows with no gaps and no near-duplicates.

~~~bash
nohup caffeinate -dimsu >/dev/null 2>&1 &         # keep the Mac awake, plugged in, lid open
date -u +"%Y-%m-%d %H:%M:%S" | tee sink/run/m0_start.txt
~~~

After 24 h:

~~~bash
export ORACLE_PASSWORD="$(grep ORACLE_SOURCE_PWD deploy/.env | cut -d= -f2)"
./scripts/m0_check.sh        # counts only rows after the start time in m0_start.txt
~~~

Pass criteria: no gaps over 90 s, no near-duplicates under 30 s, equal row counts per
metric. Do not restart anything during the window.

## Collector stall and self-recovery

Incident on the reference run: after restarting Oracle, the collector was restarted while the
PDB view `gv$con_sysmetric` was still empty (Oracle needs about a minute to repopulate its
metrics). The collector chooses its query view at startup, fell back to the empty root view
`gv$sysmetric`, and published nothing for about 40 minutes while its status still said
"running". The Kafka end offset stopped moving and no new rows reached Oracle.

Fix in `collector.py`: when a poll returns no rows it re-runs `pick_query` (so it switches to
the right view as soon as data exists) and logs `ERROR STALL: N consecutive empty polls` once
per minute while the stall lasts. Search the log for it:

~~~bash
grep -E "STALL|no rows|query failed|metric source" collector/run/collector.log | tail
~~~

### Oracle restart test

~~~bash
docker restart oracle-source
# the image has no health check; wait for the PDB open message instead
until docker logs --since 3m oracle-source 2>&1 | grep -q "Pluggable database FREEPDB1 opened read write"; do sleep 5; done; echo ready
sleep 150
grep -E "no rows|STALL|query failed|metric source|published=24" collector/run/collector.log | tail -12
tail -3 sink/run/sink.log
tail -3 sink/run/rollup_sink.log
~~~

Result: the collector logged `no rows` for about 45 s, raised one `STALL` error, re-picked
`gv$con_sysmetric group 18` and returned to `published=24` without a manual restart. The sink
resumed with `messages=24`. The rollup sink logged `DPY-4011 ... Broken pipe`, rewound the
batch, reconnected and merged again.

Operational rule: a collector that is running but publishing zero rows is a failure. Alert on
`STALL` in the log, or on a flat Kafka end offset:

~~~bash
docker compose -f deploy/docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:19092 --topic ora.metric.raw.v1
~~~

## Flink (M1): 5-minute rollup

Flink runs from a second compose file that adds the Apple Silicon JVM fix
(`JAVA_TOOL_OPTIONS=-XX:UseSVE=0`) and mounts `flink/extra` (connector jars) and `flink/sql`.

~~~bash
./scripts/get-flink-jars.sh                      # Kafka SQL connector, not stored in git
dcf() { docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.flink.yml "$@"; }
dcf up -d flink-jobmanager flink-taskmanager
sleep 15
dcf ps
curl -s localhost:8081/overview; echo            # expect 1 taskmanager, 4 slots
~~~

Use a shell function for `dcf`. A variable holding the command does not work in zsh.

Submit the job (`flink/sql/rollup_5m.sql`):

~~~bash
dcf exec -T flink-jobmanager ./bin/sql-client.sh -l /opt/flink/extra -f /opt/flink/sql/rollup_5m.sql
curl -s localhost:8081/jobs/overview | python3 -m json.tool | grep -E '"name"|"state"'
~~~

Submitting twice creates a second identical job and every bucket is published twice. List the
jobs first and cancel the newer one if needed:

~~~bash
curl -s -X PATCH "localhost:8081/jobs/<JOB_ID>?mode=cancel"
~~~

The job reads `ora.metric.raw.v1` from the earliest offset, uses `end_time` as event time with
a 2 minute watermark, and writes tumbling 5 minute windows to `ora.metric.rollup.v1`.
A window is emitted only after the watermark passes its end, so the newest bucket appears
about 7 minutes after it starts. Example output message:

~~~
{"fleet":"local","db_unique_name":"FREE","inst_id":1,"metric_name":"Session Count","bucket_sec":300,"bucket_ts":"2026-10-11T17:50:00Z","val_avg":1.0,"val_min":1.0,"val_max":1.0,"sample_cnt":3}
~~~

`sample_cnt` is 5 for a full window. The first window after a start is partial.

Limitation: the job has no checkpointing or HA. If the Flink containers restart, the job is
lost and must be resubmitted.

## Rollup sink

`sink/rollup_sink.py` writes `ora.metric.rollup.v1` into `fd_metric_rollup` with the bucket size
from the message (300). Same MERGE pattern and offset handling as `sink.py`, consumer group
`fleetdiag-adb-rollup-sink`.

~~~bash
source collector/.venv/bin/activate
export ORACLE_PASSWORD="$(grep ORACLE_SOURCE_PWD deploy/.env | cut -d= -f2)"
python sink/rollup_sink.py start      # also: stop | status
tail -f sink/run/rollup_sink.log
~~~

Verify that Flink's averages match plain SQL over the 60 s rows of the same window:

~~~bash
q <<'SQL'
select r.bucket_ts, r.sample_cnt, r.val_avg flink_avg,
       (select avg(m.val_avg) from fd_metric_rollup m
         where m.bucket_sec = 60 and m.db_id = r.db_id and m.inst_id = r.inst_id
           and m.metric_name = r.metric_name
           and m.bucket_ts >= r.bucket_ts and m.bucket_ts < r.bucket_ts + interval '5' minute) sql_avg
from fd_metric_rollup r
where r.bucket_sec = 300 and r.metric_name = 'Redo Generated Per Sec'
order by r.bucket_ts desc fetch first 6 rows only;
SQL
~~~

Result on the reference run: values equal in every full window.

Note: any query over `fd_metric_rollup` that checks gaps or duplicates must filter on
`bucket_sec`, otherwise 60 s and 300 s rows mix. `scripts/m0_check.sh` already does.

## Spark (M1): seasonal baselines

Spark runs locally from its own venv (`spark/.venv`, PySpark 4.2) so it does not touch the
collector venv. The Oracle JDBC driver is downloaded, not stored in git. Java 23 works with
PySpark 4.2 (warning `Using incubator modules` is harmless).

~~~bash
python3 -m venv spark/.venv
source spark/.venv/bin/activate
pip install -r spark/requirements.txt
mkdir -p spark/jars
curl -fL -o spark/jars/ojdbc11.jar \
  https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc11/23.5.0.24.07/ojdbc11-23.5.0.24.07.jar
export ORACLE_PASSWORD="$(grep ORACLE_SOURCE_PWD deploy/.env | cut -d= -f2)"
python spark/read_check.py            # JDBC read of the 60 s rows, counts per metric
python spark/baseline.py --dry-run    # compute only
python spark/baseline.py              # MERGE into fd_baseline
~~~

`baseline.py` reads the 60 s rows of the last 56 days (`--lookback-days`), groups them by series
and hour of week in UTC (slot 0 = Monday 00:00, 167 = Sunday 23:00) and stores median, MAD and
sample count per slot as a 168-entry JSON array in `fd_baseline.slots`. A slot with fewer than
`--min-samples` (30) values is `null`; scoring falls back to EWMA there. `weeks_used` is the
largest number of weekly observations behind any filled slot. Re-running is idempotent (MERGE
on the primary key).

Check:

~~~sql
select jt.slot - 1 slot, jt.med, jt.mad, jt.n
from fd_baseline b, json_table(b.slots, '$[*]' columns (slot for ordinality,
     med number path '$.med', mad number path '$.mad', n number path '$.n')) jt
where b.metric_name = 'Redo Generated Per Sec' and jt.med is not null order by 1;
~~~
