# Installation (local development stack)

Written on 2026-10-11 on macOS with Apple Silicon (arm64) and Docker Desktop. Commands run from the repo root unless noted.

| Step | Status |
|---|---|
| Git repo and GitHub remote | done |
| Kafka container | verified: broker starts, port 9092 reachable from the host |
| Kafka topics and smoke test | commands below, not yet confirmed |
| Oracle Free container | verified: starts, version banner returned |
| `gv$sysmetric` check | pending: confirm `group_id` for 60 s metrics |
| Python environment | commands below, not yet confirmed |

## Prerequisites

- Docker Desktop (or OrbStack/Colima), running
- Python 3.11 or newer
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

Seen so far: `Oracle AI Database 26ai Free Release 23.26.3.0.0`. The collector assumes 60-second metrics are `group_id = 2`. That is still to be confirmed from the second query.

### Oracle troubleshooting

- **`cannot attach stdin to a TTY-enabled container because stdin is not a terminal`.** Use `docker compose exec -T` when piping a heredoc.
- **ORA errors right after start.** The database is still opening. Wait for `DATABASE IS READY TO USE!` in the logs.
- **Start over.** `docker compose down -v` removes the containers and the `oracle-data` volume.

## 4. Python environment (collector)

```bash
cd "$HOME/Documents/ClaudeCode/FleetDiag"
python3 -m venv collector/.venv
source collector/.venv/bin/activate
pip install --upgrade pip
pip install oracledb confluent-kafka pyyaml jsonschema
python -c "import oracledb, confluent_kafka; print('oracledb', oracledb.__version__, '| confluent-kafka', confluent_kafka.version()[0])"
```

## Next

1. Grant the `fleetdiag` user what the schema needs and load `sql/001_schema.sql` and `sql/002_views.sql` into the local database.
2. Write the collector, then the sink.
