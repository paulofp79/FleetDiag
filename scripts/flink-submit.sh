#!/usr/bin/env bash
# Submit a FleetDiag Flink job only if it is not already running, resuming from its latest
# retained checkpoint when there is one.
#
#   scripts/flink-submit.sh [rollup|scoring] [--fresh]     (default job: rollup)
#
#   rollup   flink/sql/rollup_5m.sql (SQL client), checkpoints in flink/checkpoints/<job id>/
#   scoring  flink-jobs jar, io.fleetdiag.flink.ScoringJob, checkpoints in
#            flink/checkpoints/scoring/<job id>/  (build first: cd flink-jobs && mvn -q package)
#
# --fresh ignores checkpoints. For rollup that replays ora.metric.raw.v1 from the beginning and
# republishes every window (the rollup sink MERGEs them: correct, slower). For scoring it starts
# from the committed group offsets, or from now on the very first start.
# Resuming keeps open windows / per-series state and Kafka offsets.
set -euo pipefail
cd "$(dirname "$0")/.."

JOB="rollup"
FRESH=""
for a in "$@"; do
  case "$a" in
    rollup|scoring) JOB="$a" ;;
    --fresh) FRESH=1 ;;
    *) echo "usage: $0 [rollup|scoring] [--fresh]" >&2; exit 2 ;;
  esac
done

UI="http://localhost:8081"
dcf() { docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.flink.yml "$@"; }

case "$JOB" in
  rollup)  JOB_NAME="fleetdiag-rollup-5m"; CKPT_GLOB="flink/checkpoints/*/chk-*/" ;;
  scoring) JOB_NAME="fleetdiag-scoring";   CKPT_GLOB="flink/checkpoints/scoring/*/chk-*/"
           JAR="flink-jobs/target/fleetdiag-flink-jobs.jar"
           [ -f "$JAR" ] || { echo "missing $JAR (cd flink-jobs && mvn -q package)" >&2; exit 1; } ;;
esac

if ! curl -sf "$UI/overview" >/dev/null; then
  echo "Flink is not reachable at $UI (dcf up -d flink-jobmanager flink-taskmanager)" >&2
  exit 1
fi

active=$(curl -sf "$UI/jobs/overview" | python3 -c '
import json, sys
name = sys.argv[1]
live = {"INITIALIZING", "CREATED", "RUNNING", "RESTARTING", "RECONCILING"}
for j in json.load(sys.stdin)["jobs"]:
    if j["name"] == name and j["state"] in live:
        print(j["jid"], j["state"])
' "$JOB_NAME")
if [ -n "$active" ]; then
  echo "already running, not submitting: $active"
  exit 0
fi

restore=""
if [ -z "$FRESH" ]; then
  # newest completed checkpoint (has _metadata) across all previous job ids of this job
  latest=$(ls -td $CKPT_GLOB 2>/dev/null | while read -r d; do
             [ -f "$d/_metadata" ] && { echo "${d%/}"; break; }; done || true)
  if [ -n "$latest" ]; then
    restore="/opt/flink/checkpoints/${latest#flink/checkpoints/}"
  fi
fi
if [ -n "$restore" ]; then echo "resuming from $restore"; else echo "fresh start"; fi

case "$JOB" in
  rollup)
    {
      if [ -n "$restore" ]; then
        echo "SET 'execution.savepoint.path' = '$restore';"
      fi
      cat flink/sql/rollup_5m.sql
    } > flink/sql/_submit.sql
    dcf exec -T flink-jobmanager ./bin/sql-client.sh -l /opt/flink/extra -f /opt/flink/sql/_submit.sql
    ;;
  scoring)
    dcf cp "$JAR" flink-jobmanager:/tmp/fleetdiag-flink-jobs.jar
    args=(./bin/flink run -d)
    if [ -n "$restore" ]; then args+=(-s "$restore"); fi
    args+=(-c io.fleetdiag.flink.ScoringJob /tmp/fleetdiag-flink-jobs.jar)
    dcf exec -T flink-jobmanager "${args[@]}"
    ;;
esac

sleep 3
curl -sf "$UI/jobs/overview" | python3 -c '
import json, sys
for j in json.load(sys.stdin)["jobs"]:
    print(j["jid"], j["state"], j["name"])'
