#!/usr/bin/env bash
# Submit the 5 minute rollup job only if it is not already running, resuming from the latest
# retained checkpoint when there is one.
#
#   scripts/flink-submit.sh            # resume from latest checkpoint, else fresh start
#   scripts/flink-submit.sh --fresh    # ignore checkpoints (full replay from earliest offset)
#
# A fresh start replays ora.metric.raw.v1 from the beginning and republishes every window; the
# rollup sink MERGEs them, so that is correct but slower. Resuming keeps the open windows and the
# Kafka offsets, so no partial window is published after a Flink restart.
set -euo pipefail
cd "$(dirname "$0")/.."

JOB_NAME="fleetdiag-rollup-5m"
SQL="flink/sql/rollup_5m.sql"
UI="http://localhost:8081"
dcf() { docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.flink.yml "$@"; }

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
if [ "${1:-}" != "--fresh" ]; then
  # newest completed checkpoint (has _metadata) across all previous job ids
  latest=$(ls -td flink/checkpoints/*/chk-*/ 2>/dev/null | while read -r d; do
             [ -f "$d/_metadata" ] && { echo "${d%/}"; break; }; done || true)
  if [ -n "$latest" ]; then
    restore="/opt/flink/checkpoints/${latest#flink/checkpoints/}"
  fi
fi

{
  if [ -n "$restore" ]; then
    echo "SET 'execution.savepoint.path' = '$restore';"
  fi
  cat "$SQL"
} > flink/sql/_submit.sql

if [ -n "$restore" ]; then echo "resuming from $restore"; else echo "fresh start (earliest offset)"; fi
dcf exec -T flink-jobmanager ./bin/sql-client.sh -l /opt/flink/extra -f /opt/flink/sql/_submit.sql
sleep 3
curl -sf "$UI/jobs/overview" | python3 -c '
import json, sys
for j in json.load(sys.stdin)["jobs"]:
    print(j["jid"], j["state"], j["name"])'
