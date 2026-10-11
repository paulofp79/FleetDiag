#!/usr/bin/env bash
# Creates the FleetDiag topics on the local compose Kafka. Run from the deploy/ folder.
set -euo pipefail
BOOTSTRAP="${BOOTSTRAP:-kafka:19092}"
KT="docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server $BOOTSTRAP"

for t in ora.metric.raw.v1 ora.ash.raw.v1 ora.metric.rollup.v1 ora.wait.rollup.v1 ora.anomaly.v1 ora.incident.v1; do
  $KT --create --if-not-exists --topic "$t" --partitions 6 --replication-factor 1
done

# Baselines are the latest value per series, so the topic is compacted.
$KT --create --if-not-exists --topic ora.baseline.v1 --partitions 6 --replication-factor 1 --config cleanup.policy=compact
