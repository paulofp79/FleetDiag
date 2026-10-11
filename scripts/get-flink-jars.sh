#!/usr/bin/env bash
set -e
cd "$(dirname "$0")/.."
mkdir -p flink/extra
curl -fL -o flink/extra/flink-sql-connector-kafka-3.3.0-1.20.jar \
  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar
ls -l flink/extra
