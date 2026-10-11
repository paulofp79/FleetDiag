#!/usr/bin/env python3
"""FleetDiag collector (M0): GV$SYSMETRIC 60 s metrics -> Kafka topic ora.metric.raw.v1."""
import json
import logging
import os
import signal
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import oracledb
import yaml
from confluent_kafka import Producer
from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parent
SCHEMA = json.loads((ROOT.parent / "schemas" / "metric_sample.v1.json").read_text())
SQL = (ROOT / "queries" / "sysmetric_60s.sql").read_text().strip().rstrip(";")
ALLOW = set(yaml.safe_load((ROOT / "config" / "metrics.yaml").read_text())["metrics"])

DSN = os.environ.get("ORACLE_DSN", "localhost:1521/FREEPDB1")
USER = os.environ.get("ORACLE_USER", "fleetdiag")
PASSWORD = os.environ["ORACLE_PASSWORD"]
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "ora.metric.raw.v1")
FLEET = os.environ.get("FLEET", "local")
DB_NAME = os.environ.get("DB_UNIQUE_NAME", "FREE")
POLL_SEC = int(os.environ.get("POLL_SEC", "15"))
COLLECTOR_ID = os.environ.get("COLLECTOR_ID", "collector-1")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("collector")
stop = threading.Event()


def iso(dt):
    return dt.replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def connect():
    while not stop.is_set():
        try:
            conn = oracledb.connect(user=USER, password=PASSWORD, dsn=DSN)
            log.info("connected to %s as %s", DSN, USER)
            return conn
        except oracledb.Error as exc:
            log.error("connect failed: %s (retry in 10 s)", exc)
            stop.wait(10)
    return None


def utc_offset(cur):
    # V$SYSMETRIC begin/end times are DATEs in the database server's local time.
    cur.execute("SELECT SYSDATE, SYS_EXTRACT_UTC(SYSTIMESTAMP) FROM dual")
    local, utc = cur.fetchone()
    return timedelta(minutes=round((local - utc).total_seconds() / 60))


def on_delivery(err, msg):
    if err is not None:
        log.error("delivery failed: %s", err)


def main():
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    validator = Draft202012Validator(SCHEMA)
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "client.id": COLLECTOR_ID,
        "enable.idempotence": True,
        "acks": "all",
    })
    conn = connect()
    seen = {}  # (inst_id, metric_name) -> end_time already published
    first_cycle = True

    while not stop.is_set() and conn is not None:
        started = time.monotonic()
        try:
            with conn.cursor() as cur:
                offset = utc_offset(cur)
                cur.execute(SQL)
                rows = cur.fetchall()
        except oracledb.Error as exc:
            log.error("query failed: %s; reconnecting", exc)
            try:
                conn.close()
            except oracledb.Error:
                pass
            conn = connect()
            continue

        if not rows:
            log.warning("query returned no rows; check group_id in collector/queries/sysmetric_60s.sql")
        if first_cycle and rows:
            names = {r[1] for r in rows}
            missing = sorted(ALLOW - names)
            if missing:
                log.warning("allow-listed metrics not found in this database: %s", missing)
            first_cycle = False

        sent = 0
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        for inst_id, name, unit, value, begin, end in rows:
            if name not in ALLOW or value is None:
                continue
            if seen.get((inst_id, name)) == end:
                continue
            event = {
                "schema": "metric_sample.v1",
                "fleet": FLEET,
                "db_unique_name": DB_NAME,
                "inst_id": int(inst_id),
                "metric_name": name,
                "metric_unit": unit or "",
                "value": float(value),
                "begin_time": iso(begin - offset),
                "end_time": iso(end - offset),
                "collected_at": now,
                "collector_id": COLLECTOR_ID,
            }
            errors = list(validator.iter_errors(event))
            if errors:
                log.error("invalid event for %s: %s", name, errors[0].message)
                continue
            key = f"{FLEET}/{DB_NAME}/{inst_id}"
            producer.produce(TOPIC, key=key, value=json.dumps(event), on_delivery=on_delivery)
            seen[(inst_id, name)] = end
            sent += 1

        producer.poll(0)
        producer.flush(10)
        log.info("rows=%d published=%d", len(rows), sent)
        stop.wait(max(0.0, POLL_SEC - (time.monotonic() - started)))

    producer.flush(10)
    log.info("stopped")


if __name__ == "__main__":
    main()
