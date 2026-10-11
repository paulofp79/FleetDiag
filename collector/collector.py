#!/usr/bin/env python3
"""FleetDiag collector (M0): GV$SYSMETRIC 60 s metrics -> Kafka topic ora.metric.raw.v1."""
import json
import logging
import os
import signal
import subprocess
import sys
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
SQL_PDB = (ROOT / "queries" / "sysmetric_pdb_60s.sql").read_text().strip().rstrip(";")
ALLOW = set(yaml.safe_load((ROOT / "config" / "metrics.yaml").read_text())["metrics"])

DSN = os.environ.get("ORACLE_DSN", "localhost:1521/FREEPDB1")
USER = os.environ.get("ORACLE_USER", "fleetdiag")
PASSWORD = os.environ.get("ORACLE_PASSWORD", "")
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


def pick_query(conn):
    """Use gv$sysmetric (group 2) when it has data, else the PDB view gv$con_sysmetric (group 18)."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM gv$sysmetric WHERE group_id = 2")
        if cur.fetchone()[0] > 0:
            return SQL, "gv$sysmetric group 2"
        cur.execute("SELECT COUNT(*) FROM gv$con_sysmetric WHERE group_id = 18")
        if cur.fetchone()[0] > 0:
            return SQL_PDB, "gv$con_sysmetric group 18 (pluggable database)"
    return SQL, "none found"


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
    sql, source = pick_query(conn) if conn is not None else (SQL, "none")
    log.info("metric source: %s", source)
    seen = {}  # (inst_id, metric_name) -> end_time already published
    first_cycle = True

    while not stop.is_set() and conn is not None:
        started = time.monotonic()
        try:
            with conn.cursor() as cur:
                offset = utc_offset(cur)
                cur.execute(sql)
                rows = cur.fetchall()
        except oracledb.Error as exc:
            log.error("query failed: %s; reconnecting", exc)
            try:
                conn.close()
            except oracledb.Error:
                pass
            conn = connect()
            if conn is not None:
                sql, source = pick_query(conn)
                log.info("metric source: %s", source)
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


RUN_DIR = ROOT / "run"
PID_FILE = RUN_DIR / "collector.pid"
LOG_FILE = RUN_DIR / "collector.log"


def read_pid():
    """Return the pid of the running background collector, or None."""
    try:
        pid = int(PID_FILE.read_text().strip())
    except (FileNotFoundError, ValueError):
        return None
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return None
    except PermissionError:
        return pid
    return pid


def cmd_start():
    if not PASSWORD:
        sys.exit("ORACLE_PASSWORD is not set in this shell (see the export line in the install notes).")
    pid = read_pid()
    if pid:
        sys.exit(f"collector is already running (pid {pid})")
    RUN_DIR.mkdir(exist_ok=True)
    with open(LOG_FILE, "ab") as log_out:
        proc = subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), "run"],
            stdout=log_out,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
    PID_FILE.write_text(str(proc.pid))
    time.sleep(2)
    if proc.poll() is not None:
        PID_FILE.unlink(missing_ok=True)
        sys.exit(f"collector exited right away; check {LOG_FILE}")
    print(f"collector started (pid {proc.pid})")
    print(f"follow the log:  tail -f {LOG_FILE}")
    print("stop it:         python collector/collector.py stop")


def cmd_stop():
    pid = read_pid()
    if not pid:
        PID_FILE.unlink(missing_ok=True)
        print("collector is not running")
        return
    os.kill(pid, signal.SIGTERM)
    for _ in range(30):
        time.sleep(0.5)
        if read_pid() is None:
            break
    else:
        print("collector did not exit within 15 s, sending SIGKILL")
        os.kill(pid, signal.SIGKILL)
    PID_FILE.unlink(missing_ok=True)
    print("collector stopped")


def cmd_status():
    pid = read_pid()
    if pid:
        print(f"collector is running (pid {pid})")
        print(f"log: {LOG_FILE}")
    else:
        print("collector is not running")


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else ""
    if command == "start":
        cmd_start()
    elif command == "stop":
        cmd_stop()
    elif command == "status":
        cmd_status()
    elif command == "run":
        if not PASSWORD:
            sys.exit("ORACLE_PASSWORD is not set in this shell")
        main()
    else:
        print("usage: python collector/collector.py {start|stop|status|run}")
        sys.exit(2)
