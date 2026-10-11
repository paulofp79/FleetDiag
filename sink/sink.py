#!/usr/bin/env python3
"""FleetDiag sink: Kafka ora.metric.raw.v1 -> fd_metric_rollup (idempotent MERGE).
Offsets are committed only after the DB commit (at-least-once + MERGE = no duplicates)."""
import json, logging, os, signal, subprocess, sys, time
from datetime import datetime, timezone
from pathlib import Path

import oracledb
from confluent_kafka import Consumer, TopicPartition
from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parent
RUN = ROOT / "run"; RUN.mkdir(exist_ok=True)
PID, LOG = RUN / "sink.pid", RUN / "sink.log"
SCHEMA = json.loads((ROOT.parent / "schemas" / "metric_sample.v1.json").read_text())

BOOT = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "ora.metric.raw.v1")
GROUP = os.getenv("SINK_GROUP", "fleetdiag-adb-sink")
DSN = os.getenv("ORACLE_DSN", "localhost:1521/FREEPDB1")
USER = os.getenv("ORACLE_USER", "fleetdiag")
PLATFORM = os.getenv("PLATFORM", "OTHER")
BATCH_MAX, BATCH_WAIT = 500, 2.0
BUCKET_SEC = 60

MERGE = """
MERGE INTO fd_metric_rollup t
USING (SELECT :db_id db_id, :inst_id inst_id, :metric_name metric_name,
              :bucket_sec bucket_sec, :bucket_ts bucket_ts, :val val FROM dual) s
ON (t.db_id=s.db_id AND t.inst_id=s.inst_id AND t.metric_name=s.metric_name
    AND t.bucket_sec=s.bucket_sec AND t.bucket_ts=s.bucket_ts)
WHEN MATCHED THEN UPDATE SET t.val_avg=s.val, t.val_min=s.val, t.val_max=s.val, t.sample_cnt=1
WHEN NOT MATCHED THEN INSERT (db_id,inst_id,metric_name,bucket_sec,bucket_ts,val_avg,val_min,val_max,sample_cnt)
VALUES (s.db_id,s.inst_id,s.metric_name,s.bucket_sec,s.bucket_ts,s.val,s.val,s.val,1)
"""

def parse_ts(s):
    d = datetime.fromisoformat(s.replace("Z", "+00:00"))
    return d.astimezone(timezone.utc).replace(tzinfo=None, microsecond=0)

def connect():
    return oracledb.connect(user=USER, password=os.environ["ORACLE_PASSWORD"], dsn=DSN)

def get_db_id(conn, cache, fleet, name):
    k = (fleet, name)
    if k in cache: return cache[k]
    cur = conn.cursor()
    cur.execute("SELECT db_id FROM fd_database WHERE fleet=:1 AND db_unique_name=:2", [fleet, name])
    r = cur.fetchone()
    if not r:
        cur.execute("INSERT INTO fd_database (fleet, db_unique_name, platform) VALUES (:1,:2,:3)",
                    [fleet, name, PLATFORM])
        conn.commit()
        cur.execute("SELECT db_id FROM fd_database WHERE fleet=:1 AND db_unique_name=:2", [fleet, name])
        r = cur.fetchone()
    cache[k] = r[0]
    return r[0]

def run():
    logging.basicConfig(filename=str(LOG), level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("sink")
    stop = {"v": False}
    for s in (signal.SIGINT, signal.SIGTERM):
        signal.signal(s, lambda *_: stop.__setitem__("v", True))
    val = Draft202012Validator(SCHEMA)
    consumer = Consumer({"bootstrap.servers": BOOT, "group.id": GROUP,
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    consumer.subscribe([TOPIC])
    conn, cache = None, {}
    log.info("sink started topic=%s group=%s dsn=%s", TOPIC, GROUP, DSN)
    while not stop["v"]:
        msgs = consumer.consume(BATCH_MAX, BATCH_WAIT)
        if not msgs: continue
        first = {}
        for m in msgs:
            if m.error(): continue
            first.setdefault(m.partition(), m.offset())
        try:
            if conn is None: conn = connect()
            rows, bad = [], 0
            for m in msgs:
                if m.error(): continue
                try:
                    e = json.loads(m.value())
                    val.validate(e)
                    rows.append({"db_id": get_db_id(conn, cache, e["fleet"], e["db_unique_name"]),
                                 "inst_id": e["inst_id"], "metric_name": e["metric_name"],
                                 "bucket_sec": BUCKET_SEC, "bucket_ts": parse_ts(e["end_time"]),
                                 "val": float(e["value"])})
                except Exception as ex:
                    bad += 1; log.warning("bad message offset=%s: %s", m.offset(), str(ex)[:120])
            if rows:
                conn.cursor().executemany(MERGE, rows)
            conn.commit()
            consumer.commit(asynchronous=False)
            log.info("messages=%d merged=%d bad=%d", len(msgs), len(rows), bad)
        except oracledb.Error as ex:
            log.error("db error, rewinding: %s", ex)
            for p, o in first.items():
                consumer.seek(TopicPartition(TOPIC, p, o))
            cache.clear()
            try:
                if conn: conn.close()
            except Exception: pass
            conn = None
            time.sleep(5)
    consumer.close()
    if conn: conn.close()
    log.info("sink stopped")

def pid_alive():
    try:
        p = int(PID.read_text()); os.kill(p, 0); return p
    except Exception:
        return None

def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    if cmd == "run":
        if "ORACLE_PASSWORD" not in os.environ: sys.exit("ORACLE_PASSWORD not set")
        run()
    elif cmd == "start":
        if "ORACLE_PASSWORD" not in os.environ: sys.exit("ORACLE_PASSWORD not set")
        if pid_alive(): sys.exit(f"already running (pid {pid_alive()})")
        p = subprocess.Popen([sys.executable, __file__, "run"], start_new_session=True,
                             stdout=subprocess.DEVNULL, stderr=open(RUN / "sink.err", "a"))
        PID.write_text(str(p.pid))
        print(f"started pid {p.pid}\ntail -f {LOG}")
    elif cmd == "stop":
        p = pid_alive()
        if not p: print("not running"); return
        os.kill(p, signal.SIGTERM); PID.unlink(missing_ok=True); print("stopped")
    elif cmd == "status":
        p = pid_alive(); print(f"running (pid {p})" if p else "not running")
    else:
        sys.exit("usage: sink.py start|stop|status|run")

if __name__ == "__main__":
    main()
