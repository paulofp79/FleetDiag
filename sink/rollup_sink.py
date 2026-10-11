#!/usr/bin/env python3
"""Kafka ora.metric.rollup.v1 -> fd_metric_rollup (any bucket_sec), idempotent MERGE.
Reuses connect/get_db_id/parse_ts from sink.py. Offsets committed after DB commit."""
import json, logging, os, signal, subprocess, sys, time
from pathlib import Path
import oracledb
from confluent_kafka import Consumer, TopicPartition
from sink import connect, get_db_id, parse_ts

ROOT = Path(__file__).resolve().parent
RUN = ROOT / "run"; RUN.mkdir(exist_ok=True)
PID, LOG = RUN / "rollup_sink.pid", RUN / "rollup_sink.log"
BOOT = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("ROLLUP_TOPIC", "ora.metric.rollup.v1")
GROUP = os.getenv("ROLLUP_GROUP", "fleetdiag-adb-rollup-sink")
BATCH_MAX, BATCH_WAIT = 500, 2.0
REQUIRED = ("fleet", "db_unique_name", "inst_id", "metric_name", "bucket_sec", "bucket_ts",
            "val_avg", "val_min", "val_max", "sample_cnt")

MERGE = """
MERGE INTO fd_metric_rollup t
USING (SELECT :db_id db_id, :inst_id inst_id, :metric_name metric_name,
              :bucket_sec bucket_sec, :bucket_ts bucket_ts FROM dual) s
ON (t.db_id=s.db_id AND t.inst_id=s.inst_id AND t.metric_name=s.metric_name
    AND t.bucket_sec=s.bucket_sec AND t.bucket_ts=s.bucket_ts)
WHEN MATCHED THEN UPDATE SET t.val_avg=:val_avg, t.val_min=:val_min, t.val_max=:val_max, t.sample_cnt=:cnt
WHEN NOT MATCHED THEN INSERT (db_id,inst_id,metric_name,bucket_sec,bucket_ts,val_avg,val_min,val_max,sample_cnt)
VALUES (s.db_id,s.inst_id,s.metric_name,s.bucket_sec,s.bucket_ts,:val_avg,:val_min,:val_max,:cnt)
"""

def run():
    logging.basicConfig(filename=str(LOG), level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("rollup_sink")
    stop = {"v": False}
    for s in (signal.SIGINT, signal.SIGTERM):
        signal.signal(s, lambda *_: stop.__setitem__("v", True))
    consumer = Consumer({"bootstrap.servers": BOOT, "group.id": GROUP,
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    consumer.subscribe([TOPIC])
    conn, cache = None, {}
    log.info("rollup sink started topic=%s group=%s", TOPIC, GROUP)
    while not stop["v"]:
        msgs = consumer.consume(BATCH_MAX, BATCH_WAIT)
        if not msgs: continue
        first = {}
        for m in msgs:
            if not m.error(): first.setdefault(m.partition(), m.offset())
        try:
            if conn is None: conn = connect()
            rows, bad = [], 0
            for m in msgs:
                if m.error(): continue
                try:
                    e = json.loads(m.value())
                    missing = [k for k in REQUIRED if e.get(k) is None]
                    if missing: raise ValueError("missing " + ",".join(missing))
                    rows.append({"db_id": get_db_id(conn, cache, e["fleet"], e["db_unique_name"]),
                                 "inst_id": e["inst_id"], "metric_name": e["metric_name"],
                                 "bucket_sec": e["bucket_sec"], "bucket_ts": parse_ts(e["bucket_ts"]),
                                 "val_avg": float(e["val_avg"]), "val_min": float(e["val_min"]),
                                 "val_max": float(e["val_max"]), "cnt": int(e["sample_cnt"])})
                except Exception as ex:
                    bad += 1; log.warning("bad message offset=%s: %s", m.offset(), str(ex)[:120])
            if rows: conn.cursor().executemany(MERGE, rows)
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
    log.info("rollup sink stopped")

def pid_alive():
    try:
        p = int(PID.read_text()); os.kill(p, 0); return p
    except Exception:
        return None

def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"
    if cmd in ("run", "start") and "ORACLE_PASSWORD" not in os.environ:
        sys.exit("ORACLE_PASSWORD not set")
    if cmd == "run":
        run()
    elif cmd == "start":
        if pid_alive(): sys.exit(f"already running (pid {pid_alive()})")
        p = subprocess.Popen([sys.executable, __file__, "run"], start_new_session=True,
                             stdout=subprocess.DEVNULL, stderr=open(RUN / "rollup_sink.err", "a"))
        PID.write_text(str(p.pid)); print(f"started pid {p.pid}\ntail -f {LOG}")
    elif cmd == "stop":
        p = pid_alive()
        if not p: print("not running"); return
        os.kill(p, signal.SIGTERM); PID.unlink(missing_ok=True); print("stopped")
    elif cmd == "status":
        p = pid_alive(); print(f"running (pid {p})" if p else "not running")
    else:
        sys.exit("usage: rollup_sink.py start|stop|status|run")

if __name__ == "__main__":
    main()
