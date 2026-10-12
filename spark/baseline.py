#!/usr/bin/env python3
"""Seasonal baselines: median and MAD per series and hour of week, upserted into fd_baseline.

Series = (db_id, inst_id, metric_name). Slot = hour of week in UTC, 0 = Monday 00:00,
167 = Sunday 23:00. Input is the 60 s rows of fd_metric_rollup over the lookback window.

fd_baseline.slots is a JSON array of 168 entries, each {"med": x, "mad": y, "n": k} or null
when the slot has fewer than --min-samples values. Scoring uses its EWMA fallback for null
slots and applies the MAD floor itself, so raw MAD is stored here.

Usage (spark venv active, ORACLE_PASSWORD exported):
    python spark/baseline.py [--lookback-days 56] [--min-samples 30] [--dry-run]
"""
import argparse
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession, Window, functions as F

ROOT = Path(__file__).resolve().parent
JAR = ROOT / "jars" / "ojdbc11.jar"
DSN = os.getenv("ORACLE_DSN", "localhost:1521/FREEPDB1")
USER = os.getenv("ORACLE_USER", "fleetdiag")
SLOTS = 168
KEYS = ["db_id", "inst_id", "metric_name"]

MERGE_SQL = """
merge into fd_baseline t
using (select :db_id db_id, :inst_id inst_id, :metric_name metric_name from dual) s
on (t.db_id = s.db_id and t.inst_id = s.inst_id and t.metric_name = s.metric_name)
when matched then update set
  t.weeks_used = :weeks_used, t.slots = :slots, t.computed_at = sys_extract_utc(systimestamp)
when not matched then insert (db_id, inst_id, metric_name, weeks_used, computed_at, slots)
  values (:db_id, :inst_id, :metric_name, :weeks_used, sys_extract_utc(systimestamp), :slots)
"""


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--lookback-days", type=int, default=56)
    p.add_argument("--min-samples", type=int, default=30)
    p.add_argument("--dry-run", action="store_true", help="compute and print, do not write")
    return p.parse_args()


def spark_session():
    # All data timestamps are UTC: keep the JVM and the session on UTC.
    return (
        SparkSession.builder.appName("fd-baseline")
        .master("local[*]")
        .config("spark.jars", str(JAR))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def read_rollups(spark, lookback_days):
    query = (
        "select db_id, inst_id, metric_name, bucket_ts, val_avg from fd_metric_rollup "
        "where bucket_sec = 60 and val_avg is not null "
        f"and bucket_ts >= sys_extract_utc(systimestamp) - numtodsinterval({int(lookback_days)}, 'DAY')"
    )
    df = (
        spark.read.format("jdbc")
        .option("url", f"jdbc:oracle:thin:@//{DSN}")
        .option("user", USER)
        .option("password", os.environ["ORACLE_PASSWORD"])
        .option("driver", "oracle.jdbc.OracleDriver")
        .option("query", query)
        .option("fetchsize", "10000")
        .load()
    )
    # Oracle NUMBER comes back as decimal; lower-case names to match KEYS.
    return df.select(
        F.col("DB_ID").cast("long").alias("db_id"),
        F.col("INST_ID").cast("int").alias("inst_id"),
        F.col("METRIC_NAME").alias("metric_name"),
        F.col("BUCKET_TS").alias("bucket_ts"),
        F.col("VAL_AVG").alias("val"),
    )


def compute(df, min_samples):
    # weekday(): 0 = Monday ... 6 = Sunday, evaluated in the UTC session time zone.
    df = df.withColumn("slot", F.expr("weekday(bucket_ts) * 24 + hour(bucket_ts)"))

    slot_keys = KEYS + ["slot"]
    w = Window.partitionBy(*slot_keys)
    stats = (
        df.withColumn("med", F.median("val").over(w))
          .groupBy(*slot_keys)
          .agg(F.first("med").alias("med"),
               F.median(F.abs(F.col("val") - F.col("med"))).alias("mad"),
               F.count("*").alias("n"))
    )

    weeks = df.groupBy(*KEYS).agg(
        F.countDistinct(F.date_trunc("week", "bucket_ts")).alias("weeks_used"))

    entry = F.when(
        F.col("n") >= min_samples,
        F.struct(F.round("med", 6).alias("med"), F.round("mad", 6).alias("mad"), "n"),
    )
    filled = (
        stats.groupBy(*KEYS)
             .agg(F.map_from_entries(F.collect_list(F.struct("slot", entry.alias("e")))).alias("m"),
                  F.sum(F.when(F.col("n") >= min_samples, 1).otherwise(0)).alias("filled"))
    )
    # Expand the slot map into a dense 168-entry array (null where missing or too few samples).
    dense = F.transform(F.sequence(F.lit(0), F.lit(SLOTS - 1)), lambda s: F.col("m")[s])
    return (
        filled.join(weeks, KEYS)
              .select(*KEYS, "weeks_used", "filled", F.to_json(dense).alias("slots_json"))
    )


def write_partition(rows):
    import json
    import oracledb

    rows = list(rows)
    if not rows:
        return
    conn = oracledb.connect(user=USER, password=os.environ["ORACLE_PASSWORD"], dsn=DSN)
    try:
        cur = conn.cursor()
        cur.setinputsizes(slots=oracledb.DB_TYPE_JSON)
        batch = [
            {"db_id": r.db_id, "inst_id": r.inst_id, "metric_name": r.metric_name,
             "weeks_used": r.weeks_used, "slots": json.loads(r.slots_json)}
            for r in rows
        ]
        cur.executemany(MERGE_SQL, batch)
        conn.commit()
    finally:
        conn.close()


def main():
    args = parse_args()
    if "ORACLE_PASSWORD" not in os.environ:
        sys.exit("ORACLE_PASSWORD not set")
    if not JAR.exists():
        sys.exit(f"missing {JAR}")

    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw = read_rollups(spark, args.lookback_days).cache()
    print(f"input rows: {raw.count()}  lookback_days={args.lookback_days}  "
          f"min_samples={args.min_samples}")

    out = compute(raw, args.min_samples).cache()
    print(f"series: {out.count()}")
    out.select(*KEYS, "weeks_used", "filled").orderBy(*KEYS).show(50, truncate=False)

    if args.dry_run:
        print("dry run: nothing written")
    else:
        out.select(*KEYS, "weeks_used", "slots_json").foreachPartition(write_partition)
        print("fd_baseline merged")

    spark.stop()


if __name__ == "__main__":
    main()
