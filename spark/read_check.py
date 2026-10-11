#!/usr/bin/env python3
"""Check that Spark can read fd_metric_rollup from Oracle over JDBC.

Usage (spark venv active, ORACLE_PASSWORD exported):
    python spark/read_check.py
"""
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parent
JAR = ROOT / "jars" / "ojdbc11.jar"
DSN = os.getenv("ORACLE_DSN", "localhost:1521/FREEPDB1")
USER = os.getenv("ORACLE_USER", "fleetdiag")

if "ORACLE_PASSWORD" not in os.environ:
    sys.exit("ORACLE_PASSWORD not set")
if not JAR.exists():
    sys.exit(f"missing {JAR}")

# All data timestamps are UTC: keep the JVM and the Spark session on UTC so
# Oracle TIMESTAMP values are not shifted to America/Sao_Paulo.
spark = (
    SparkSession.builder.appName("fd-read-check")
    .master("local[2]")
    .config("spark.jars", str(JAR))
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

df = (
    spark.read.format("jdbc")
    .option("url", f"jdbc:oracle:thin:@//{DSN}")
    .option("user", USER)
    .option("password", os.environ["ORACLE_PASSWORD"])
    .option("driver", "oracle.jdbc.OracleDriver")
    .option("query", "select db_id, inst_id, metric_name, bucket_ts, val_avg "
                     "from fd_metric_rollup where bucket_sec = 60")
    .option("fetchsize", "5000")
    .load()
)

df.printSchema()
print("rows:", df.count())
(df.groupBy("metric_name")
   .agg(F.count("*").alias("n"),
        F.min("bucket_ts").alias("first_ts"),
        F.max("bucket_ts").alias("last_ts"),
        F.round(F.avg("val_avg"), 2).alias("avg"))
   .orderBy("metric_name")
   .show(30, truncate=False))

spark.stop()
