#!/usr/bin/env bash
# M0 exit check: gaps, near-duplicates, rows per metric, counted from the 24h start time (UTC)
set -e
cd "$(dirname "$0")/.."
START="$(tail -1 sink/run/m0_start.txt)"
q() { docker exec -i oracle-source sqlplus -s fleetdiag/"$ORACLE_PASSWORD"@localhost:1521/FREEPDB1; }
echo "window starts at $START UTC"
q <<SQL
set linesize 200 pagesize 100
prompt == rows and window ==
select count(*) rows_total, min(bucket_ts) first_ts, max(bucket_ts) last_ts from fd_metric_rollup where bucket_sec = 60 and bucket_ts > timestamp '$START';
prompt == gaps over 90 s (expect none) ==
select metric_name, count(*) gaps from (
  select metric_name, bucket_ts - lag(bucket_ts) over (partition by db_id,inst_id,metric_name order by bucket_ts) d
  from fd_metric_rollup where bucket_sec = 60 and bucket_ts > timestamp '$START') where d > interval '90' second group by metric_name;
prompt == near duplicates under 30 s (expect none) ==
select metric_name, count(*) dups from (
  select metric_name, bucket_ts - lag(bucket_ts) over (partition by db_id,inst_id,metric_name order by bucket_ts) d
  from fd_metric_rollup where bucket_sec = 60 and bucket_ts > timestamp '$START') where d < interval '30' second group by metric_name;
prompt == rows per metric (expect all equal) ==
select metric_name, count(*) n from fd_metric_rollup where bucket_sec = 60 and bucket_ts > timestamp '$START' group by metric_name order by n, metric_name;
SQL
