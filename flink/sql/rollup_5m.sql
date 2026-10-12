SET 'sql-client.execution.result-mode' = 'tableau';
SET 'table.local-time-zone' = 'UTC';
SET 'pipeline.name' = 'fleetdiag-rollup-5m';
-- Checkpoints every minute to flink/checkpoints (mounted on both containers). Kept after
-- cancel or a container crash so scripts/flink-submit.sh can resume from the latest one.
SET 'execution.checkpointing.interval' = '60 s';
SET 'state.checkpoints.dir' = 'file:///opt/flink/checkpoints';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
SET 'execution.checkpointing.num-retained' = '3';
-- A Kafka partition with no data would hold the watermark back and no window would close.
SET 'table.exec.source.idle-timeout' = '2 min';

CREATE TABLE metric_raw (
  fleet STRING,
  db_unique_name STRING,
  inst_id INT,
  metric_name STRING,
  `value` DOUBLE,
  end_time TIMESTAMP_LTZ(3),
  WATERMARK FOR end_time AS end_time - INTERVAL '2' MINUTE
) WITH (
  'connector' = 'kafka',
  'topic' = 'ora.metric.raw.v1',
  'properties.bootstrap.servers' = 'kafka:19092',
  'properties.group.id' = 'fleetdiag-flink-rollup-5m',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true',
  'json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE metric_rollup_5m (
  fleet STRING,
  db_unique_name STRING,
  inst_id INT,
  metric_name STRING,
  bucket_sec INT,
  bucket_ts TIMESTAMP_LTZ(3),
  val_avg DOUBLE,
  val_min DOUBLE,
  val_max DOUBLE,
  sample_cnt BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'ora.metric.rollup.v1',
  'properties.bootstrap.servers' = 'kafka:19092',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

INSERT INTO metric_rollup_5m
SELECT fleet, db_unique_name, inst_id, metric_name, 300,
       window_start, AVG(`value`), MIN(`value`), MAX(`value`), COUNT(*)
FROM TABLE(TUMBLE(TABLE metric_raw, DESCRIPTOR(end_time), INTERVAL '5' MINUTE))
GROUP BY fleet, db_unique_name, inst_id, metric_name, window_start, window_end;
