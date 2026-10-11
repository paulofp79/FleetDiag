-- FleetDiag schema for Oracle Autonomous Database (23ai / 26ai syntax).
-- DRAFT, NOT YET RUN: execute in a scratch schema first (milestone M0).
-- The VECTOR dimension is a placeholder until the embedding model is chosen (DECISIONS D9).
-- Writers are idempotent: anomaly_key and incident_key are deterministic keys from Flink.

-- Inventory ---------------------------------------------------------------
CREATE TABLE fd_database (
  db_id          NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  fleet          VARCHAR2(64)  NOT NULL,
  db_unique_name VARCHAR2(128) NOT NULL,
  platform       VARCHAR2(32)  NOT NULL,
  db_version     VARCHAR2(32),
  is_rac         NUMBER(1)     DEFAULT 0 NOT NULL,
  instance_count NUMBER(3)     DEFAULT 1 NOT NULL,
  tags           JSON,
  created_at     TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
  CONSTRAINT fd_database_uk UNIQUE (fleet, db_unique_name),
  CONSTRAINT fd_database_platform_ck CHECK (platform IN ('EXADATA','ADB','ONPREM','OTHER'))
);

-- Metric rollups: bucket_sec = 60, 300 or 3600 --------------------------------
CREATE TABLE fd_metric_rollup (
  db_id       NUMBER        NOT NULL REFERENCES fd_database (db_id),
  inst_id     NUMBER(4)     NOT NULL,
  metric_name VARCHAR2(128) NOT NULL,
  bucket_sec  NUMBER(5)     NOT NULL,
  bucket_ts   TIMESTAMP(0)  NOT NULL,
  val_avg     BINARY_DOUBLE,
  val_min     BINARY_DOUBLE,
  val_max     BINARY_DOUBLE,
  sample_cnt  NUMBER(6),
  CONSTRAINT fd_metric_rollup_pk
    PRIMARY KEY (db_id, inst_id, metric_name, bucket_sec, bucket_ts) USING INDEX LOCAL
)
PARTITION BY RANGE (bucket_ts) INTERVAL (NUMTODSINTERVAL(1, 'DAY'))
(PARTITION p_initial VALUES LESS THAN (TIMESTAMP '2026-01-01 00:00:00'));

-- Wait profile: average active sessions per minute by wait class and event ---
CREATE TABLE fd_wait_profile (
  db_id      NUMBER        NOT NULL REFERENCES fd_database (db_id),
  inst_id    NUMBER(4)     NOT NULL,
  bucket_ts  TIMESTAMP(0)  NOT NULL,
  wait_class VARCHAR2(64)  NOT NULL,
  event_name VARCHAR2(128) NOT NULL,
  aas        BINARY_DOUBLE NOT NULL,
  top_sql_id VARCHAR2(13),
  CONSTRAINT fd_wait_profile_pk
    PRIMARY KEY (db_id, inst_id, bucket_ts, wait_class, event_name) USING INDEX LOCAL
)
PARTITION BY RANGE (bucket_ts) INTERVAL (NUMTODSINTERVAL(1, 'DAY'))
(PARTITION p_initial VALUES LESS THAN (TIMESTAMP '2026-01-01 00:00:00'));

-- Seasonal baselines: one row per series, 168 hour-of-week slots in JSON -----
-- slots = [{"m": median, "mad": mad, "n": sample_count}, ...]  (index 0 = Monday 00:00)
CREATE TABLE fd_baseline (
  db_id       NUMBER        NOT NULL REFERENCES fd_database (db_id),
  inst_id     NUMBER(4)     NOT NULL,
  metric_name VARCHAR2(128) NOT NULL,
  weeks_used  NUMBER(3)     NOT NULL,
  computed_at TIMESTAMP(0)  DEFAULT SYSTIMESTAMP NOT NULL,
  slots       JSON          NOT NULL,
  CONSTRAINT fd_baseline_pk PRIMARY KEY (db_id, inst_id, metric_name)
);

-- Anomalies -----------------------------------------------------------------
CREATE TABLE fd_anomaly (
  anomaly_id   NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  anomaly_key  VARCHAR2(40)  NOT NULL,
  db_id        NUMBER        NOT NULL REFERENCES fd_database (db_id),
  inst_id      NUMBER(4),
  kind         VARCHAR2(40)  NOT NULL,   -- METRIC_DEVIATION, WAIT_CLASS_SHIFT, GC_LATENCY, GC_SKEW, BLOCKER_CHAIN
  subject      VARCHAR2(256) NOT NULL,   -- metric name or wait event
  window_start TIMESTAMP(0)  NOT NULL,
  window_end   TIMESTAMP(0)  NOT NULL,
  observed_val BINARY_DOUBLE,
  baseline_val BINARY_DOUBLE,
  score        BINARY_DOUBLE NOT NULL,
  severity     VARCHAR2(10)  NOT NULL,
  evidence     JSON,
  incident_key VARCHAR2(40),             -- no FK on purpose: topics can arrive out of order
  CONSTRAINT fd_anomaly_key_uk UNIQUE (anomaly_key),
  CONSTRAINT fd_anomaly_sev_ck CHECK (severity IN ('INFO','WARNING','CRITICAL'))
);
CREATE INDEX fd_anomaly_db_ts ON fd_anomaly (db_id, window_start);
CREATE INDEX fd_anomaly_incident ON fd_anomaly (incident_key);

-- Incidents -----------------------------------------------------------------
CREATE TABLE fd_incident (
  incident_id          NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  incident_key         VARCHAR2(40) NOT NULL,
  db_id                NUMBER       NOT NULL REFERENCES fd_database (db_id),
  opened_at            TIMESTAMP(0) NOT NULL,
  closed_at            TIMESTAMP(0),
  severity             VARCHAR2(10) NOT NULL,
  status               VARCHAR2(12) DEFAULT 'OPEN' NOT NULL,
  title                VARCHAR2(300),
  evidence             JSON,                      -- written by Flink
  hypothesis           CLOB,                      -- written by the agent via save_hypothesis
  hypothesis_confidence VARCHAR2(10),
  confirmed_root_cause CLOB,                      -- written by a DBA
  resolution           CLOB,
  embedding            VECTOR(1024, FLOAT32),     -- placeholder dimension, see D9
  CONSTRAINT fd_incident_key_uk UNIQUE (incident_key),
  CONSTRAINT fd_incident_sev_ck CHECK (severity IN ('INFO','WARNING','CRITICAL')),
  CONSTRAINT fd_incident_status_ck CHECK (status IN ('OPEN','CLOSED','CONFIRMED'))
);
CREATE INDEX fd_incident_db_ts ON fd_incident (db_id, opened_at);

-- Runbook knowledge -----------------------------------------------------------
CREATE TABLE fd_runbook_chunk (
  chunk_id   NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  source     VARCHAR2(400) NOT NULL,
  title      VARCHAR2(300),
  body       CLOB          NOT NULL,
  applies_to JSON,                                 -- e.g. {"platform":["EXADATA"],"rac":true,"min_version":"19"}
  embedding  VECTOR(1024, FLOAT32)
);

-- Vector indexes (IVF). Switch to HNSW if the vector pool is sized for it.
CREATE VECTOR INDEX fd_incident_vidx ON fd_incident (embedding)
  ORGANIZATION NEIGHBOR PARTITIONS
  DISTANCE COSINE
  WITH TARGET ACCURACY 95;

CREATE VECTOR INDEX fd_runbook_vidx ON fd_runbook_chunk (embedding)
  ORGANIZATION NEIGHBOR PARTITIONS
  DISTANCE COSINE
  WITH TARGET ACCURACY 95;
