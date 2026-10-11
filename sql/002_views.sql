-- Curated read surface for Select AI and the MCP server.
-- Grant only these views to the reader role. DRAFT, NOT YET RUN.

CREATE OR REPLACE VIEW fd_v_incident AS
SELECT i.incident_key, d.fleet, d.db_unique_name, d.platform, d.db_version, d.is_rac,
       i.opened_at, i.closed_at, i.severity, i.status, i.title,
       i.hypothesis, i.hypothesis_confidence, i.confirmed_root_cause, i.resolution
FROM   fd_incident i
JOIN   fd_database d ON d.db_id = i.db_id;

CREATE OR REPLACE VIEW fd_v_anomaly_recent AS
SELECT a.anomaly_key, a.incident_key, d.fleet, d.db_unique_name, a.inst_id, a.kind, a.subject,
       a.window_start, a.window_end, a.observed_val, a.baseline_val, a.score, a.severity
FROM   fd_anomaly a
JOIN   fd_database d ON d.db_id = a.db_id
WHERE  a.window_start > SYSTIMESTAMP - INTERVAL '14' DAY;

CREATE OR REPLACE VIEW fd_v_metric_5m AS
SELECT d.fleet, d.db_unique_name, r.inst_id, r.metric_name, r.bucket_ts,
       r.val_avg, r.val_min, r.val_max
FROM   fd_metric_rollup r
JOIN   fd_database d ON d.db_id = r.db_id
WHERE  r.bucket_sec = 300
  AND  r.bucket_ts > SYSTIMESTAMP - INTERVAL '14' DAY;

CREATE OR REPLACE VIEW fd_v_wait_profile AS
SELECT d.fleet, d.db_unique_name, w.inst_id, w.bucket_ts, w.wait_class, w.event_name, w.aas, w.top_sql_id
FROM   fd_wait_profile w
JOIN   fd_database d ON d.db_id = w.db_id
WHERE  w.bucket_ts > SYSTIMESTAMP - INTERVAL '14' DAY;

-- Comments give Select AI the context it needs to map questions to columns.
COMMENT ON TABLE fd_v_incident IS 'One row per detected performance incident on a database, with the agent hypothesis and the DBA-confirmed root cause.';
COMMENT ON COLUMN fd_v_incident.confirmed_root_cause IS 'Root cause confirmed by a DBA. Null if not yet reviewed.';
COMMENT ON COLUMN fd_v_incident.hypothesis_confidence IS 'LOW, MEDIUM or HIGH, set by the agent.';
COMMENT ON TABLE fd_v_anomaly_recent IS 'Statistical deviations from the seasonal baseline in the last 14 days. score is a robust z-score.';
COMMENT ON TABLE fd_v_metric_5m IS 'Five-minute rollups of system metrics for the last 14 days.';
COMMENT ON TABLE fd_v_wait_profile IS 'Average active sessions (aas) per minute by wait class and wait event for the last 14 days.';

CREATE ROLE fd_reader;
GRANT SELECT ON fd_v_incident TO fd_reader;
GRANT SELECT ON fd_v_anomaly_recent TO fd_reader;
GRANT SELECT ON fd_v_metric_5m TO fd_reader;
GRANT SELECT ON fd_v_wait_profile TO fd_reader;
