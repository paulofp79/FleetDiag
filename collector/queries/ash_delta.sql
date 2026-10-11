-- diag-pack mode only: requires the Oracle Diagnostics Pack license.
-- :inst_id and :watermark are bound by the collector, which stores the last sample_time
-- it saw per instance. Run one query per instance to keep each cursor cheap.
SELECT inst_id, sample_time, session_id, session_serial#, session_state,
       sql_id, event, wait_class,
       blocking_session, blocking_inst_id, module
FROM   gv$active_session_history
WHERE  inst_id = :inst_id
  AND  sample_time > :watermark
ORDER  BY sample_time
