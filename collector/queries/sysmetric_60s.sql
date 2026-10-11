-- Long-duration (60 s) system metrics for all instances.
-- Needs SELECT on GV_$SYSMETRIC. Verify GROUP_ID against V$METRICGROUP on each release
-- (expected: 2 = "System Metrics Long Duration"). The collector filters by the allow-list
-- in config/metrics.yaml, so this query stays generic.
SELECT inst_id, metric_name, metric_unit, value, begin_time, end_time
FROM   gv$sysmetric
WHERE  group_id = 2
