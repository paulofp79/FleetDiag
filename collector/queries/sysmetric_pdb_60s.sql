-- Pluggable databases: V$SYSMETRIC is empty there. The 60 s metrics are in
-- V$CON_SYSMETRIC, group 18 ("PDB System Metrics Long Duration").
SELECT inst_id, metric_name, metric_unit, value, begin_time, end_time
FROM   gv$con_sysmetric
WHERE  group_id = 18
