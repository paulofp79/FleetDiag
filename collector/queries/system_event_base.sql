-- Base mode (no ASH): cumulative wait statistics. The collector keeps the previous value
-- per (inst_id, event) and publishes the delta per interval.
SELECT inst_id, event, wait_class, total_waits, time_waited_micro
FROM   gv$system_event
WHERE  wait_class <> 'Idle'
