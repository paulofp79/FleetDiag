# ADB sink

Consumes `ora.metric.rollup.v1`, `ora.wait.rollup.v1`, `ora.anomaly.v1` and `ora.incident.v1` and applies batched MERGE statements into Autonomous Database (python-oracledb `executemany`).

First task (M0): MERGE rollups into `fd_metric_rollup`, commit Kafka offsets only after the database commit, and prove that replaying a topic from the start produces no duplicates.
