# Spark jobs

PySpark. Planned jobs: `baselines`, `period_diff`, `archive`, `embed`.

First task (M1): `baselines` reads N weeks of 5 minute rollups from ADB over JDBC (Oracle JDBC driver and wallet), computes median and MAD per series and hour of week, and publishes one message per series to `ora.baseline.v1` plus an upsert into `fd_baseline`.
