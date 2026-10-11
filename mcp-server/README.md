# MCP server

Tools over the curated views in `../sql/002_views.sql`: `list_incidents`, `get_incident_evidence`, `get_metric_series`, `compare_periods`, `find_similar_incidents`, `search_runbooks`.

One write tool, `save_hypothesis`, runs under a separate database account that can update only the hypothesis columns of `fd_incident`.

First task (M3): `list_incidents` and `get_incident_evidence`, tested from Claude Desktop through the existing Node.js ADB MCP proxy.
