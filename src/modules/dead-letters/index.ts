Key change:
- Extended `requeueSourceRecord(...)` to support `source_table = 'ga4_bigquery_hourly_jobs'`
- Replay now resets the GA4 hourly job row to `pending`, clears dead-letter metadata, and makes the window eligible for the next job execution
