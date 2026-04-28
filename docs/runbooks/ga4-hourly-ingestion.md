New runbook covering:
- scheduler/job/queue/dead-letter topology
- normal operation
- queue and dead-letter verification SQL
- dead-letter replay using `npm run dead-letters:replay`
- manual hour-range replay/backfill using `GA4_INGESTION_START_HOUR` and `GA4_INGESTION_END_HOUR`
- troubleshooting and rollback
