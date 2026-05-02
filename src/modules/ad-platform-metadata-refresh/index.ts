Added:
- configurable freshness threshold resolution with default `48` hours
- freshness query over active metadata entities in the requested backfill window
- lifecycle log emission for started/completed/failed sync runs
- freshness snapshot emission per platform/entity type after backfill completes

Completed lifecycle logs now include:
- durationMs
- planned inserts / updates
- campaign resolved rate
- overall unresolved rate
- stale entity count
