Key additions:
- `listHourlyRange(...)` for inclusive UTC hour-range replay/backfill
- `enqueueHours(...)` to seed the hourly queue
- `claimHourlyJobs(...)` to claim due or stale-locked windows
- exponential retry/backoff handling
- dead-letter emission via `recordDeadLetter(...)`
- `processGa4SessionAttributionHourlyJobs(...)` as the main scheduler/job batch runner
- protection so normal hourly runs do not auto-resurrect dead-lettered windows, while explicit manual ranges do
