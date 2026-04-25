# Attribution Worker Backlog Runbook

## Triggers

- `ROAS Radar * Attribution Backlog` alert
- `ROAS Radar * Attribution Failures` alert
- dashboard shows rising pending jobs, oldest job age, or stale processing locks

## Immediate Checks

1. Confirm the worker service is still receiving traffic and has healthy revisions in Cloud Run.
2. Inspect `attribution_backlog_snapshot`, `attribution_queue_run`, `attribution_job_failed`, and `order_attribution_backfill_job_lifecycle` logs for the same time window.
3. Check whether `claimedJobs` is non-zero while `succeededJobs` stays flat; that usually indicates a deterministic processing error.
4. Compare `pendingJobs` with `staleProcessingJobs`. A high stale count usually means a worker crash or long-running database contention.
5. For backfill failures, filter `order_attribution_backfill_job_lifecycle` on `jsonPayload.stage="failed"` and pivot by `jsonPayload.jobId`, `jsonPayload.code`, and `jsonPayload.report.failureCount`.

## Likely Causes

- a bad order payload or schema drift causing repeated attribution retries
- Cloud SQL contention or degraded query performance
- worker concurrency or instance limits too low for the current webhook volume
- a deployment introduced a deterministic error in attribution persistence or aggregate refresh

## Remediation

1. Review a failed order from `attribution_job_failed` and reproduce locally if the same order keeps retrying.
2. For a failed backfill run, inspect the lifecycle log's `report.sampleFailures`, `failureMessage`, and option flags before re-running the same date window.
3. If Cloud SQL latency is elevated, reduce worker pressure temporarily or scale the database before raising worker instances.
4. Increase `WORKER_MAX_INSTANCES` only after checking that database pool limits can absorb the additional concurrency.
5. If stale locks grow while the service is crashing, roll back to the last healthy revision and re-run the backlog.

## Escalation

- Escalate immediately when oldest pending job age exceeds 30 minutes in production.
- Open a data quality follow-up if attribution catches up but order counts in reporting remain flat.
