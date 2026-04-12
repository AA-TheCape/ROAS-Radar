# API Latency Runbook

## Triggers

- `ROAS Radar * API Latency` alert
- dashboard shows elevated p95 latency across multiple API routes

## Immediate Checks

1. Use the dashboard route breakdown to determine whether latency is isolated to reporting endpoints or public ingestion.
2. In Cloud Logging, filter on `jsonPayload.event="http_request_completed"` and sort by `jsonPayload.durationMs`.
3. Compare the timing with Cloud SQL connection saturation, statement timeout logs, and recent deploys.
4. Validate whether latency affects only one revision or all active revisions.

## Likely Causes

- slow reporting queries or aggregate refresh contention
- exhausted Cloud SQL connection pool or elevated database CPU
- insufficient API min instances after a traffic increase
- noisy downstream dependency retries during webhook processing

## Remediation

1. If reporting routes are the outlier, inspect query plans and narrow the impacted date range or filters before scaling blindly.
2. If latency spans all routes, verify database connectivity and pool exhaustion first.
3. Increase `API_MIN_INSTANCES` or `API_MAX_INSTANCES` only if the bottleneck is request concurrency rather than database saturation.
4. Roll back the current revision if latency started immediately after deployment and no infrastructure issue is visible.

## Escalation

- Escalate when p95 exceeds 3 seconds for more than 30 minutes or when public ingestion also degrades.
