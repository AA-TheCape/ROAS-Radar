# Operations And Freshness Guide

This guide is for operators, engineers, and analysts who need to understand how fresh ROAS Radar data should be, which background loops control that freshness, how to verify system health, and when stale or missing data is an expected lag versus an incident.

Use this guide alongside:

- `docs/implementation-guide.md` for local startup and codebase ownership
- `docs/analytics-playbook.md` for event, attribution, and reporting semantics
- `docs/runbooks/ingestion-failures.md` for `/track` and Shopify webhook issues
- `docs/runbooks/attribution-worker-backlog.md` for queue and worker failures
- `docs/runbooks/api-latency.md` for API latency incidents

## Scope

This document covers the current MVP implementation:

- API ingestion through `/track` and `/webhooks/shopify/*`
- attribution queue processing and reporting aggregate refresh
- reporting API freshness expectations
- Meta Ads and Google Ads spend sync freshness dependencies
- daily data quality and reconciliation output
- Cloud Run health checks, logging signals, and alert-to-runbook mapping

## Freshness Targets

Treat these as operational targets for the MVP, not hard guarantees.

| Area | Expected freshness target | Derived from |
| --- | --- | --- |
| Tracking ingestion | Near-real-time after API acceptance | `POST /track` writes synchronously before responding |
| Shopify order ingestion | Near-real-time after webhook receipt | webhook persistence happens in-request before returning `200` |
| Attribution queue processing | Usually within the next worker poll cycle; target under 10 seconds when backlog is healthy | `ATTRIBUTION_WORKER_POLL_INTERVAL_MS=10000` |
| Stale attribution lock recovery | Recover stuck `processing` jobs within the next worker scan after 15 minutes | `JOB_STALE_AFTER_MINUTES=15` in `src/modules/attribution/index.ts` |
| Reporting aggregates after attribution | Updated in the same attribution job that persists credits | `refreshDailyReportingMetrics(...)` runs during attribution processing |
| Meta Ads spend sync | Rolling sync loop checks for work every 60 seconds; fresh data depends on active connection and queue health | `META_ADS_WORKER_POLL_INTERVAL_MS=60000` |
| Google Ads spend sync | Rolling sync loop checks for work every 60 seconds; fresh data depends on active connection and queue health | `GOOGLE_ADS_WORKER_POLL_INTERVAL_MS=60000` |
| Spend sync lookback window | Rolling 7-day refresh, with 30-day initial backfill defaults | `*_SYNC_LOOKBACK_DAYS=7`, `*_SYNC_INITIAL_LOOKBACK_DAYS=30` |
| Data quality reconciliation | Intentionally one day behind reporting dates | `DATA_QUALITY_TARGET_LAG_DAYS=1` |
| Data quality check cadence | One run every 24 hours by default | `DATA_QUALITY_CHECK_INTERVAL_MS=86400000` |

### What “fresh” means in practice

- Tracking and Shopify ingestion are the fastest parts of the system. Once the API accepts the payload, the core raw tables are updated immediately.
- Attribution is eventually consistent. Orders can exist in `shopify_orders` before `attribution_results`, `attribution_order_credits`, and `daily_reporting_metrics` catch up.
- ROAS can lag revenue and order counts when spend sync jobs are behind or a platform connection is unhealthy.
- Reconciliation output is intentionally date-lagged. A missing check for today is expected; checks run against the target date, which defaults to yesterday.

## Background Loops And Cadences

### API service

The API in `src/app.ts` is request-driven. It does not poll.

Key synchronous behaviors:

- `/track` validates, deduplicates, upserts `tracking_sessions`, inserts `tracking_events`, and may enqueue attribution work tied to checkout/cart evidence before responding.
- Shopify webhooks validate HMAC, persist `shopify_webhook_receipts`, normalize `shopify_orders`, stitch customer identity when possible, enqueue attribution, and return quickly.

Operational implication:

- ingestion freshness depends mostly on API health and database availability, not on background scheduler cadence

### Attribution worker

The attribution worker runs from `src/worker.ts` or `src/worker-service.ts`.

Current defaults:

- batch size: `ATTRIBUTION_JOB_BATCH_SIZE=25`
- stale scan batch size: `ATTRIBUTION_STALE_SCAN_BATCH_SIZE=100`
- poll interval: `ATTRIBUTION_WORKER_POLL_INTERVAL_MS=10000`
- stale lock threshold: 15 minutes in `src/modules/attribution/index.ts`

Operational implication:

- if pending jobs are low, attribution should normally settle inside one poll cycle
- if oldest pending job age exceeds 15 minutes, treat it as an incident rather than ordinary lag

### Spend sync workers

Meta Ads and Google Ads each run as separate loops.

Current defaults:

- poll interval: `60000` ms for both workers
- batch size: `10` sync jobs per loop
- rolling sync lookback: `7` days
- initial backfill window: `30` days
- retry cap: `5` attempts

Operational implication:

- spend freshness is not tied to order ingestion freshness
- ROAS or spend columns can lag even when visits, orders, and attributed revenue are current
- if workers are run as one-shot jobs instead of long-running loops, schedule them often enough that the 7-day rolling window stays warm

### Data quality worker

The data quality loop in `src/data-quality-worker.ts` runs `runDailyDataQualityChecks()`.

Current defaults:

- target lag: `1` day
- anomaly lookback baseline: `7` trailing days plus the run date
- anomaly threshold: `35%` drop versus baseline
- minimum baseline volume: `5`
- check interval: every `24` hours

Operational implication:

- reconciliation output is meant to confirm prior-day completeness and anomalies, not minute-by-minute freshness
- a warning does not necessarily mean data is wrong; it means the current run date deviated materially from its trailing baseline

## Health Checks And Operational Signals

### HTTP health endpoints

API service:

- `GET /healthz`: liveness only, returns `{ ok: true }`
- `GET /readyz`: database readiness check through `checkDatabaseHealth()`

Attribution worker service:

- `GET /healthz`: liveness plus in-process loop state
- `GET /readyz`: database readiness plus worker loop state

Reporting and analyst checks:

- `GET /api/reporting/reconciliation`: authenticated data quality report for a `runDate` or the default lagged run date

Interpretation:

- `healthz` confirms the container is up
- `readyz` is the meaningful operational probe because it exercises database connectivity
- `/api/reporting/reconciliation` is not a liveness probe; it is an analyst-facing completeness and anomaly check

### Key Cloud Logging events

Watch these structured events in Cloud Logging:

- `http_request_completed`
- `http_request_failed`
- `tracking_ingest_accepted`
- `tracking_ingest_duplicate`
- `tracking_ingest_rejected`
- `tracking_ingest_failed`
- `shopify_webhook_processed`
- `shopify_webhook_duplicate`
- `shopify_webhook_ignored`
- `shopify_webhook_rejected`
- `shopify_webhook_failed`
- `attribution_queue_run`
- `attribution_backlog_snapshot`
- `attribution_job_failed`
- `data_quality_run`
- `meta_ads_sync_run`
- `meta_ads_sync_job_failed`
- `google_ads_sync_run`
- `google_ads_sync_job_failed`

### What to look for in logs

- Use `requestContext.requestId` to trace a single API request across failures and downstream logs.
- Use `pendingJobs`, `oldestJobAgeSeconds`, and `staleProcessingJobs` from `attribution_backlog_snapshot` to distinguish slow throughput from crashed workers.
- Use `claimedJobs`, `succeededJobs`, and `failedJobs` from `attribution_queue_run` to confirm whether the worker is catching up or only retrying failures.
- Use `tracking_ingest_rejected` and `shopify_webhook_rejected` to separate validation/auth failures from infrastructure failures.
- Use spend worker `*_sync_job_failed` logs to explain stale `spend` and `roas` metrics without blaming reporting.

## Alert To Runbook Mapping

The current monitoring configuration under `infra/monitoring/alert-policies/` maps to these runbooks.

| Alert policy | Condition | Runbook |
| --- | --- | --- |
| `ROAS Radar * Ingestion Errors` | More than 5 ingestion errors in 10 minutes | `docs/runbooks/ingestion-failures.md` |
| `ROAS Radar * Attribution Backlog` | More than 100 pending jobs for 15 minutes, or oldest pending job older than 900 seconds for 15 minutes | `docs/runbooks/attribution-worker-backlog.md` |
| `ROAS Radar * Attribution Failures` | More than 3 attribution failures in 15 minutes | `docs/runbooks/attribution-worker-backlog.md` |
| `ROAS Radar * API Latency` | p95 API latency above 1500 ms for 10 minutes | `docs/runbooks/api-latency.md` |

### Gaps to remember

- There is no dedicated alert policy yet for Meta Ads sync failures, Google Ads sync failures, or data quality warnings.
- Spend freshness and reconciliation issues currently rely on dashboard review, worker logs, and the reporting reconciliation endpoint.

## Analyst-Facing Freshness Caveats

- Orders can appear before attribution. Analysts should expect short-lived gaps where order ingestion is current but campaign-level attributed revenue is still catching up.
- ROAS can be `null` or stale while other reporting fields are fresh if spend ingestion is not configured, is delayed, or is retrying.
- Reconciliation checks are designed to lag by one day. The absence of today’s reconciliation output is expected.
- Campaign taxonomy drift can surface as `unknown`, `unmapped`, or unexpected source/medium combinations before it is normalized.
- Duplicate network attempts do not imply duplicate accepted events because `/track` deduplicates by `clientEventId` and normalized fingerprint.

## Incident Triage Workflow

Use this order when freshness looks wrong:

1. Check `GET /readyz` on the API and the attribution worker service.
2. Confirm whether raw ingestion is current:
   Look for recent `tracking_ingest_accepted` and `shopify_webhook_processed` logs.
3. Check attribution backlog:
   Inspect `attribution_backlog_snapshot` and `attribution_queue_run`.
4. Check reporting completeness:
   Call `GET /api/reporting/reconciliation` for the affected `runDate`.
5. If ROAS or spend is stale but orders are fresh:
   inspect `meta_ads_sync_run`, `meta_ads_sync_job_failed`, `google_ads_sync_run`, and `google_ads_sync_job_failed`.
6. If APIs are slow across all endpoints:
   follow `docs/runbooks/api-latency.md` and verify Cloud SQL health before scaling services.

## Troubleshooting Matrix

| Symptom | What to check first | Likely cause | Action |
| --- | --- | --- | --- |
| `/track` requests return 4xx | `tracking_ingest_rejected` logs and response body | bad payload, disallowed origin, future/old timestamp, rate limit | fix storefront payloads or origin config before changing backend capacity |
| `/track` requests return 5xx | `tracking_ingest_failed`, `readyz`, Cloud SQL health | database outage or server-side exception | restore database health, then replay or retry traffic if needed |
| Shopify orders missing from reporting | `shopify_webhook_processed` versus `shopify_webhook_rejected` or `shopify_webhook_failed` | HMAC secret mismatch, webhook delivery issue, database write failure | follow `docs/runbooks/ingestion-failures.md`, verify secrets and webhook delivery |
| Orders exist but campaign attribution is missing | `attribution_backlog_snapshot`, `attribution_queue_run`, `attribution_job_failed` | worker backlog, stale locks, deterministic retry failure | follow `docs/runbooks/attribution-worker-backlog.md` |
| Oldest pending attribution job keeps rising | `pendingJobs`, `oldestJobAgeSeconds`, worker revision health | worker stopped, too little worker capacity, DB contention | restore worker health, then address capacity or query contention |
| `staleProcessingJobs` is non-zero | worker crashes and lock age | worker died mid-job or jobs are blocked on long DB work | inspect crashing revision, roll back if needed, let stale scan requeue work |
| Summary metrics look current but ROAS is wrong or empty | spend worker logs, connection status, spend rows | spend ingestion missing, delayed, or failed | repair platform connection and rerun spend sync or reconcile gaps |
| Reconciliation shows warnings for the latest run date | `GET /api/reporting/reconciliation`, recent deploys, upstream gaps | real drop, late upstream ingestion, taxonomy drift, spend lag | confirm whether the affected date is still settling, then investigate the flagged metrics |
| Dashboard is stale across all charts | API `readyz`, `http_request_completed` latency, database health | API outage or slow reporting queries | follow `docs/runbooks/api-latency.md` |
| Reconciliation endpoint has no row for today | requested `runDate` and target lag config | expected one-day lag | query yesterday or allow the next daily data quality run to complete |

## Known MVP Operational Limitations

- Freshness is eventual, not transactional across ingestion, attribution, spend sync, and reporting.
- Attribution freshness has monitoring, but spend sync freshness and data quality warnings do not yet have first-class alert policies.
- The data quality worker currently persists one anomaly-style check against `daily_reporting_metrics`; it is not a full warehouse reconciliation suite yet.
- One-shot workers can be deployed, but freshness then depends on external scheduling discipline rather than continuous polling.
- The reporting reconciliation endpoint reports persisted checks; it does not itself recompute or repair stale data.
- The MVP remains single-store and single-tenant in practice, so operational docs assume one Shopify installation and one shared reporting dataset.

## Operational Targets Summary

Use these targets when deciding whether lag is normal or incident-worthy:

- Tracking and Shopify ingestion should look current within seconds of accepted requests.
- Attribution should normally settle within one worker cycle and should be treated as degraded well before 15 minutes.
- API p95 should stay below the 1500 ms alert threshold and materially below the 3 second runbook escalation threshold during normal operation.
- Spend sync should keep the rolling 7-day window warm; stale spend older than one sync cycle deserves investigation if connectors are active.
- Reconciliation should be available for the prior day after the daily data quality run completes.
