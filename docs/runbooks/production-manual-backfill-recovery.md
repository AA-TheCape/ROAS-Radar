# Production Manual Backfill And Recovery Runbook

Use this runbook when production needs a manual backfill or recovery run for Shopify order imports, Shopify attribution hints, GA4 fallback attribution, order attribution backfill, campaign metadata history, or dead-letter replay.

Production execution is allowed only after the same window has been rehearsed in staging and reviewed by engineering plus the primary on-call. On-call sign-off means the current on-call engineer can execute this document from a clean operator shell without source-code inspection or extra verbal instructions.

## Scope

- Environment: production
- Project: `roas-radar-production`
- Region: `us-central1`
- Required operator role: Cloud Run Job invoker for the target manual job, Cloud Logging viewer, and read-only database access for validation queries
- Required review artifacts: staging execution log URLs, staging dry-run report, staging write-enabled report when applicable, before/after validation snapshots, and signed handoff checklist

Manual jobs in scope:

- `roas-radar-shopify-order-reimport-production`
- `roas-radar-shopify-attribution-recovery-production`
- `roas-radar-ga4-fallback-recovery-production`
- `roas-radar-order-attribution-backfill-production`
- `roas-radar-campaign-metadata-backfill-production`
- `roas-radar-dead-letter-replay-production`

## Stop Conditions

Do not run production if any item is true:

- The matching staging rehearsal for the same date window is missing or failed.
- The staging dry-run and write-enabled run disagree on touched records without a written explanation.
- The requested production window is broader than the reviewed staging window.
- `docs/runbooks/staging-backfill-rehearsal-2026-05-31.md` or a newer rehearsal report still recommends no-go.
- On-call cannot identify the exact Cloud Run Job, date window, dry-run command, write-enabled command, validation query, and rollback or replay path from this runbook alone.
- `MANUAL_JOB_INVOKER_MEMBERS` does not include the approved operator identity or break-glass group.

## Production Readiness Gate

Before issuing any production command, record these fields in the incident or change ticket:

| Field | Required value |
| --- | --- |
| Change ticket | Link to approved production change |
| Incident, if any | Link to incident or `none` |
| Production window | ISO start and end timestamps, UTC |
| Requested by | Human or incident alias used in `--requested-by` |
| Staging evidence | Cloud Run execution log URLs plus validation query output |
| Expected records | Dry-run candidate count and write-enabled upper bound |
| Jobs approved | Exact job names from the scope section |
| Engineering approver | Name and timestamp |
| On-call approver | Name and timestamp |

Use one stable `--requested-by` value for the whole workflow, such as `incident-1234-oncall@example.com`, so logs and database rows can be joined.

## Operator Setup

Run from a shell with Google Cloud SDK installed and authenticated to the production project.

```sh
gcloud config set project roas-radar-production
gcloud config set run/region us-central1
gcloud auth list --filter=status:ACTIVE --format='value(account)'
gcloud run jobs list --project=roas-radar-production --region=us-central1 --format='value(metadata.name)'
```

Confirm the target job appears in the list and the active account is the approved operator.

## Execution Order

Use the smallest date window that remediates the issue. Reuse the same window across every step.

1. Reimport Shopify orders when production is missing local Shopify orders for the window.
2. Recover Shopify attribution hints when orders exist but Shopify web attribution fields are missing.
3. Recover GA4 fallback attribution only after Shopify hint recovery is complete or inapplicable.
4. Run order attribution backfill last, after the source-specific recovery steps.
5. Run campaign metadata history backfill only for descriptive campaign name or hierarchy gaps.
6. Run dead-letter replay only after the upstream cause is fixed and dry-run replay matches the intended source records.

This order preserves the recovery precedence documented in `docs/recovery-job-framework.md`: Shopify, then GA4, then ad platforms.

## Commands

Replace `WINDOW_START`, `WINDOW_END`, `DATE_START`, `DATE_END`, and `REQUESTED_BY` before running. `WINDOW_END` is exclusive.

### Shopify Order Reimport

Use this first if orders are missing locally. The command accepts date-only bounds.

```sh
gcloud run jobs execute roas-radar-shopify-order-reimport-production \
  --project=roas-radar-production \
  --region=us-central1 \
  --args=run,shopify:reimport-orders:start,--,--from,DATE_START,--to,DATE_END \
  --wait
```

### Shopify Attribution Hint Recovery

Dry run:

```sh
gcloud run jobs execute roas-radar-shopify-attribution-recovery-production \
  --project=roas-radar-production \
  --region=us-central1 \
  --args=run,attribution:recover-shopify-hints:start,--,--from,WINDOW_START,--to,WINDOW_END,--requested-by,REQUESTED_BY,--scope-key,REQUESTED_BY \
  --wait
```

Write-enabled run:

```sh
gcloud run jobs execute roas-radar-shopify-attribution-recovery-production \
  --project=roas-radar-production \
  --region=us-central1 \
  --args=run,attribution:recover-shopify-hints:start,--,--from,WINDOW_START,--to,WINDOW_END,--requested-by,REQUESTED_BY,--scope-key,REQUESTED_BY,--apply \
  --wait
```

### GA4 Fallback Recovery

Dry run:

```sh
gcloud run jobs execute roas-radar-ga4-fallback-recovery-production \
  --project=roas-radar-production \
  --region=us-central1 \
  --args=run,attribution:recover-ga4-fallback:start,--,--from,WINDOW_START,--to,WINDOW_END,--requested-by,REQUESTED_BY,--scope-key,REQUESTED_BY \
  --wait
```

Write-enabled run:

```sh
gcloud run jobs execute roas-radar-ga4-fallback-recovery-production \
  --project=roas-radar-production \
  --region=us-central1 \
  --args=run,attribution:recover-ga4-fallback:start,--,--from,WINDOW_START,--to,WINDOW_END,--requested-by,REQUESTED_BY,--scope-key,REQUESTED_BY,--apply \
  --wait
```

### Order Attribution Backfill

Dry run:

```sh
gcloud run jobs execute roas-radar-order-attribution-backfill-production \
  --project=roas-radar-production \
  --region=us-central1 \
  --args=run,attribution:backfill-orders:start,--,--from,WINDOW_START,--to,WINDOW_END,--requested-by,REQUESTED_BY,--dry-run,--limit,500 \
  --wait
```

Write-enabled run:

```sh
gcloud run jobs execute roas-radar-order-attribution-backfill-production \
  --project=roas-radar-production \
  --region=us-central1 \
  --args=run,attribution:backfill-orders:start,--,--from,WINDOW_START,--to,WINDOW_END,--requested-by,REQUESTED_BY,--limit,500 \
  --wait
```

Keep the default Shopify writeback enabled unless the approved change explicitly says local-only recovery. Keep the default web-order filter unless the staging report covers non-web orders.

### Campaign Metadata Backfill

History dry run:

```sh
gcloud run jobs execute roas-radar-campaign-metadata-backfill-production \
  --project=roas-radar-production \
  --region=us-central1 \
  --args=run,campaign-metadata:backfill:start,--,--mode,history,--start-date,DATE_START,--end-date,DATE_END,--requested-by,REQUESTED_BY,--dry-run,true \
  --wait
```

History write-enabled run:

```sh
gcloud run jobs execute roas-radar-campaign-metadata-backfill-production \
  --project=roas-radar-production \
  --region=us-central1 \
  --args=run,campaign-metadata:backfill:start,--,--mode,history,--start-date,DATE_START,--end-date,DATE_END,--requested-by,REQUESTED_BY,--dry-run,false \
  --wait
```

API refresh dry run for one bounded platform. Run one command per platform so the `--platforms` value is unambiguous in the Cloud Run argument override.

```sh
gcloud run jobs execute roas-radar-campaign-metadata-backfill-production \
  --project=roas-radar-production \
  --region=us-central1 \
  --args=run,campaign-metadata:backfill:start,--,--mode,api-refresh,--requested-by,REQUESTED_BY,--platforms,meta_ads,--dry-run,true \
  --wait
```

### Dead-Letter Replay

Dry run first:

```sh
gcloud run jobs execute roas-radar-dead-letter-replay-production \
  --project=roas-radar-production \
  --region=us-central1 \
  --args=run,dead-letters:replay:start,--,--source-table,recovery_job_runs,--from,WINDOW_START,--to,WINDOW_END,--requested-by,REQUESTED_BY,--dry-run \
  --wait
```

Write-enabled replay:

```sh
gcloud run jobs execute roas-radar-dead-letter-replay-production \
  --project=roas-radar-production \
  --region=us-central1 \
  --args=run,dead-letters:replay:start,--,--source-table,recovery_job_runs,--from,WINDOW_START,--to,WINDOW_END,--requested-by,REQUESTED_BY \
  --wait
```

## Live Monitoring

Watch each run until the Cloud Run execution reaches terminal state:

```sh
gcloud run jobs executions list \
  --job=roas-radar-order-attribution-backfill-production \
  --project=roas-radar-production \
  --region=us-central1 \
  --limit=5
```

Check structured logs by `REQUESTED_BY`:

```sh
gcloud logging read 'jsonPayload.initiatedBy="REQUESTED_BY" OR jsonPayload.requestedBy="REQUESTED_BY"' \
  --project=roas-radar-production \
  --limit=100 \
  --format=json
```

During execution, monitor:

- `ROAS Radar * Recovery Run Failure Rate`
- `ROAS Radar * Recovery Long Running Jobs`
- `ROAS Radar * Order Attribution Backfill Failures`
- `ROAS Radar * Attribution Worker Backlog`
- `ROAS Radar * Shopify Writeback Success`
- `ROAS Radar * Resolver Unattributed Rate`
- `ROAS Radar * Campaign Metadata Freshness Breach`

Abort the workflow and page engineering if failure rate rises, writeback success drops below the alert threshold, or a job touches records outside the approved window.

## Validation Queries

Capture before and after snapshots for the approved window.

```sql
SELECT attribution_tier, attribution_status, COUNT(*) AS orders
FROM shopify_order_attribution_snapshots
WHERE processed_at >= 'WINDOW_START'
  AND processed_at < 'WINDOW_END'
GROUP BY attribution_tier, attribution_status
ORDER BY attribution_tier, attribution_status;

SELECT job_type, dry_run, status, records_discovered, records_processed,
       records_succeeded, records_failed, records_skipped,
       side_effects_attempted, side_effects_succeeded, side_effects_suppressed
FROM recovery_job_runs
WHERE initiated_by = 'REQUESTED_BY'
ORDER BY queued_at DESC;

SELECT status, COUNT(*) AS records
FROM order_attribution_backfill_runs
WHERE requested_by = 'REQUESTED_BY'
GROUP BY status
ORDER BY status;

SELECT COUNT(*) AS campaign_metadata_runs
FROM campaign_metadata_backfill_runs
WHERE requested_by = 'REQUESTED_BY';
```

Record-level spot checks:

```sql
SELECT recovery_run_id, shopify_order_id, previous_attribution_tier,
       recovered_attribution_tier, applied, dry_run
FROM attribution_recovery_audit_logs
WHERE created_at >= now() - interval '1 day'
ORDER BY created_at DESC
LIMIT 50;

SELECT shopify_order_id, COUNT(*) AS applied_recoveries
FROM attribution_recovery_audit_logs
WHERE dry_run = false
  AND applied = true
  AND created_at >= now() - interval '1 day'
GROUP BY shopify_order_id
HAVING COUNT(*) > 1
ORDER BY applied_recoveries DESC;
```

Expected result:

- Dry-run side effects are suppressed.
- Write-enabled run count does not exceed the reviewed dry-run candidate count without an explanation.
- Failed records are classified as retryable or permanent.
- Shopify-derived values are not overwritten by lower-precedence GA4 or ad-platform values.
- Any Shopify writebacks have matching local attribution snapshots.

## Recovery And Rollback

Manual backfills are idempotent but not globally reversible. Prefer forward recovery:

- If a job fails from transient upstream errors, leave application retry and dead-letter handling intact, then rerun the same dry-run command after the upstream issue clears.
- If records dead-letter, use the dead-letter replay workflow after confirming the source table, window, and failure code.
- If Shopify writeback is suspected to be wrong, stop further write-enabled attribution jobs, preserve the affected order IDs, and page engineering before changing Shopify notes.
- If campaign metadata refresh writes stale names, pause the affected metadata scheduler and rerun a bounded API refresh after provider data is corrected.

## Handoff Checklist

Complete this checklist in the production change ticket.

- [ ] Staging dry-run for the exact production window completed.
- [ ] Staging write-enabled run, if needed, completed and was validated.
- [ ] Engineering reviewed the staging evidence and approved the production command set.
- [ ] Primary on-call performed a documentation-only walkthrough using this runbook.
- [ ] On-call confirmed they can identify the command, window, validation query, monitoring signals, and stop conditions without source-code inspection.
- [ ] Production dry-run completed and output was attached to the ticket.
- [ ] Production write-enabled run, if needed, completed and output was attached to the ticket.
- [ ] Before/after validation query output was attached to the ticket.
- [ ] Any discrepancies were triaged or explicitly accepted by engineering.
- [ ] Final on-call sign-off recorded: `name`, `timestamp`, `change ticket`, and `I can execute production manual backfill and recovery from documentation alone`.

## Sign-Off Record

Use this table in the production change ticket.

| Role | Name | Timestamp UTC | Decision | Notes |
| --- | --- | --- | --- | --- |
| Engineering reviewer |  |  | pending |  |
| Primary on-call |  |  | pending | Must include documentation-only execution statement |
| Incident commander or change owner |  |  | pending |  |
