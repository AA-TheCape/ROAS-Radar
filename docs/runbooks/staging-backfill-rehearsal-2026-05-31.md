# Staging Backfill Rehearsal Validation Report - 2026-05-31

## Scope

- Environment: staging (`roas-radar-staging`, `us-central1`)
- Window rehearsed: intended last 30 days, `2026-05-01T00:00:00Z` through `2026-05-31T00:00:00Z`
- Jobs in scope:
  - `roas-radar-campaign-metadata-backfill-staging`
  - `roas-radar-order-attribution-backfill-staging`
  - `roas-radar-shopify-attribution-recovery-staging`
  - `roas-radar-ga4-fallback-recovery-staging`
- Operator: Todd the DevOps dude via Codex workspace
- Requested outcome: dry-run, live-run, before/after metric comparison, record-level spot checks, discrepancy triage, and production go/no-go recommendation.

## Execution Status

Blocked. The attached execution workspace cannot invoke staging:

- `gcloud --version` failed with `/bin/bash: line 1: gcloud: command not found`.
- `gcloud auth list --filter=status:ACTIVE --format=value\(account\)` failed with `/bin/bash: line 1: gcloud: command not found`.
- `gcloud run jobs list --project=roas-radar-staging --region=us-central1 --format=value\(metadata.name\)` failed with `/bin/bash: line 1: gcloud: command not found`.
- No local `.env`, staging `DATABASE_URL`, or staging `REPORTING_API_TOKEN` is present in the repository workspace. The staging environment file references Secret Manager names only.

No staging dry-run or live-run was executed from this workspace, and no staging data was modified.

Capability checks re-run from the attached workspace on 2026-05-31:

```text
$ gcloud --version
/bin/bash: line 1: gcloud: command not found

$ gcloud auth list --filter=status:ACTIVE '--format=value(account)'
/bin/bash: line 1: gcloud: command not found

$ gcloud run jobs list --project=roas-radar-staging --region=us-central1 '--format=value(metadata.name)'
/bin/bash: line 1: gcloud: command not found

$ env | rg '^(DATABASE_URL|PGHOST|PGDATABASE|PGUSER|PGPASSWORD|GCP_PROJECT_ID|GOOGLE_APPLICATION_CREDENTIALS|CLOUDSDK_CONFIG|REPORTING_API_TOKEN)='
<no output>
```

## Intended Staging Commands

Use the same window and `requested-by` value across dry and live runs so outputs are comparable.

```sh
gcloud run jobs execute roas-radar-campaign-metadata-backfill-staging \
  --project=roas-radar-staging \
  --region=us-central1 \
  --args=run,campaign-metadata:backfill:start,--,--mode,history,--start-date,2026-05-01,--end-date,2026-05-31,--requested-by,todd-devops-staging-rehearsal,--dry-run,true \
  --wait

gcloud run jobs execute roas-radar-campaign-metadata-backfill-staging \
  --project=roas-radar-staging \
  --region=us-central1 \
  --args=run,campaign-metadata:backfill:start,--,--mode,history,--start-date,2026-05-01,--end-date,2026-05-31,--requested-by,todd-devops-staging-rehearsal,--dry-run,false \
  --wait
```

```sh
gcloud run jobs execute roas-radar-order-attribution-backfill-staging \
  --project=roas-radar-staging \
  --region=us-central1 \
  --args=run,attribution:backfill-orders:start,--,--from,2026-05-01T00:00:00Z,--to,2026-05-31T00:00:00Z,--requested-by,todd-devops-staging-rehearsal,--dry-run \
  --wait

gcloud run jobs execute roas-radar-order-attribution-backfill-staging \
  --project=roas-radar-staging \
  --region=us-central1 \
  --args=run,attribution:backfill-orders:start,--,--from,2026-05-01T00:00:00Z,--to,2026-05-31T00:00:00Z,--requested-by,todd-devops-staging-rehearsal \
  --wait
```

```sh
gcloud run jobs execute roas-radar-shopify-attribution-recovery-staging \
  --project=roas-radar-staging \
  --region=us-central1 \
  --args=run,attribution:recover-shopify-hints:start,--,--from,2026-05-01T00:00:00Z,--to,2026-05-31T00:00:00Z,--requested-by,todd-devops-staging-rehearsal \
  --wait

gcloud run jobs execute roas-radar-shopify-attribution-recovery-staging \
  --project=roas-radar-staging \
  --region=us-central1 \
  --args=run,attribution:recover-shopify-hints:start,--,--from,2026-05-01T00:00:00Z,--to,2026-05-31T00:00:00Z,--requested-by,todd-devops-staging-rehearsal,--apply \
  --wait
```

```sh
gcloud run jobs execute roas-radar-ga4-fallback-recovery-staging \
  --project=roas-radar-staging \
  --region=us-central1 \
  --args=run,attribution:recover-ga4-fallback:start,--,--from,2026-05-01T00:00:00Z,--to,2026-05-31T00:00:00Z,--requested-by,todd-devops-staging-rehearsal \
  --wait

gcloud run jobs execute roas-radar-ga4-fallback-recovery-staging \
  --project=roas-radar-staging \
  --region=us-central1 \
  --args=run,attribution:recover-ga4-fallback:start,--,--from,2026-05-01T00:00:00Z,--to,2026-05-31T00:00:00Z,--requested-by,todd-devops-staging-rehearsal,--apply \
  --wait
```

## Validation Checklist

Capture before and after snapshots for the same 30-day window:

```sql
SELECT attribution_tier, attribution_status, COUNT(*) AS orders
FROM shopify_order_attribution_snapshots
WHERE processed_at >= '2026-05-01T00:00:00Z'
  AND processed_at < '2026-05-31T00:00:00Z'
GROUP BY attribution_tier, attribution_status
ORDER BY attribution_tier, attribution_status;

SELECT job_type, dry_run, status, records_discovered, records_processed,
       records_succeeded, records_failed, records_skipped,
       side_effects_attempted, side_effects_succeeded, side_effects_suppressed
FROM recovery_job_runs
WHERE initiated_by = 'todd-devops-staging-rehearsal'
ORDER BY queued_at DESC;

SELECT status, COUNT(*) AS records
FROM order_attribution_backfill_runs
WHERE requested_by = 'todd-devops-staging-rehearsal'
GROUP BY status
ORDER BY status;

SELECT COUNT(*) AS campaign_metadata_runs
FROM campaign_metadata_backfill_runs
WHERE requested_by = 'todd-devops-staging-rehearsal';
```

Record-level spot checks should include:

- At least five orders changed by Shopify hint recovery.
- At least five orders changed by GA4 fallback recovery.
- At least five orders evaluated but intentionally skipped, with skip reason retained.
- Any order touched by both recovery paths, confirming precedence remains deterministic first-party, then Shopify hint, then GA4 fallback.
- Campaign metadata rows updated during the window, confirming existing non-null campaign names and platform IDs were not overwritten by null or stale values.

Unintended overwrite checks:

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

Expected dry-run behavior:

- Dry-run recovery jobs may discover, claim, process, skip, or suppress side effects.
- Dry-run side effects must be suppressed.
- Live-run side effects must be bounded to the same records reviewed in dry-run, except where idempotency or source freshness changes explain the difference.

## Discrepancies And Remediation

| Finding | Status | Remediation |
| --- | --- | --- |
| Staging execution unavailable from workspace because `gcloud` is not installed. | Open | Run from an operator host with Google Cloud SDK installed and authenticated to `roas-radar-staging`, or install Cloud SDK in this workspace and provide staging credentials. |
| Staging DB/reporting credentials unavailable in workspace. | Open | Provide a staging-safe `DATABASE_URL` or Cloud SQL access path plus `REPORTING_API_TOKEN` for read-only validation, or run the SQL checks from an authorized operator shell. |
| Dry-run/live-run output comparison not produced. | Blocked | Execute the commands above and attach Cloud Run execution log URLs plus JSON stdout reports. |
| Record-level spot checks not completed. | Blocked | Run the validation SQL after live-run and paste the selected record IDs plus before/after fields into this report. |

## Production Go/No-Go

Recommendation: No-go for production execution as of 2026-05-31.

Reason: staging rehearsal could not be executed or validated from the attached workspace. Production should remain blocked until staging dry-run and live-run both complete, record-level spot checks pass, dry-run/live-run discrepancies are triaged, and Cloud Run execution log URLs are attached to this report.

## Sign-Off

- Operator sign-off: not signed
- Engineering sign-off: not signed
- Production approval: no-go
