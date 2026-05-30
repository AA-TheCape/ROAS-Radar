# Meta Deterministic Ingestion Runbook

Use this runbook for the Meta deterministic view/impression Cloud Run Job, `roas-radar-meta-deterministic-sync-*`.

## Triggers

- sustained `meta_ads_deterministic_worker_failed` or `meta_ads_deterministic_sync_job_failed` log events
- flatlined `roas_meta_deterministic_rows_fetched`, `roas_meta_deterministic_raw_rows_upserted`, or `roas_meta_deterministic_aggregate_rows_upserted`
- `meta_ads_deterministic_api_reconciliation` data-quality mismatches

## Immediate Checks

1. Check the scheduler state: `sh infra/cloud-run/scheduler.sh <environment> meta-deterministic status`.
2. Review Cloud Logging for `jsonPayload.event=("meta_ads_deterministic_worker_started" OR "meta_ads_deterministic_sync_job_completed" OR "meta_ads_deterministic_sync_job_failed")`.
3. Confirm the job service account is `META_ADS_DETERMINISTIC_JOB_SERVICE_ACCOUNT_NAME` and that it can read only `DATABASE_URL`, `META_ADS_APP_SECRET`, and `META_ADS_ENCRYPTION_KEY`.
4. Inspect whether failures are isolated to one ad account, date, entity type, or event type before widening backfill windows.

## Rollout Validation

Run this checklist before enabling deterministic view/impression sync for a new environment, after production promotion, and after any schema or attribution processing change that touches deterministic view/impression inputs.

### Cloud Run Job Verification

1. Confirm the scheduler target is active:
   `sh infra/cloud-run/scheduler.sh <environment> meta-deterministic status`
2. Confirm the deployed job uses the expected service account, command, env vars, timeout, retry count, and Cloud SQL attachment:
   `gcloud run jobs describe roas-radar-meta-deterministic-sync-<environment> --project <project> --region <region>`
3. Confirm the latest execution succeeded:
   `gcloud run jobs executions list --job roas-radar-meta-deterministic-sync-<environment> --project <project> --region <region> --limit 5`
4. Execute a bounded manual run when promoting or recovering:
   `gcloud run jobs execute roas-radar-meta-deterministic-sync-<environment> --project <project> --region <region> --wait`
5. Verify logs from the same run include `meta_ads_deterministic_worker_started`, at least one `meta_ads_deterministic_sync_job_completed` when enabled connections exist, and `meta_ads_deterministic_worker_completed`. Treat `meta_ads_deterministic_worker_skipped` as healthy only when `META_ADS_DETERMINISTIC_SYNC_ENABLED="false"` was intentionally deployed.

### Database Constraint Checks

Run the rollout verifier from a machine with database access:

```sh
DATABASE_URL="<database-url>" npm run ops:verify-meta-deterministic-rollout
```

The verifier checks the deterministic Meta API provenance constraints, verified-evidence constraints, aggregate traceability constraints, model output separation, queue health, row counts, freshness, quarantine rate, and the latest reconciliation run.

Optional thresholds:

- `META_DETERMINISTIC_FRESHNESS_HOURS`: maximum age for the latest completed sync job. Default: `30`.
- `META_DETERMINISTIC_ROLLOUT_WINDOW_HOURS`: lookback window for row counts, unhealthy jobs, and quarantine rate. Default: `48`.
- `META_DETERMINISTIC_MAX_QUARANTINE_RATE`: maximum quarantined share of accepted plus quarantined rows. Default: `0.25`.
- `META_DETERMINISTIC_STALE_PROCESSING_HOURS`: maximum age for a locked `processing` job before it is treated as stale. Default: `2`.

### Row Count And Freshness Checks

Use these SQL checks when you need a manual audit trail in rollout notes.

```sql
SELECT
  status,
  COUNT(*) AS jobs,
  MAX(completed_at) AS latest_completed_at
FROM meta_ads_deterministic_sync_jobs
WHERE created_at >= now() - interval '48 hours'
GROUP BY status
ORDER BY status;

SELECT
  'raw' AS surface,
  COUNT(*) AS rows,
  MAX(ingested_at_utc) AS latest_write
FROM raw_deterministic_events
WHERE platform = 'meta_ads'
UNION ALL
SELECT
  'facts',
  COUNT(*),
  MAX(normalized_at_utc)
FROM deterministic_event_facts
WHERE platform = 'meta_ads'
UNION ALL
SELECT
  'aggregates',
  COUNT(*),
  MAX(created_at)
FROM meta_ads_deterministic_attribution_aggregates
UNION ALL
SELECT
  'model_outputs',
  COUNT(*),
  MAX(generated_at_utc)
FROM deterministic_model_outputs
WHERE platform = 'meta_ads';

SELECT
  report_date,
  event_type,
  attribution_family,
  SUM(aggregate_count) AS aggregate_count,
  COUNT(*) AS aggregate_rows
FROM meta_ads_deterministic_attribution_aggregates
WHERE report_date >= current_date - interval '7 days'
  AND platform_verified = true
  AND verification_status = 'verified'
GROUP BY report_date, event_type, attribution_family
ORDER BY report_date DESC, event_type, attribution_family;
```

Expected rollout evidence:

- at least one completed job for every enabled active connection inside the freshness threshold
- recent raw, fact, and aggregate writes when enabled accounts have Meta delivery in the window
- aggregate rows retain `attribution_window='7d_view'`, `attribution_window_days=7`, `evidence_origin='api'`, and verified raw traceability metadata
- deterministic model output rows, when attribution processing is enabled, stay in `deterministic_model_outputs` and do not change primary attribution winner fields

### Quarantine Monitoring

Use quarantine to confirm the rollout fails closed instead of accepting unverified rows.

```sql
SELECT
  reason_code,
  COUNT(*) AS quarantined_rows,
  MIN(quarantined_at_utc) AS first_seen,
  MAX(quarantined_at_utc) AS last_seen
FROM deterministic_event_evidence_quarantine
WHERE platform = 'meta_ads'
  AND quarantined_at_utc >= now() - interval '48 hours'
GROUP BY reason_code
ORDER BY quarantined_rows DESC, reason_code;

SELECT
  q.account_id,
  q.event_type,
  q.event_date,
  q.reason_code,
  q.reason_detail,
  q.source_id,
  q.dedupe_key,
  q.quarantined_at_utc
FROM deterministic_event_evidence_quarantine q
WHERE q.platform = 'meta_ads'
  AND q.quarantined_at_utc >= now() - interval '48 hours'
ORDER BY q.quarantined_at_utc DESC
LIMIT 50;
```

If quarantine spikes above the rollout threshold, keep the scheduler paused or disable extraction until the dominant reason is understood. Common rollback-worthy reasons are missing API provenance, non-API evidence, missing account or entity identifiers, negative or unparsable metrics, and duplicate or unstable dedupe keys.

### Reconciliation Checks

Confirm data quality has compared API pull summaries against persisted rows:

```sql
SELECT
  run_date,
  status,
  checked_at,
  compared_scope_count,
  mismatch_count,
  total_api_expected_count,
  total_raw_ingested_count,
  total_fact_count
FROM meta_ads_deterministic_reconciliation_runs
ORDER BY checked_at DESC
LIMIT 5;

SELECT *
FROM meta_ads_deterministic_reconciliation_investigation
WHERE checked_at >= now() - interval '48 hours'
ORDER BY checked_at DESC, absolute_delta DESC
LIMIT 50;
```

Do not proceed with rollout if the latest reconciliation is `failed` for the promoted date range unless the mismatch is explained and accepted in the incident or release notes.

## Restart

1. If a scheduler invocation failed transiently, rerun the job:
   `gcloud run jobs execute roas-radar-meta-deterministic-sync-<environment> --project <project> --region <region> --wait`
2. If failures continue, pause the scheduler:
   `sh infra/cloud-run/scheduler.sh <environment> meta-deterministic pause`
3. Resume only after a manual job execution completes and emits `meta_ads_deterministic_worker_completed`.

## Backfill

1. Set `META_ADS_DETERMINISTIC_SYNC_LOOKBACK_DAYS` to the required bounded window in the target environment file.
2. Rerun `sh infra/cloud-run/deploy.sh <environment>` so the Cloud Run Job picks up the backfill window.
3. Execute the deterministic job manually and wait for completion.
4. Restore the normal lookback value and redeploy before resuming the scheduler.

Keep `META_ADS_DETERMINISTIC_SYNC_INITIAL_LOOKBACK_DAYS` at the wider initial planning window for new account/entity checkpoints. Use `META_ADS_DETERMINISTIC_SYNC_ENABLED="false"` only as an emergency kill switch when the job should stay deployed but do no extraction.

## Rollback And Disablement

Use the narrowest rollback that stops the faulty surface while preserving unrelated ingestion.

1. Pause scheduling first when a rollout is unhealthy but no bad rows are being actively consumed:
   `sh infra/cloud-run/scheduler.sh <environment> meta-deterministic pause`
2. Stop extraction while leaving the Cloud Run Job deployed by setting `META_ADS_DETERMINISTIC_SYNC_ENABLED="false"` in `infra/cloud-run/environments/<environment>.env`, then redeploy:
   `sh infra/cloud-run/deploy.sh <environment>`
3. Disable deterministic sync for affected Meta connections when only a subset of accounts is unhealthy:
   `UPDATE meta_ads_connections SET deterministic_view_impression_sync_enabled = false WHERE id = <connection_id>;`
4. Stop attribution processing from producing deterministic view/impression model outputs by submitting attribution runs without `run_metadata.deterministicViewImpressionAttributionEnabled=true`. Existing primary attribution models continue to run and must not read `deterministic_model_outputs`.
5. If deployed code or infrastructure must be reverted, use the Cloud Run deploy metadata:
   `sh infra/cloud-run/rollback.sh <environment> <deploy-metadata-file> previous`
6. Keep quarantined rows in place for triage. Replay or delete only after application engineering confirms the validation failure and raw Meta traceability are corrected.
7. Resume scheduling only after `npm run ops:verify-meta-deterministic-rollout` passes and a manual Cloud Run Job execution emits `meta_ads_deterministic_worker_completed`.

After rollback, capture the affected connection ids, job ids, sync dates, top quarantine reason codes, latest reconciliation status, and whether attribution processing was disabled.

## Escalation

- Escalate to application engineering if the job repeatedly fails for the same planned date or cannot persist raw deterministic evidence.
- Escalate to the ads owner if Meta returns permission errors or empty deterministic view/impression fields for active campaigns.
