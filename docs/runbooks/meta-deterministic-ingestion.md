# Meta Deterministic Ingestion Runbook

Use this runbook for the Meta deterministic view/impression Cloud Run Job, `roas-radar-meta-deterministic-sync-*`.

Related references:

- `docs/meta-deterministic-view-attribution-contract-v1.md` for the approved API-only, aggregate-only attribution contract
- `docs/deterministic-attribution-behavior.md` for analyst-facing reporting behavior
- `docs/runbooks/cloud-run-pipelines.md` for Cloud Run deploy, scheduler, IAM, and rollback commands

## Triggers

- sustained `meta_ads_deterministic_worker_failed` or `meta_ads_deterministic_sync_job_failed` log events
- flatlined `roas_meta_deterministic_rows_fetched`, `roas_meta_deterministic_raw_rows_upserted`, or `roas_meta_deterministic_aggregate_rows_upserted`
- `meta_ads_deterministic_api_reconciliation` data-quality mismatches
- elevated `roas_meta_deterministic_verification_rejection_rate`
- stale deterministic view model output reported by `combined_report_api_health`

## Immediate Checks

1. Check the scheduler state: `sh infra/cloud-run/scheduler.sh <environment> meta-deterministic status`.
2. Review Cloud Logging for `jsonPayload.event=("meta_ads_deterministic_worker_started" OR "meta_ads_deterministic_sync_job_completed" OR "meta_ads_deterministic_sync_job_failed")`.
3. Confirm the job service account is `META_ADS_DETERMINISTIC_JOB_SERVICE_ACCOUNT_NAME` and that it can read only `DATABASE_URL`, `META_ADS_APP_SECRET`, and `META_ADS_ENCRYPTION_KEY`.
4. Inspect whether failures are isolated to one ad account, date, entity type, or event type before widening backfill windows.

## Sync Operations

The scheduled sync plans work for Meta connections where `deterministic_view_impression_sync_enabled = true`. For each enabled active connection, the planner inserts bounded `meta_ads_deterministic_sync_jobs` by account/date scope, using checkpoints for steady-state planning and the initial lookback for first-run planning.

Normal operating rules:

- keep the scheduler enabled only when the latest rollout verifier passes and recent jobs complete inside the freshness target
- use per-connection enablement for account-level rollout; do not enable every account during an incident
- treat `META_ADS_DETERMINISTIC_SYNC_LOOKBACK_DAYS` as the routine catch-up window and keep it small enough for one scheduled job to finish predictably
- treat `META_ADS_DETERMINISTIC_SYNC_INITIAL_LOOKBACK_DAYS` as the onboarding window for accounts without checkpoints
- do not manually edit checkpoints unless application engineering confirms the affected job/date scope and replay behavior

Useful operational SQL:

```sql
SELECT
  c.id AS connection_id,
  c.ad_account_id,
  c.status,
  c.deterministic_view_impression_sync_enabled,
  c.deterministic_view_impression_last_planned_for,
  cp.last_completed_date
FROM meta_ads_connections c
LEFT JOIN meta_ads_deterministic_sync_checkpoints cp
  ON cp.connection_id = c.id
WHERE c.status = 'active'
ORDER BY c.deterministic_view_impression_sync_enabled DESC, c.id;

SELECT
  j.id,
  j.connection_id,
  c.ad_account_id,
  j.sync_date,
  j.status,
  j.requested_by,
  j.attempts,
  j.available_at,
  j.locked_at,
  j.locked_by,
  j.completed_at,
  j.last_error
FROM meta_ads_deterministic_sync_jobs j
JOIN meta_ads_connections c ON c.id = j.connection_id
WHERE j.created_at >= now() - interval '48 hours'
ORDER BY j.created_at DESC
LIMIT 100;
```

Healthy sync characteristics:

- scheduled runs emit one `meta_ads_deterministic_worker_started` and one `meta_ads_deterministic_worker_completed`
- enabled accounts with Meta delivery emit `meta_ads_deterministic_sync_job_completed`
- each completed job records non-negative fetched, raw-upserted, fact-upserted, aggregate-upserted, accepted, and quarantined counts
- completed jobs advance checkpoints only after accepted persistence and verification have finished
- skipped workers occur only when `META_ADS_DETERMINISTIC_SYNC_ENABLED="false"` is intentionally deployed

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

### Verification Criteria

Use these criteria before declaring a rollout, recovery, or backfill healthy:

- Scheduler: `meta-deterministic status` shows the expected enabled or paused state for the environment.
- Execution: the latest manual or scheduled Cloud Run execution exits successfully and has no unhandled worker failure log.
- Job scope: every enabled active connection has a completed job for the intended date window, or a documented Meta delivery/permission reason for no rows.
- API provenance: accepted aggregates have `evidence_origin = 'api'`, `platform_verified = true`, `verification_status = 'verified'`, and a retained raw source reference.
- Window: accepted aggregates use `attribution_window = '7d_view'` and `attribution_window_days = 7`.
- Quarantine: quarantine rate is below `META_DETERMINISTIC_MAX_QUARANTINE_RATE`, and the top reasons are understood.
- Reconciliation: the latest `meta_ads_deterministic_reconciliation_runs` row for the checked date range is not `failed`, unless the mismatch has an accepted incident note.
- Separation: deterministic view model outputs are present only in `deterministic_model_outputs`; canonical click attribution tables and Shopify writeback fields are unchanged by this sync.

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

## Anomaly Triage

Use this sequence when metrics flatline, rejection rate rises, reconciliation fails, or reporting shows stale deterministic view outputs.

1. Determine whether the anomaly is ingestion, verification, reconciliation, or reporting freshness.
2. Scope by `adAccountId`, `connectionId`, `sync_date`, `event_type`, `entity_type`, and `batchId` before changing scheduler or lookback settings.
3. Compare Meta API request failures against completed job counts:
   - `meta_ads_api_request_failed` with `transactionSource="meta_ads_deterministic_insights"` means upstream API or permission triage.
   - `meta_ads_deterministic_sync_job_failed` after successful API calls means persistence, validation, or DB triage.
4. Inspect `meta_ads_deterministic_verification_summary` for rejection reasons. Missing raw references, non-API evidence, negative metrics, or duplicate dedupe keys are fail-closed conditions.
5. Inspect quarantine samples before replaying. Do not delete quarantined rows to make dashboards look healthy.
6. If reconciliation fails, compare `total_api_expected_count`, `total_raw_ingested_count`, and `total_fact_count` to identify whether the gap is before raw persistence, between raw and normalized facts, or between facts and aggregates.
7. If reporting is stale but ingestion is healthy, inspect `combined_report_api_health` and the attribution model materialization path. Do not rerun ingestion repeatedly when only model output freshness is stale.

Common patterns:

- Zero fetched rows for every active account: check Meta permissions, token validity, app credentials, and requested fields.
- Zero fetched rows for one account: check account-level Meta delivery and `deterministic_view_impression_sync_enabled`.
- Raw rows present but aggregate rows flatline: inspect verification and quarantine reasons.
- Aggregates present but Deterministic Views are missing in reporting: inspect `deterministic_model_outputs` freshness and attribution run metadata.
- Click attribution changed after deterministic sync: treat as a regression and escalate; deterministic view/impression sync must not mutate canonical click winners.

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

Use backfill only for a bounded account/date recovery window. Pause the scheduler before widening lookback values so the scheduled cadence does not repeatedly enqueue the same recovery window.

1. Identify the affected connection ids, account ids, and inclusive date range.
2. Pause the scheduler:
   `sh infra/cloud-run/scheduler.sh <environment> meta-deterministic pause`
3. Set `META_ADS_DETERMINISTIC_SYNC_LOOKBACK_DAYS` to the required bounded window in the target environment file. Keep the value no wider than the incident window plus the 7-day deterministic attribution window unless application engineering approves a larger replay.
4. If only a subset of accounts is affected, keep unrelated connections disabled for deterministic sync until the backfill completes:
   `UPDATE meta_ads_connections SET deterministic_view_impression_sync_enabled = false WHERE id NOT IN (<affected_connection_ids>);`
5. Rerun `sh infra/cloud-run/deploy.sh <environment>` so the Cloud Run Job picks up the backfill window.
6. Execute the deterministic job manually and wait for completion:
   `gcloud run jobs execute roas-radar-meta-deterministic-sync-<environment> --project <project> --region <region> --wait`
7. Run the rollout verifier and the row-count, quarantine, and reconciliation checks in this runbook.
8. Restore the normal lookback value, restore per-connection enablement, redeploy, and resume the scheduler only after verification passes.

Keep `META_ADS_DETERMINISTIC_SYNC_INITIAL_LOOKBACK_DAYS` at the wider initial planning window for new account/entity checkpoints. Use `META_ADS_DETERMINISTIC_SYNC_ENABLED="false"` only as an emergency kill switch when the job should stay deployed but do no extraction.

Bounded backfill rules:

- do not increase `META_ADS_DETERMINISTIC_SYNC_INITIAL_LOOKBACK_DAYS` for incident replay of existing checkpoints
- do not run open-ended historical replays from production scheduler cadence
- record the temporary lookback, affected connection ids, requested date range, Cloud Run execution id, verifier result, and reconciliation status in the incident notes
- restore the steady-state environment values before closing the incident

## Emergency Controls

Use the narrowest control that stops the bad behavior.

| Control | Use when | Effect |
| --- | --- | --- |
| Pause scheduler | jobs are unhealthy or backfill is in progress | stops new scheduled invocations but keeps the job runnable manually |
| `META_ADS_DETERMINISTIC_SYNC_ENABLED="false"` | extraction must stop globally | deployed job emits `meta_ads_deterministic_worker_skipped` and does no extraction |
| per-connection disable | one account or token is unhealthy | excludes affected connection from planning while other accounts can continue |
| attribution run metadata without deterministic view flag | model output materialization is bad | click attribution continues while deterministic view outputs stop updating |
| Cloud Run rollback | deployed job or infra change is faulty | returns job configuration/code to previous deploy metadata |

Emergency criteria:

- API failures are repeated and broad across accounts
- verification rejects accepted-looking rows for API provenance or traceability reasons
- reconciliation shows persisted counts diverging from Meta API pull summaries
- deterministic view outputs appear in canonical click attribution or Shopify writeback surfaces
- backfill volume threatens job timeout, database stability, or reporting freshness

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

## QA Checks

Run these checks after rollout, backfill, rollback, or any reporting change that touches deterministic view/impression inputs.

```sh
DATABASE_URL="<database-url>" npm run ops:verify-meta-deterministic-rollout
```

Manual QA checklist:

- Cloud Run execution logs show worker start, job completion for enabled accounts, verification summary, and worker completion.
- No unexpected `meta_ads_deterministic_worker_skipped` event appears while sync is meant to be enabled.
- Quarantine samples have known reason codes and are not used by aggregate or reporting queries.
- Reconciliation for the checked date range is passed or documented.
- `GET /api/reporting/summary` without `reportingMode` returns canonical Clicks totals with `totalsCanonical=true`.
- `reportingMode=deterministic_views` returns only the deterministic view layer with `totalsCanonical=false`.
- `reportingMode=meta_view_through&source=meta` reads Meta view-through aggregates, not deterministic model outputs.
- `reportingMode=combined` returns comparison-only Clicks plus Deterministic Views with `totalsCanonical=false`.
- `comparisonTotals.combined` is labeled non-canonical in API/dashboard output.
- Primary attribution winners, `attribution_results`, `attribution_order_credits` click models, and Shopify writeback fields do not change solely because Meta deterministic ingestion ran.

## Escalation

- Escalate to application engineering if the job repeatedly fails for the same planned date or cannot persist raw deterministic evidence.
- Escalate to the ads owner if Meta returns permission errors or empty deterministic view/impression fields for active campaigns.
- Escalate immediately if deterministic view/impression data appears to overwrite canonical click attribution, Shopify writeback, or primary order winner fields.
