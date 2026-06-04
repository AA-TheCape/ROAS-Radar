# GA4 Hourly Ingestion

Use this runbook when the GA4 session attribution Cloud Run Job is lagging, dead-lettering hours, or needs a targeted replay.

## Normal schedule

- Cloud Scheduler runs the GA4 ingestion job hourly.
- The default schedule is `5 * * * *`.
- The Cloud Run Job executes `npm run ga4:ingest:start`.
- Hour-level retries and dead-lettering happen inside the application, so the Cloud Run Job itself should stay at retry count `0`.

## Quick verification

Confirm the scheduler and job contract first:

```bash
gcloud run jobs describe roas-radar-ga4-session-attribution-staging --region us-central1
gcloud scheduler jobs describe roas-radar-ga4-session-attribution-scheduler-staging --location us-central1
```

The expected deploy shape is:

- Cloud Run Job command: `npm`
- Cloud Run Job args: `run,ga4:ingest:start`
- Cloud Run Job retries: `0`
- Scheduler target: Run Jobs API `.../jobs/roas-radar-ga4-session-attribution-staging:run`
- Scheduler frequency: hourly, default `5 * * * *`

Then execute the job and inspect logs:

```bash
gcloud run jobs execute roas-radar-ga4-session-attribution-staging --region us-central1 --wait
gcloud logging read 'jsonPayload.event="ga4_session_attribution_worker_started"' --limit 20
```

Also inspect failure and progress signals when the execution does not behave as expected:

```bash
gcloud logging read 'jsonPayload.event="ga4_session_attribution_worker_failed"' --limit 20
gcloud logging read 'jsonPayload.event="ga4_hour_dead_lettered"' --limit 20
```

Verify the queue and dead-letter tables directly when logs are not enough:

```sql
SELECT pipeline_name, hour_start, status, attempts, available_at, locked_by, dead_lettered_at
FROM ga4_bigquery_hourly_jobs
ORDER BY hour_start DESC
LIMIT 50;
```

```sql
SELECT source_table, source_record_id, source_queue_key, status, failure_count, last_error_message
FROM event_dead_letters
WHERE source_table = 'ga4_bigquery_hourly_jobs'
ORDER BY updated_at DESC
LIMIT 50;
```

## Normalized schema mapping

The hourly worker reads GA4 daily and intraday export rows for one complete UTC hour, rolls them up to session grain, and writes two internal stores in one database transaction:

| GA4 source | Internal field |
| --- | --- |
| `user_pseudo_id` + `ga_session_id` event param | `ga4_session_key`, `ga4_client_id`, `ga4_session_id` |
| `user_id`, falling back to `user_pseudo_id` | `ga4_user_key` |
| min/max `event_timestamp` in the session | `session_started_at`, `last_event_at`, fallback `occurred_at` |
| `collected_traffic_source.manual_*` and matching `utm_*` event params | canonical `source`, `medium`, `campaign_id`, `campaign`, `content`, `term` |
| supported click ID params (`gclid`, `dclid`, `gbraid`, `wbraid`, `fbclid`, `ttclid`, `msclkid`) | `click_id_type`, `click_id_value` |
| `ecommerce.transaction_id` or `transaction_id`/`order_id` event params | fallback `transaction_id` lookup |
| `email_sha256`, `email_hash`, or `hashed_email` event params | fallback `email_hash` lookup |
| `customer_identity_id` or `roas_radar_customer_identity_id` event params | fallback `customer_identity_id` lookup |
| linked GA4 Google Ads campaign fields, enriched from the configured transfer table when possible | `account_id`, `account_name`, `channel_type`, `channel_subtype`, metadata source tags |
| export family and hour | `source_dataset`, `source_table_type`, `source_export_hour` |

Rows are source-tagged through `campaign_metadata_source`, `account_metadata_source`, and `channel_metadata_source` as `google_ads_transfer`, `ga4_raw`, or `unresolved`. The same normalized rows are also upserted into `ga4_fallback_candidates`; fallback lookup only returns candidates with required session fields and at least one attribution signal.

## Targeted replay

Replay dead letters after the underlying cause is fixed:

```bash
npm run dead-letters:replay
```

To force a specific inclusive hour window, execute the worker with explicit bounds:

```bash
GA4_INGESTION_START_HOUR=2026-04-27T08:00:00.000Z \
GA4_INGESTION_END_HOUR=2026-04-27T10:00:00.000Z \
npm run ga4:ingest
```

The worker normalizes both values to whole UTC hours and processes every hour in the range.

For Cloud Run parity, keep production and staging replays on `npm run ga4:ingest:start` unless you explicitly need a local developer replay with `tsx`.

## Common failures

- `Invalid GA4_INGESTION_* value`: the scheduler or job env file contains a non-positive integer or malformed timestamp.
- `GA4 BigQuery ingestion is disabled`: `GA4_BIGQUERY_ENABLED` is false or missing in the job environment.
- repeated BigQuery transport failures: confirm the job service account still has BigQuery access and that the dataset location matches the configured executor location.
- stale job locks: verify `GA4_INGESTION_STALE_LOCK_MINUTES` and check whether an older execution is still running.
- scheduler executes but no useful work is claimed: verify `GA4_INGESTION_REQUESTED_BY`, `GA4_INGESTION_BATCH_SIZE`, and the BigQuery enablement flag in the environment file that `deploy.sh` loaded.

## Rollback

1. Pause the Cloud Scheduler job.
2. Redeploy the last known-good image tag for the GA4 ingestion job.
3. Re-run one controlled hour range before resuming the scheduler.

If the worker itself is healthy but live attribution behavior is questionable, continue in `docs/runbooks/ga4-fallback-rollout.md`.
