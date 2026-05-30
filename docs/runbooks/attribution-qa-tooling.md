# Attribution QA Tooling

Use this runbook when Attribution QA payloads, admin debug lookups, raw evidence retention, or snapshot write alerts need support.

## Support Surface

- Public payload endpoint: `GET /api/attribution/orders/{orderId}/qa-payload`
- Admin debug endpoint: `GET /api/admin/attribution/orders/{orderId}/qa-debug`
- Dashboard view: Attribution QA tooling
- Payload contract: `docs/attribution-qa-payload-schema-v1.md`
- Retention job: `attribution-qa:retention`
- Cloud Run scheduler target: `attribution-qa-retention`

Attribution QA writes are part of the live attribution worker, order backfill, and attribution run executor paths. They are not gated by a separate feature flag in the implemented code. The operational controls are the deployed service revision, the `attribution-qa-retention` scheduler, and the retention variables `ATTRIBUTION_QA_RETENTION_DAYS`, `ATTRIBUTION_QA_RETENTION_BATCH_SIZE`, and `ATTRIBUTION_QA_RETENTION_MAX_BATCHES`.

## Alerts And Metrics

Start here for the `ROAS Radar * Attribution QA Snapshot Write Failures` alert.

Primary log events:

- `attribution_qa_snapshot_write`: emitted by realtime and backfill attribution paths. Filter by `jsonPayload.order_id`, `jsonPayload.pipeline`, and `jsonPayload.status`.
- `attribution_qa_payload_fetch`: emitted by public and admin fetch paths. Filter by `jsonPayload.endpoint`, `jsonPayload.statusCode`, `jsonPayload.source`, and `jsonPayload.order_id`.
- `attribution_qa_retention_batch_completed`: emitted by the retention job with deleted raw evidence and pruned embedded snapshot counts.
- `attribution_qa_retention_completed`: emitted after a full retention run.
- `attribution_qa_retention_failed`: emitted when the retention worker fails.

Monitoring metrics:

- `logging.googleapis.com/user/roas_attribution_qa_snapshot_writes`
- `logging.googleapis.com/user/roas_attribution_qa_fetch_latency_ms`
- `logging.googleapis.com/user/roas_attribution_qa_evidence_size_bytes`
- `logging.googleapis.com/user/roas_attribution_qa_cleanup_deletions`

## Snapshot Write Failures

1. Open logs for `attribution_qa_snapshot_write` with `jsonPayload.status="failure"` over the alert window.
2. Group failures by `jsonPayload.pipeline` and `jsonPayload.order_id`.
3. Check whether the same orders also have attribution worker, order backfill, or run executor errors in the same window.
4. For a representative order, request `GET /api/admin/attribution/orders/{orderId}/qa-debug`.
5. If the response has `source: persisted_snapshot`, compare the payload against `docs/attribution-qa-payload-schema-v1.md`.
6. If the response has `source: generated_on_read`, check whether the persisted `qaSnapshot` was absent, expired, or pruned.
7. If the endpoint returns `404 shopify_order_not_found`, verify the order exists in Shopify ingestion before treating the QA surface as the failing component.
8. If failures are limited to raw evidence insert or snapshot persistence, keep attribution worker health checks separate from QA payload availability. The long-retention operational attribution summary remains in `shopify_orders.attribution_snapshot`.

## Payload Fetch Failures

1. Inspect `attribution_qa_payload_fetch` logs by `endpoint`:
   - `public_qa_payload`
   - `admin_qa_debug`
2. For public payload failures, confirm the order id maps to an ingested Shopify order.
3. For admin debug failures, confirm the caller is an authenticated internal admin user. Internal service tokens are intentionally rejected for this endpoint.
4. If `rawEvidence` is `expired_or_pruned`, verify the order age against `ATTRIBUTION_QA_RETENTION_DAYS`.
5. If fetch latency p95 is high, compare `rawEvidenceCount`, `rawEvidenceSizeBytes`, and `roas_attribution_qa_evidence_size_bytes` to identify oversized raw evidence payloads.

## Retention Operations

Check scheduler status:

```bash
sh infra/cloud-run/scheduler.sh <environment> attribution-qa-retention status
```

Pause retention cleanup:

```bash
sh infra/cloud-run/scheduler.sh <environment> attribution-qa-retention pause
```

Resume retention cleanup:

```bash
sh infra/cloud-run/scheduler.sh <environment> attribution-qa-retention resume
```

The retention job deletes expired rows from `attribution_raw_evidence` and removes only the `qaSnapshot` key from `shopify_orders.attribution_snapshot`. It does not remove operational attribution summary fields.

## Rollback

1. If the issue is only retention cleanup, pause the scheduler:
   `sh infra/cloud-run/scheduler.sh <environment> attribution-qa-retention pause`
2. If retention is too aggressive, increase `ATTRIBUTION_QA_RETENTION_DAYS` in the target environment file and redeploy with `sh infra/cloud-run/deploy.sh <environment>`.
3. If cleanup batches are causing database pressure, reduce `ATTRIBUTION_QA_RETENTION_BATCH_SIZE` or `ATTRIBUTION_QA_RETENTION_MAX_BATCHES` and redeploy.
4. If the API, dashboard, or attribution worker revision introduced the regression, use `sh infra/cloud-run/rollback.sh <environment> <deploy-metadata-file> previous`.
5. After rollback, confirm new `attribution_qa_snapshot_write` logs are successful and `GET /api/admin/attribution/orders/{orderId}/qa-debug` works for a known recent order.
6. Resume the retention scheduler only after the root cause is fixed and the retention window has been reviewed against `ATTRIBUTION_QA_RETENTION_DAYS`.

## Release Gate

Before publishing or enabling a release that changes Attribution QA support:

- Confirm `ATTRIBUTION_QA_RETENTION_DAYS`, `ATTRIBUTION_QA_RETENTION_BATCH_SIZE`, and `ATTRIBUTION_QA_RETENTION_MAX_BATCHES` match the deployed environment files.
- Confirm there is no documented QA feature flag beyond the implemented retention controls.
- Confirm rollback steps name `attribution-qa-retention`, not the broader session retention job.
- Confirm the alert template links to this runbook.
