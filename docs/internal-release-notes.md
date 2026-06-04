# Internal Release Notes

## Attribution QA Support Tooling

### Summary

Attribution QA support tooling is ready for operator use. The release adds documented support paths for per-order QA payloads, admin debug responses, raw evidence retention, and snapshot write monitoring.

### Operator Impact

- Use `GET /api/attribution/orders/{orderId}/qa-payload` for the sanitized per-order QA payload.
- Use `GET /api/admin/attribution/orders/{orderId}/qa-debug` for authenticated internal admin debugging with raw evidence state.
- Use `docs/runbooks/attribution-qa-tooling.md` for snapshot write alerts, payload fetch issues, and retention cleanup support.
- Watch the Attribution QA dashboard widgets backed by `roas_attribution_qa_snapshot_writes`, `roas_attribution_qa_fetch_latency_ms`, `roas_attribution_qa_evidence_size_bytes`, and `roas_attribution_qa_cleanup_deletions`.

### Rollback

- Pause only Attribution QA cleanup with `sh infra/cloud-run/scheduler.sh <environment> attribution-qa-retention pause`.
- Adjust retention using `ATTRIBUTION_QA_RETENTION_DAYS`, `ATTRIBUTION_QA_RETENTION_BATCH_SIZE`, and `ATTRIBUTION_QA_RETENTION_MAX_BATCHES`, then redeploy the target environment.
- Roll back the affected Cloud Run revision with `sh infra/cloud-run/rollback.sh <environment> <deploy-metadata-file> previous` if the API, dashboard, or attribution worker revision caused the regression.

### Feature Flag Check

No separate Attribution QA feature flag exists in the implemented code. The documented operational controls match the code and deployment scripts: the `attribution-qa-retention` scheduler plus `ATTRIBUTION_QA_RETENTION_DAYS`, `ATTRIBUTION_QA_RETENTION_BATCH_SIZE`, and `ATTRIBUTION_QA_RETENTION_MAX_BATCHES`.

### Dependency Audit

Production dependency audit findings were remediated before release. The backend audit findings were in existing runtime dependency chains, not new Attribution QA-specific package dependencies: Express/body-parser/qs came from the API framework path, and the uuid finding came through `@google-cloud/bigquery`/Google transport packages used by GA4 BigQuery ingestion. The release updates Express to `4.22.2` and `@google-cloud/bigquery` to `8.3.1`, which resolves the qs DoS advisory, the transitive uuid advisory, and the related `@tootallnate/once` finding. No production audit risk is accepted as of the final check: `npm audit --omit=dev` is clean for the backend and dashboard package roots.
