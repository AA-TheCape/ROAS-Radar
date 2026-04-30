# Meta Order Value Ingestion Runbook

## Triggers

- `ROAS Radar * Meta Order Value Sync Failures`
- `ROAS Radar * Meta Order Value Zero Ingestion`
- `ROAS Radar * Meta Order Value Null Spikes`
- sustained `meta_ads_order_value_sync_connection_failed`, `meta_ads_order_value_sync_anomaly`, or `meta_ads_api_request_failed` log events

## Immediate Checks

1. Open the observability dashboard and review the Meta tiles for API latency, rows fetched, rows upserted, and sync anomalies.
2. Filter Cloud Logging on `jsonPayload.event=("meta_ads_order_value_sync_connection_completed" OR "meta_ads_order_value_sync_connection_failed" OR "meta_ads_order_value_sync_anomaly")`.
3. Confirm whether the issue is isolated to one `jsonPayload.adAccountId`, one `jsonPayload.triggerSource`, or all Meta order-value sync runs.
4. Inspect `jsonPayload.apiRequestErrorCount`, `jsonPayload.apiRequestRetryCount`, `jsonPayload.anomalyTypes`, and any anomaly `jsonPayload.details` payloads before retrying jobs.

## Likely Causes

- expired or revoked Meta access token causing 401 or 403 responses
- Meta Insights latency, throttling, or 5xx responses causing repeated retries
- `action_values` or `actions` disappearing from the upstream payload for the active reporting window
- account attribution-setting or conversion-reporting changes producing a sudden rise in null canonical metrics
- genuinely quiet traffic causing temporary zero-row pulls for a narrow window

## Remediation

1. If failures are token-related, refresh the Meta connection from the admin surface and verify the account still has `ads_read`.
2. If API latency or request failures spike, inspect recent `meta_ads_api_request_failed` logs by `transactionSource` and compare with Meta platform status before widening retries.
3. If zero-row pulls persist for more than two scheduled windows, compare the raw API payload in `meta_ads_order_value_raw_records` and the transaction audit table to confirm whether Meta returned empty data or normalization dropped rows.
4. If null-canonical spike alerts fire, inspect the raw rows for missing `action_values`, missing `actions`, or changed purchase-like action types before changing the allowed action-type set.
5. After remediation, trigger a manual `/api/admin/meta-ads/sync` window or rerun the Cloud Run job to repopulate the affected dates.

## Escalation

- Escalate to application engineering if repeated sync failures continue for more than 30 minutes.
- Escalate to the ads owner if Meta returns persistent zero rows or materially different purchase action types after a campaign or attribution-setting change.
- Escalate to database operations only after confirming the upstream Meta request succeeded and rows still failed to persist.
