# Ingestion Failures Runbook

## Triggers

- `ROAS Radar * Ingestion Errors` alert
- sustained `tracking_ingest_rejected`, `tracking_ingest_failed`, `shopify_webhook_rejected`, or `shopify_webhook_failed` log events

## Immediate Checks

1. Open the pipeline dashboard and confirm whether the failures are isolated to `/track` or Shopify webhooks.
2. Filter Cloud Logging on `jsonPayload.event` for the failing event name and review `jsonPayload.code`, `jsonPayload.details`, `jsonPayload.requestContext.requestId`, and `httpRequest.status`.
3. If only Shopify webhooks are failing, check whether the `x-shopify-hmac-sha256` signature failures started after a secret rotation.
4. If only `/track` is failing, sample `origin_not_allowed`, `unsupported_media_type`, and `rate_limit_exceeded` errors before changing traffic controls.

## Likely Causes

- Secret mismatch after rotating `SHOPIFY_WEBHOOK_SECRET` or `SHOPIFY_APP_API_SECRET`
- misconfigured storefront origins in `TRACKING_ALLOWED_ORIGINS`
- malformed storefront payloads after a theme or tracker rollout
- downstream database failure causing 5xx ingestion errors

## Remediation

1. Validate the current Secret Manager versions bound to the API service and redeploy if the wrong version is attached.
2. Confirm the storefront tracker is still posting JSON payloads to `/track` with a valid `sessionId` and timestamp.
3. If rate limiting is the only error mode, compare event volume against release changes before increasing `TRACKING_RATE_LIMIT_MAX`.
4. If the database is unhealthy, follow the Cloud SQL incident procedure first; ingestion will keep failing until writes recover.

## Escalation

- Escalate to application engineering if 5xx ingestion failures persist for more than 15 minutes.
- Escalate to the ecommerce owner if Shopify webhooks are rejected because the connected store changed unexpectedly.
