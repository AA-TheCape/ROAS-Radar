# Attribution Completeness Runbook

## Triggers

- `ROAS Radar * Attribution Capture Rate`
- `ROAS Radar * Missing Session ID Rate`
- `ROAS Radar * Client Server Event Mismatch`
- `ROAS Radar * Shopify Writeback Success`
- `ROAS Radar * Resolver Unattributed Rate`

## Immediate Checks

1. Open the monitoring dashboard and inspect the five attribution completeness widgets.
2. Filter Cloud Logging on `attribution_capture_observed`, `tracking_dual_write_consistency`, `shopify_writeback_observed`, and `attribution_resolver_outcome`.
3. Split the issue by source before changing code or infra.
