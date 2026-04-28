# Attribution Completeness Runbook

## Triggers

- `ROAS Radar * Attribution Capture Rate`
- `ROAS Radar * Missing Session ID Rate`
- `ROAS Radar * Client Server Event Mismatch`
- `ROAS Radar * Shopify Writeback Success`
- `ROAS Radar * Resolver Unattributed Spike`

## Immediate Checks

1. Open the monitoring dashboard and inspect the attribution tier distribution, resolver fallthrough, unattributed trend, and unattributed-by-reason widgets.
2. Filter Cloud Logging on `attribution_capture_observed`, `tracking_dual_write_consistency`, `shopify_writeback_observed`, and `attribution_resolver_outcome`.
3. Split the issue by `pipeline`, `attributionTier`, `fallthroughStage`, and `firstNormalizationFailureReason` before changing code or infra.

## Resolver Outcome Interpretation

- `deterministic_first_party` means the resolver stayed in the highest-confidence tier.
- `deterministic_shopify_hint` means first-party matching failed and Shopify hint recovery won.
- `ga4_fallback` means both first-party and Shopify hint matching failed and GA4 fallback won.
- `unattributed` means all tiers failed or order timestamp normalization made the order ineligible.

Treat a rise in lower tiers or `unattributed` as a fallthrough problem first, then isolate whether the break is caused by capture loss, hint extraction, GA4 matching, or timestamp normalization.

## Triage Order

1. Check whether the spike is isolated to `order_backfill` or `realtime_queue`.
2. If only `order_backfill` is affected, inspect the exact rollout window and recent recovery jobs before changing live ingest.
3. If both pipelines are affected, compare `fallthroughStage` and `firstNormalizationFailureReason` labels to see whether the issue is upstream capture loss or resolver eligibility failure.
4. If unattributed volume rises without matching normalization failures, inspect Shopify hint extraction and GA4 candidate availability next.
