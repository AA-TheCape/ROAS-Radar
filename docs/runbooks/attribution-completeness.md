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

## Shopify Recovery Order

Use the Shopify recovery tools in this order for the same storefront date window:

1. Import Shopify orders
2. Recover attribution hints
3. Backfill order attribution

Do not start with attribution backfill. Import orders first so the target Shopify orders exist locally, then recover Shopify-specific hints, and only queue the broader asynchronous attribution backfill if the earlier steps still leave gaps.

## Recovery Actions

### 1. Import Shopify orders

Use this first when historical Shopify orders are missing locally for a known date range.

- This action imports Shopify orders for the selected storefront date window.
- It can pull a large backlog, so start with the narrowest range that fixes the gap.
- Later recovery actions assume these orders already exist locally.

### 2. Recover attribution hints

Use this second when the orders exist locally but Shopify web orders in the window are still unattributed or only have empty attribution dimensions.

- This rescans only Shopify web orders in the selected window.
- It retries landing-session relinking and customer identity stitching.
- If deterministic matching still fails, it can apply Shopify-hint fallback attribution.
- Orders that still need broader recovery are requeued for standard attribution processing.

### 3. Backfill order attribution

Use this last when import plus hint recovery still leave attribution gaps and you need the broader resolver-driven pass.

- This endpoint is asynchronous and queues a job instead of running inline.
- Always run a dry run first for the exact same date window before queueing a write-enabled run.
- Keep `webOrdersOnly` enabled unless there is a clear reason to include non-web orders.
- Leave Shopify writeback enabled unless you explicitly want local attribution repair without external order note updates.

## Attribution Backfill Options

The admin UI and API expose these implemented backfill options:

- `dryRun`: defaults to `true`. Use this first so the job analyzes the window without writing attribution changes.
- `limit`: defaults to `500` and is capped at `5000`. Use it to bound how many orders are scanned in one run.
- `webOrdersOnly`: defaults to `true`. Keep the backfill focused on Shopify web orders unless you need other sources.
- `skipShopifyWriteback`: defaults to `false`. Set this only when you want local attribution updates without Shopify writeback.

## Operator Notes

- Reuse the same date window across import, hint recovery, the dry run, and any later write-enabled backfill so results stay comparable.
- A dry run can still report scanned and unrecoverable orders even when recovered and writeback counts remain `0`.
- If the dry run looks too broad, reduce the window or the limit before queueing a non-dry-run job.
