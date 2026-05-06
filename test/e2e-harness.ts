process.env.DATABASE_URL ??=
	"postgres://postgres:postgres@127.0.0.1:5432/roas_radar";

import { resetIntegrationTables } from './integration-test-helpers.js';

let cachedPool: typeof import('../src/db/pool.js').pool | null = null;

async function getPool() {
	if (cachedPool) {
		return cachedPool;
	}

	const poolModule = await import("../src/db/pool.js");
	cachedPool = poolModule.pool;
	return cachedPool;
}

export async function resetE2EDatabase(): Promise<void> {
  const pool = await getPool();
  await resetIntegrationTables(pool, [
    'app_sessions',
    'app_users',
    'campaign_metadata_backfill_runs',
    'ad_platform_entity_metadata',
    'attribution_explain_records',
    'attribution_model_credits',
    'attribution_model_summaries',
    'attribution_touchpoint_inputs',
    'attribution_order_inputs',
    'attribution_runs',
    'ad_sync_api_transactions',
    'google_ads_oauth_states',
    'google_ads_settings',
    'meta_ads_order_value_sync_jobs',
    'meta_ads_order_value_raw_records',
    'meta_ads_order_value_sync_runs',
    'meta_ads_order_value_aggregates',
    'meta_ads_oauth_states',
    'meta_ads_settings',
    'google_ads_reconciliation_runs',
    'google_ads_daily_spend',
    'google_ads_raw_spend_records',
    'google_ads_sync_jobs',
    'google_ads_connections',
    'meta_ads_daily_spend',
    'meta_ads_raw_spend_records',
    'meta_ads_sync_jobs',
    'meta_ads_connections',
    'attribution_jobs',
    'shopify_order_writeback_jobs',
    'attribution_order_credits',
    'attribution_results',
    'daily_reporting_metrics',
    'customer_journey',
    'order_attribution_links',
    'identity_edge_ingestion_runs',
    'identity_edges',
    'identity_nodes',
    'identity_journeys',
    'session_attribution_touch_events',
    'session_attribution_identities',
    'shopify_order_line_items',
    'shopify_orders',
    'shopify_webhook_receipts',
    'tracking_events',
    'tracking_sessions',
    'shopify_customers',
    'customer_identities',
    'event_replay_run_items',
    'event_replay_runs',
    'event_dead_letters'
  ]);
}
