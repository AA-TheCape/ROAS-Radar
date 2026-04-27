process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar';

let cachedPool: typeof import('../src/db/pool.js').pool | null = null;

async function getPool() {
  if (cachedPool) {
    return cachedPool;
  }

  const poolModule = await import('../src/db/pool.js');
  cachedPool = poolModule.pool;
  return cachedPool;
}

export async function resetE2EDatabase(): Promise<void> {
  const pool = await getPool();
  await pool.query(`
    TRUNCATE TABLE
      ad_sync_api_transactions,
      google_ads_reconciliation_runs,
      google_ads_daily_spend,
      google_ads_raw_spend_records,
      google_ads_sync_jobs,
      google_ads_connections,
      meta_ads_daily_spend,
      meta_ads_raw_spend_records,
      meta_ads_sync_jobs,
      meta_ads_connections,
      ga4_fallback_candidates,
      ga4_session_attribution,
      ga4_bigquery_ingestion_state,
      attribution_jobs,
      shopify_order_writeback_jobs,
      attribution_order_credits,
      attribution_results,
      daily_reporting_metrics,
      customer_journey,
      order_attribution_links,
      identity_edge_ingestion_runs,
      identity_edges,
      identity_nodes,
      identity_journeys,
      session_attribution_touch_events,
      session_attribution_identities,
      shopify_order_line_items,
      shopify_orders,
      shopify_webhook_receipts,
      tracking_events,
      tracking_sessions,
      shopify_customers,
      customer_identities,
      event_replay_run_items,
      event_replay_runs,
      event_dead_letters
    RESTART IDENTITY CASCADE
  `);
}
