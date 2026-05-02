import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar';

const [{ pool }, { backfillCampaignMetadataHistory }, { resolveCampaignDisplayMetadata }, { resetE2EDatabase }] =
  await Promise.all([
    import('../src/db/pool.js'),
    import('../src/modules/ad-platform-metadata-refresh/index.js'),
    import('../src/modules/reporting/metadata-resolution.js'),
    import('./e2e-harness.js')
  ]);

async function seedHistoricalSpend(): Promise<void> {
  await pool.query(
    `
      INSERT INTO google_ads_connections (
        id,
        customer_id,
        developer_token_encrypted,
        client_id,
        client_secret_encrypted,
        refresh_token_encrypted,
        status,
        raw_customer_data,
        raw_customer_source,
        raw_customer_received_at,
        raw_customer_external_id,
        raw_customer_payload_size_bytes,
        raw_customer_payload_hash
      )
      VALUES (1, 'acct-google', '\\x00'::bytea, 'client', '\\x00'::bytea, '\\x00'::bytea, 'active', '{}'::jsonb, 'google_ads_customer', now(), 'acct-google', 2, 'seed')
    `
  );

  await pool.query(
    `
      INSERT INTO google_ads_sync_jobs (id, connection_id, sync_date, status)
      VALUES
        (1, 1, '2026-04-10'::date, 'completed'),
        (2, 1, '2026-04-11'::date, 'completed')
    `
  );

  await pool.query(
    `
      INSERT INTO meta_ads_connections (
        id,
        ad_account_id,
        access_token_encrypted,
        status,
        raw_account_data,
        raw_account_source,
        raw_account_received_at,
        raw_account_external_id,
        raw_account_payload_size_bytes,
        raw_account_payload_hash
      )
      VALUES (1, 'acct-meta', '\\x00'::bytea, 'active', '{}'::jsonb, 'meta_ads_account', now(), 'acct-meta', 2, 'seed')
    `
  );

  await pool.query(
    `
      INSERT INTO meta_ads_sync_jobs (id, connection_id, sync_date, status)
      VALUES (1, 1, '2026-04-10'::date, 'completed')
    `
  );

  await pool.query(
    `
      INSERT INTO daily_reporting_metrics (
        metric_date,
        attribution_model,
        source,
        medium,
        campaign,
        content,
        term,
        visits,
        attributed_orders,
        attributed_revenue,
        spend,
        impressions,
        clicks,
        new_customer_orders,
        returning_customer_orders,
        new_customer_revenue,
        returning_customer_revenue,
        last_computed_at
      )
      VALUES
        ('2026-04-10'::date, 'last_touch', 'google', 'cpc', 'brand-search', 'unknown', 'unknown', 100, 4, '600.00', '250.00', 0, 0, 0, 0, 0, 0, now()),
        ('2026-04-10'::date, 'last_touch', 'meta', 'paid_social', 'meta-unknown', 'unknown', 'unknown', 30, 1, '90.00', '70.00', 0, 0, 0, 0, 0, 0, now())
    `
  );

  await pool.query(
    `
      INSERT INTO google_ads_daily_spend (
        connection_id,
        sync_job_id,
        report_date,
        granularity,
        entity_key,
        account_id,
        account_name,
        campaign_id,
        campaign_name,
        adset_id,
        adset_name,
        ad_id,
        ad_name,
        canonical_source,
        canonical_medium,
        canonical_campaign,
        canonical_content,
        canonical_term,
        currency,
        spend,
        impressions,
        clicks,
        raw_payload
      )
      VALUES
        (1, 1, '2026-04-10'::date, 'ad', 'google-ad-1', 'acct-google', 'Google Account', 'cmp_google_1', '  Brand   Search  ', 'adset_google_1', ' Search   US ', 'ad_google_1', ' Headline   A ', 'google', 'cpc', 'brand-search', 'unknown', 'unknown', 'USD', '125.00', 0, 0, '{}'::jsonb),
        (1, 2, '2026-04-11'::date, 'campaign', 'google-campaign-1', 'acct-google', 'Google Account', 'cmp_google_1', ' Brand Search Latest ', NULL, NULL, NULL, NULL, 'google', 'cpc', 'brand-search', 'unknown', 'unknown', 'USD', '125.00', 0, 0, '{}'::jsonb)
    `
  );

  await pool.query(
    `
      INSERT INTO meta_ads_daily_spend (
        connection_id,
        sync_job_id,
        report_date,
        granularity,
        entity_key,
        account_id,
        account_name,
        campaign_id,
        campaign_name,
        canonical_source,
        canonical_medium,
        canonical_campaign,
        canonical_content,
        canonical_term,
        currency,
        spend,
        impressions,
        clicks,
        raw_payload
      )
      VALUES
        (1, 1, '2026-04-10'::date, 'campaign', 'meta-campaign-1', 'acct-meta', 'Meta Account', 'cmp_meta_1', NULL, 'meta', 'paid_social', 'meta-unknown', 'unknown', 'unknown', 'USD', '70.00', 0, 0, '{}'::jsonb)
    `
  );
}

test('campaign metadata backfill upserts latest names, reports unresolved ids, and improves reporting coverage', async () => {
  await resetE2EDatabase();

  try {
    await seedHistoricalSpend();

    const beforeResolution = await resolveCampaignDisplayMetadata('2026-04-10', '2026-04-11', ['brand-search']);
    const beforeCampaign = beforeResolution.byCampaign.get('brand-search');

    assert.ok(beforeCampaign);
    assert.equal(beforeCampaign.campaignNameResolutionStatus, 'fallback_name');
    assert.equal(beforeCampaign.campaignDisplayName, 'Brand Search Latest');

    const report = await backfillCampaignMetadataHistory({
      requestedBy: 'integration-test',
      workerId: 'campaign-metadata-backfill-test',
      startDate: '2026-04-10',
      endDate: '2026-04-11',
      unresolvedSampleLimit: 10
    });

    assert.equal(report.status, 'completed');
    assert.equal(report.plannedInserts, 3);
    assert.equal(report.plannedUpdates, 0);
    assert.equal(report.campaignCoverageBefore.totalEntities, 2);
    assert.equal(report.campaignCoverageBefore.resolvedEntities, 0);
    assert.equal(report.campaignCoverageAfter.totalEntities, 2);
    assert.equal(report.campaignCoverageAfter.resolvedEntities, 1);
    assert.equal(report.unresolvedRate.totalEntities, 4);
    assert.equal(report.unresolvedRate.unresolvedEntities, 1);
    assert.equal(report.unresolvedSamples.length, 1);
    assert.deepEqual(report.unresolvedSamples[0], {
      platform: 'meta_ads',
      accountId: 'acct-meta',
      entityType: 'campaign',
      entityId: 'cmp_meta_1',
      lastSeenAt: '2026-04-10T23:59:59.000Z',
      hadNameInHistory: false
    });

    const metadataRows = await pool.query<{
      platform: string;
      account_id: string;
      entity_type: string;
      entity_id: string;
      latest_name: string;
    }>(
      `
        SELECT platform, account_id, entity_type, entity_id, latest_name
        FROM ad_platform_entity_metadata
        ORDER BY entity_type ASC, entity_id ASC
      `
    );

    assert.deepEqual(metadataRows.rows, [
      {
        platform: 'google_ads',
        account_id: 'acct-google',
        entity_type: 'ad',
        entity_id: 'ad_google_1',
        latest_name: 'Headline A'
      },
      {
        platform: 'google_ads',
        account_id: 'acct-google',
        entity_type: 'adset',
        entity_id: 'adset_google_1',
        latest_name: 'Search US'
      },
      {
        platform: 'google_ads',
        account_id: 'acct-google',
        entity_type: 'campaign',
        entity_id: 'cmp_google_1',
        latest_name: 'Brand Search Latest'
      }
    ]);

    const afterResolution = await resolveCampaignDisplayMetadata('2026-04-10', '2026-04-11', ['brand-search']);
    const afterCampaign = afterResolution.byCampaign.get('brand-search');

    assert.ok(afterCampaign);
    assert.equal(afterCampaign.campaignNameResolutionStatus, 'resolved');
    assert.equal(afterCampaign.campaignDisplayName, 'Brand Search Latest');

    const persistedRun = await pool.query<{ status: string; dry_run: boolean; report: { runId: string } }>(
      `
        SELECT status, dry_run, report
        FROM campaign_metadata_backfill_runs
        WHERE id = $1::uuid
      `,
      [report.runId]
    );

    assert.equal(persistedRun.rows[0]?.status, 'completed');
    assert.equal(persistedRun.rows[0]?.dry_run, false);
    assert.equal(persistedRun.rows[0]?.report.runId, report.runId);
  } finally {
    await resetE2EDatabase();
  }
});
