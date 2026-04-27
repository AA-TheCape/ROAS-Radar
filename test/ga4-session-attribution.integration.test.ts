import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar';

const enabledConfig = {
  enabled: true as const,
  ga4: {
    projectId: 'analytics-prod1',
    location: 'US',
    dataset: 'ga4_export',
    eventsTablePattern: 'events_*',
    intradayTablePattern: 'events_intraday_*',
    lookbackHours: 2,
    backfillHours: 2,
    eventsTableExpression: '`analytics-prod1.ga4_export.events_*`',
    intradayTableExpression: '`analytics-prod1.ga4_export.events_intraday_*`'
  },
  googleAdsTransfer: {
    projectId: 'analytics-prod1',
    location: 'US',
    dataset: 'google_ads_transfer',
    tablePattern: 'p_ads_*',
    lookbackDays: 14,
    tableExpression: '`analytics-prod1.google_ads_transfer.p_ads_*`',
    customerIds: []
  }
};

async function resetGa4Fixtures(): Promise<void> {
  const { pool } = await import('../src/db/pool.js');
  await pool.query(`
    TRUNCATE TABLE
      ga4_session_attribution,
      ga4_bigquery_ingestion_state
    RESTART IDENTITY CASCADE
  `);
}

test.beforeEach(async () => {
  await resetGa4Fixtures();
});

test.after(async () => {
  await resetGa4Fixtures();
  const { pool } = await import('../src/db/pool.js');
  await pool.end();
});

test('GA4 session attribution ingestion upserts normalized rows idempotently and advances the hourly watermark', async () => {
  const [{ ingestGa4SessionAttribution, listGa4SessionAttributionRows, getGa4SessionAttributionWatermark }, { pool }] =
    await Promise.all([
      import('../src/modules/attribution/ga4-session-attribution.js'),
      import('../src/db/pool.js')
    ]);

  const extractorCalls: string[] = [];
  const executor = {
    async runQuery(input: { params: Record<string, unknown> }) {
      extractorCalls.push(String(input.params.window_start));

      if (input.params.window_start === '2026-04-27T10:00:00.000Z') {
        return [
          {
            ga4_session_key: 'pseudo-1:1001',
            ga4_user_key: 'pseudo-1',
            ga4_client_id: 'pseudo-1',
            ga4_session_id: '1001',
            session_started_at: '2026-04-27T10:05:00.000Z',
            last_event_at: '2026-04-27T10:25:00.000Z',
            source: 'google',
            medium: 'cpc',
            campaign_id: '1001',
            campaign: 'Brand Search',
            content: 'Hero',
            term: 'shoes',
            click_id_type: 'gclid',
            click_id_value: 'aaa111',
            account_id: '1234567890',
            account_name: 'Brand Account',
            channel_type: 'SEARCH',
            channel_subtype: 'SEARCH_STANDARD',
            campaign_metadata_source: 'google_ads_transfer',
            account_metadata_source: 'google_ads_transfer',
            channel_metadata_source: 'google_ads_transfer',
            source_export_hour: '2026-04-27T10:00:00.000Z',
            source_dataset: 'ga4_export',
            source_table_type: 'events'
          }
        ];
      }

      if (input.params.window_start === '2026-04-27T11:00:00.000Z') {
        return [
          {
            ga4_session_key: 'pseudo-1:1001',
            ga4_user_key: 'customer-42',
            ga4_client_id: 'pseudo-1',
            ga4_session_id: '1001',
            session_started_at: '2026-04-27T10:05:00.000Z',
            last_event_at: '2026-04-27T11:15:00.000Z',
            source: 'google',
            medium: 'cpc',
            campaign_id: '1001',
            campaign: 'Brand Search',
            content: 'Retargeting',
            term: 'shoes',
            click_id_type: 'gclid',
            click_id_value: 'aaa111',
            account_id: '1234567890',
            account_name: 'Brand Account',
            channel_type: 'SEARCH',
            channel_subtype: 'SEARCH_STANDARD',
            campaign_metadata_source: 'google_ads_transfer',
            account_metadata_source: 'google_ads_transfer',
            channel_metadata_source: 'google_ads_transfer',
            source_export_hour: '2026-04-27T11:00:00.000Z',
            source_dataset: 'ga4_export',
            source_table_type: 'intraday'
          },
          {
            ga4_session_key: 'pseudo-2:2002',
            ga4_user_key: 'pseudo-2',
            ga4_client_id: 'pseudo-2',
            ga4_session_id: '2002',
            session_started_at: '2026-04-27T11:03:00.000Z',
            last_event_at: '2026-04-27T11:22:00.000Z',
            source: 'email',
            medium: 'newsletter',
            campaign_id: null,
            campaign: 'Spring',
            content: null,
            term: null,
            click_id_type: null,
            click_id_value: null,
            account_id: null,
            account_name: null,
            channel_type: null,
            channel_subtype: null,
            campaign_metadata_source: 'ga4_raw',
            account_metadata_source: 'unresolved',
            channel_metadata_source: 'unresolved',
            source_export_hour: '2026-04-27T11:00:00.000Z',
            source_dataset: 'ga4_export',
            source_table_type: 'events'
          }
        ];
      }

      return [];
    }
  };

  const firstRun = await ingestGa4SessionAttribution({
    config: enabledConfig,
    executor,
    now: new Date('2026-04-27T12:30:00.000Z')
  });

  assert.deepEqual(firstRun.processedHours, ['2026-04-27T10:00:00.000Z', '2026-04-27T11:00:00.000Z']);
  assert.equal(firstRun.extractedRows, 3);
  assert.equal(firstRun.upsertedRows, 3);
  assert.equal(firstRun.watermarkBefore, null);
  assert.equal(firstRun.watermarkAfter, '2026-04-27T11:00:00.000Z');

  const persistedAfterFirstRun = await listGa4SessionAttributionRows(pool);
  assert.deepEqual(
    persistedAfterFirstRun.map((row) => ({
      ga4SessionKey: row.ga4SessionKey,
      ga4UserKey: row.ga4UserKey,
      lastEventAt: row.lastEventAt,
      sourceTableType: row.sourceTableType,
      content: row.content,
      campaignId: row.campaignId,
      campaign: row.campaign,
      accountId: row.accountId,
      channelType: row.channelType,
      campaignMetadataSource: row.campaignMetadataSource
    })),
    [
      {
        ga4SessionKey: 'pseudo-2:2002',
        ga4UserKey: 'pseudo-2',
        lastEventAt: '2026-04-27T11:22:00.000Z',
        sourceTableType: 'events',
        content: null,
        campaignId: null,
        campaign: 'Spring',
        accountId: null,
        channelType: null,
        campaignMetadataSource: 'ga4_raw'
      },
      {
        ga4SessionKey: 'pseudo-1:1001',
        ga4UserKey: 'customer-42',
        lastEventAt: '2026-04-27T11:15:00.000Z',
        sourceTableType: 'intraday',
        content: 'Retargeting',
        campaignId: '1001',
        campaign: 'Brand Search',
        accountId: '1234567890',
        channelType: 'SEARCH',
        campaignMetadataSource: 'google_ads_transfer'
      }
    ]
  );
  assert.equal(await getGa4SessionAttributionWatermark(pool), '2026-04-27T11:00:00.000Z');

  const secondRun = await ingestGa4SessionAttribution({
    config: enabledConfig,
    executor,
    now: new Date('2026-04-27T12:30:00.000Z')
  });

  assert.deepEqual(secondRun.processedHours, ['2026-04-27T10:00:00.000Z', '2026-04-27T11:00:00.000Z']);
  assert.equal(secondRun.watermarkBefore, '2026-04-27T11:00:00.000Z');
  assert.equal(secondRun.watermarkAfter, '2026-04-27T11:00:00.000Z');

  const counts = await pool.query<{ count: string }>('SELECT COUNT(*)::text AS count FROM ga4_session_attribution');
  assert.equal(counts.rows[0]?.count, '2');
  assert.deepEqual(extractorCalls, [
    '2026-04-27T10:00:00.000Z',
    '2026-04-27T11:00:00.000Z',
    '2026-04-27T10:00:00.000Z',
    '2026-04-27T11:00:00.000Z'
  ]);
});

test('GA4 session attribution watermark advances only after a successful database commit', async () => {
  const [{ ingestGa4SessionAttribution, getGa4SessionAttributionWatermark, markGa4SessionAttributionRunFailed }, { pool }] =
    await Promise.all([
      import('../src/modules/attribution/ga4-session-attribution.js'),
      import('../src/db/pool.js')
    ]);

  const executor = {
    async runQuery() {
      return [
        {
          ga4_session_key: 'pseudo-rollback:3003',
          ga4_user_key: 'pseudo-rollback',
          ga4_client_id: 'pseudo-rollback',
          ga4_session_id: '3003',
          session_started_at: '2026-04-27T11:10:00.000Z',
          last_event_at: '2026-04-27T11:20:00.000Z',
          source: 'google',
          medium: 'cpc',
          campaign_id: '3003',
          campaign: 'Rollback',
          content: null,
          term: null,
          click_id_type: 'gclid',
          click_id_value: 'rollback-click',
          account_id: '1234567890',
          account_name: 'Rollback Account',
          channel_type: 'SEARCH',
          channel_subtype: 'SEARCH_STANDARD',
          campaign_metadata_source: 'google_ads_transfer',
          account_metadata_source: 'google_ads_transfer',
          channel_metadata_source: 'google_ads_transfer',
          source_export_hour: '2026-04-27T11:00:00.000Z',
          source_dataset: 'ga4_export',
          source_table_type: 'events'
        }
      ];
    }
  };

  await assert.rejects(
    () =>
      ingestGa4SessionAttribution({
        config: enabledConfig,
        executor,
        now: new Date('2026-04-27T12:30:00.000Z'),
        beforeCommit: async () => {
          throw new Error('forced commit failure');
        }
      }),
    /forced commit failure/
  );

  assert.equal(await getGa4SessionAttributionWatermark(pool), null);
  const rowCount = await pool.query<{ count: string }>('SELECT COUNT(*)::text AS count FROM ga4_session_attribution');
  assert.equal(rowCount.rows[0]?.count, '0');

  await markGa4SessionAttributionRunFailed(new Error('forced commit failure'));

  const state = await pool.query<{ last_run_status: string; last_error: string | null }>(
    `
      SELECT last_run_status, last_error
      FROM ga4_bigquery_ingestion_state
      WHERE pipeline_name = 'ga4_session_attribution'
    `
  );
  assert.deepEqual(state.rows[0], {
    last_run_status: 'failed',
    last_error: 'forced commit failure'
  });
});
