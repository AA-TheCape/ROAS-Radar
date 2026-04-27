import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';

const {
  buildGa4SessionAttributionHourlyQuery,
  extractAllowedGa4ClickIdsFromEventParams,
  extractGa4SessionAttributionForHour,
  planGa4SessionAttributionHourlyWindows
} =
  await import('../src/modules/attribution/ga4-session-attribution.js');

const enabledConfig = {
  enabled: true as const,
  ga4: {
    projectId: 'analytics-prod1',
    location: 'US',
    dataset: 'ga4_export',
    eventsTablePattern: 'events_*',
    intradayTablePattern: 'events_intraday_*',
    lookbackHours: 6,
    backfillHours: 3,
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

test('planGa4SessionAttributionHourlyWindows uses lookback on first run and backfill on resumed runs', () => {
  const firstRunHours = planGa4SessionAttributionHourlyWindows({
    now: new Date('2026-04-27T12:35:00.000Z'),
    watermarkHour: null,
    config: enabledConfig
  });

  assert.deepEqual(firstRunHours.map((entry) => entry.hourStart), [
    '2026-04-27T06:00:00.000Z',
    '2026-04-27T07:00:00.000Z',
    '2026-04-27T08:00:00.000Z',
    '2026-04-27T09:00:00.000Z',
    '2026-04-27T10:00:00.000Z',
    '2026-04-27T11:00:00.000Z'
  ]);

  const resumedHours = planGa4SessionAttributionHourlyWindows({
    now: new Date('2026-04-27T12:35:00.000Z'),
    watermarkHour: new Date('2026-04-27T10:00:00.000Z'),
    config: enabledConfig
  });

  assert.deepEqual(resumedHours.map((entry) => entry.hourStart), [
    '2026-04-27T08:00:00.000Z',
    '2026-04-27T09:00:00.000Z',
    '2026-04-27T10:00:00.000Z',
    '2026-04-27T11:00:00.000Z'
  ]);
});

test('buildGa4SessionAttributionHourlyQuery targets daily and intraday exports for one hour', () => {
  const query = buildGa4SessionAttributionHourlyQuery({
    config: enabledConfig,
    hourStart: '2026-04-27T11:00:00.000Z',
    hourEndExclusive: '2026-04-27T12:00:00.000Z'
  });

  assert.match(query.query, /FROM `analytics-prod1\.ga4_export\.events_\*`/);
  assert.match(query.query, /FROM `analytics-prod1\.ga4_export\.events_intraday_\*`/);
  assert.match(query.query, /FROM `analytics-prod1\.google_ads_transfer\.p_ads_\*`/);
  assert.match(query.query, /LEFT JOIN ads_linked_campaigns/);
  assert.match(query.query, /LOWER\(ep\.key\) = 'gclid'/);
  assert.match(query.query, /LOWER\(ep\.key\) = 'dclid'/);
  assert.match(query.query, /AS dclid/);
  assert.equal(query.params.window_start, '2026-04-27T11:00:00.000Z');
  assert.equal(query.params.window_end, '2026-04-27T12:00:00.000Z');
  assert.equal(query.params.start_date_suffix, '20260427');
  assert.equal(query.params.end_date_suffix, '20260427');
  assert.equal(query.params.ads_metadata_lookback_days, 14);
  assert.deepEqual(query.params.google_ads_customer_ids, []);
  assert.equal(query.params.google_ads_customer_id_count, 0);
});

test('extractAllowedGa4ClickIdsFromEventParams normalizes allowed keys and rejects malformed values', () => {
  const extracted = extractAllowedGa4ClickIdsFromEventParams([
    {
      key: ' GCLID ',
      value: {
        string_value: '  ABC123  '
      }
    },
    {
      key: 'DCLID',
      value: {
        string_value: 'DCLID-9'
      }
    },
    {
      key: 'gbraid',
      value: {
        int_value: 12345
      }
    },
    {
      key: 'wbraid',
      value: {
        string_value: 'BAD VALUE'
      }
    },
    {
      key: 'fbclid',
      value: {
        string_value: 'bad\u0000value'
      }
    },
    {
      key: 'ignored_key',
      value: {
        string_value: 'SHOULD-NOT-APPEAR'
      }
    },
    {
      key: 'gclid',
      value: {
        string_value: 'SECOND-VALUE-IGNORED'
      }
    }
  ]);

  assert.deepEqual(extracted, {
    gclid: 'ABC123',
    dclid: 'DCLID-9',
    gbraid: '12345'
  });
});

test('extractGa4SessionAttributionForHour normalizes stable session and user keys', async () => {
  const result = await extractGa4SessionAttributionForHour({
    config: enabledConfig,
    hourStart: '2026-04-27T11:00:00.000Z',
    executor: {
      async runQuery() {
        return [
          {
            ga4_session_key: 'pseudo-1:12345',
            ga4_user_key: 'customer-7',
            ga4_client_id: 'pseudo-1',
            ga4_session_id: '12345',
            session_started_at: '2026-04-27T11:01:00.000Z',
            last_event_at: '2026-04-27T11:14:00.000Z',
            source: ' Google ',
            medium: ' CPC ',
            campaign_id: '9001',
            campaign: 'Brand Search',
            content: 'Hero',
            term: 'boots',
            click_id_type: null,
            click_id_value: null,
            account_id: '1234567890',
            account_name: ' Example Ads ',
            channel_type: ' SEARCH ',
            channel_subtype: ' SEARCH_MOBILE_APP ',
            campaign_metadata_source: 'google_ads_transfer',
            account_metadata_source: 'google_ads_transfer',
            channel_metadata_source: 'google_ads_transfer',
            gclid: 'abc123',
            gbraid: null,
            wbraid: null,
            fbclid: null,
            ttclid: null,
            msclkid: null,
            source_export_hour: '2026-04-27T11:00:00.000Z',
            source_dataset: 'ga4_export',
            source_table_type: 'events'
          }
        ];
      }
    }
  });

  assert.equal(result.hourStart, '2026-04-27T11:00:00.000Z');
  assert.deepEqual(result.rows, [
    {
      ga4SessionKey: 'pseudo-1:12345',
      ga4UserKey: 'customer-7',
      ga4ClientId: 'pseudo-1',
      ga4SessionId: '12345',
      sessionStartedAt: '2026-04-27T11:01:00.000Z',
      lastEventAt: '2026-04-27T11:14:00.000Z',
      source: 'google',
      medium: 'cpc',
      campaignId: '9001',
      campaign: 'Brand Search',
      content: 'Hero',
      term: 'boots',
      clickIdType: 'gclid',
      clickIdValue: 'abc123',
      accountId: '1234567890',
      accountName: 'Example Ads',
      channelType: 'SEARCH',
      channelSubtype: 'SEARCH_MOBILE_APP',
      campaignMetadataSource: 'google_ads_transfer',
      accountMetadataSource: 'google_ads_transfer',
      channelMetadataSource: 'google_ads_transfer',
      sourceExportHour: '2026-04-27T11:00:00.000Z',
      sourceDataset: 'ga4_export',
      sourceTableType: 'events'
    }
  ]);
});

test('extractGa4SessionAttributionForHour falls back to nested event_params click ids with session linkage metadata', async () => {
  const result = await extractGa4SessionAttributionForHour({
    config: enabledConfig,
    hourStart: '2026-04-27T11:00:00.000Z',
    executor: {
      async runQuery() {
        return [
          {
            ga4_session_key: 'pseudo-3:98765',
            ga4_user_key: 'pseudo-3',
            ga4_client_id: 'pseudo-3',
            ga4_session_id: '98765',
            session_started_at: '2026-04-27T11:07:00.000Z',
            last_event_at: '2026-04-27T11:19:00.000Z',
            source: ' Google ',
            medium: ' CPC ',
            campaign_id: null,
            campaign: 'Demand Gen',
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
            event_params: [
              {
                key: 'WBRAID',
                value: {
                  string_value: ' WB-456 '
                }
              },
              {
                key: 'dclid',
                value: {
                  string_value: 'DCLID-should-lose'
                }
              }
            ],
            source_export_hour: '2026-04-27T11:00:00.000Z',
            source_dataset: 'ga4_export',
            source_table_type: 'intraday'
          }
        ];
      }
    }
  });

  assert.deepEqual(result.rows, [
    {
      ga4SessionKey: 'pseudo-3:98765',
      ga4UserKey: 'pseudo-3',
      ga4ClientId: 'pseudo-3',
      ga4SessionId: '98765',
      sessionStartedAt: '2026-04-27T11:07:00.000Z',
      lastEventAt: '2026-04-27T11:19:00.000Z',
      source: 'google',
      medium: 'cpc',
      campaignId: null,
      campaign: 'Demand Gen',
      content: null,
      term: null,
      clickIdType: 'dclid',
      clickIdValue: 'DCLID-should-lose',
      accountId: null,
      accountName: null,
      channelType: null,
      channelSubtype: null,
      campaignMetadataSource: 'ga4_raw',
      accountMetadataSource: 'unresolved',
      channelMetadataSource: 'unresolved',
      sourceExportHour: '2026-04-27T11:00:00.000Z',
      sourceDataset: 'ga4_export',
      sourceTableType: 'intraday'
    }
  ]);
});

test('extractGa4SessionAttributionForHour preserves ads enrichment when click ids are parsed from event params', async () => {
  const result = await extractGa4SessionAttributionForHour({
    config: enabledConfig,
    hourStart: '2026-04-27T11:00:00.000Z',
    executor: {
      async runQuery() {
        return [
          {
            ga4_session_key: 'pseudo-9:54321',
            ga4_user_key: 'customer-99',
            ga4_client_id: 'pseudo-9',
            ga4_session_id: '54321',
            session_started_at: '2026-04-27T11:11:00.000Z',
            last_event_at: '2026-04-27T11:29:00.000Z',
            source: ' Google ',
            medium: ' CPC ',
            campaign_id: '9009',
            campaign: ' PMax Prospecting ',
            content: ' Carousel ',
            term: ' boots ',
            click_id_type: null,
            click_id_value: null,
            account_id: '9988776655',
            account_name: ' Growth Account ',
            channel_type: ' PERFORMANCE_MAX ',
            channel_subtype: ' SHOPPING ',
            campaign_metadata_source: 'google_ads_transfer',
            account_metadata_source: 'google_ads_transfer',
            channel_metadata_source: 'google_ads_transfer',
            event_params: [
              {
                key: ' GCLID ',
                value: {
                  string_value: ' GCLID-FROM-PARAMS '
                }
              }
            ],
            source_export_hour: '2026-04-27T11:00:00.000Z',
            source_dataset: 'ga4_export',
            source_table_type: 'events'
          }
        ];
      }
    }
  });

  assert.deepEqual(result.rows, [
    {
      ga4SessionKey: 'pseudo-9:54321',
      ga4UserKey: 'customer-99',
      ga4ClientId: 'pseudo-9',
      ga4SessionId: '54321',
      sessionStartedAt: '2026-04-27T11:11:00.000Z',
      lastEventAt: '2026-04-27T11:29:00.000Z',
      source: 'google',
      medium: 'cpc',
      campaignId: '9009',
      campaign: 'PMax Prospecting',
      content: 'Carousel',
      term: 'boots',
      clickIdType: 'gclid',
      clickIdValue: 'GCLID-FROM-PARAMS',
      accountId: '9988776655',
      accountName: 'Growth Account',
      channelType: 'PERFORMANCE_MAX',
      channelSubtype: 'SHOPPING',
      campaignMetadataSource: 'google_ads_transfer',
      accountMetadataSource: 'google_ads_transfer',
      channelMetadataSource: 'google_ads_transfer',
      sourceExportHour: '2026-04-27T11:00:00.000Z',
      sourceDataset: 'ga4_export',
      sourceTableType: 'events'
    }
  ]);
});
