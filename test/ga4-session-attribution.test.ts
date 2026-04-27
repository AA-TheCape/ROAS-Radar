import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';

const { buildGa4SessionAttributionHourlyQuery, extractGa4SessionAttributionForHour, planGa4SessionAttributionHourlyWindows } =
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
  assert.equal(query.params.window_start, '2026-04-27T11:00:00.000Z');
  assert.equal(query.params.window_end, '2026-04-27T12:00:00.000Z');
  assert.equal(query.params.start_date_suffix, '20260427');
  assert.equal(query.params.end_date_suffix, '20260427');
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
            campaign: 'Spring Launch',
            content: 'Hero',
            term: 'boots',
            click_id_type: null,
            click_id_value: null,
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
      campaign: 'Spring Launch',
      content: 'Hero',
      term: 'boots',
      clickIdType: 'gclid',
      clickIdValue: 'abc123',
      sourceExportHour: '2026-04-27T11:00:00.000Z',
      sourceDataset: 'ga4_export',
      sourceTableType: 'events'
    }
  ]);
});
