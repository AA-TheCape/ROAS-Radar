import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar_test';
process.env.META_ADS_APP_ID ??= 'meta-app-id';
process.env.META_ADS_APP_SECRET ??= 'meta-app-secret';
process.env.META_ADS_APP_BASE_URL ??= 'https://api.example.com';
process.env.META_ADS_APP_SCOPES ??= 'ads_read,business_management';
process.env.META_ADS_ENCRYPTION_KEY ??= 'meta-encryption-key';
process.env.META_ADS_ORDER_VALUE_WINDOW_DAYS = '1';

const { pool } = await import('../src/db/pool.js');
const { runMetaAdsOrderValueSync } = await import('../src/modules/meta-ads/index.js');
const { resetE2EDatabase } = await import('./e2e-harness.js');

async function captureStructuredLogs<T>(callback: () => Promise<T>): Promise<{
  entries: Array<Record<string, unknown>>;
  result: T;
}> {
  const entries: Array<Record<string, unknown>> = [];
  const originalStdoutWrite = process.stdout.write.bind(process.stdout);
  const originalStderrWrite = process.stderr.write.bind(process.stderr);

  const capture =
    (originalWrite: typeof process.stdout.write) =>
    ((chunk: string | Uint8Array, encoding?: BufferEncoding | ((error?: Error | null) => void), callback?: (error?: Error | null) => void) => {
      const text = typeof chunk === 'string' ? chunk : chunk.toString(typeof encoding === 'string' ? encoding : 'utf8');

      for (const line of text.split('\n')) {
        const trimmed = line.trim();

        if (!trimmed) {
          continue;
        }

        try {
          entries.push(JSON.parse(trimmed) as Record<string, unknown>);
        } catch {
          // Ignore non-JSON writes.
        }
      }

      return originalWrite(chunk, encoding as never, callback as never);
    }) as typeof process.stdout.write;

  process.stdout.write = capture(originalStdoutWrite);
  process.stderr.write = capture(originalStderrWrite);

  try {
    const result = await callback();
    return { entries, result };
  } finally {
    process.stdout.write = originalStdoutWrite;
    process.stderr.write = originalStderrWrite;
  }
}

async function seedMetaConnection(): Promise<number> {
  const rawAccount = {
    id: '123456789',
    name: 'Meta Account',
    currency: 'USD'
  };
  const rawAccountJson = JSON.stringify(rawAccount);

  const result = await pool.query<{ id: number | string }>(
    `
      INSERT INTO meta_ads_connections (
        ad_account_id,
        access_token_encrypted,
        token_type,
        granted_scopes,
        status,
        account_name,
        account_currency,
        raw_account_data,
        raw_account_source,
        raw_account_received_at,
        raw_account_external_id,
        raw_account_payload_size_bytes,
        raw_account_payload_hash
      )
      VALUES (
        '123456789',
        pgp_sym_encrypt($1, $2, 'cipher-algo=aes256, compress-algo=0'),
        'Bearer',
        ARRAY['ads_read']::text[],
        'active',
        'Meta Account',
        'USD',
        $3::jsonb,
        'meta_ads_account',
        now(),
        '123456789',
        $4,
        $5
      )
      RETURNING id
    `,
    [
      'meta-access-token',
      process.env.META_ADS_ENCRYPTION_KEY,
      rawAccountJson,
      Buffer.byteLength(rawAccountJson, 'utf8'),
      createHash('sha256').update(rawAccountJson).digest('hex')
    ]
  );

  const rawConnectionId = result.rows[0]?.id;

  if (rawConnectionId === undefined) {
    return 0;
  }

  return Number(rawConnectionId);
}

async function loadOrderValuePersistence() {
  const [rawRecords, aggregateRows, syncRuns, syncJobs] = await Promise.all([
    pool.query<{
      campaign_id: string;
      action_type: string | null;
      raw_payload: Record<string, unknown>;
    }>(
      `
        SELECT campaign_id, action_type, raw_payload
        FROM meta_ads_order_value_raw_records
        ORDER BY id ASC
      `
    ),
    pool.query<{
      campaign_id: string;
      attributed_revenue: string | null;
      purchase_count: string | null;
      spend: string;
      purchase_roas: string | null;
      canonical_action_type: string | null;
      canonical_selection_mode: 'priority' | 'fallback' | 'none';
      raw_revenue_record_ids: number[];
    }>(
      `
        SELECT
          campaign_id,
          attributed_revenue::text,
          purchase_count::text,
          spend::text,
          purchase_roas::text,
          canonical_action_type,
          canonical_selection_mode,
          raw_revenue_record_ids
        FROM meta_ads_order_value_aggregates
        ORDER BY campaign_id ASC
      `
    ),
    pool.query<{
      status: string;
      records_received: number;
      raw_rows_persisted: number;
      aggregate_rows_upserted: number;
    }>(
      `
        SELECT status, records_received, raw_rows_persisted, aggregate_rows_upserted
        FROM meta_ads_order_value_sync_runs
        ORDER BY id ASC
      `
    ),
    pool.query<{ status: string }>(
      `
        SELECT status
        FROM meta_ads_order_value_sync_jobs
        ORDER BY id ASC
      `
    )
  ]);

  return {
    rawRecords: rawRecords.rows,
    aggregateRows: aggregateRows.rows,
    syncRuns: syncRuns.rows,
    syncJobs: syncJobs.rows
  };
}

test.beforeEach(async () => {
  await resetE2EDatabase();
});

test.afterEach(async () => {
  await resetE2EDatabase();
});

test.after(async () => {
  await pool.end();
});

test(
  'runMetaAdsOrderValueSync persists raw rows, replaces same-window aggregates, and keeps aggregate row counts idempotent',
  { concurrency: false },
  async () => {
  const previousFetch = globalThis.fetch;

  try {
    const connectionId = await seedMetaConnection();
    assert.equal(typeof connectionId, 'number');
    assert.ok(connectionId > 0);

    const pageOne = {
      data: [
        {
          campaign_id: 'cmp_live_1',
          campaign_name: 'Live Campaign One',
          date_start: '2026-04-29',
          date_stop: '2026-04-29',
          spend: '12.34',
          action_type: 'omni_purchase',
          actions: [{ action_type: 'omni_purchase', value: '2' }],
          action_values: [{ action_type: 'omni_purchase', value: '39.50' }],
          purchase_roas: [{ action_type: 'omni_purchase', value: '3.200000' }]
        },
        {
          campaign_id: 'cmp_live_1',
          campaign_name: 'Live Campaign One',
          date_start: '2026-04-29',
          date_stop: '2026-04-29',
          spend: '12.34',
          action_type: 'purchase',
          actions: [{ action_type: 'purchase', value: '3' }],
          action_values: [{ action_type: 'purchase', value: '45.67' }],
          purchase_roas: [{ action_type: 'purchase', value: '3.701135' }]
        }
      ],
      paging: {
        next: 'https://graph.facebook.com/v99.0/act_123456789/insights?after=page-2'
      }
    };
    const pageTwo = {
      data: [
        {
          campaign_id: 'cmp_live_2',
          campaign_name: 'Live Campaign Two',
          date_start: '2026-04-29',
          date_stop: '2026-04-29',
          spend: '22.00',
          action_type: 'offsite_conversion.fb_pixel_purchase',
          actions: [{ action_type: 'offsite_conversion.fb_pixel_purchase', value: '4' }],
          action_values: [{ action_type: 'offsite_conversion.fb_pixel_purchase', value: '88.80' }],
          purchase_roas: [{ action_type: 'offsite_conversion.fb_pixel_purchase', value: '4.036364' }]
        },
        {
          campaign_id: 'cmp_live_3',
          campaign_name: 'Live Campaign Three',
          date_start: '2026-04-29',
          date_stop: '2026-04-29',
          spend: '19.25',
          action_type: 'link_click',
          actions: [{ action_type: 'link_click', value: '10' }],
          action_values: [],
          purchase_roas: []
        }
      ]
    };

    let fetchCount = 0;
    globalThis.fetch = (async (input: string | URL | Request) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.toString() : input.url;
      fetchCount += 1;
      const payload = fetchCount % 2 === 1 ? pageOne : pageTwo;
      return new Response(JSON.stringify(payload), {
        status: 200,
        headers: { 'content-type': 'application/json' }
      });
    }) as typeof globalThis.fetch;

    const { entries, result: firstRun } = await captureStructuredLogs(() =>
      runMetaAdsOrderValueSync({
        now: new Date('2026-04-29T15:00:00.000Z'),
        triggerSource: 'test'
      })
    );

    assert.equal(firstRun.succeededConnections, 1);
    assert.equal(firstRun.failedConnections, 0);
    assert.equal(firstRun.recordsReceived, 3);
    assert.equal(firstRun.rawRowsFetched, 4);
    assert.equal(firstRun.rawRowsPersisted, 4);
    assert.equal(firstRun.aggregateRowsUpserted, 3);
    assert.equal(firstRun.apiRequestCount, 2);
    assert.equal(firstRun.anomalyCount, 0);

    const apiRequestLogs = entries.filter((entry) => entry.event === 'meta_ads_api_request_completed');
    assert.equal(apiRequestLogs.length, 2);

    const connectionCompletedLog = entries.find(
      (entry) => entry.event === 'meta_ads_order_value_sync_connection_completed'
    );
    assert.ok(connectionCompletedLog);
    assert.equal(connectionCompletedLog.rawRowsFetched, 4);
    assert.equal(connectionCompletedLog.normalizedRecordsReceived, 3);
    assert.equal(connectionCompletedLog.aggregateRowsUpserted, 3);
    assert.equal(connectionCompletedLog.apiRequestCount, 2);
    assert.equal(connectionCompletedLog.zeroRowsPulled, false);

    let persisted = await loadOrderValuePersistence();
    assert.equal(persisted.rawRecords.length, 4);
    assert.equal(persisted.aggregateRows.length, 3);
    assert.equal(persisted.syncRuns.length, 1);
    assert.equal(persisted.syncRuns[0]?.status, 'completed');
    assert.equal(persisted.syncRuns[0]?.records_received, 3);
    assert.equal(persisted.syncJobs.length, 1);
    assert.equal(persisted.syncJobs[0]?.status, 'completed');
    assert.deepEqual(
      persisted.rawRecords.map((row) => row.action_type),
      ['omni_purchase', 'purchase', 'offsite_conversion.fb_pixel_purchase', 'link_click']
    );

    const campaignOne = persisted.aggregateRows.find((row) => row.campaign_id === 'cmp_live_1');
    assert.ok(campaignOne);
    assert.equal(campaignOne.attributed_revenue, '45.67');
    assert.equal(campaignOne.purchase_count, '3');
    assert.equal(campaignOne.purchase_roas, '3.701135');
    assert.equal(campaignOne.canonical_action_type, 'purchase');
    assert.equal(campaignOne.canonical_selection_mode, 'priority');
    assert.deepEqual(campaignOne.raw_revenue_record_ids, ['1', '2']);

    const campaignThree = persisted.aggregateRows.find((row) => row.campaign_id === 'cmp_live_3');
    assert.ok(campaignThree);
    assert.equal(campaignThree.attributed_revenue, null);
    assert.equal(campaignThree.purchase_count, null);
    assert.equal(campaignThree.canonical_action_type, null);
    assert.equal(campaignThree.canonical_selection_mode, 'none');

    const { result: secondRun } = await captureStructuredLogs(() =>
      runMetaAdsOrderValueSync({
        now: new Date('2026-04-29T16:00:00.000Z'),
        triggerSource: 'test'
      })
    );

    assert.equal(secondRun.succeededConnections, 1);
    assert.equal(secondRun.aggregateRowsUpserted, 3);

    persisted = await loadOrderValuePersistence();
    assert.equal(persisted.rawRecords.length, 8);
    assert.equal(persisted.aggregateRows.length, 3);
    assert.equal(persisted.syncRuns.length, 2);
  } finally {
    globalThis.fetch = previousFetch;
  }
  }
);

test('runMetaAdsOrderValueSync emits a zero-row anomaly when Meta returns no campaign-day order-value data', { concurrency: false }, async () => {
  const previousFetch = globalThis.fetch;

  try {
    const connectionId = await seedMetaConnection();
    assert.equal(typeof connectionId, 'number');
    assert.ok(connectionId > 0);

    globalThis.fetch = (async () =>
      new Response(JSON.stringify({ data: [], paging: {} }), {
        status: 200,
        headers: { 'content-type': 'application/json' }
      })) as typeof globalThis.fetch;

    const { entries, result } = await captureStructuredLogs(() =>
      runMetaAdsOrderValueSync({
        now: new Date('2026-04-29T15:00:00.000Z'),
        triggerSource: 'test_zero_rows'
      })
    );

    assert.equal(result.succeededConnections, 1);
    assert.equal(result.recordsReceived, 0);
    assert.equal(result.rawRowsFetched, 0);
    assert.equal(result.aggregateRowsUpserted, 0);
    assert.equal(result.anomalyCount, 1);

    const persisted = await loadOrderValuePersistence();
    assert.equal(persisted.rawRecords.length, 0);
    assert.equal(persisted.aggregateRows.length, 0);
    assert.equal(persisted.syncRuns.length, 1);
    assert.equal(persisted.syncRuns[0]?.status, 'completed');

    const anomalyLog = entries.find((entry) => entry.event === 'meta_ads_order_value_sync_anomaly');
    assert.ok(anomalyLog);
    assert.equal(anomalyLog.anomalyType, 'zero_rows_pulled');
    assert.equal(anomalyLog.triggerSource, 'test_zero_rows');
    assert.equal(anomalyLog.alertable, true);
  } finally {
    globalThis.fetch = previousFetch;
  }
});
