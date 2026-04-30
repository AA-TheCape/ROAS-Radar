import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar_test';
process.env.META_ADS_APP_ID = 'meta-app-id';
process.env.META_ADS_APP_SECRET = 'meta-app-secret';
process.env.META_ADS_APP_BASE_URL = 'https://api.example.com';
process.env.META_ADS_APP_SCOPES = 'ads_read,business_management';
process.env.META_ADS_ENCRYPTION_KEY = 'meta-encryption-key';
process.env.META_ADS_AD_ACCOUNT_ID = 'act_123456789';
process.env.META_ADS_ORDER_VALUE_WINDOW_DAYS = '2';
process.env.DEFAULT_ORGANIZATION_ID = '77';

const { pool } = await import('../src/db/pool.js');
const { withTransaction } = await import('../src/db/pool.js');
const { runMetaAdsOrderValueSync, __metaAdsTestUtils } = await import('../src/modules/meta-ads/index.js');
const { resetE2EDatabase } = await import('./e2e-harness.js');

const fixturesDirectory = path.join(path.dirname(fileURLToPath(import.meta.url)), 'fixtures', 'meta-ads');

async function loadJsonFixture<T>(filename: string): Promise<T> {
  const fixture = await readFile(path.join(fixturesDirectory, filename), 'utf8');
  return JSON.parse(fixture) as T;
}

async function seedConnection(id: number, adAccountId: string, accountName: string): Promise<void> {
  const rawAccount = {
    id: adAccountId,
    name: accountName,
    currency: 'USD'
  };
  const rawAccountJson = JSON.stringify(rawAccount);

  await pool.query(
    `
      INSERT INTO meta_ads_connections (
        id,
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
        $1,
        $2,
        pgp_sym_encrypt($3, $4, 'cipher-algo=aes256, compress-algo=0'),
        'Bearer',
        ARRAY['ads_read']::text[],
        'active',
        $5,
        'USD',
        $6::jsonb,
        'meta_ads_account',
        now(),
        $2,
        $7,
        $8
      )
      ON CONFLICT (ad_account_id)
      DO UPDATE SET
        access_token_encrypted = pgp_sym_encrypt($3, $4, 'cipher-algo=aes256, compress-algo=0'),
        token_type = 'Bearer',
        granted_scopes = ARRAY['ads_read']::text[],
        status = 'active',
        account_name = $5,
        account_currency = 'USD',
        raw_account_data = $6::jsonb,
        raw_account_source = 'meta_ads_account',
        raw_account_received_at = now(),
        raw_account_external_id = $2,
        raw_account_payload_size_bytes = $7,
        raw_account_payload_hash = $8,
        updated_at = now()
    `,
    [
      id,
      adAccountId,
      `${adAccountId}-token`,
      process.env.META_ADS_ENCRYPTION_KEY,
      accountName,
      rawAccountJson,
      Buffer.byteLength(rawAccountJson, 'utf8'),
      createHash('sha256').update(rawAccountJson).digest('hex')
    ]
  );
}

test.beforeEach(async () => {
  await resetE2EDatabase();
});

test.after(async () => {
  await resetE2EDatabase();
  await pool.end();
});

test('runMetaAdsOrderValueSync upserts current-window aggregates without duplicating rows across repeated hourly runs', async () => {
  await seedConnection(1, '123456789', 'Primary Meta Account');

  const page1 = await loadJsonFixture<Record<string, unknown>>('campaign-daily-revenue-live-page-1.json');
  const page2 = await loadJsonFixture<Record<string, unknown>>('campaign-daily-revenue-live-page-2.json');
  const previousFetch = globalThis.fetch;

  try {
    globalThis.fetch = (async (input: string | URL | Request) => {
      const url = new URL(typeof input === 'string' || input instanceof URL ? input : input.url);

      if (url.searchParams.get('after') === 'page-2') {
        return new Response(JSON.stringify(page2), { status: 200 });
      }

      return new Response(JSON.stringify(page1), { status: 200 });
    }) as typeof globalThis.fetch;

    const firstRun = await runMetaAdsOrderValueSync({
      now: new Date('2026-04-29T15:00:00.000Z'),
      triggerSource: 'test'
    });

    assert.equal(firstRun.windowStartDate, '2026-04-28');
    assert.equal(firstRun.windowEndDate, '2026-04-29');
    assert.equal(firstRun.succeededConnections, 1);
    assert.equal(firstRun.failedConnections, 0);

    const records = __metaAdsTestUtils.normalizeCampaignDailyRevenueRecords(
      '123456789',
      [...((page1.data as Record<string, unknown>[]) ?? []), ...((page2.data as Record<string, unknown>[]) ?? [])],
      {
        currency: 'USD',
        actionReportTime: 'conversion',
        useAccountAttributionSetting: true
      }
    );
    const syncJobs = await pool.query<{ id: number; sync_date: string }>(
      `
        SELECT id, sync_date::text
        FROM meta_ads_sync_jobs
        WHERE connection_id = 1
      `
    );
    const rawRecords = await pool.query<{ report_date: string; campaign_id: string; id: number }>(
      `
        SELECT report_date::text, campaign_id, id
        FROM meta_ads_order_value_raw_records
        WHERE connection_id = 1
      `
    );
    const syncJobsByDate = new Map(syncJobs.rows.map((row) => [row.sync_date, row.id]));
    const rawRecordIdsByAggregateKey = new Map<string, number[]>();

    for (const row of rawRecords.rows) {
      const key = `${row.report_date}:${row.campaign_id}`;
      const ids = rawRecordIdsByAggregateKey.get(key) ?? [];
      ids.push(row.id);
      rawRecordIdsByAggregateKey.set(key, ids);
    }

    const upsertedRows = await withTransaction((client) =>
      __metaAdsTestUtils.upsertOrderValueAggregates(client, {
        connectionId: 1,
        syncJobsByDate,
        records,
        rawRecordIdsByAggregateKey,
        organizationId: 77
      })
    );

    assert.equal(upsertedRows, 3);

    const aggregateResult = await pool.query<{
      report_date: string;
      campaign_id: string;
      campaign_name: string | null;
      sync_job_id: number;
      raw_revenue_record_ids: number[];
      organization_id: number;
    }>(
      `
        SELECT
          report_date::text,
          campaign_id,
          campaign_name,
          sync_job_id,
          raw_revenue_record_ids,
          organization_id
        FROM meta_ads_order_value_aggregates
        ORDER BY report_date ASC, campaign_id ASC
      `
    );

    assert.equal(aggregateResult.rowCount, 3);
    assert.deepEqual(
      aggregateResult.rows.map((row) => row.campaign_id),
      ['cmp_live_1', 'cmp_live_2', 'cmp_live_3']
    );
    assert.ok(aggregateResult.rows.every((row) => Number(row.organization_id) === 77));
    assert.ok(aggregateResult.rows.every((row) => Array.isArray(row.raw_revenue_record_ids) && row.raw_revenue_record_ids.length >= 1));

    const rawResult = await pool.query<{ count: string }>(
      `
        SELECT count(*)::text AS count
        FROM meta_ads_order_value_raw_records
        WHERE connection_id = 1
      `
    );
    assert.equal(Number(rawResult.rows[0]?.count ?? '0'), 4);

    const syncRunResult = await pool.query<{
      connection_id: number;
      status: string;
      window_start_date: string;
      window_end_date: string;
      records_received: number;
      raw_rows_persisted: number;
      aggregate_rows_upserted: number;
      error_count: number;
    }>(
      `
        SELECT
          connection_id,
          status,
          window_start_date::text,
          window_end_date::text,
          records_received,
          raw_rows_persisted,
          aggregate_rows_upserted,
          error_count
        FROM meta_ads_order_value_sync_runs
        ORDER BY id ASC
      `
    );

    assert.equal(syncRunResult.rowCount, 1);
    assert.deepEqual(
      syncRunResult.rows.map((row) => ({
        connectionId: Number(row.connection_id),
        status: row.status,
        recordsReceived: row.records_received,
        rawRowsPersisted: row.raw_rows_persisted,
        aggregateRowsUpserted: row.aggregate_rows_upserted,
        errorCount: row.error_count,
        windowStartDate: row.window_start_date,
        windowEndDate: row.window_end_date
      })),
      [
        {
          connectionId: 1,
          status: 'completed',
          recordsReceived: 3,
          rawRowsPersisted: 4,
          aggregateRowsUpserted: 3,
          errorCount: 0,
          windowStartDate: '2026-04-28',
          windowEndDate: '2026-04-29'
        }
      ]
    );
  } finally {
    globalThis.fetch = previousFetch;
  }
});
