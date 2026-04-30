import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import type { AddressInfo } from 'node:net';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar_test';
process.env.REPORTING_API_TOKEN = 'test-reporting-token';
process.env.DEFAULT_ORGANIZATION_ID = '77';

const { pool } = await import('../src/db/pool.js');
const { closeServer, createServer } = await import('../src/server.js');
const { resetE2EDatabase } = await import('./e2e-harness.js');
const { buildRawPayloadFixture } = await import('./integration-test-helpers.js');

function buildInternalHeaders(extraHeaders: Record<string, string> = {}): Record<string, string> {
  return {
    authorization: 'Bearer test-reporting-token',
    ...extraHeaders
  };
}

function computeSessionDigest(token: string): string {
  return createHash('sha256').update(token).digest('hex');
}

async function requestJson(server: ReturnType<typeof createServer>, path: string, headers?: Record<string, string>) {
  const address = server.address() as AddressInfo;
  const response = await fetch(`http://127.0.0.1:${address.port}${path}`, { headers });
  const body = await response.json();

  return { response, body };
}

async function seedMetaConnection(adAccountId: string) {
  const rawAccountFixture = buildRawPayloadFixture(
    {
      id: adAccountId,
      name: `Account ${adAccountId}`,
      currency: 'USD'
    },
    adAccountId
  );

  const result = await pool.query<{ id: number }>(
    `
      INSERT INTO meta_ads_connections (
        ad_account_id,
        access_token_encrypted,
        account_currency,
        raw_account_data,
        raw_account_source,
        raw_account_received_at,
        raw_account_payload_size_bytes,
        raw_account_payload_hash,
        raw_account_external_id
      )
      VALUES (
        $1,
        '\\x01'::bytea,
        'USD',
        $2::jsonb,
        'meta_ads_account',
        '2026-04-29T15:55:00.000Z',
        $3,
        $4,
        $5
      )
      RETURNING id
    `,
    [
      adAccountId,
      rawAccountFixture.rawPayloadJson,
      rawAccountFixture.payloadSizeBytes,
      rawAccountFixture.payloadHash,
      rawAccountFixture.payloadExternalId
    ]
  );

  return result.rows[0].id;
}

async function seedSyncJob(connectionId: number, syncDate: string) {
  const result = await pool.query<{ id: number }>(
    `
      INSERT INTO meta_ads_sync_jobs (connection_id, sync_date)
      VALUES ($1, $2::date)
      RETURNING id
    `,
    [connectionId, syncDate]
  );

  return result.rows[0].id;
}

async function seedAggregateRow(params: {
  organizationId: number;
  connectionId: number;
  syncJobId: number;
  adAccountId: string;
  reportDate: string;
  campaignId: string;
  campaignName: string;
  attributedRevenue: number | null;
  purchaseCount: number | null;
  spend: number;
  purchaseRoas: number | null;
  canonicalActionType: string | null;
  canonicalSelectionMode?: 'priority' | 'fallback' | 'none';
  actionReportTime?: 'conversion' | 'impression' | 'mixed';
}) {
  await pool.query(
    `
      INSERT INTO meta_ads_order_value_aggregates (
        organization_id,
        meta_connection_id,
        sync_job_id,
        ad_account_id,
        report_date,
        raw_date_start,
        raw_date_stop,
        campaign_id,
        campaign_name,
        attributed_revenue,
        purchase_count,
        spend,
        purchase_roas,
        currency,
        canonical_action_type,
        canonical_selection_mode,
        raw_action_values,
        raw_actions,
        raw_revenue_record_ids,
        source_synced_at,
        action_report_time,
        use_account_attribution_setting
      )
      VALUES (
        $1,
        $2,
        $3,
        $4,
        $5::date,
        $5::date,
        $5::date,
        $6,
        $7,
        $8,
        $9,
        $10,
        $11,
        'USD',
        $12,
        $13,
        '[]'::jsonb,
        '[]'::jsonb,
        '[]'::jsonb,
        '2026-04-29T16:00:00.000Z',
        $14,
        true
      )
    `,
    [
      params.organizationId,
      params.connectionId,
      params.syncJobId,
      params.adAccountId,
      params.reportDate,
      params.campaignId,
      params.campaignName,
      params.attributedRevenue,
      params.purchaseCount,
      params.spend,
      params.purchaseRoas,
      params.canonicalActionType,
      params.canonicalSelectionMode ?? 'priority',
      params.actionReportTime ?? 'conversion'
    ]
  );
}

async function seedUserSession(token: string) {
  const userResult = await pool.query<{ id: number }>(
    `
      INSERT INTO app_users (
        email,
        password_hash,
        display_name,
        is_admin,
        status
      )
      VALUES (
        'analyst@example.com',
        'scrypt$fixture$fixture',
        'Analyst',
        false,
        'active'
      )
      RETURNING id
    `
  );

  await pool.query(
    `
      INSERT INTO app_sessions (
        user_id,
        token_digest,
        expires_at
      )
      VALUES (
        $1,
        $2,
        now() + interval '7 days'
      )
    `,
    [userResult.rows[0].id, computeSessionDigest(token)]
  );
}

async function seedMetaOrderValueData() {
  const connectionId = await seedMetaConnection('123456789');
  const april28SyncJobId = await seedSyncJob(connectionId, '2026-04-28');
  const april29SyncJobId = await seedSyncJob(connectionId, '2026-04-29');

  await seedAggregateRow({
    organizationId: 77,
    connectionId,
    syncJobId: april29SyncJobId,
    adAccountId: '123456789',
    reportDate: '2026-04-29',
    campaignId: 'cmp_2',
    campaignName: 'Retargeting US',
    attributedRevenue: 150,
    purchaseCount: 3,
    spend: 50,
    purchaseRoas: 3,
    canonicalActionType: 'purchase'
  });
  await seedAggregateRow({
    organizationId: 77,
    connectionId,
    syncJobId: april29SyncJobId,
    adAccountId: '123456789',
    reportDate: '2026-04-29',
    campaignId: 'cmp_1',
    campaignName: 'Prospecting US',
    attributedRevenue: 120,
    purchaseCount: 2,
    spend: 40,
    purchaseRoas: 3,
    canonicalActionType: 'omni_purchase',
    canonicalSelectionMode: 'fallback'
  });
  await seedAggregateRow({
    organizationId: 77,
    connectionId,
    syncJobId: april28SyncJobId,
    adAccountId: '123456789',
    reportDate: '2026-04-28',
    campaignId: 'cmp_1',
    campaignName: 'Prospecting US',
    attributedRevenue: 80,
    purchaseCount: 1,
    spend: 20,
    purchaseRoas: 4,
    canonicalActionType: 'purchase'
  });
  await seedAggregateRow({
    organizationId: 77,
    connectionId,
    syncJobId: april29SyncJobId,
    adAccountId: '123456789',
    reportDate: '2026-04-29',
    campaignId: 'cmp_2',
    campaignName: 'Retargeting US',
    attributedRevenue: 999,
    purchaseCount: 99,
    spend: 1,
    purchaseRoas: 999,
    canonicalActionType: 'purchase',
    actionReportTime: 'impression'
  });
  await seedAggregateRow({
    organizationId: 88,
    connectionId,
    syncJobId: april29SyncJobId,
    adAccountId: '123456789',
    reportDate: '2026-04-29',
    campaignId: 'cmp_9',
    campaignName: 'Other Org Campaign',
    attributedRevenue: 999,
    purchaseCount: 9,
    spend: 111,
    purchaseRoas: 9,
    canonicalActionType: 'purchase'
  });
}

test.beforeEach(async () => {
  await resetE2EDatabase();
});

test.after(async () => {
  await pool.end();
});

test('meta order value API requires authentication', async () => {
  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/reporting/meta-order-value?startDate=2026-04-28&endDate=2026-04-29'
    );

    assert.equal(response.status, 401);
    assert.equal(body.error, 'unauthorized');
  } finally {
    await closeServer(server);
  }
});

test('meta order value API scopes reads by organization and honors internal tenant overrides', async () => {
  await seedMetaOrderValueData();

  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/reporting/meta-order-value?startDate=2026-04-28&endDate=2026-04-29',
      buildInternalHeaders({
        'x-roas-radar-tenant-id': '88'
      })
    );

    assert.equal(response.status, 200);
    assert.deepEqual(body.scope, { organizationId: 88 });
    assert.deepEqual(body.totals, {
      attributedRevenue: 999,
      purchaseCount: 9,
      spend: 111,
      roas: 9
    });
    assert.equal(body.pagination.totalRows, 1);
    assert.deepEqual(body.rows, [
      {
        date: '2026-04-29',
        campaignId: 'cmp_9',
        campaignName: 'Other Org Campaign',
        attributedRevenue: 999,
        purchaseCount: 9,
        spend: 111,
        roas: 9,
        calculatedRoas: 9,
        canonicalActionType: 'purchase',
        canonicalSelectionMode: 'priority',
        currency: 'USD'
      }
    ]);
  } finally {
    await closeServer(server);
  }
});

test('meta order value API applies campaign filters, excludes non-conversion rows, and paginates with defaults', async () => {
  await seedMetaOrderValueData();
  const sessionToken = 'rrs_user_meta_order_value_token';
  await seedUserSession(sessionToken);

  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/reporting/meta-order-value?startDate=2026-04-28&endDate=2026-04-29&campaignSearch=Prospecting&limit=1',
      {
        authorization: `Bearer ${sessionToken}`
      }
    );

    assert.equal(response.status, 200);
    assert.deepEqual(body.scope, { organizationId: 77 });
    assert.deepEqual(body.sort, {
      by: 'reportDate',
      direction: 'desc'
    });
    assert.deepEqual(body.pagination, {
      limit: 1,
      offset: 0,
      returned: 1,
      totalRows: 2,
      hasMore: true
    });
    assert.deepEqual(body.totals, {
      attributedRevenue: 200,
      purchaseCount: 3,
      spend: 60,
      roas: 200 / 60
    });
    assert.deepEqual(body.rows, [
      {
        date: '2026-04-29',
        campaignId: 'cmp_1',
        campaignName: 'Prospecting US',
        attributedRevenue: 120,
        purchaseCount: 2,
        spend: 40,
        roas: 3,
        calculatedRoas: 3,
        canonicalActionType: 'omni_purchase',
        canonicalSelectionMode: 'fallback',
        currency: 'USD'
      }
    ]);
  } finally {
    await closeServer(server);
  }
});

test('meta order value API rejects tenant overrides for authenticated user sessions and supports action-type sorting filters', async () => {
  await seedMetaOrderValueData();
  const sessionToken = 'rrs_user_meta_order_value_override_token';
  await seedUserSession(sessionToken);

  const server = createServer();

  try {
    const forbidden = await requestJson(
      server,
      '/api/reporting/meta-order-value?startDate=2026-04-28&endDate=2026-04-29',
      {
        authorization: `Bearer ${sessionToken}`,
        'x-roas-radar-tenant-id': '88'
      }
    );

    assert.equal(forbidden.response.status, 403);
    assert.equal(forbidden.body.error, 'tenant_override_forbidden');

    const filtered = await requestJson(
      server,
      '/api/reporting/meta-order-value?startDate=2026-04-28&endDate=2026-04-29&actionType=purchase&sortBy=attributedRevenue&sortDirection=asc',
      {
        authorization: `Bearer ${sessionToken}`
      }
    );

    assert.equal(filtered.response.status, 200);
    assert.deepEqual(
      filtered.body.rows.map((row: { date: string; campaignId: string; attributedRevenue: number | null }) => ({
        date: row.date,
        campaignId: row.campaignId,
        attributedRevenue: row.attributedRevenue
      })),
      [
        {
          date: '2026-04-28',
          campaignId: 'cmp_1',
          attributedRevenue: 80
        },
        {
          date: '2026-04-29',
          campaignId: 'cmp_2',
          attributedRevenue: 150
        }
      ]
    );
    assert.deepEqual(filtered.body.totals, {
      attributedRevenue: 230,
      purchaseCount: 4,
      spend: 70,
      roas: 230 / 70
    });
  } finally {
    await closeServer(server);
  }
});
