import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar_test';
process.env.META_ADS_APP_ID ??= 'meta-app-id';
process.env.META_ADS_APP_SECRET ??= 'meta-app-secret';
process.env.META_ADS_APP_BASE_URL ??= 'https://api.example.com';
process.env.META_ADS_APP_SCOPES ??= 'ads_read,business_management';
process.env.META_ADS_ENCRYPTION_KEY ??= 'meta-encryption-key';
process.env.META_ADS_AD_ACCOUNT_ID ??= 'act_123456789';
process.env.GOOGLE_ADS_CLIENT_ID ??= 'google-client-id';
process.env.GOOGLE_ADS_CLIENT_SECRET ??= 'google-client-secret';
process.env.GOOGLE_ADS_DEVELOPER_TOKEN ??= 'google-developer-token';
process.env.GOOGLE_ADS_APP_BASE_URL ??= 'https://api.example.com';
process.env.GOOGLE_ADS_APP_SCOPES ??= 'https://www.googleapis.com/auth/adwords';
process.env.GOOGLE_ADS_ENCRYPTION_KEY ??= 'google-ads-encryption-key';

const { __rawPayloadStorageTestUtils } = await import('../src/shared/raw-payload-storage.js');
const { pool } = await import('../src/db/pool.js');
const { resetE2EDatabase } = await import('./e2e-harness.js');
const { processMetaAdsSyncQueue } = await import('../src/modules/meta-ads/index.js');
const { processGoogleAdsSyncQueue } = await import('../src/modules/google-ads/index.js');

function buildLargeText(prefix: string): string {
  return `${prefix}-${'y'.repeat(10_000)}`;
}

function assertExactRawPayloadInvariant(params: {
  persistedRawPayload: Record<string, unknown>;
  expectedRawPayload: Record<string, unknown>;
  unmappedPathValue: unknown;
  expectedUnmappedPathValue: unknown;
  normalizedVariant: Record<string, unknown>;
  subsetVariant: Record<string, unknown>;
  enrichedVariant: Record<string, unknown>;
}) {
  assert.deepEqual(params.persistedRawPayload, params.expectedRawPayload);
  assert.deepEqual(params.unmappedPathValue, params.expectedUnmappedPathValue);
  assert.notDeepEqual(params.persistedRawPayload, params.normalizedVariant);
  assert.notDeepEqual(params.persistedRawPayload, params.subsetVariant);
  assert.notDeepEqual(params.persistedRawPayload, params.enrichedVariant);
}

function buildMetaAccountFixture() {
  return {
    id: '123456789',
    name: 'Meta Account',
    currency: 'USD',
    account_status: 1,
    nested_debug: {
      unknownField: true,
      nullableValue: null,
      arrays: ['alpha', { beta: ['gamma', null] }]
    },
    oversized_notes: [buildLargeText('meta-account')]
  };
}

function buildMetaInsightFixture(level: 'account' | 'campaign' | 'adset' | 'ad') {
  return {
    account_id: '123456789',
    account_name: 'Meta Account',
    ...(level === 'account' ? {} : { campaign_id: 'cmp_1', campaign_name: 'Campaign One' }),
    ...(level === 'adset' || level === 'ad' ? { adset_id: 'adset_1', adset_name: 'Adset One' } : {}),
    ...(level === 'ad' ? { ad_id: 'ad_1', ad_name: 'Ad One' } : {}),
    spend: '12.34',
    impressions: '100',
    clicks: '5',
    objective: 'OUTCOME_TRAFFIC',
    date_start: '2026-04-11',
    date_stop: '2026-04-11',
    source_debug: {
      level,
      untouched: true,
      nested: {
        kept: ['raw', null, { marker: level }]
      }
    },
    action_values: [
      { action_type: 'purchase', value: '1.00' },
      { action_type: 'landing_page_view', value: null }
    ],
    oversized_blob: buildLargeText(`meta-${level}`)
  };
}

function buildMetaMalformedAdInsightFixture() {
  const row = {
    ...buildMetaInsightFixture('ad'),
    ad_name: 'Missing Entity Id Ad',
    malformed_debug: {
      kept: true
    }
  };

  Reflect.deleteProperty(row, 'ad_id');

  return row;
}

function buildMetaOrderValueInsightFixture(params: {
  actionType: string;
  campaignId?: string;
  campaignName?: string;
  revenue?: string | null;
  purchaseCount?: string | null;
}) {
  return {
    campaign_id: params.campaignId ?? 'cmp_1',
    campaign_name: params.campaignName ?? 'Campaign One',
    date_start: '2026-04-11',
    date_stop: '2026-04-11',
    spend: '12.34',
    action_type: params.actionType,
    source_debug: {
      actionType: params.actionType,
      nested: {
        kept: ['raw', null, { marker: params.actionType }]
      }
    },
    actions: params.purchaseCount === null ? [] : [{ action_type: params.actionType, value: params.purchaseCount ?? '2' }],
    action_values: params.revenue === null ? [] : [{ action_type: params.actionType, value: params.revenue ?? '39.50' }],
    purchase_roas: [{ action_type: params.actionType, value: '3.200000' }],
    oversized_blob: buildLargeText(`meta-order-value-${params.actionType}`)
  };
}

function buildGoogleCustomerFixture() {
  return {
    customer: {
      id: '1234567890',
      descriptiveName: 'Main Account',
      currencyCode: 'USD'
    },
    untouched_customer_debug: {
      kept: true,
      nested: {
        values: ['first', null, { deep: 'value' }]
      }
    },
    oversized_blob: buildLargeText('google-customer')
  };
}

function buildGoogleCampaignFixture() {
  return {
    customer: {
      id: '1234567890',
      descriptiveName: 'Main Account',
      currencyCode: 'USD'
    },
    campaign: {
      id: 'cmp_1',
      name: 'Brand Search'
    },
    metrics: {
      costMicros: '12340000',
      impressions: '100',
      clicks: '5'
    },
    segments: {
      date: '2026-04-11'
    },
    untouched_campaign_debug: {
      kept: true,
      labels: ['branded', 'exact']
    },
    null_debug: null,
    oversized_blob: buildLargeText('google-campaign')
  };
}

function buildGoogleMalformedCampaignFixture() {
  const row = {
    ...buildGoogleCampaignFixture(),
    campaign: {
      name: 'Missing Campaign Id'
    },
    malformed_debug: {
      kept: true
    }
  };

  Reflect.deleteProperty(row.campaign, 'id');

  return row;
}

function buildGoogleAdFixture() {
  return {
    customer: {
      id: '1234567890',
      descriptiveName: 'Main Account',
      currencyCode: 'USD'
    },
    campaign: {
      id: 'cmp_1',
      name: 'Brand Search'
    },
    adGroup: {
      id: 'adgroup_1',
      name: 'Ad Group One'
    },
    adGroupAd: {
      ad: {
        id: 'ad_1',
        name: 'Headline A',
        resourceName: 'customers/1234567890/adGroupAds/1'
      }
    },
    metrics: {
      costMicros: '2500000',
      impressions: '40',
      clicks: '2'
    },
    segments: {
      date: '2026-04-11'
    },
    untouched_ad_debug: {
      kept: true,
      nested: [{ asset: 'image-1' }, null, { asset: 'image-2' }]
    },
    oversized_blob: buildLargeText('google-ad')
  };
}

function buildGoogleMalformedAdFixture() {
  const row = {
    ...buildGoogleAdFixture(),
    adGroupAd: {
      ad: {
        name: 'Missing Ad Id',
        resourceName: 'customers/1234567890/adGroupAds/malformed'
      }
    },
    malformed_debug: {
      kept: true
    }
  };

  Reflect.deleteProperty(row.adGroupAd.ad, 'id');

  return row;
}

type SeededMetaConnection = {
  connectionId: number;
};

type SeededGoogleConnection = {
  connectionId: number;
};

async function loadMetaRawPersistence(connectionId: number) {
  const [connectionResult, rawResult, dailyResult, orderValueRawResult, orderValueAggregateResult] = await Promise.all([
    pool.query<{
      raw_account_data: Record<string, unknown>;
      raw_account_source: string;
      raw_account_external_id: string | null;
      raw_account_payload_size_bytes: number;
      raw_account_payload_hash: string;
    }>(
      `
        SELECT
          raw_account_data,
          raw_account_source,
          raw_account_external_id,
          raw_account_payload_size_bytes,
          raw_account_payload_hash
        FROM meta_ads_connections
        WHERE id = $1
      `,
      [connectionId]
    ),
    pool.query<{
      level: string;
      raw_payload: Record<string, unknown>;
      payload_source: string;
      payload_external_id: string | null;
      payload_size_bytes: number;
      payload_hash: string;
    }>(
      `
        SELECT level, raw_payload, payload_source, payload_external_id, payload_size_bytes, payload_hash
        FROM meta_ads_raw_spend_records
        WHERE connection_id = $1
        ORDER BY id ASC
      `,
      [connectionId]
    ),
    pool.query<{
      granularity: string;
      entity_key: string;
      raw_payload: Record<string, unknown>;
    }>(
      `
        SELECT granularity, entity_key, raw_payload
        FROM meta_ads_daily_spend
        WHERE connection_id = $1
        ORDER BY id ASC
      `,
      [connectionId]
    ),
    pool.query<{ count: string }>(
      'SELECT count(*)::text AS count FROM meta_ads_order_value_raw_records WHERE connection_id = $1',
      [connectionId]
    ),
    pool.query<{ count: string }>(
      'SELECT count(*)::text AS count FROM meta_ads_order_value_aggregates WHERE meta_connection_id = $1',
      [connectionId]
    )
  ]);

  return {
    connection: connectionResult.rows[0] ?? null,
    rawRows: rawResult.rows,
    dailyRows: dailyResult.rows,
    orderValueRawCount: Number(orderValueRawResult.rows[0]?.count ?? '0'),
    orderValueAggregateCount: Number(orderValueAggregateResult.rows[0]?.count ?? '0')
  };
}

async function loadGoogleRawPersistence(connectionId: number) {
  const [connectionResult, spendResult] = await Promise.all([
    pool.query<{
      raw_customer_data: Record<string, unknown>;
      raw_customer_source: string;
      raw_customer_external_id: string | null;
      raw_customer_payload_size_bytes: number;
      raw_customer_payload_hash: string;
    }>(
      `
        SELECT
          raw_customer_data,
          raw_customer_source,
          raw_customer_external_id,
          raw_customer_payload_size_bytes,
          raw_customer_payload_hash
        FROM google_ads_connections
        WHERE id = $1
      `,
      [connectionId]
    ),
    pool.query<{
      level: string;
      raw_payload: Record<string, unknown>;
      payload_source: string;
      payload_external_id: string | null;
      payload_size_bytes: number;
      payload_hash: string;
    }>(
      `
        SELECT level, raw_payload, payload_source, payload_external_id, payload_size_bytes, payload_hash
        FROM google_ads_raw_spend_records
        WHERE connection_id = $1
        ORDER BY id ASC
      `,
      [connectionId]
    )
  ]);

  return {
    connection: connectionResult.rows[0] ?? null,
    spendRows: spendResult.rows
  };
}

async function loadAdProjectionCounts(params: {
  metaConnectionId?: number;
  googleConnectionId?: number;
}) {
  const [metaResult, googleResult] = await Promise.all([
    params.metaConnectionId
      ? pool.query<{ count: string }>(
          'SELECT count(*)::text AS count FROM meta_ads_daily_spend WHERE connection_id = $1',
          [params.metaConnectionId]
        )
      : Promise.resolve({ rows: [{ count: '0' }] }),
    params.googleConnectionId
      ? pool.query<{ count: string }>(
          'SELECT count(*)::text AS count FROM google_ads_daily_spend WHERE connection_id = $1',
          [params.googleConnectionId]
        )
      : Promise.resolve({ rows: [{ count: '0' }] })
  ]);

  return {
    metaDailyCount: Number(metaResult.rows[0]?.count ?? '0'),
    googleDailyCount: Number(googleResult.rows[0]?.count ?? '0')
  };
}

async function seedMetaSyncJob(): Promise<SeededMetaConnection> {
  const rawAccountData = buildMetaAccountFixture();
  const rawAccountJson = JSON.stringify(rawAccountData);

  const connectionResult = await pool.query<{ id: number }>(
    `
      INSERT INTO meta_ads_connections (
        ad_account_id,
        access_token_encrypted,
        token_type,
        granted_scopes,
        last_sync_planned_for,
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
        '2026-04-11'::date,
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

  const connectionId = connectionResult.rows[0]?.id;
  assert.ok(connectionId);

  const syncJobResult = await pool.query<{ id: number }>(
    `
      INSERT INTO meta_ads_sync_jobs (
        connection_id,
        sync_date,
        status,
        available_at,
        updated_at
      )
      VALUES ($1, '2026-04-11'::date, 'pending', now(), now())
      RETURNING id
    `,
    [connectionId]
  );

  assert.ok(syncJobResult.rows[0]?.id);

  return { connectionId };
}

async function seedGoogleSyncJob(): Promise<SeededGoogleConnection> {
  const rawCustomerData = buildGoogleCustomerFixture();
  const rawCustomerJson = JSON.stringify(rawCustomerData);

  const connectionResult = await pool.query<{ id: number }>(
    `
      INSERT INTO google_ads_connections (
        customer_id,
        login_customer_id,
        developer_token_encrypted,
        client_id,
        client_secret_encrypted,
        refresh_token_encrypted,
        token_scopes,
        last_sync_planned_for,
        last_sync_completed_at,
        status,
        customer_descriptive_name,
        currency_code,
        raw_customer_data,
        raw_customer_source,
        raw_customer_received_at,
        raw_customer_external_id,
        raw_customer_payload_size_bytes,
        raw_customer_payload_hash
      )
      VALUES (
        '1234567890',
        NULL,
        pgp_sym_encrypt($1, $4, 'cipher-algo=aes256, compress-algo=0'),
        $2,
        pgp_sym_encrypt($3, $4, 'cipher-algo=aes256, compress-algo=0'),
        pgp_sym_encrypt($5, $4, 'cipher-algo=aes256, compress-algo=0'),
        ARRAY['https://www.googleapis.com/auth/adwords']::text[],
        '2026-04-11'::date,
        NULL,
        'active',
        'Main Account',
        'USD',
        $6::jsonb,
        'google_ads_customer',
        now(),
        '1234567890',
        $7,
        $8
      )
      RETURNING id
    `,
    [
      process.env.GOOGLE_ADS_DEVELOPER_TOKEN,
      process.env.GOOGLE_ADS_CLIENT_ID,
      process.env.GOOGLE_ADS_CLIENT_SECRET,
      process.env.GOOGLE_ADS_ENCRYPTION_KEY,
      'google-refresh-token',
      rawCustomerJson,
      Buffer.byteLength(rawCustomerJson, 'utf8'),
      createHash('sha256').update(rawCustomerJson).digest('hex')
    ]
  );

  const connectionId = connectionResult.rows[0]?.id;
  assert.ok(connectionId);

  const syncJobResult = await pool.query<{ id: number }>(
    `
      INSERT INTO google_ads_sync_jobs (
        connection_id,
        sync_date,
        status,
        available_at,
        updated_at
      )
      VALUES ($1, '2026-04-11'::date, 'pending', now(), now())
      RETURNING id
    `,
    [connectionId]
  );

  assert.ok(syncJobResult.rows[0]?.id);

  return { connectionId };
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

test('Meta Ads and Google Ads sync preserve raw payloads without trimming', { concurrency: false }, async () => {
  const previousFetch = globalThis.fetch;

  try {
    const metaSeed = await seedMetaSyncJob();
    const expectedMetaAccount = buildMetaAccountFixture();

    globalThis.fetch = (async (input: string | URL | Request) => {
      const url = new URL(typeof input === 'string' ? input : input instanceof URL ? input.toString() : input.url);

      if (url.pathname.endsWith('/insights')) {
        const level = url.searchParams.get('level');
        const fixtures =
          level === 'account'
            ? [buildMetaInsightFixture('account')]
            : level === 'campaign'
              ? [buildMetaInsightFixture('campaign')]
              : level === 'adset'
                ? [buildMetaInsightFixture('adset')]
                : [buildMetaInsightFixture('ad'), buildMetaMalformedAdInsightFixture()];
        return new Response(
          JSON.stringify({
            data: fixtures,
            paging: {},
            extra_page_field: {
              kept: level ?? 'unknown'
            }
          }),
          { status: 200, headers: { 'content-type': 'application/json' } }
        );
      }

      if (url.searchParams.get('ids') === 'ad_1' && url.searchParams.get('fields') === 'creative{id,name}') {
        return new Response(
          JSON.stringify({
            ad_1: {
              creative: {
                id: 'creative_1',
                name: 'Ad One Creative'
              }
            }
          }),
          { status: 200, headers: { 'content-type': 'application/json' } }
        );
      }

      throw new Error(`Unexpected Meta Ads fetch ${url.toString()}`);
    }) as typeof globalThis.fetch;

    const result = await processMetaAdsSyncQueue({
      limit: 1,
      now: new Date('2026-04-11T12:00:00.000Z')
    });

    assert.equal(result.succeededJobs, 1);
    const persisted = await loadMetaRawPersistence(metaSeed.connectionId);
    const projectionCountsAfterMeta = await loadAdProjectionCounts({ metaConnectionId: metaSeed.connectionId });
    assertExactRawPayloadInvariant({
      persistedRawPayload: persisted.connection?.raw_account_data ?? {},
      expectedRawPayload: expectedMetaAccount,
      unmappedPathValue: (
        (persisted.connection?.raw_account_data ?? {}) as {
          nested_debug: { arrays: Array<string | { beta: Array<string | null> }> };
        }
      ).nested_debug.arrays[1],
      expectedUnmappedPathValue: expectedMetaAccount.nested_debug.arrays[1],
      normalizedVariant: {
        ...expectedMetaAccount,
        name: 'meta account'
      },
      subsetVariant: {
        id: expectedMetaAccount.id,
        name: expectedMetaAccount.name,
        currency: expectedMetaAccount.currency
      },
      enrichedVariant: {
        ...expectedMetaAccount,
        canonical_source: 'meta'
      }
    });
    assert.equal(persisted.connection?.raw_account_source, 'meta_ads_account');
    assert.equal(persisted.connection?.raw_account_external_id, '123456789');
    assert.equal(persisted.rawRows.length, 5);
    assert.equal(persisted.dailyRows.length, 5);
    assert.equal(projectionCountsAfterMeta.metaDailyCount, 5);
    assert.equal(persisted.orderValueRawCount, 0);
    assert.equal(persisted.orderValueAggregateCount, 0);
    const expectedAccountInsight = buildMetaInsightFixture('account');
    const expectedCampaignInsight = buildMetaInsightFixture('campaign');
    const expectedAdsetInsight = buildMetaInsightFixture('adset');
    const expectedAdInsight = buildMetaInsightFixture('ad');
    const expectedMalformedAdInsight = buildMetaMalformedAdInsightFixture();
    assertExactRawPayloadInvariant({
      persistedRawPayload: persisted.rawRows[0].raw_payload,
      expectedRawPayload: expectedAccountInsight,
      unmappedPathValue: (
        persisted.rawRows[0].raw_payload as {
          source_debug: { nested: { kept: Array<string | null | { marker: string }> } };
        }
      ).source_debug.nested.kept[2],
      expectedUnmappedPathValue: expectedAccountInsight.source_debug.nested.kept[2],
      normalizedVariant: {
        ...expectedAccountInsight,
        account_name: 'meta account'
      },
      subsetVariant: {
        account_id: expectedAccountInsight.account_id,
        account_name: expectedAccountInsight.account_name,
        spend: expectedAccountInsight.spend,
        date_start: expectedAccountInsight.date_start,
        date_stop: expectedAccountInsight.date_stop
      },
      enrichedVariant: {
        ...expectedAccountInsight,
        granularity: 'account'
      }
    });
    assertExactRawPayloadInvariant({
      persistedRawPayload: persisted.rawRows[1].raw_payload,
      expectedRawPayload: expectedCampaignInsight,
      unmappedPathValue: (
        persisted.rawRows[1].raw_payload as {
          source_debug: { nested: { kept: Array<string | null | { marker: string }> } };
        }
      ).source_debug.nested.kept[2],
      expectedUnmappedPathValue: expectedCampaignInsight.source_debug.nested.kept[2],
      normalizedVariant: {
        ...expectedCampaignInsight,
        campaign_name: 'campaign one'
      },
      subsetVariant: {
        campaign_id: expectedCampaignInsight.campaign_id,
        spend: expectedCampaignInsight.spend
      },
      enrichedVariant: {
        ...expectedCampaignInsight,
        granularity: 'campaign'
      }
    });
    assert.deepEqual(
      persisted.rawRows.map((row) => row.level),
      ['account', 'campaign', 'adset', 'ad', 'ad']
    );
    assert.deepEqual(
      persisted.dailyRows.map((row) => row.granularity),
      ['account', 'campaign', 'adset', 'ad', 'creative']
    );
    assert.deepEqual(
      persisted.rawRows.map((row) => row.payload_external_id),
      ['123456789', 'cmp_1', 'adset_1', 'ad_1', null]
    );
    assert.equal(persisted.rawRows[3].payload_hash.length, 64);
    assert.equal(persisted.rawRows[4].payload_hash.length, 64);
    assertExactRawPayloadInvariant({
      persistedRawPayload: persisted.rawRows[2].raw_payload,
      expectedRawPayload: expectedAdsetInsight,
      unmappedPathValue: (
        persisted.rawRows[2].raw_payload as {
          source_debug: { nested: { kept: Array<string | null | { marker: string }> } };
        }
      ).source_debug.nested.kept[2],
      expectedUnmappedPathValue: expectedAdsetInsight.source_debug.nested.kept[2],
      normalizedVariant: {
        ...expectedAdsetInsight,
        adset_name: 'adset one'
      },
      subsetVariant: {
        adset_id: expectedAdsetInsight.adset_id,
        spend: expectedAdsetInsight.spend
      },
      enrichedVariant: {
        ...expectedAdsetInsight,
        granularity: 'adset'
      }
    });
    assertExactRawPayloadInvariant({
      persistedRawPayload: persisted.rawRows[3].raw_payload,
      expectedRawPayload: expectedAdInsight,
      unmappedPathValue: (
        persisted.rawRows[3].raw_payload as {
          source_debug: { nested: { kept: Array<string | null | { marker: string }> } };
        }
      ).source_debug.nested.kept[2],
      expectedUnmappedPathValue: expectedAdInsight.source_debug.nested.kept[2],
      normalizedVariant: {
        ...expectedAdInsight,
        ad_name: 'ad one'
      },
      subsetVariant: {
        ad_id: expectedAdInsight.ad_id,
        spend: expectedAdInsight.spend
      },
      enrichedVariant: {
        ...expectedAdInsight,
        granularity: 'ad'
      }
    });
    assertExactRawPayloadInvariant({
      persistedRawPayload: persisted.rawRows[4].raw_payload,
      expectedRawPayload: expectedMalformedAdInsight,
      unmappedPathValue: (
        persisted.rawRows[4].raw_payload as {
          malformed_debug: { kept: boolean };
        }
      ).malformed_debug,
      expectedUnmappedPathValue: expectedMalformedAdInsight.malformed_debug,
      normalizedVariant: {
        ...expectedMalformedAdInsight,
        ad_name: 'missing entity id ad'
      },
      subsetVariant: {
        ad_name: expectedMalformedAdInsight.ad_name,
        spend: expectedMalformedAdInsight.spend
      },
      enrichedVariant: {
        ...expectedMalformedAdInsight,
        granularity: 'ad'
      }
    });

    await resetE2EDatabase();
    const googleSeed = await seedGoogleSyncJob();
    const expectedGoogleCustomer = buildGoogleCustomerFixture();

    globalThis.fetch = (async (input: string | URL | Request, init?: RequestInit) => {
      const url = new URL(typeof input === 'string' ? input : input instanceof URL ? input.toString() : input.url);

      if (url.toString() === 'https://oauth2.googleapis.com/token') {
        return new Response(JSON.stringify({ access_token: 'google-access-token', expires_in: 3600, token_type: 'Bearer' }), {
          status: 200,
          headers: { 'content-type': 'application/json' }
        });
      }

      if (url.pathname.endsWith('/googleAds:searchStream')) {
        const body = JSON.parse(String(init?.body ?? '{}')) as { query?: string };

        if (body.query?.includes('FROM customer')) {
          return new Response(
            JSON.stringify([
              {
                results: [buildGoogleCustomerFixture()]
              }
            ]),
            { status: 200, headers: { 'content-type': 'application/json' } }
          );
        }

        if (body.query?.includes('FROM campaign')) {
          return new Response(
            JSON.stringify([
              {
                results: [buildGoogleCampaignFixture(), buildGoogleMalformedCampaignFixture()]
              }
            ]),
            { status: 200, headers: { 'content-type': 'application/json' } }
          );
        }

        if (body.query?.includes('FROM ad_group_ad')) {
          return new Response(
            JSON.stringify([
              {
                results: [buildGoogleAdFixture(), buildGoogleMalformedAdFixture()]
              }
            ]),
            { status: 200, headers: { 'content-type': 'application/json' } }
          );
        }
      }

      throw new Error(`Unexpected Google Ads fetch ${url.toString()}`);
    }) as typeof globalThis.fetch;

    const googleResult = await processGoogleAdsSyncQueue({
      limit: 1,
      now: new Date('2026-04-11T12:00:00.000Z')
    });

    assert.equal(googleResult.succeededJobs, 1);
    const googlePersisted = await loadGoogleRawPersistence(googleSeed.connectionId);
    const projectionCountsAfterGoogle = await loadAdProjectionCounts({ googleConnectionId: googleSeed.connectionId });
    assertExactRawPayloadInvariant({
      persistedRawPayload: googlePersisted.connection?.raw_customer_data ?? {},
      expectedRawPayload: expectedGoogleCustomer,
      unmappedPathValue: (
        (googlePersisted.connection?.raw_customer_data ?? {}) as {
          untouched_customer_debug: { nested: { values: Array<string | null | { deep: string }> } };
        }
      ).untouched_customer_debug.nested.values[2],
      expectedUnmappedPathValue: expectedGoogleCustomer.untouched_customer_debug.nested.values[2],
      normalizedVariant: {
        ...expectedGoogleCustomer,
        customer: {
          ...expectedGoogleCustomer.customer,
          descriptiveName: 'main account'
        }
      },
      subsetVariant: {
        customer: expectedGoogleCustomer.customer
      },
      enrichedVariant: {
        ...expectedGoogleCustomer,
        canonical_source: 'google'
      }
    });
    assert.equal(googlePersisted.connection?.raw_customer_source, 'google_ads_customer');
    assert.equal(googlePersisted.connection?.raw_customer_external_id, '1234567890');
    assert.equal(googlePersisted.spendRows.length, 4);
    assert.equal(projectionCountsAfterGoogle.googleDailyCount, 5);
    const expectedGoogleCampaign = buildGoogleCampaignFixture();
    const expectedGoogleAd = buildGoogleAdFixture();
    const expectedMalformedGoogleCampaign = buildGoogleMalformedCampaignFixture();
    const expectedMalformedGoogleAd = buildGoogleMalformedAdFixture();
    const expectedGoogleCampaignMetadata = __rawPayloadStorageTestUtils.buildRawPayloadStorageMetadata(expectedGoogleCampaign);
    const expectedGoogleAdMetadata = __rawPayloadStorageTestUtils.buildRawPayloadStorageMetadata(expectedGoogleAd);
    const expectedMalformedGoogleCampaignMetadata =
      __rawPayloadStorageTestUtils.buildRawPayloadStorageMetadata(expectedMalformedGoogleCampaign);
    const expectedMalformedGoogleAdMetadata = __rawPayloadStorageTestUtils.buildRawPayloadStorageMetadata(expectedMalformedGoogleAd);
    assertExactRawPayloadInvariant({
      persistedRawPayload: googlePersisted.spendRows[0].raw_payload,
      expectedRawPayload: expectedGoogleCampaign,
      unmappedPathValue: (
        googlePersisted.spendRows[0].raw_payload as {
          untouched_campaign_debug: { labels: string[] };
        }
      ).untouched_campaign_debug.labels,
      expectedUnmappedPathValue: expectedGoogleCampaign.untouched_campaign_debug.labels,
      normalizedVariant: {
        ...expectedGoogleCampaign,
        campaign: {
          ...expectedGoogleCampaign.campaign,
          name: 'brand search'
        }
      },
      subsetVariant: {
        customer: expectedGoogleCampaign.customer,
        campaign: expectedGoogleCampaign.campaign,
        metrics: expectedGoogleCampaign.metrics,
        segments: expectedGoogleCampaign.segments
      },
      enrichedVariant: {
        ...expectedGoogleCampaign,
        canonical_campaign: 'Brand Search'
      }
    });
    assert.equal(googlePersisted.spendRows[0].payload_source, 'google_ads_api');
    assert.equal(googlePersisted.spendRows[0].payload_external_id, 'cmp_1');
    assert.equal(googlePersisted.spendRows[0].payload_size_bytes, expectedGoogleCampaignMetadata.payloadSizeBytes);
    assert.equal(googlePersisted.spendRows[0].payload_hash, expectedGoogleCampaignMetadata.payloadHash);
    assertExactRawPayloadInvariant({
      persistedRawPayload: googlePersisted.spendRows[1].raw_payload,
      expectedRawPayload: expectedMalformedGoogleCampaign,
      unmappedPathValue: (
        googlePersisted.spendRows[1].raw_payload as {
          malformed_debug: { kept: boolean };
        }
      ).malformed_debug,
      expectedUnmappedPathValue: expectedMalformedGoogleCampaign.malformed_debug,
      normalizedVariant: {
        ...expectedMalformedGoogleCampaign,
        campaign: {
          ...expectedMalformedGoogleCampaign.campaign,
          name: 'missing campaign id'
        }
      },
      subsetVariant: {
        customer: expectedMalformedGoogleCampaign.customer,
        campaign: expectedMalformedGoogleCampaign.campaign
      },
      enrichedVariant: {
        ...expectedMalformedGoogleCampaign,
        roas_radar_debug: true
      }
    });
    assert.equal(googlePersisted.spendRows[1].payload_source, 'google_ads_api');
    assert.equal(googlePersisted.spendRows[1].payload_external_id, null);
    assert.equal(googlePersisted.spendRows[1].payload_size_bytes, expectedMalformedGoogleCampaignMetadata.payloadSizeBytes);
    assert.equal(googlePersisted.spendRows[1].payload_hash, expectedMalformedGoogleCampaignMetadata.payloadHash);
    assertExactRawPayloadInvariant({
      persistedRawPayload: googlePersisted.spendRows[2].raw_payload,
      expectedRawPayload: expectedGoogleAd,
      unmappedPathValue: (
        googlePersisted.spendRows[2].raw_payload as {
          untouched_ad_debug: { nested: Array<{ asset: string } | null> };
        }
      ).untouched_ad_debug.nested[2],
      expectedUnmappedPathValue: expectedGoogleAd.untouched_ad_debug.nested[2],
      normalizedVariant: {
        ...expectedGoogleAd,
        adGroupAd: {
          ...expectedGoogleAd.adGroupAd,
          ad: {
            ...expectedGoogleAd.adGroupAd.ad,
            name: 'headline a'
          }
        }
      },
      subsetVariant: {
        customer: expectedGoogleAd.customer,
        campaign: expectedGoogleAd.campaign,
        adGroup: expectedGoogleAd.adGroup,
        adGroupAd: expectedGoogleAd.adGroupAd,
        metrics: expectedGoogleAd.metrics
      },
      enrichedVariant: {
        ...expectedGoogleAd,
        canonical_medium: 'cpc'
      }
    });
    assert.equal(googlePersisted.spendRows[2].payload_source, 'google_ads_api');
    assert.equal(googlePersisted.spendRows[2].payload_external_id, 'ad_1');
    assert.equal(googlePersisted.spendRows[2].payload_size_bytes, expectedGoogleAdMetadata.payloadSizeBytes);
    assert.equal(googlePersisted.spendRows[2].payload_hash, expectedGoogleAdMetadata.payloadHash);
    assertExactRawPayloadInvariant({
      persistedRawPayload: googlePersisted.spendRows[3].raw_payload,
      expectedRawPayload: expectedMalformedGoogleAd,
      unmappedPathValue: (
        googlePersisted.spendRows[3].raw_payload as {
          malformed_debug: { kept: boolean };
        }
      ).malformed_debug,
      expectedUnmappedPathValue: expectedMalformedGoogleAd.malformed_debug,
      normalizedVariant: {
        ...expectedMalformedGoogleAd,
        adGroupAd: {
          ...expectedMalformedGoogleAd.adGroupAd,
          ad: {
            ...expectedMalformedGoogleAd.adGroupAd.ad,
            name: 'missing ad id'
          }
        }
      },
      subsetVariant: {
        customer: expectedMalformedGoogleAd.customer,
        campaign: expectedMalformedGoogleAd.campaign,
        adGroupAd: expectedMalformedGoogleAd.adGroupAd
      },
      enrichedVariant: {
        ...expectedMalformedGoogleAd,
        roas_radar_debug: true
      }
    });
    assert.equal(googlePersisted.spendRows[3].payload_source, 'google_ads_api');
    assert.equal(googlePersisted.spendRows[3].payload_external_id, null);
    assert.equal(googlePersisted.spendRows[3].payload_size_bytes, expectedMalformedGoogleAdMetadata.payloadSizeBytes);
    assert.equal(googlePersisted.spendRows[3].payload_hash, expectedMalformedGoogleAdMetadata.payloadHash);
  } finally {
    globalThis.fetch = previousFetch;
  }
});
