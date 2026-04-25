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

  delete (row as { ad_id?: string }).ad_id;

  return row;
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

  delete (row.campaign as { id?: string }).id;

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

  delete (row.adGroupAd.ad as { id?: string }).id;

  return row;
}

async function loadMetaRawPersistence() {
  const [connectionResult, spendResult] = await Promise.all([
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
        WHERE id = 1
      `
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
        WHERE connection_id = 1
        ORDER BY id ASC
      `
    )
  ]);

  return {
    connection: connectionResult.rows[0] ?? null,
    spendRows: spendResult.rows
  };
}

async function loadGoogleRawPersistence() {
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
        WHERE id = 1
      `
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
        WHERE connection_id = 1
        ORDER BY id ASC
      `
    )
  ]);

  return {
    connection: connectionResult.rows[0] ?? null,
    spendRows: spendResult.rows
  };
}

async function loadAdProjectionCounts() {
  const [metaResult, googleResult] = await Promise.all([
    pool.query<{ count: string }>('SELECT count(*)::text AS count FROM meta_ads_daily_spend WHERE connection_id = 1'),
    pool.query<{ count: string }>('SELECT count(*)::text AS count FROM google_ads_daily_spend WHERE connection_id = 1')
  ]);

  return {
    metaDailyCount: Number(metaResult.rows[0]?.count ?? '0'),
    googleDailyCount: Number(googleResult.rows[0]?.count ?? '0')
  };
}

async function seedMetaSyncJob(): Promise<void> {
  const rawAccountData = buildMetaAccountFixture();
  const rawAccountJson = JSON.stringify(rawAccountData);

  await pool.query(
    `
      INSERT INTO meta_ads_connections (
        id,
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
        1,
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
    `,
    [
      'meta-access-token',
      process.env.META_ADS_ENCRYPTION_KEY,
      rawAccountJson,
      Buffer.byteLength(rawAccountJson, 'utf8'),
      createHash('sha256').update(rawAccountJson).digest('hex')
    ]
  );

  await pool.query(
    `
      INSERT INTO meta_ads_sync_jobs (
        id,
        connection_id,
        sync_date,
        status,
        available_at,
        updated_at
      )
      VALUES (1, 1, '2026-04-11'::date, 'pending', now(), now())
    `
  );
}

async function seedGoogleSyncJob(): Promise<void> {
  const rawCustomerData = buildGoogleCustomerFixture();
  const rawCustomerJson = JSON.stringify(rawCustomerData);

  await pool.query(
    `
      INSERT INTO google_ads_connections (
        id,
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
        1,
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

  await pool.query(
    `
      INSERT INTO google_ads_sync_jobs (
        connection_id,
        sync_date,
        status,
        available_at,
        updated_at
      )
      VALUES (1, '2026-04-11'::date, 'pending', now(), now())
    `
  );
}

test('Meta Ads and Google Ads sync preserve raw payloads without trimming', async () => {
  const previousFetch = globalThis.fetch;

  try {
    await resetE2EDatabase();
    await seedMetaSyncJob();
    const expectedMetaAccount = buildMetaAccountFixture();

    globalThis.fetch = (async (input: string | URL | Request) => {
      const url = new URL(typeof input === 'string' ? input : input instanceof URL ? input.toString() : input.url);
      const level = url.searchParams.get('level');

      if (url.pathname.endsWith('/insights') && level) {
        const rawInsight = buildMetaInsightFixture(level as 'account' | 'campaign' | 'adset' | 'ad');
        const malformedInsight = level === 'ad' ? [buildMetaMalformedAdInsightFixture()] : [];
        return new Response(
          JSON.stringify({
            data: [rawInsight, ...malformedInsight],
            paging: {},
            extra_page_field: {
              kept: level
            }
          }),
          { status: 200, headers: { 'content-type': 'application/json' } }
        );
      }

      if (url.searchParams.get('ids') === 'ad_1') {
        return new Response(
          JSON.stringify({
            ad_1: {
              creative: {
                id: 'creative_1',
                name: 'Creative One'
              }
            },
            batch_debug: {
              untouched: true
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
    const persisted = await loadMetaRawPersistence();
    const projectionCountsAfterMeta = await loadAdProjectionCounts();
    assert.deepEqual(persisted.connection?.raw_account_data, expectedMetaAccount);
    assert.equal(persisted.connection?.raw_account_source, 'meta_ads_account');
    assert.equal(persisted.connection?.raw_account_external_id, '123456789');
    assert.equal(persisted.spendRows.length, 5);
    assert.equal(projectionCountsAfterMeta.metaDailyCount, 5);
    const expectedAccountInsight = buildMetaInsightFixture('account');
    const expectedAdInsight = buildMetaInsightFixture('ad');
    const expectedMalformedAdInsight = buildMetaMalformedAdInsightFixture();
    const expectedAccountMetadata = __rawPayloadStorageTestUtils.buildRawPayloadStorageMetadata(expectedAccountInsight);
    const expectedAdMetadata = __rawPayloadStorageTestUtils.buildRawPayloadStorageMetadata(expectedAdInsight);
    const expectedMalformedAdMetadata = __rawPayloadStorageTestUtils.buildRawPayloadStorageMetadata(expectedMalformedAdInsight);
    assert.deepEqual(persisted.spendRows[0].raw_payload, expectedAccountInsight);
    assert.equal(persisted.spendRows[0].payload_source, 'meta_ads_insights');
    assert.equal(persisted.spendRows[0].payload_external_id, '123456789');
    assert.equal(persisted.spendRows[0].payload_size_bytes, expectedAccountMetadata.payloadSizeBytes);
    assert.equal(persisted.spendRows[0].payload_hash, expectedAccountMetadata.payloadHash);
    assert.equal(persisted.spendRows[3].level, 'ad');
    assert.deepEqual(persisted.spendRows[3].raw_payload, expectedAdInsight);
    assert.equal(persisted.spendRows[3].payload_source, 'meta_ads_insights');
    assert.equal(persisted.spendRows[3].payload_external_id, 'ad_1');
    assert.equal(persisted.spendRows[3].payload_size_bytes, expectedAdMetadata.payloadSizeBytes);
    assert.equal(persisted.spendRows[3].payload_hash, expectedAdMetadata.payloadHash);
    assert.equal(persisted.spendRows[4].level, 'ad');
    assert.deepEqual(persisted.spendRows[4].raw_payload, expectedMalformedAdInsight);
    assert.equal(persisted.spendRows[4].payload_source, 'meta_ads_insights');
    assert.equal(persisted.spendRows[4].payload_external_id, null);
    assert.equal(persisted.spendRows[4].payload_size_bytes, expectedMalformedAdMetadata.payloadSizeBytes);
    assert.equal(persisted.spendRows[4].payload_hash, expectedMalformedAdMetadata.payloadHash);

    await resetE2EDatabase();
    await seedGoogleSyncJob();
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
    const googlePersisted = await loadGoogleRawPersistence();
    const projectionCountsAfterGoogle = await loadAdProjectionCounts();
    assert.deepEqual(googlePersisted.connection?.raw_customer_data, expectedGoogleCustomer);
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
    assert.deepEqual(googlePersisted.spendRows[0].raw_payload, expectedGoogleCampaign);
    assert.equal(googlePersisted.spendRows[0].payload_source, 'google_ads_api');
    assert.equal(googlePersisted.spendRows[0].payload_external_id, 'cmp_1');
    assert.equal(googlePersisted.spendRows[0].payload_size_bytes, expectedGoogleCampaignMetadata.payloadSizeBytes);
    assert.equal(googlePersisted.spendRows[0].payload_hash, expectedGoogleCampaignMetadata.payloadHash);
    assert.deepEqual(googlePersisted.spendRows[1].raw_payload, expectedMalformedGoogleCampaign);
    assert.equal(googlePersisted.spendRows[1].payload_source, 'google_ads_api');
    assert.equal(googlePersisted.spendRows[1].payload_external_id, null);
    assert.equal(googlePersisted.spendRows[1].payload_size_bytes, expectedMalformedGoogleCampaignMetadata.payloadSizeBytes);
    assert.equal(googlePersisted.spendRows[1].payload_hash, expectedMalformedGoogleCampaignMetadata.payloadHash);
    assert.deepEqual(googlePersisted.spendRows[2].raw_payload, expectedGoogleAd);
    assert.equal(googlePersisted.spendRows[2].payload_source, 'google_ads_api');
    assert.equal(googlePersisted.spendRows[2].payload_external_id, 'ad_1');
    assert.equal(googlePersisted.spendRows[2].payload_size_bytes, expectedGoogleAdMetadata.payloadSizeBytes);
    assert.equal(googlePersisted.spendRows[2].payload_hash, expectedGoogleAdMetadata.payloadHash);
    assert.deepEqual(googlePersisted.spendRows[3].raw_payload, expectedMalformedGoogleAd);
    assert.equal(googlePersisted.spendRows[3].payload_source, 'google_ads_api');
    assert.equal(googlePersisted.spendRows[3].payload_external_id, null);
    assert.equal(googlePersisted.spendRows[3].payload_size_bytes, expectedMalformedGoogleAdMetadata.payloadSizeBytes);
    assert.equal(googlePersisted.spendRows[3].payload_hash, expectedMalformedGoogleAdMetadata.payloadHash);
  } finally {
    globalThis.fetch = previousFetch;
    await resetE2EDatabase();
    await pool.end();
  }
});
