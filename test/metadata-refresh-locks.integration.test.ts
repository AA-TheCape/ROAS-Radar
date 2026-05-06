import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar_test';
process.env.GOOGLE_ADS_CLIENT_ID ??= 'google-client-id';
process.env.GOOGLE_ADS_CLIENT_SECRET ??= 'google-client-secret';
process.env.GOOGLE_ADS_DEVELOPER_TOKEN ??= 'google-developer-token';
process.env.GOOGLE_ADS_APP_BASE_URL ??= 'https://api.example.com';
process.env.GOOGLE_ADS_APP_SCOPES ??= 'https://www.googleapis.com/auth/adwords';
process.env.GOOGLE_ADS_ENCRYPTION_KEY ??= 'google-ads-encryption-key';
process.env.META_ADS_APP_ID ??= 'meta-app-id';
process.env.META_ADS_APP_SECRET ??= 'meta-app-secret';
process.env.META_ADS_APP_BASE_URL ??= 'https://api.example.com';
process.env.META_ADS_APP_SCOPES ??= 'ads_read,business_management';
process.env.META_ADS_ENCRYPTION_KEY ??= 'meta-encryption-key';
process.env.META_ADS_AD_ACCOUNT_ID ??= 'act_123456789';
process.env.META_ADS_SYNC_LOOKBACK_DAYS ??= '3';
process.env.META_ADS_SYNC_INITIAL_LOOKBACK_DAYS ??= '5';

const [
  { pool },
  {
    processGoogleAdsSyncQueue,
    refreshGoogleAdsMetadataForConnection,
    processMetaAdsSyncQueue,
    refreshMetaAdsMetadataForConnection
  },
  { resolveCampaignDisplayMetadata },
  { resetE2EDatabase }
] = await Promise.all([
  import('../src/db/pool.js'),
  Promise.all([import('../src/modules/google-ads/index.js'), import('../src/modules/meta-ads/index.js')]).then(
    ([googleAds, metaAds]) => ({
      processGoogleAdsSyncQueue: googleAds.processGoogleAdsSyncQueue,
      refreshGoogleAdsMetadataForConnection: googleAds.refreshGoogleAdsMetadataForConnection,
      processMetaAdsSyncQueue: metaAds.processMetaAdsSyncQueue,
      refreshMetaAdsMetadataForConnection: metaAds.refreshMetaAdsMetadataForConnection
    })
  ),
  import('../src/modules/reporting/metadata-resolution.js'),
  import('./e2e-harness.js')
]);

function buildGoogleSpendCampaignFixture(syncDate: string) {
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
      costMicros: '1200000',
      impressions: '100',
      clicks: '4'
    },
    segments: {
      date: syncDate
    }
  };
}

function buildGoogleSpendAdFixture(syncDate: string) {
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
      name: 'Search US'
    },
    adGroupAd: {
      ad: {
        id: 'ad_1',
        name: 'Headline A',
        resourceName: 'customers/1234567890/adGroupAds/1'
      }
    },
    metrics: {
      costMicros: '300000',
      impressions: '25',
      clicks: '2'
    },
    segments: {
      date: syncDate
    }
  };
}

function buildMetaInsightsFixture(syncDate: string, level: 'account' | 'campaign' | 'adset' | 'ad') {
  const base = {
    date_start: syncDate,
    date_stop: syncDate,
    account_id: '123456789',
    account_name: 'Meta Account',
    campaign_id: 'cmp_1',
    campaign_name: 'Brand Search',
    adset_id: 'adset_1',
    adset_name: 'US Ad Set',
    ad_id: 'ad_1',
    ad_name: 'Headline A',
    spend: '12.34',
    impressions: '100',
    clicks: '5',
    objective: 'OUTCOME_SALES'
  };

  switch (level) {
    case 'account':
      return [
        {
          ...base,
          campaign_id: undefined,
          campaign_name: undefined,
          adset_id: undefined,
          adset_name: undefined,
          ad_id: undefined,
          ad_name: undefined
        }
      ];
    case 'campaign':
      return [{ ...base, adset_id: undefined, adset_name: undefined, ad_id: undefined, ad_name: undefined }];
    case 'adset':
      return [{ ...base, ad_id: undefined, ad_name: undefined }];
    case 'ad':
      return [base];
  }
}

async function seedGoogleSyncJob(customerId = '1234567890'): Promise<void> {
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
        $1,
        NULL,
        pgp_sym_encrypt($2, $5, 'cipher-algo=aes256, compress-algo=0'),
        $3,
        pgp_sym_encrypt($4, $5, 'cipher-algo=aes256, compress-algo=0'),
        pgp_sym_encrypt($6, $5, 'cipher-algo=aes256, compress-algo=0'),
        ARRAY['https://www.googleapis.com/auth/adwords']::text[],
        '2026-04-11'::date,
        NULL,
        'active',
        'Main Account',
        'USD',
        $7::jsonb,
        'google_ads_customer',
        now(),
        $1,
        $8,
        $9
      )
    `,
    [
      customerId,
      process.env.GOOGLE_ADS_DEVELOPER_TOKEN,
      process.env.GOOGLE_ADS_CLIENT_ID,
      process.env.GOOGLE_ADS_CLIENT_SECRET,
      process.env.GOOGLE_ADS_ENCRYPTION_KEY,
      'google-refresh-token',
      JSON.stringify({
        customer: {
          id: customerId,
          descriptiveName: 'Main Account',
          currencyCode: 'USD'
        }
      }),
      85,
      'seeded'
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

async function seedMetaSyncJob(accountId = '123456789'): Promise<void> {
  const rawAccountData = {
    id: accountId,
    name: 'Meta Account',
    currency: 'USD'
  };
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
        $1,
        pgp_sym_encrypt($2, $3, 'cipher-algo=aes256, compress-algo=0'),
        'Bearer',
        ARRAY['ads_read']::text[],
        '2026-04-11'::date,
        'active',
        'Meta Account',
        'USD',
        $4::jsonb,
        'meta_ads_account',
        now(),
        $1,
        $5,
        $6
      )
    `,
    [
      accountId,
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

async function loadMetadataRows() {
  const result = await pool.query<{
    platform: string;
    account_id: string;
    entity_type: string;
    entity_id: string;
    latest_name: string;
  }>(
    `
      SELECT platform, account_id, entity_type, entity_id, latest_name
      FROM ad_platform_entity_metadata
      ORDER BY platform ASC, account_id ASC, entity_type ASC, entity_id ASC
    `
  );

  return result.rows;
}

test(
  'metadata refresh skips overlapping Google account work while allowing the same account id on a different platform',
  { concurrency: false },
  async () => {
    const previousFetch = globalThis.fetch;
    const lockClient = await pool.connect();
    const now = new Date('2026-04-11T12:00:00.000Z');
    let googleFetches = 0;
    let metaFetches = 0;

    try {
      await resetE2EDatabase();

      globalThis.fetch = (async (input: string | URL | Request, init?: RequestInit) => {
        const url = new URL(typeof input === 'string' ? input : input instanceof URL ? input.toString() : input.url);

        if (url.toString() === 'https://oauth2.googleapis.com/token') {
          googleFetches += 1;
          return new Response(
            JSON.stringify({ access_token: 'google-access-token', expires_in: 3600, token_type: 'Bearer' }),
            {
              status: 200,
              headers: { 'content-type': 'application/json' }
            }
          );
        }

        if (url.pathname.endsWith('/googleAds:searchStream')) {
          googleFetches += 1;
          const body = typeof init?.body === 'string' ? JSON.parse(init.body) : {};
          const queryText = typeof body.query === 'string' ? body.query : '';

          if (queryText.includes('FROM campaign')) {
            return new Response(
              JSON.stringify([{ results: [{ customer: { id: 'shared-account' }, campaign: { id: 'cmp_1', name: 'Blocked' } }] }]),
              {
                status: 200,
                headers: { 'content-type': 'application/json' }
              }
            );
          }

          if (queryText.includes('FROM ad_group')) {
            return new Response(
              JSON.stringify([{ results: [{ customer: { id: 'shared-account' }, adGroup: { id: 'adset_1', name: 'Blocked' } }] }]),
              {
                status: 200,
                headers: { 'content-type': 'application/json' }
              }
            );
          }

          return new Response(
            JSON.stringify([
              {
                results: [
                  {
                    customer: { id: 'shared-account' },
                    adGroupAd: {
                      ad: {
                        id: 'ad_1',
                        name: 'Blocked',
                        resourceName: 'customers/shared-account/adGroupAds/1'
                      }
                    }
                  }
                ]
              }
            ]),
            {
              status: 200,
              headers: { 'content-type': 'application/json' }
            }
          );
        }

        if (url.pathname.endsWith('/campaigns')) {
          metaFetches += 1;
          return new Response(JSON.stringify({ data: [{ id: 'cmp_1', name: 'Meta Shared Campaign' }] }), {
            status: 200,
            headers: { 'content-type': 'application/json' }
          });
        }

        if (url.pathname.endsWith('/adsets')) {
          metaFetches += 1;
          return new Response(JSON.stringify({ data: [{ id: 'adset_1', name: 'Meta Shared Ad Set' }] }), {
            status: 200,
            headers: { 'content-type': 'application/json' }
          });
        }

        if (url.pathname.endsWith('/ads')) {
          metaFetches += 1;
          return new Response(JSON.stringify({ data: [{ id: 'ad_1', name: 'Meta Shared Ad' }] }), {
            status: 200,
            headers: { 'content-type': 'application/json' }
          });
        }

        throw new Error(`Unexpected fetch ${url.toString()}`);
      }) as typeof globalThis.fetch;

      await lockClient.query("SELECT pg_advisory_lock(hashtext($1), hashtext($2))", ['google_ads', 'shared-account']);

      const skippedGoogleRefresh = await refreshGoogleAdsMetadataForConnection(
        {
          id: 91,
          customer_id: 'shared-account',
          login_customer_id: null,
          developer_token: 'developer-token',
          client_id: 'client-id',
          client_secret: 'client-secret',
          refresh_token: 'refresh-token'
        },
        now,
        'google-ads-metadata-refresh-worker',
        'test-suite'
      );

      assert.deepEqual(skippedGoogleRefresh, {
        skipped: true,
        recordCount: 0
      });
      assert.equal(googleFetches, 0);

      const metaRefresh = await refreshMetaAdsMetadataForConnection(
        {
          id: 92,
          ad_account_id: 'shared-account',
          access_token: 'meta-access-token'
        },
        now,
        'meta-ads-metadata-refresh-worker',
        'test-suite'
      );

      assert.deepEqual(metaRefresh, {
        skipped: false,
        recordCount: 3
      });
      assert.equal(metaFetches, 3);
      assert.deepEqual(await loadMetadataRows(), [
        {
          platform: 'meta_ads',
          account_id: 'shared-account',
          entity_type: 'ad',
          entity_id: 'ad_1',
          latest_name: 'Meta Shared Ad'
        },
        {
          platform: 'meta_ads',
          account_id: 'shared-account',
          entity_type: 'adset',
          entity_id: 'adset_1',
          latest_name: 'Meta Shared Ad Set'
        },
        {
          platform: 'meta_ads',
          account_id: 'shared-account',
          entity_type: 'campaign',
          entity_id: 'cmp_1',
          latest_name: 'Meta Shared Campaign'
        }
      ]);
    } finally {
      await lockClient
        .query("SELECT pg_advisory_unlock(hashtext($1), hashtext($2))", ['google_ads', 'shared-account'])
        .catch(() => undefined);
      lockClient.release();
      globalThis.fetch = previousFetch;
    }
  }
);

test('Google Ads sync queue does not trigger metadata refresh API calls', { concurrency: false }, async () => {
  const previousFetch = globalThis.fetch;
  const metadataFetches: string[] = [];

  try {
    await resetE2EDatabase();
    await seedGoogleSyncJob();

    globalThis.fetch = (async (input: string | URL | Request, init?: RequestInit) => {
      const url = new URL(typeof input === 'string' ? input : input instanceof URL ? input.toString() : input.url);

      if (url.toString() === 'https://oauth2.googleapis.com/token') {
        return new Response(JSON.stringify({ access_token: 'google-access-token', expires_in: 3600, token_type: 'Bearer' }), {
          status: 200,
          headers: { 'content-type': 'application/json' }
        });
      }

      if (!url.pathname.endsWith('/googleAds:searchStream')) {
        throw new Error(`Unexpected Google Ads fetch ${url.toString()}`);
      }

      const body = typeof init?.body === 'string' ? JSON.parse(init.body) : {};
      const queryText = typeof body.query === 'string' ? body.query : '';

      const isMetadataRefreshQuery =
        !queryText.includes('metrics.cost_micros') &&
        (queryText.includes('FROM campaign') || queryText.includes('FROM ad_group') || queryText.includes('FROM ad_group_ad'));

      if (isMetadataRefreshQuery) {
        metadataFetches.push(queryText);
        throw new Error(`Unexpected metadata refresh query during sync: ${queryText}`);
      }

      if (queryText.includes('FROM customer')) {
        return new Response(
          JSON.stringify([
            {
              results: [
                {
                  customer: {
                    id: '1234567890',
                    descriptiveName: 'Main Account',
                    currencyCode: 'USD'
                  }
                }
              ]
            }
          ]),
          {
            status: 200,
            headers: { 'content-type': 'application/json' }
          }
        );
      }

      if (queryText.includes('FROM campaign')) {
        return new Response(JSON.stringify([{ results: [buildGoogleSpendCampaignFixture('2026-04-11')] }]), {
          status: 200,
          headers: { 'content-type': 'application/json' }
        });
      }

      if (queryText.includes('FROM ad_group_ad')) {
        return new Response(JSON.stringify([{ results: [buildGoogleSpendAdFixture('2026-04-11')] }]), {
          status: 200,
          headers: { 'content-type': 'application/json' }
        });
      }

      throw new Error(`Unexpected Google Ads query ${queryText}`);
    }) as typeof globalThis.fetch;

    const result = await processGoogleAdsSyncQueue({
      limit: 1,
      now: new Date('2026-04-11T12:00:00.000Z')
    });

    assert.equal(result.succeededJobs, 1);
    assert.equal('metadataRefresh' in result, false);
    assert.deepEqual(metadataFetches, []);
    assert.deepEqual(await loadMetadataRows(), []);
  } finally {
    globalThis.fetch = previousFetch;
  }
});

test('Meta Ads sync queue does not trigger metadata refresh API calls', { concurrency: false }, async () => {
  const previousFetch = globalThis.fetch;
  const metadataFetches: string[] = [];

  try {
    await resetE2EDatabase();
    await seedMetaSyncJob();

    globalThis.fetch = (async (input: string | URL | Request) => {
      const url = new URL(typeof input === 'string' ? input : input instanceof URL ? input.toString() : input.url);

      if (url.pathname.endsWith('/campaigns') || url.pathname.endsWith('/adsets') || url.pathname.endsWith('/ads')) {
        metadataFetches.push(url.pathname);
        throw new Error(`Unexpected metadata refresh call during sync: ${url.pathname}`);
      }

      if (url.pathname.endsWith('/insights')) {
        const level = url.searchParams.get('level') as 'account' | 'campaign' | 'adset' | 'ad';
        const timeRange = JSON.parse(url.searchParams.get('time_range') ?? '{}') as { since?: string };
        const syncDate = timeRange.since ?? '2026-04-11';

        return new Response(JSON.stringify({ data: buildMetaInsightsFixture(syncDate, level) }), {
          status: 200,
          headers: { 'content-type': 'application/json' }
        });
      }

      if (url.pathname.endsWith('/v23.0/')) {
        return new Response(
          JSON.stringify({
            ad_1: {
              creative: {
                id: 'creative_1',
                name: 'Creative One'
              }
            }
          }),
          {
            status: 200,
            headers: { 'content-type': 'application/json' }
          }
        );
      }

      throw new Error(`Unexpected Meta Ads fetch ${url.toString()}`);
    }) as typeof globalThis.fetch;

    const result = await processMetaAdsSyncQueue({
      limit: 1,
      now: new Date('2026-04-11T12:00:00.000Z')
    });

    assert.equal(result.succeededJobs, 1);
    assert.equal('metadataRefresh' in result, false);
    assert.deepEqual(metadataFetches, []);
    assert.deepEqual(await loadMetadataRows(), []);
  } finally {
    globalThis.fetch = previousFetch;
  }
});

test('campaign metadata resolution isolates duplicate entity ids by platform and account scope', { concurrency: false }, async () => {
  await resetE2EDatabase();

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
      VALUES
        (1, 'acct-google-1', '\\x00'::bytea, 'client', '\\x00'::bytea, '\\x00'::bytea, 'active', '{}'::jsonb, 'google_ads_customer', now(), 'acct-google-1', 2, 'seed'),
        (2, 'acct-google-2', '\\x00'::bytea, 'client', '\\x00'::bytea, '\\x00'::bytea, 'active', '{}'::jsonb, 'google_ads_customer', now(), 'acct-google-2', 2, 'seed')
    `
  );

  await pool.query(
    `
      INSERT INTO google_ads_sync_jobs (id, connection_id, sync_date, status)
      VALUES
        (1, 1, '2026-04-10'::date, 'completed'),
        (2, 2, '2026-04-10'::date, 'completed')
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
      VALUES (1, 'acct-meta-1', '\\x00'::bytea, 'active', '{}'::jsonb, 'meta_ads_account', now(), 'acct-meta-1', 2, 'seed')
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
        (1, 1, '2026-04-10'::date, 'campaign', 'google-1', 'acct-google-1', 'Google One', 'cmp_dup', 'First Google Name', 'google', 'cpc', 'dup-google-one', 'unknown', 'unknown', 'USD', '10.00', 10, 1, '{}'::jsonb),
        (2, 2, '2026-04-10'::date, 'campaign', 'google-2', 'acct-google-2', 'Google Two', 'cmp_dup', 'Second Google Name', 'google', 'cpc', 'dup-google-two', 'unknown', 'unknown', 'USD', '11.00', 11, 2, '{}'::jsonb)
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
        (1, 1, '2026-04-10'::date, 'campaign', 'meta-1', 'acct-meta-1', 'Meta One', 'cmp_dup', 'Meta Name', 'meta', 'paid_social', 'dup-meta', 'unknown', 'unknown', 'USD', '12.00', 12, 3, '{}'::jsonb)
    `
  );

  await pool.query(
    `
      INSERT INTO ad_platform_entity_metadata (
        platform,
        account_id,
        entity_type,
        entity_id,
        latest_name,
        last_seen_at,
        updated_at
      )
      VALUES
        ('google_ads', 'acct-google-1', 'campaign', 'cmp_dup', 'Scoped Google One', '2026-04-10T12:00:00.000Z', now()),
        ('google_ads', 'acct-google-2', 'campaign', 'cmp_dup', 'Scoped Google Two', '2026-04-10T12:00:00.000Z', now()),
        ('meta_ads', 'acct-meta-1', 'campaign', 'cmp_dup', 'Scoped Meta', '2026-04-10T12:00:00.000Z', now())
    `
  );

  const resolution = await resolveCampaignDisplayMetadata('2026-04-10', '2026-04-10', [
    'dup-google-one',
    'dup-google-two',
    'dup-meta'
  ]);

  assert.deepEqual(resolution.byCampaign.get('dup-google-one'), {
    campaign: 'dup-google-one',
    source: 'google',
    medium: 'cpc',
    campaignDisplayName: 'Scoped Google One',
    campaignEntityId: 'cmp_dup',
    campaignPlatform: 'google_ads',
    campaignNameResolutionStatus: 'resolved',
    lastSeenAt: '2026-04-10T12:00:00.000Z',
    updatedAt: resolution.byCampaign.get('dup-google-one')?.updatedAt ?? null
  });
  assert.deepEqual(resolution.byCampaign.get('dup-google-two'), {
    campaign: 'dup-google-two',
    source: 'google',
    medium: 'cpc',
    campaignDisplayName: 'Scoped Google Two',
    campaignEntityId: 'cmp_dup',
    campaignPlatform: 'google_ads',
    campaignNameResolutionStatus: 'resolved',
    lastSeenAt: '2026-04-10T12:00:00.000Z',
    updatedAt: resolution.byCampaign.get('dup-google-two')?.updatedAt ?? null
  });
  assert.deepEqual(resolution.byCampaign.get('dup-meta'), {
    campaign: 'dup-meta',
    source: 'meta',
    medium: 'paid_social',
    campaignDisplayName: 'Scoped Meta',
    campaignEntityId: 'cmp_dup',
    campaignPlatform: 'meta_ads',
    campaignNameResolutionStatus: 'resolved',
    lastSeenAt: '2026-04-10T12:00:00.000Z',
    updatedAt: resolution.byCampaign.get('dup-meta')?.updatedAt ?? null
  });
});
