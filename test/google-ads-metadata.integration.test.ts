import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar_test';
process.env.GOOGLE_ADS_CLIENT_ID ??= 'google-client-id';
process.env.GOOGLE_ADS_CLIENT_SECRET ??= 'google-client-secret';
process.env.GOOGLE_ADS_DEVELOPER_TOKEN ??= 'google-developer-token';
process.env.GOOGLE_ADS_APP_BASE_URL ??= 'https://api.example.com';
process.env.GOOGLE_ADS_APP_SCOPES ??= 'https://www.googleapis.com/auth/adwords';
process.env.GOOGLE_ADS_ENCRYPTION_KEY ??= 'google-ads-encryption-key';

const { pool } = await import('../src/db/pool.js');
const { processGoogleAdsSyncQueue } = await import('../src/modules/google-ads/index.js');
const { resetE2EDatabase } = await import('./e2e-harness.js');

type MetadataPhase = 'initial' | 'renamed';

function buildGoogleCustomerFixture() {
  return {
    customer: {
      id: '1234567890',
      descriptiveName: 'Main Account',
      currencyCode: 'USD'
    }
  };
}

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

function buildMetadataResponse(query: string, phase: MetadataPhase) {
  if (query.includes('FROM campaign') && !query.includes('metrics.cost_micros')) {
    return [
      {
        results: [
          {
            customer: { id: '1234567890' },
            campaign: {
              id: 'cmp_1',
              name: phase === 'initial' ? '  Brand   Search  ' : '  Brand Search - Updated  '
            }
          }
        ]
      },
      {
        results: [
          {
            customer: { id: '1234567890' },
            campaign: {
              id: 'cmp_2',
              name: ' Prospecting '
            }
          }
        ]
      }
    ];
  }

  if (query.includes('FROM ad_group_ad') && !query.includes('metrics.cost_micros')) {
    return [
      {
        results: [
          {
            customer: { id: '1234567890' },
            adGroupAd: {
              ad: {
                id: 'ad_1',
                name: phase === 'initial' ? ' Headline   A ' : ' Headline A - Updated ',
                resourceName: 'customers/1234567890/adGroupAds/1'
              }
            }
          }
        ]
      }
    ];
  }

  if (query.includes('FROM ad_group') && !query.includes('metrics.cost_micros')) {
    return [
      {
        results: [
          {
            customer: { id: '1234567890' },
            adGroup: {
              id: 'adgroup_1',
              name: phase === 'initial' ? ' Search   US ' : ' Search US - Updated '
            }
          }
        ]
      }
    ];
  }

  return null;
}

async function seedGoogleSyncJob(): Promise<void> {
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
        '{"customer":{"id":"1234567890","descriptiveName":"Main Account","currencyCode":"USD"}}'::jsonb,
        'google_ads_customer',
        now(),
        '1234567890',
        75,
        'seeded'
      )
    `,
    [
      process.env.GOOGLE_ADS_DEVELOPER_TOKEN,
      process.env.GOOGLE_ADS_CLIENT_ID,
      process.env.GOOGLE_ADS_CLIENT_SECRET,
      process.env.GOOGLE_ADS_ENCRYPTION_KEY,
      'google-refresh-token'
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
      ORDER BY entity_type ASC, entity_id ASC
    `
  );

  return result.rows;
}

test('Google Ads metadata sync upserts entity names without duplicate rows across reruns', { concurrency: false }, async () => {
  const previousFetch = globalThis.fetch;
  let phase: MetadataPhase = 'initial';

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

      const body = JSON.parse(String(init?.body ?? '{}')) as { query?: string };
      const query = body.query ?? '';

      if (query.includes('FROM customer')) {
        return new Response(JSON.stringify([{ results: [buildGoogleCustomerFixture()] }]), {
          status: 200,
          headers: { 'content-type': 'application/json' }
        });
      }

      const metadataResponse = buildMetadataResponse(query, phase);

      if (metadataResponse) {
        return new Response(JSON.stringify(metadataResponse), {
          status: 200,
          headers: { 'content-type': 'application/json' }
        });
      }

      if (query.includes('FROM campaign') && query.includes('metrics.cost_micros')) {
        const syncDate = /segments\.date = '([^']+)'/.exec(query)?.[1] ?? '2026-04-11';
        return new Response(JSON.stringify([{ results: [buildGoogleSpendCampaignFixture(syncDate)] }]), {
          status: 200,
          headers: { 'content-type': 'application/json' }
        });
      }

      if (query.includes('FROM ad_group_ad') && query.includes('metrics.cost_micros')) {
        const syncDate = /segments\.date = '([^']+)'/.exec(query)?.[1] ?? '2026-04-11';
        return new Response(JSON.stringify([{ results: [buildGoogleSpendAdFixture(syncDate)] }]), {
          status: 200,
          headers: { 'content-type': 'application/json' }
        });
      }

      throw new Error(`Unexpected Google Ads search query ${query}`);
    }) as typeof globalThis.fetch;

    const firstRun = await processGoogleAdsSyncQueue({
      limit: 1,
      now: new Date('2026-04-11T12:00:00.000Z')
    });

    assert.equal(firstRun.succeededJobs, 1);
    assert.deepEqual(await loadMetadataRows(), [
      {
        platform: 'google_ads',
        account_id: '1234567890',
        entity_type: 'ad',
        entity_id: 'ad_1',
        latest_name: 'Headline A'
      },
      {
        platform: 'google_ads',
        account_id: '1234567890',
        entity_type: 'adset',
        entity_id: 'adgroup_1',
        latest_name: 'Search US'
      },
      {
        platform: 'google_ads',
        account_id: '1234567890',
        entity_type: 'campaign',
        entity_id: 'cmp_1',
        latest_name: 'Brand Search'
      },
      {
        platform: 'google_ads',
        account_id: '1234567890',
        entity_type: 'campaign',
        entity_id: 'cmp_2',
        latest_name: 'Prospecting'
      }
    ]);

    phase = 'renamed';

    const secondRun = await processGoogleAdsSyncQueue({
      limit: 1,
      now: new Date('2026-04-12T12:00:00.000Z')
    });

    assert.equal(secondRun.succeededJobs, 1);
    const metadataRows = await loadMetadataRows();
    assert.equal(metadataRows.length, 4);
    assert.deepEqual(metadataRows, [
      {
        platform: 'google_ads',
        account_id: '1234567890',
        entity_type: 'ad',
        entity_id: 'ad_1',
        latest_name: 'Headline A - Updated'
      },
      {
        platform: 'google_ads',
        account_id: '1234567890',
        entity_type: 'adset',
        entity_id: 'adgroup_1',
        latest_name: 'Search US - Updated'
      },
      {
        platform: 'google_ads',
        account_id: '1234567890',
        entity_type: 'campaign',
        entity_id: 'cmp_1',
        latest_name: 'Brand Search - Updated'
      },
      {
        platform: 'google_ads',
        account_id: '1234567890',
        entity_type: 'campaign',
        entity_id: 'cmp_2',
        latest_name: 'Prospecting'
      }
    ]);
  } finally {
    globalThis.fetch = previousFetch;
    await resetE2EDatabase();
  }
});
