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
process.env.META_ADS_SYNC_LOOKBACK_DAYS ??= '3';
process.env.META_ADS_SYNC_INITIAL_LOOKBACK_DAYS ??= '5';

const { pool } = await import('../src/db/pool.js');
const { processMetaAdsSyncQueue } = await import('../src/modules/meta-ads/index.js');
const { resetE2EDatabase } = await import('./e2e-harness.js');

type MetadataPhase = 'initial' | 'renamed';

function buildMetaAccountFixture() {
  return {
    id: '123456789',
    name: 'Meta Account',
    currency: 'USD'
  };
}

function buildInsightsFixture(syncDate: string, level: 'account' | 'campaign' | 'adset' | 'ad') {
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
      return [{ ...base, campaign_id: undefined, campaign_name: undefined, adset_id: undefined, adset_name: undefined, ad_id: undefined, ad_name: undefined }];
    case 'campaign':
      return [{ ...base, adset_id: undefined, adset_name: undefined, ad_id: undefined, ad_name: undefined }];
    case 'adset':
      return [{ ...base, ad_id: undefined, ad_name: undefined }];
    case 'ad':
      return [base];
  }
}

function buildMetadataPage(entityType: 'campaign' | 'adset' | 'ad', phase: MetadataPhase) {
  if (entityType === 'campaign') {
    return {
      data: [
        {
          id: 'cmp_1',
          name: phase === 'initial' ? '  Brand   Search ' : ' Brand Search - Updated '
        },
        {
          id: 'cmp_2',
          name: ' Prospecting '
        }
      ]
    };
  }

  if (entityType === 'adset') {
    return {
      data: [
        {
          id: 'adset_1',
          name: phase === 'initial' ? ' US   Ad Set ' : ' US Ad Set - Updated '
        }
      ]
    };
  }

  return {
    data: [
      {
        id: 'ad_1',
        name: phase === 'initial' ? ' Headline   A ' : ' Headline A - Updated '
      }
    ]
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

test('Meta Ads metadata sync upserts entity names without duplicate rows across reruns', { concurrency: false }, async () => {
  const previousFetch = globalThis.fetch;
  let phase: MetadataPhase = 'initial';

  try {
    await resetE2EDatabase();
    await seedMetaSyncJob();

    globalThis.fetch = (async (input: string | URL | Request) => {
      const url = new URL(typeof input === 'string' ? input : input instanceof URL ? input.toString() : input.url);

      if (url.pathname.endsWith('/insights')) {
        const level = url.searchParams.get('level') as 'account' | 'campaign' | 'adset' | 'ad';
        const timeRange = JSON.parse(url.searchParams.get('time_range') ?? '{}') as { since?: string };
        const syncDate = timeRange.since ?? '2026-04-11';

        return new Response(JSON.stringify({ data: buildInsightsFixture(syncDate, level) }), {
          status: 200,
          headers: { 'content-type': 'application/json' }
        });
      }

      if (url.pathname.endsWith('/campaigns')) {
        return new Response(JSON.stringify(buildMetadataPage('campaign', phase)), {
          status: 200,
          headers: { 'content-type': 'application/json' }
        });
      }

      if (url.pathname.endsWith('/adsets')) {
        return new Response(JSON.stringify(buildMetadataPage('adset', phase)), {
          status: 200,
          headers: { 'content-type': 'application/json' }
        });
      }

      if (url.pathname.endsWith('/ads')) {
        return new Response(JSON.stringify(buildMetadataPage('ad', phase)), {
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

    const firstRun = await processMetaAdsSyncQueue({
      limit: 1,
      now: new Date('2026-04-11T12:00:00.000Z')
    });

    assert.equal(firstRun.succeededJobs, 1);
    assert.deepEqual(await loadMetadataRows(), [
      {
        platform: 'meta_ads',
        account_id: '123456789',
        entity_type: 'ad',
        entity_id: 'ad_1',
        latest_name: 'Headline A'
      },
      {
        platform: 'meta_ads',
        account_id: '123456789',
        entity_type: 'adset',
        entity_id: 'adset_1',
        latest_name: 'US Ad Set'
      },
      {
        platform: 'meta_ads',
        account_id: '123456789',
        entity_type: 'campaign',
        entity_id: 'cmp_1',
        latest_name: 'Brand Search'
      },
      {
        platform: 'meta_ads',
        account_id: '123456789',
        entity_type: 'campaign',
        entity_id: 'cmp_2',
        latest_name: 'Prospecting'
      }
    ]);

    phase = 'renamed';

    await pool.query("DELETE FROM meta_ads_sync_jobs WHERE status <> 'completed'");
    await pool.query(
      `
        UPDATE meta_ads_connections
        SET
          last_sync_planned_for = '2026-04-12'::date,
          updated_at = now()
        WHERE id = 1
      `
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
        VALUES (2, 1, '2026-04-12'::date, 'pending', now(), now())
      `
    );

    const secondRun = await processMetaAdsSyncQueue({
      limit: 1,
      now: new Date('2026-04-12T12:00:00.000Z')
    });

    assert.equal(secondRun.succeededJobs, 1);
    assert.deepEqual(await loadMetadataRows(), [
      {
        platform: 'meta_ads',
        account_id: '123456789',
        entity_type: 'ad',
        entity_id: 'ad_1',
        latest_name: 'Headline A - Updated'
      },
      {
        platform: 'meta_ads',
        account_id: '123456789',
        entity_type: 'adset',
        entity_id: 'adset_1',
        latest_name: 'US Ad Set - Updated'
      },
      {
        platform: 'meta_ads',
        account_id: '123456789',
        entity_type: 'campaign',
        entity_id: 'cmp_1',
        latest_name: 'Brand Search - Updated'
      },
      {
        platform: 'meta_ads',
        account_id: '123456789',
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
