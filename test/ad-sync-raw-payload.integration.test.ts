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

const { pool } = await import('../src/db/pool.js');
const { resetE2EDatabase } = await import('./e2e-harness.js');
const { processMetaAdsSyncQueue } = await import('../src/modules/meta-ads/index.js');
const { processGoogleAdsSyncQueue } = await import('../src/modules/google-ads/index.js');

type AuditRow = {
  platform: 'meta_ads' | 'google_ads';
  transaction_source: string;
  source_metadata: Record<string, unknown>;
  request_method: string;
  request_url: string;
  request_payload: unknown;
  response_status: number | null;
  response_payload: unknown;
  request_started_at: Date;
  response_received_at: Date | null;
  error_message: string | null;
};

async function loadMetaRawPersistence() {
  const [connectionResult, spendResult] = await Promise.all([
    pool.query<{ raw_account_data: Record<string, unknown> }>(
      `
        SELECT raw_account_data
        FROM meta_ads_connections
        WHERE id = 1
      `
    ),
    pool.query<{ level: string; raw_payload: Record<string, unknown> }>(
      `
        SELECT level, raw_payload
        FROM meta_ads_raw_spend_records
        WHERE connection_id = 1
        ORDER BY id ASC
      `
    )
  ]);

  return {
    connection: connectionResult.rows[0]?.raw_account_data ?? null,
    spendRows: spendResult.rows
  };
}

async function loadGoogleRawPersistence() {
  const [connectionResult, spendResult] = await Promise.all([
    pool.query<{ raw_customer_data: Record<string, unknown> }>(
      `
        SELECT raw_customer_data
        FROM google_ads_connections
        WHERE id = 1
      `
    ),
    pool.query<{ level: string; raw_payload: Record<string, unknown> }>(
      `
        SELECT level, raw_payload
        FROM google_ads_raw_spend_records
        WHERE connection_id = 1
        ORDER BY id ASC
      `
    )
  ]);

  return {
    connection: connectionResult.rows[0]?.raw_customer_data ?? null,
    spendRows: spendResult.rows
  };
}

async function seedMetaSyncJob(): Promise<void> {
  const rawAccountData = { id: '123456789', name: 'Meta Account' };
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
  const rawCustomerData = {
    customer: { id: '1234567890', descriptiveName: 'Main Account', currencyCode: 'USD' }
  };
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

async function loadAuditRows(platform: 'meta_ads' | 'google_ads'): Promise<AuditRow[]> {
  const result = await pool.query<AuditRow>(
    `
      SELECT
        platform,
        transaction_source,
        source_metadata,
        request_method,
        request_url,
        request_payload,
        response_status,
        response_payload,
        request_started_at,
        response_received_at,
        error_message
      FROM ad_sync_api_transactions
      WHERE platform = $1
      ORDER BY id ASC
    `,
    [platform]
  );

  return result.rows;
}

test.beforeEach(async () => {
  await resetE2EDatabase();
});

test.after(async () => {
  await resetE2EDatabase();
  await pool.end();
});

test('Meta Ads sync stores raw request and response audit payloads for each API transaction', async () => {
  await seedMetaSyncJob();

  const previousFetch = globalThis.fetch;

  globalThis.fetch = (async (input: string | URL | Request) => {
    const url = new URL(typeof input === 'string' ? input : input instanceof URL ? input.toString() : input.url);
    const level = url.searchParams.get('level');

    if (url.pathname.endsWith('/insights') && level) {
      return new Response(
        JSON.stringify({
          data: [
            {
              account_id: '123456789',
              account_name: 'Meta Account',
              campaign_id: level === 'account' ? undefined : 'cmp_1',
              campaign_name: level === 'account' ? undefined : 'Campaign One',
              adset_id: level === 'adset' || level === 'ad' ? 'adset_1' : undefined,
              adset_name: level === 'adset' || level === 'ad' ? 'Adset One' : undefined,
              ad_id: level === 'ad' ? 'ad_1' : undefined,
              ad_name: level === 'ad' ? 'Ad One' : undefined,
              spend: '12.34',
              impressions: '100',
              clicks: '5',
              objective: 'OUTCOME_TRAFFIC',
              date_start: '2026-04-11',
              date_stop: '2026-04-11',
              source_debug: {
                level,
                untouched: true
              }
            }
          ],
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

  try {
    const result = await processMetaAdsSyncQueue({
      limit: 1,
      now: new Date('2026-04-11T12:00:00.000Z')
    });

    assert.equal(result.succeededJobs, 1);
    const persisted = await loadMetaRawPersistence();

    const rows = await loadAuditRows('meta_ads');
    assert.equal(rows.length, 5);
    assert.equal(rows[0].transaction_source, 'meta_ads_insights');
    assert.equal(rows[0].request_method, 'GET');
    assert.match(rows[0].request_url, /^https:\/\/graph\.facebook\.com\/v\d+\.\d+\/act_123456789\/insights$/);
    assert.deepEqual(rows[0].request_payload, {
      fields: 'account_id,account_name,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,spend,impressions,clicks,objective,date_start,date_stop',
      access_token: '[redacted]',
      level: 'account',
      time_increment: '1',
      limit: '500',
      time_range: '{"since":"2026-04-11","until":"2026-04-11"}'
    });
    assert.deepEqual(rows[0].source_metadata, {
      adAccountId: '123456789',
      syncDate: '2026-04-11',
      level: 'account',
      attempt: 1
    });
    assert.equal(rows[0].response_status, 200);
    assert.deepEqual(rows[0].response_payload, {
      data: [
        {
          account_id: '123456789',
          account_name: 'Meta Account',
          spend: '12.34',
          impressions: '100',
          clicks: '5',
          objective: 'OUTCOME_TRAFFIC',
          date_start: '2026-04-11',
          date_stop: '2026-04-11',
          source_debug: {
            level: 'account',
            untouched: true
          }
        }
      ],
      paging: {},
      extra_page_field: {
        kept: 'account'
      }
    });
    assert.equal(rows[4].transaction_source, 'meta_ads_creatives');
    assert.deepEqual(rows[4].response_payload, {
      ad_1: {
        creative: {
          id: 'creative_1',
          name: 'Creative One'
        }
      },
      batch_debug: {
        untouched: true
      }
    });
    assert.equal(rows[4].error_message, null);
    assert.ok(rows[4].request_started_at instanceof Date);
    assert.ok(rows[4].response_received_at instanceof Date);
    assert.deepEqual(persisted.connection, {
      id: '123456789',
      name: 'Meta Account'
    });
    assert.equal(persisted.spendRows.length, 4);
    assert.deepEqual(persisted.spendRows[0], {
      level: 'account',
      raw_payload: {
        account_id: '123456789',
        account_name: 'Meta Account',
        spend: '12.34',
        impressions: '100',
        clicks: '5',
        objective: 'OUTCOME_TRAFFIC',
        date_start: '2026-04-11',
        date_stop: '2026-04-11',
        source_debug: {
          level: 'account',
          untouched: true
        }
      }
    });
    assert.equal(persisted.spendRows[3].level, 'ad');
    assert.deepEqual(persisted.spendRows[3].raw_payload, {
      account_id: '123456789',
      account_name: 'Meta Account',
      campaign_id: 'cmp_1',
      campaign_name: 'Campaign One',
      adset_id: 'adset_1',
      adset_name: 'Adset One',
      ad_id: 'ad_1',
      ad_name: 'Ad One',
      spend: '12.34',
      impressions: '100',
      clicks: '5',
      objective: 'OUTCOME_TRAFFIC',
      date_start: '2026-04-11',
      date_stop: '2026-04-11',
      source_debug: {
        level: 'ad',
        untouched: true
      }
    });
  } finally {
    globalThis.fetch = previousFetch;
  }
});

test('Google Ads sync stores raw request and response audit payloads for each API transaction', async () => {
  await seedGoogleSyncJob();

  const previousFetch = globalThis.fetch;

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
              results: [
                {
                  customer: {
                    id: '1234567890',
                    descriptiveName: 'Main Account',
                    currencyCode: 'USD'
                  },
                  untouched_customer_debug: {
                    kept: true
                  }
                }
              ]
            }
          ]),
          { status: 200, headers: { 'content-type': 'application/json' } }
        );
      }

      if (body.query?.includes('FROM campaign')) {
        return new Response(
          JSON.stringify([
            {
              results: [
                {
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
                    kept: true
                  }
                }
              ]
            }
          ]),
          { status: 200, headers: { 'content-type': 'application/json' } }
        );
      }

      if (body.query?.includes('FROM ad_group_ad')) {
        return new Response(
          JSON.stringify([
            {
              results: [
                {
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
                    kept: true
                  }
                }
              ]
            }
          ]),
          { status: 200, headers: { 'content-type': 'application/json' } }
        );
      }
    }

    throw new Error(`Unexpected Google Ads fetch ${url.toString()}`);
  }) as typeof globalThis.fetch;

  try {
    const result = await processGoogleAdsSyncQueue({
      limit: 1,
      now: new Date('2026-04-11T12:00:00.000Z')
    });

    assert.equal(result.succeededJobs, 1);
    const persisted = await loadGoogleRawPersistence();

    const rows = await loadAuditRows('google_ads');
    assert.equal(rows.length, 3);
    assert.equal(rows[0].transaction_source, 'google_ads_customer_search');
    assert.equal(rows[0].request_method, 'POST');
    assert.match(
      rows[0].request_url,
      /^https:\/\/googleads\.googleapis\.com\/v\d+\/customers\/1234567890\/googleAds:searchStream$/
    );
    assert.deepEqual(rows[0].request_payload, {
      query: 'SELECT customer.id, customer.descriptive_name, customer.currency_code FROM customer LIMIT 1'
    });
    assert.deepEqual(rows[0].response_payload, [
      {
        results: [
          {
            customer: {
              id: '1234567890',
              descriptiveName: 'Main Account',
              currencyCode: 'USD'
            },
            untouched_customer_debug: {
              kept: true
            }
          }
        ]
      }
    ]);
    assert.equal(rows[1].transaction_source, 'google_ads_campaign_search');
    assert.match(String((rows[1].request_payload as { query?: string }).query), /FROM campaign/);
    assert.equal(rows[2].transaction_source, 'google_ads_ad_search');
    assert.match(String((rows[2].request_payload as { query?: string }).query), /FROM ad_group_ad/);
    assert.equal(rows[2].response_status, 200);
    assert.equal(rows[2].error_message, null);
    assert.ok(rows[2].request_started_at instanceof Date);
    assert.ok(rows[2].response_received_at instanceof Date);
    assert.deepEqual(persisted.connection, {
      customer: {
        id: '1234567890',
        descriptiveName: 'Main Account',
        currencyCode: 'USD'
      },
      untouched_customer_debug: {
        kept: true
      }
    });
    assert.equal(persisted.spendRows.length, 2);
    assert.deepEqual(persisted.spendRows[0], {
      level: 'campaign',
      raw_payload: {
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
          kept: true
        }
      }
    });
    assert.deepEqual(persisted.spendRows[1], {
      level: 'ad',
      raw_payload: {
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
          kept: true
        }
      }
    });
  } finally {
    globalThis.fetch = previousFetch;
  }
});
