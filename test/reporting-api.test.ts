import assert from 'node:assert/strict';
import type { AddressInfo } from 'node:net';
import test, { after } from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';
process.env.REPORTING_API_TOKEN = 'test-reporting-token';

const poolModule = await import('../src/db/pool.js');
const serverModule = await import('../src/server.js');

const { pool } = poolModule;
const { closeServer, createServer } = serverModule;
const originalPoolQuery = pool.query.bind(pool);
const REPORTING_SCHEMA_VERSION = '2026-05-27';

after(async () => {
  pool.query = originalPoolQuery as typeof pool.query;
  await pool.end();
});

function buildHeaders(): Record<string, string> {
  return {
    authorization: 'Bearer test-reporting-token'
  };
}

function buildCampaignLabel(
  displayName: string,
  entityId: string | null,
  platform: 'google_ads' | 'meta_ads' | null,
  resolutionStatus: 'resolved' | 'fallback_name' | 'unresolved',
  lastSeenAt: string | null = null,
  updatedAt: string | null = null,
  hierarchy: {
    source?: string;
    rawId?: string;
    objectType?: 'campaign' | 'adset' | null;
    entityType?: 'campaign' | 'adset';
    parentCampaignEntityId?: string | null;
    parentCampaignDisplayName?: string | null;
  } = {}
) {
  const parentCampaign =
    hierarchy.parentCampaignEntityId || hierarchy.parentCampaignDisplayName
      ? {
          entityId: hierarchy.parentCampaignEntityId ?? null,
          displayName: hierarchy.parentCampaignDisplayName ?? null
        }
      : null;

  return {
    displayName,
    source:
      hierarchy.source ??
      (platform === 'meta_ads' ? 'meta' : platform === 'google_ads' ? 'google' : 'unknown'),
    rawId: hierarchy.rawId ?? entityId ?? displayName,
    entityId,
    objectType: hierarchy.objectType ?? hierarchy.entityType ?? null,
    ...hierarchy,
    parentCampaign,
    platform,
    resolutionStatus,
    lastSeenAt,
    updatedAt
  };
}

async function requestJson(server: ReturnType<typeof createServer>, path: string, headers = buildHeaders()) {
  const address = server.address() as AddressInfo;
  const response = await fetch(`http://127.0.0.1:${address.port}${path}`, {
    headers
  });
  const body = await response.json();

  return { response, body };
}

test('reporting routes require the configured bearer token', async () => {
  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/reporting/summary?startDate=2026-04-01&endDate=2026-04-10',
      {}
    );

    assert.equal(response.status, 401);
    assert.equal(body.error, 'unauthorized');
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('reporting summary returns headline metrics from daily campaign aggregates', async () => {
  pool.query = (async (text: string, params?: unknown[]) => {
    if (text.includes('FROM daily_reporting_metrics')) {
      assert.deepEqual(params, ['2026-04-01', '2026-04-10', 'last_touch', 'google', 'spring-sale']);

      return {
        rows: [
          {
            visits: '1240',
            orders: '48',
            revenue: '5210.50',
            spend: '0.00'
          }
        ]
      };
    }

    if (text.includes('FROM deterministic_model_outputs')) {
      assert.deepEqual(params, ['2026-04-01', '2026-04-10', 'google', 'spring-sale']);
      return {
        rows: [
          {
            visits: '0',
            orders: '0',
            revenue: '0.00',
            spend: '0.00'
          }
        ]
      };
    }

    assert.match(text, /FROM meta_ads_order_value_aggregates/);
    assert.deepEqual(params, ['2026-04-01', '2026-04-10', 1, 'google', 'spring-sale']);
    return {
      rows: [
        {
          visits: '0',
          orders: '0',
          revenue: '0.00',
          spend: '0.00'
        }
      ]
    };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/reporting/summary?startDate=2026-04-01&endDate=2026-04-10&source=google&campaign=spring-sale'
    );

    assert.equal(response.status, 200);
    assert.equal(response.headers.get('x-roas-radar-reporting-schema'), REPORTING_SCHEMA_VERSION);
    assert.deepEqual(body, {
      range: {
        startDate: '2026-04-01',
        endDate: '2026-04-10'
      },
      totals: {
        visits: 1240,
        orders: 48,
        revenue: 5210.5,
        spend: 0,
        conversionRate: 48 / 1240,
        roas: null
      },
      reportingMode: 'clicks',
      reportingModeLabel: 'Click attribution',
      totalsLabel: 'Click attribution',
      totalsCanonical: true,
      totalsDescription: 'Canonical reporting totals from click-attributed order credits.',
      comparisonTotals: {
        combined: {
          label: 'Non-canonical comparison total',
          canonical: false,
          description: 'Comparison-only sum of click attribution and deterministic view attribution; do not treat as canonical revenue.',
          totals: {
            visits: 1240,
            orders: 48,
            revenue: 5210.5,
            spend: 0,
            conversionRate: 48 / 1240,
            roas: null
          }
        }
      },
      layers: {
        clicks: {
          label: 'Click attribution',
          canonical: true,
          description: 'Canonical reporting totals from click-attributed order credits.',
          totals: {
            visits: 1240,
            orders: 48,
            revenue: 5210.5,
            spend: 0,
            conversionRate: 48 / 1240,
            roas: null
          }
        },
        deterministicViews: {
          label: 'Deterministic view layer',
          canonical: false,
          description: 'Layer-only Meta API-verified deterministic view/impression attribution.',
          totals: {
            visits: 0,
            orders: 0,
            revenue: 0,
            spend: 0,
            conversionRate: 0,
            roas: null
          }
        },
        metaViewThrough: {
          label: 'Meta API view-through',
          canonical: false,
          description: 'Meta API-reported view-through purchase revenue, purchases, and ROAS from impression-time reporting.',
          totals: {
            visits: 0,
            orders: 0,
            revenue: 0,
            spend: 0,
            conversionRate: 0,
            roas: null
          }
        }
      }
    });
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('reporting summary defaults to click-only canonical totals and exposes non-canonical comparison labels', async () => {
  pool.query = (async (text: string) => {
    if (text.includes('FROM daily_reporting_metrics')) {
      return {
        rows: [
          {
            visits: '100',
            orders: '4',
            revenue: '400.00',
            spend: '100.00'
          }
        ]
      };
    }

    if (text.includes('FROM deterministic_model_outputs')) {
      assert.match(text, /dmo\.model_key = 'deterministic_views'/);
      return {
        rows: [
          {
            visits: '0',
            orders: '1.5',
            revenue: '150.00',
            spend: '0.00'
          }
        ]
      };
    }

    return {
      rows: [
        {
          visits: '0',
          orders: '3',
          revenue: '225.00',
          spend: '75.00'
        }
      ]
    };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const canonical = await requestJson(
      server,
      '/api/reporting/summary?startDate=2026-04-01&endDate=2026-04-10'
    );
    const deterministicViews = await requestJson(
      server,
      '/api/reporting/summary?startDate=2026-04-01&endDate=2026-04-10&reportingMode=deterministic_views'
    );
    const metaViewThrough = await requestJson(
      server,
      '/api/reporting/summary?startDate=2026-04-01&endDate=2026-04-10&reportingMode=meta_view_through'
    );
    const comparison = await requestJson(
      server,
      '/api/reporting/summary?startDate=2026-04-01&endDate=2026-04-10&reportingMode=combined'
    );

    assert.equal(canonical.response.status, 200);
    assert.equal(canonical.body.reportingMode, 'clicks');
    assert.equal(canonical.body.reportingModeLabel, 'Click attribution');
    assert.equal(canonical.body.totalsCanonical, true);
    assert.deepEqual(canonical.body.totals, {
      visits: 100,
      orders: 4,
      revenue: 400,
      spend: 100,
      conversionRate: 4 / 100,
      roas: 4
    });
    assert.deepEqual(canonical.body.comparisonTotals.combined.totals, {
      visits: 100,
      orders: 5.5,
      revenue: 550,
      spend: 100,
      conversionRate: 5.5 / 100,
      roas: 5.5
    });
    assert.equal(canonical.body.comparisonTotals.combined.canonical, false);
    assert.equal(canonical.body.comparisonTotals.combined.label, 'Non-canonical comparison total');
    assert.deepEqual(canonical.body.layers.deterministicViews.totals, {
      visits: 0,
      orders: 1.5,
      revenue: 150,
      spend: 0,
      conversionRate: 0,
      roas: null
    });
    assert.deepEqual(canonical.body.layers.metaViewThrough.totals, {
      visits: 0,
      orders: 3,
      revenue: 225,
      spend: 75,
      conversionRate: 0,
      roas: 3
    });

    assert.equal(deterministicViews.response.status, 200);
    assert.equal(deterministicViews.body.reportingMode, 'deterministic_views');
    assert.equal(deterministicViews.body.reportingModeLabel, 'Deterministic view layer');
    assert.equal(deterministicViews.body.totalsCanonical, false);
    assert.deepEqual(deterministicViews.body.totals, deterministicViews.body.layers.deterministicViews.totals);

    assert.equal(metaViewThrough.response.status, 200);
    assert.equal(metaViewThrough.body.reportingMode, 'meta_view_through');
    assert.equal(metaViewThrough.body.reportingModeLabel, 'Meta API view-through');
    assert.equal(metaViewThrough.body.totalsCanonical, false);
    assert.deepEqual(metaViewThrough.body.totals, metaViewThrough.body.layers.metaViewThrough.totals);

    assert.equal(comparison.response.status, 200);
    assert.equal(comparison.body.reportingMode, 'combined');
    assert.equal(comparison.body.reportingModeLabel, 'Non-canonical comparison total');
    assert.equal(comparison.body.totalsCanonical, false);
    assert.deepEqual(comparison.body.totals, comparison.body.comparisonTotals.combined.totals);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('reporting summary supports click and deterministic-view layer-only responses', async () => {
  pool.query = (async (text: string) => {
    if (text.includes('FROM daily_reporting_metrics')) {
      return {
        rows: [
          {
            visits: '20',
            orders: '2',
            revenue: '80.00',
            spend: '40.00'
          }
        ]
      };
    }

    return {
      rows: [
        {
          visits: '0',
          orders: '0.5',
          revenue: '25.00',
          spend: '0.00'
        }
      ]
    };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const clicks = await requestJson(
      server,
      '/api/reporting/summary?startDate=2026-04-01&endDate=2026-04-10&reportingMode=clicks'
    );
    const deterministicViews = await requestJson(
      server,
      '/api/reporting/summary?startDate=2026-04-01&endDate=2026-04-10&reportingMode=deterministic_views'
    );

    assert.deepEqual(clicks.body.totals, clicks.body.layers.clicks.totals);
    assert.deepEqual(deterministicViews.body.totals, deterministicViews.body.layers.deterministicViews.totals);
    assert.notDeepEqual(clicks.body.totals, clicks.body.comparisonTotals.combined.totals);
    assert.notDeepEqual(deterministicViews.body.totals, deterministicViews.body.comparisonTotals.combined.totals);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('reporting routes reject invalid date ranges before querying aggregates', async () => {
  let queryCalls = 0;
  pool.query = (async () => {
    queryCalls += 1;
    return { rows: [] };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/reporting/summary?startDate=2026-04-10&endDate=2026-04-01'
    );

    assert.equal(response.status, 400);
    assert.equal(body.error, 'invalid_request');
    assert.equal(queryCalls, 0);
    assert.deepEqual(body.details.fieldErrors.startDate, ['startDate must be on or before endDate']);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('reporting campaigns returns campaign rows sorted for dashboard tables', async () => {
  pool.query = (async (text: string, params?: unknown[]) => {
    if (text.includes('FROM daily_reporting_metrics')) {
      assert.match(text, /GROUP BY source, medium, campaign, content/);
      assert.deepEqual(params, ['2026-04-01', '2026-04-10', 'last_touch', 2]);

      return {
        rows: [
          {
            source: 'google',
            medium: 'cpc',
            campaign: 'spring-sale',
            content: 'hero-ad-1',
            visits: '420',
            orders: '19',
            revenue: '2110.00'
          },
          {
            source: 'meta',
            medium: 'paid_social',
            campaign: 'prospecting-us',
            content: '',
            visits: '310',
            orders: '9',
            revenue: '880.25'
          }
        ]
      };
    }

    assert.match(text, /ad_platform_entity_metadata/);
    assert.deepEqual(params, ['2026-04-01', '2026-04-10', ['spring-sale', 'prospecting-us'], null]);

    return {
      rows: [
        {
          source: 'google',
          medium: 'cpc',
          campaign: 'spring-sale',
          platform: 'google_ads',
          account_id: 'acct-google',
          entity_id: 'cmp_google_1',
          fallback_name: 'Google Raw Spring Sale',
          latest_name: 'Google Spring Sale Latest',
          last_seen_at: new Date('2026-04-10T08:00:00.000Z'),
          updated_at: new Date('2026-04-10T08:05:00.000Z'),
          rank_by_group: 1,
          rank_by_campaign: 1
        }
      ]
    };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/reporting/campaigns?startDate=2026-04-01&endDate=2026-04-10&limit=2'
    );

    assert.equal(response.status, 200);
    assert.equal(response.headers.get('x-roas-radar-reporting-schema'), REPORTING_SCHEMA_VERSION);
    assert.deepEqual(body, {
      rows: [
        {
          source: 'google',
          medium: 'cpc',
          campaign: 'spring-sale',
          content: 'hero-ad-1',
          visits: 420,
          orders: 19,
          revenue: 2110,
          conversionRate: 19 / 420,
          campaignDisplayName: 'Google Spring Sale Latest',
          campaignEntityId: 'cmp_google_1',
          campaignEntityType: 'campaign',
          campaignPlatform: 'google_ads',
          campaignNameResolutionStatus: 'resolved',
          campaignLabel: buildCampaignLabel(
            'Google Spring Sale Latest',
            'cmp_google_1',
            'google_ads',
            'resolved',
            '2026-04-10T08:00:00.000Z',
            '2026-04-10T08:05:00.000Z',
            {
              rawId: 'spring-sale',
              entityType: 'campaign'
            }
          )
        },
        {
          source: 'meta',
          medium: 'paid_social',
          campaign: 'prospecting-us',
          content: null,
          visits: 310,
          orders: 9,
          revenue: 880.25,
          conversionRate: 9 / 310
        }
      ],
      nextCursor: null
    });
    assert.deepEqual(
      body.rows.map((row: { campaignDisplayName?: string; campaignLabel?: { displayName: string }; campaign: string }) => row.campaignDisplayName ?? row.campaign),
      body.rows.map((row: { campaignLabel?: { displayName: string }; campaign: string }) => row.campaignLabel?.displayName ?? row.campaign)
    );
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('reporting campaigns resolve attributed Meta campaign and ad set ids before returning rows', async () => {
  pool.query = (async (text: string, params?: unknown[]) => {
    if (text.includes('FROM daily_reporting_metrics')) {
      assert.match(text, /GROUP BY source, medium, campaign, content/);
      assert.deepEqual(params, ['2026-04-01', '2026-04-10', 'last_touch', 5]);

      return {
        rows: [
          {
            source: 'facebook',
            medium: 'paid_social',
            campaign: '333',
            content: null,
            visits: '40',
            orders: '4',
            revenue: '500.00'
          },
          {
            source: 'facebook',
            medium: 'paid_social',
            campaign: '444',
            content: null,
            visits: '25',
            orders: '3',
            revenue: '300.00'
          },
          {
            source: 'meta',
            medium: 'paid_social',
            campaign: '777',
            content: null,
            visits: '18',
            orders: '2',
            revenue: '150.00'
          },
          {
            source: 'google',
            medium: 'cpc',
            campaign: '555',
            content: null,
            visits: '10',
            orders: '1',
            revenue: '50.00'
          }
        ]
      };
    }

    if (text.includes('FROM google_candidates')) {
      assert.deepEqual(params, ['2026-04-01', '2026-04-10', ['333', '444', '777', '555'], null]);
      return { rows: [] };
    }

    if (text.includes('WITH requested_ids')) {
      assert.deepEqual(params, [['333', '444', '777', '555'], '2026-04-01', '2026-04-10', null, false, false]);
      return {
        rows: [
          {
            ad_account_id: '123456789',
            object_type: 'campaign',
            object_id: '333',
            object_name: 'Awareness Campaign',
            parent_campaign_id: null,
            parent_campaign_name: null,
            last_seen_at: new Date('2026-04-10T00:00:00.000Z'),
            metadata_source: 'spend'
          },
          {
            ad_account_id: '123456789',
            object_type: 'adset',
            object_id: '333',
            object_name: null,
            parent_campaign_id: null,
            parent_campaign_name: null,
            last_seen_at: null,
            metadata_source: 'active_account'
          },
          {
            ad_account_id: '123456789',
            object_type: 'campaign',
            object_id: '444',
            object_name: null,
            parent_campaign_id: null,
            parent_campaign_name: null,
            last_seen_at: null,
            metadata_source: 'active_account'
          },
          {
            ad_account_id: '123456789',
            object_type: 'adset',
            object_id: '444',
            object_name: 'US Prospecting Ad Set',
            parent_campaign_id: '333',
            parent_campaign_name: 'Awareness Campaign',
            last_seen_at: new Date('2026-04-10T00:00:00.000Z'),
            metadata_source: 'spend'
          },
          {
            ad_account_id: '123456789',
            object_type: 'campaign',
            object_id: '777',
            object_name: 'Campaign Using Shared Id',
            parent_campaign_id: null,
            parent_campaign_name: null,
            last_seen_at: new Date('2026-04-10T00:00:00.000Z'),
            metadata_source: 'spend'
          },
          {
            ad_account_id: '123456789',
            object_type: 'adset',
            object_id: '777',
            object_name: 'Ad Set Using Shared Id',
            parent_campaign_id: '333',
            parent_campaign_name: 'Awareness Campaign',
            last_seen_at: new Date('2026-04-10T00:00:00.000Z'),
            metadata_source: 'spend'
          }
        ]
      };
    }

    if (text.includes('FROM meta_ads_metadata_cache c')) {
      const requested = JSON.parse(String(params?.[0])) as Array<{
        ad_account_id: string;
        object_type: string;
        object_id: string;
      }>;
      assert.deepEqual(
        requested.sort((left, right) => `${left.object_type}:${left.object_id}`.localeCompare(`${right.object_type}:${right.object_id}`)),
        [
          {
            ad_account_id: '123456789',
            object_type: 'adset',
            object_id: '333'
          },
          {
            ad_account_id: '123456789',
            object_type: 'adset',
            object_id: '444'
          },
          {
            ad_account_id: '123456789',
            object_type: 'adset',
            object_id: '777'
          },
          {
            ad_account_id: '123456789',
            object_type: 'campaign',
            object_id: '333'
          },
          {
            ad_account_id: '123456789',
            object_type: 'campaign',
            object_id: '444'
          },
          {
            ad_account_id: '123456789',
            object_type: 'campaign',
            object_id: '777'
          }
        ]
      );

      return {
        rows: [
          {
            ad_account_id: '123456789',
            object_type: 'campaign',
            object_id: '333',
            object_name: 'Awareness Campaign',
            status: 'ACTIVE',
            last_fetched_at: new Date('2026-06-02T15:00:00.000Z')
          },
          {
            ad_account_id: '123456789',
            object_type: 'adset',
            object_id: '444',
            object_name: 'US Prospecting Ad Set',
            status: 'ACTIVE',
            last_fetched_at: new Date('2026-06-02T15:00:00.000Z')
          },
          {
            ad_account_id: '123456789',
            object_type: 'campaign',
            object_id: '777',
            object_name: 'Campaign Using Shared Id',
            status: 'ACTIVE',
            last_fetched_at: new Date('2026-06-02T15:00:00.000Z')
          },
          {
            ad_account_id: '123456789',
            object_type: 'adset',
            object_id: '777',
            object_name: 'Ad Set Using Shared Id',
            status: 'ACTIVE',
            last_fetched_at: new Date('2026-06-02T15:00:00.000Z')
          }
        ]
      };
    }

    throw new Error(`Unexpected query: ${text}`);
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/reporting/campaigns?startDate=2026-04-01&endDate=2026-04-10&limit=5'
    );

    assert.equal(response.status, 200);
    assert.deepEqual(body.rows, [
      {
        source: 'meta',
        medium: 'paid_social',
        campaign: '333',
        content: null,
        visits: 40,
        orders: 4,
        revenue: 500,
        conversionRate: 4 / 40,
        campaignDisplayName: 'Awareness Campaign',
        campaignEntityId: '333',
        campaignEntityType: 'campaign',
        parentCampaignEntityId: null,
        parentCampaignDisplayName: null,
        campaignPlatform: 'meta_ads',
        campaignNameResolutionStatus: 'resolved',
        campaignLabel: buildCampaignLabel(
          'Awareness Campaign',
          '333',
          'meta_ads',
          'resolved',
          '2026-04-10T00:00:00.000Z',
          '2026-04-10T00:00:00.000Z',
          {
            entityType: 'campaign',
            parentCampaignEntityId: null,
            parentCampaignDisplayName: null
          }
        )
      },
      {
        source: 'meta',
        medium: 'paid_social',
        campaign: '444',
        content: null,
        visits: 25,
        orders: 3,
        revenue: 300,
        conversionRate: 3 / 25,
        campaignDisplayName: 'US Prospecting Ad Set',
        campaignEntityId: '444',
        campaignEntityType: 'adset',
        parentCampaignEntityId: '333',
        parentCampaignDisplayName: 'Awareness Campaign',
        campaignPlatform: 'meta_ads',
        campaignNameResolutionStatus: 'resolved',
        campaignLabel: buildCampaignLabel(
          'US Prospecting Ad Set',
          '444',
          'meta_ads',
          'resolved',
          '2026-04-10T00:00:00.000Z',
          '2026-04-10T00:00:00.000Z',
          {
            entityType: 'adset',
            parentCampaignEntityId: '333',
            parentCampaignDisplayName: 'Awareness Campaign'
          }
        )
      },
      {
        source: 'meta',
        medium: 'paid_social',
        campaign: '777',
        content: null,
        visits: 18,
        orders: 2,
        revenue: 150,
        conversionRate: 2 / 18
      },
      {
        source: 'google',
        medium: 'cpc',
        campaign: '555',
        content: null,
        visits: 10,
        orders: 1,
        revenue: 50,
        conversionRate: 1 / 10
      }
    ]);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('reporting campaigns return raw Meta id label metadata when display resolution is unavailable', async () => {
  pool.query = (async (text: string, params?: unknown[]) => {
    if (text.includes('FROM daily_reporting_metrics')) {
      assert.deepEqual(params, ['2026-04-01', '2026-04-10', 'last_touch', 1]);

      return {
        rows: [
          {
            source: 'facebook',
            medium: 'paid_social',
            campaign: '888',
            content: null,
            visits: '12',
            orders: '1',
            revenue: '80.00'
          }
        ]
      };
    }

    if (text.includes('FROM google_candidates')) {
      assert.deepEqual(params, ['2026-04-01', '2026-04-10', ['888'], null]);
      return { rows: [] };
    }

    if (text.includes('WITH requested_ids')) {
      assert.deepEqual(params, [['888'], '2026-04-01', '2026-04-10', null, false, false]);
      return {
        rows: [
          {
            ad_account_id: '123456789',
            object_type: 'campaign',
            object_id: '888',
            object_name: null,
            parent_campaign_id: null,
            parent_campaign_name: null,
            last_seen_at: new Date('2026-04-10T00:00:00.000Z')
          }
        ]
      };
    }

    if (text.includes('FROM meta_ads_metadata_cache c')) {
      assert.deepEqual(JSON.parse(String(params?.[0])), [
        {
          ad_account_id: '123456789',
          object_type: 'campaign',
          object_id: '888'
        }
      ]);
      return { rows: [] };
    }

    throw new Error(`Unexpected query: ${text}`);
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/reporting/campaigns?startDate=2026-04-01&endDate=2026-04-10&limit=1'
    );

    assert.equal(response.status, 200);
    assert.deepEqual(body.rows, [
      {
        source: 'facebook',
        medium: 'paid_social',
        campaign: '888',
        content: null,
        visits: 12,
        orders: 1,
        revenue: 80,
        conversionRate: 1 / 12,
        campaignDisplayName: '888',
        campaignEntityId: '888',
        campaignEntityType: 'campaign',
        parentCampaignEntityId: null,
        parentCampaignDisplayName: null,
        campaignPlatform: null,
        campaignNameResolutionStatus: 'unresolved',
        campaignLabel: buildCampaignLabel(
          '888',
          '888',
          null,
          'unresolved',
          '2026-04-10T00:00:00.000Z',
          null,
          {
            source: 'facebook',
            rawId: '888',
            entityType: 'campaign',
            parentCampaignEntityId: null,
            parentCampaignDisplayName: null
          }
        )
      }
    ]);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('reporting campaigns resolve raw Meta attributed IDs from platform entity metadata', async () => {
  pool.query = (async (text: string, params?: unknown[]) => {
    if (text.includes('FROM daily_reporting_metrics')) {
      assert.deepEqual(params, ['2026-04-01', '2026-04-10', 'last_touch', 1]);

      return {
        rows: [
          {
            source: 'facebook',
            medium: 'paid_social',
            campaign: '999',
            content: null,
            visits: '14',
            orders: '2',
            revenue: '120.00'
          }
        ]
      };
    }

    if (text.includes('FROM google_candidates')) {
      assert.deepEqual(params, ['2026-04-01', '2026-04-10', ['999'], null]);
      return { rows: [] };
    }

    if (text.includes('WITH requested_ids')) {
      assert.deepEqual(params, [['999'], '2026-04-01', '2026-04-10', null, false, false]);
      return {
        rows: [
          {
            ad_account_id: '123456789',
            object_type: 'adset',
            object_id: '999',
            object_name: 'Platform Metadata Ad Set',
            parent_campaign_id: null,
            parent_campaign_name: null,
            last_seen_at: new Date('2026-04-09T12:00:00.000Z'),
            metadata_source: 'ad_platform_entity_metadata'
          }
        ]
      };
    }

    if (text.includes('FROM meta_ads_metadata_cache c')) {
      assert.deepEqual(JSON.parse(String(params?.[0])), [
        {
          ad_account_id: '123456789',
          object_type: 'adset',
          object_id: '999'
        }
      ]);
      return { rows: [] };
    }

    throw new Error(`Unexpected query: ${text}`);
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/reporting/campaigns?startDate=2026-04-01&endDate=2026-04-10&limit=1'
    );

    assert.equal(response.status, 200);
    assert.deepEqual(body.rows, [
      {
        source: 'meta',
        medium: 'paid_social',
        campaign: '999',
        content: null,
        visits: 14,
        orders: 2,
        revenue: 120,
        conversionRate: 2 / 14,
        campaignDisplayName: 'Platform Metadata Ad Set',
        campaignEntityId: '999',
        campaignEntityType: 'adset',
        parentCampaignEntityId: null,
        parentCampaignDisplayName: null,
        campaignPlatform: 'meta_ads',
        campaignNameResolutionStatus: 'resolved',
        campaignLabel: buildCampaignLabel(
          'Platform Metadata Ad Set',
          '999',
          'meta_ads',
          'resolved',
          '2026-04-09T12:00:00.000Z',
          '2026-04-09T12:00:00.000Z',
          {
            rawId: '999',
            entityType: 'adset',
            parentCampaignEntityId: null,
            parentCampaignDisplayName: null
          }
        )
      }
    ]);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('reporting spend details return channel groups with campaign subtotals in descending order', async () => {
  pool.query = (async (text: string, params?: unknown[]) => {
    if (text.includes('FROM daily_reporting_metrics')) {
      assert.match(text, /GROUP BY source, medium, campaign/);
      assert.match(text, /AND spend > 0/);
      assert.deepEqual(params, ['2026-04-01', '2026-04-10', 'last_touch']);

      return {
        rows: [
          {
            source: 'google',
            medium: 'cpc',
            campaign: 'spring-search',
            spend: '1200.00'
          },
          {
            source: 'google',
            medium: 'cpc',
            campaign: 'brand-search',
            spend: '300.00'
          },
          {
            source: 'meta',
            medium: 'paid_social',
            campaign: 'prospecting-us',
            spend: '900.50'
          }
        ]
      };
    }

    assert.match(text, /ad_platform_entity_metadata/);
    assert.deepEqual(params, ['2026-04-01', '2026-04-10', ['spring-search', 'brand-search', 'prospecting-us'], null]);

    return { rows: [] };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/reporting/spend-details?startDate=2026-04-01&endDate=2026-04-10'
    );

    assert.equal(response.status, 200);
    assert.equal(response.headers.get('x-roas-radar-reporting-schema'), REPORTING_SCHEMA_VERSION);
    assert.deepEqual(body, {
      summary: {
        totalSpend: 2400.5,
        activeChannels: 2,
        activeCampaigns: 3,
        averageDailySpend: 240.05,
        topChannel: {
          source: 'google',
          medium: 'cpc',
          channel: 'google / cpc',
          spend: 1500
        }
      },
      groups: [
        {
          source: 'google',
          medium: 'cpc',
          channel: 'google / cpc',
          subtotal: 1500,
          campaigns: [
            {
              campaign: 'spring-search',
              spend: 1200
            },
            {
              campaign: 'brand-search',
              spend: 300
            }
          ]
        },
        {
          source: 'meta',
          medium: 'paid_social',
          channel: 'meta / paid_social',
          subtotal: 900.5,
          campaigns: [
            {
              campaign: 'prospecting-us',
              spend: 900.5
            }
          ]
        }
      ],
      totalSpend: 2400.5
    });
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('reporting timeseries returns grouped points for the requested dimension', async () => {
  pool.query = (async (text: string, params?: unknown[]) => {
    assert.match(text, /SELECT\s+source AS bucket/);
    assert.deepEqual(params, ['2026-04-01', '2026-04-10', 'last_touch']);

    return {
      rows: [
        {
          bucket: 'google',
          visits: '900',
          orders: '33',
          revenue: '3000.00',
          spend: '1200.00'
        },
        {
          bucket: 'meta',
          visits: '340',
          orders: '15',
          revenue: '2210.50',
          spend: '900.50'
        }
      ]
    };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/reporting/timeseries?startDate=2026-04-01&endDate=2026-04-10&groupBy=source'
    );

    assert.equal(response.status, 200);
    assert.equal(response.headers.get('x-roas-radar-reporting-schema'), REPORTING_SCHEMA_VERSION);
    assert.deepEqual(body, {
      points: [
        {
          date: 'google',
          visits: 900,
          orders: 33,
          revenue: 3000
        },
        {
          date: 'meta',
          visits: 340,
          orders: 15,
          revenue: 2210.5
        }
      ],
      lowestBuckets: [
        {
          bucket: 'meta',
          visits: 340,
          orders: 15,
          revenue: 2210.5,
          spend: 900.5,
          conversionRate: 15 / 340,
          roas: 2210.5 / 900.5
        },
        {
          bucket: 'google',
          visits: 900,
          orders: 33,
          revenue: 3000,
          spend: 1200,
          conversionRate: 33 / 900,
          roas: 2.5
        }
      ]
    });
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('reporting orders returns order-level attribution details for debugging', async () => {
  pool.query = (async (text: string, params?: unknown[]) => {
    if (text.includes('INSERT INTO app_settings')) {
      assert.deepEqual(params, ['America/Los_Angeles']);
      return { rows: [], rowCount: 0 };
    }

    if (text.includes('SELECT reporting_timezone')) {
      return {
        rows: [
          {
            reporting_timezone: 'America/Los_Angeles',
            updated_at: new Date('2026-04-01T00:00:00.000Z')
          }
        ],
        rowCount: 1
      };
    }

    assert.match(text, /LEFT JOIN LATERAL/);
    assert.match(text, /COALESCE\(o\.source_name, ''\) = 'web'/);
    assert.deepEqual(
      params,
      ['2026-04-01', '2026-04-10', 'last_touch', 'facebook', 'deterministic_first_party', 'America/Los_Angeles', 1]
    );

    return {
      rows: [
        {
          shopify_order_id: '1234567890',
          processed_at: new Date('2026-04-10T13:00:00.000Z'),
          total_price: '120.00',
          attribution_tier: 'deterministic_first_party',
          attribution_source: 'checkout_token',
          attribution_source_code: 'checkout_token',
          matching_method_code: 'matched_by_checkout_token',
          order_attribution_reason: 'matched_by_checkout_token',
          attribution_matched_at: new Date('2026-04-10T13:01:00.000Z'),
          attribution_confidence_score: '1.00',
          last_attribution_run_at: new Date('2026-04-10T13:01:00.000Z'),
          attribution_snapshot: {
            confidenceScore: 1,
            winner: {
              sessionId: '11111111-1111-4111-8111-111111111111',
              source: 'facebook',
              medium: 'paid_social',
              campaign: 'prospecting-us'
            }
          },
          attributed_source: 'facebook',
          attributed_medium: 'paid_social',
          attributed_campaign: 'prospecting-us',
          primary_credit_attribution_reason: 'matched_by_checkout_token'
        }
      ]
    };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/reporting/orders?startDate=2026-04-01&endDate=2026-04-10&source=facebook&attributionTier=deterministic_first_party&limit=1'
    );

    assert.equal(response.status, 200);
    assert.deepEqual(body, {
      rows: [
        {
          shopifyOrderId: '1234567890',
          processedAt: '2026-04-10T13:00:00.000Z',
          orderOccurredAtUtc: '2026-04-10T13:00:00.000Z',
          totalPrice: 120,
          source: 'facebook',
          medium: 'paid_social',
          campaign: 'prospecting-us',
          attributionReason: 'matched_by_checkout_token',
          primaryCreditAttributionReason: 'matched_by_checkout_token',
          attributionTier: 'deterministic_first_party',
          attributionTierLabel: 'Deterministic first-party',
          attributionTierDescription:
            'Resolved from durable ROAS Radar first-party evidence such as a landing session, checkout token, cart token, or stitched identity path.',
          attributionSource: 'checkout_token',
          matchingMethod: 'matched_by_checkout_token',
          attributionMatchedAt: '2026-04-10T13:01:00.000Z',
          confidenceScore: 1,
          lastAttributionRunAt: '2026-04-10T13:01:00.000Z',
          sessionId: '11111111-1111-4111-8111-111111111111'
        }
      ]
    });
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('reporting order details expose attribution tier metadata additively', async () => {
  pool.query = (async (text: string, params?: unknown[]) => {
    if (text.includes('FROM shopify_orders o')) {
      assert.deepEqual(params, ['1234567890']);

      return {
        rows: [
          {
            shopify_order_id: '1234567890',
            shopify_order_number: 'RR-1001',
            shopify_customer_id: 'gid://shopify/Customer/42',
            customer_identity_id: '22222222-2222-4222-8222-222222222222',
            email_hash: 'hash_abc123',
            currency_code: 'USD',
            subtotal_price: '100.00',
            total_price: '120.00',
            financial_status: 'paid',
            fulfillment_status: 'fulfilled',
            processed_at: new Date('2026-04-10T13:00:00.000Z'),
            created_at_shopify: new Date('2026-04-10T12:58:00.000Z'),
            updated_at_shopify: new Date('2026-04-10T13:05:00.000Z'),
            landing_session_id: '33333333-3333-4333-8333-333333333333',
            checkout_token: 'checkout-123',
            cart_token: 'cart-123',
            source_name: 'web',
            attribution_tier: 'deterministic_first_party',
            attribution_source: 'landing_session_id',
            attribution_source_code: 'landing_session_id',
            matching_method_code: 'matched_by_landing_session',
            attribution_matched_at: new Date('2026-04-10T13:01:00.000Z'),
            attribution_reason: 'matched_by_landing_session',
            attribution_confidence_score: '1.00',
            last_attribution_run_at: new Date('2026-04-10T13:01:00.000Z'),
            attribution_snapshot: {
              confidenceScore: 1,
              winner: {
                sessionId: '33333333-3333-4333-8333-333333333333',
                source: 'google',
                medium: 'cpc',
                campaign: 'brand-search',
                content: 'hero',
                term: 'widget',
                clickIdType: 'gclid',
                clickIdValue: 'gclid-123'
              }
            },
            attribution_snapshot_updated_at: new Date('2026-04-10T13:01:30.000Z'),
            ingested_at: new Date('2026-04-10T13:02:00.000Z'),
            raw_payload: { id: '1234567890' }
          }
        ],
        rowCount: 1
      };
    }

    if (text.includes('FROM shopify_order_line_items li')) {
      return {
        rows: [],
        rowCount: 0
      };
    }

    if (text.includes('FROM attribution_order_credits c')) {
      return {
        rows: [],
        rowCount: 0
      };
    }

    throw new Error(`Unexpected SQL in order details test: ${text}`);
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(server, '/api/reporting/orders/1234567890');

    assert.equal(response.status, 200);
    assert.equal(body.order.shopifyOrderId, '1234567890');
    assert.equal(body.order.orderOccurredAtUtc, '2026-04-10T13:00:00.000Z');
    assert.equal(body.order.attributionTier, 'deterministic_first_party');
    assert.equal(body.order.attributionTierLabel, 'Deterministic first-party');
    assert.match(body.order.attributionTierDescription, /durable ROAS Radar first-party evidence/);
    assert.equal(body.order.attributionSource, 'landing_session_id');
    assert.equal(body.order.matchingMethod, 'matched_by_landing_session');
    assert.equal(body.order.attributionMatchedAt, '2026-04-10T13:01:00.000Z');
    assert.equal(body.order.attributionReason, 'matched_by_landing_session');
    assert.equal(body.order.confidenceScore, 1);
    assert.equal(body.order.lastAttributionRunAt, '2026-04-10T13:01:00.000Z');
    assert.equal(body.order.sessionId, '33333333-3333-4333-8333-333333333333');
    assert.equal(body.order.attributedSource, 'google');
    assert.equal(body.order.attributedMedium, 'cpc');
    assert.equal(body.order.attributedCampaign, 'brand-search');
    assert.equal(body.order.attributedContent, 'hero');
    assert.equal(body.order.attributedTerm, 'widget');
    assert.equal(body.order.attributedClickIdType, 'gclid');
    assert.equal(body.order.attributedClickIdValue, 'gclid-123');
    assert.equal(body.order.attributionSnapshotUpdatedAt, '2026-04-10T13:01:30.000Z');
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('reporting reconciliation returns persisted data quality checks', async () => {
  pool.query = (async (text: string) => {
    if (text.includes('FROM data_quality_check_runs')) {
      return {
        rows: [
          {
            run_date: '2026-04-10',
            check_key: 'shopify_webhook_gaps',
            status: 'failed',
            severity: 'critical',
            discrepancy_count: 3,
            summary: '3 orders are missing webhook receipts.',
            details: {
              sampleMissingOrderIds: ['1001', '1002', '1003']
            },
            checked_at: new Date('2026-04-11T00:15:00.000Z'),
            alert_emitted_at: new Date('2026-04-11T00:15:00.000Z')
          }
        ]
      };
    }

    throw new Error(`Unexpected SQL in reconciliation test: ${text}`);
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/reporting/reconciliation?runDate=2026-04-10'
    );

    assert.equal(response.status, 200);
    assert.equal(body.version, '2026-04-11');
    assert.equal(body.tenantId, 'roas-radar');
    assert.equal(body.data.runDate, '2026-04-10');
    assert.equal(body.data.totals.failedChecks, 1);
    assert.equal(body.data.totals.totalDiscrepancies, 3);
    assert.equal(body.data.checks[0].checkKey, 'shopify_webhook_gaps');
    assert.deepEqual(body.data.checks[0].details.sampleMissingOrderIds, ['1001', '1002', '1003']);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});
