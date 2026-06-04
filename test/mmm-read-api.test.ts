import assert from 'node:assert/strict';
import type { AddressInfo } from 'node:net';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';
process.env.REPORTING_API_TOKEN = 'test-reporting-token';

const poolModule = await import('../src/db/pool.js');
const serverModule = await import('../src/server.js');

const { pool } = poolModule;
const { closeServer, createServer } = serverModule;
const originalPoolQuery = pool.query.bind(pool);
const MMM_SCHEMA_VERSION = 'mmm_daily_input_mart_v1';

function buildHeaders(): Record<string, string> {
  return {
    authorization: 'Bearer test-reporting-token'
  };
}

async function request(server: ReturnType<typeof createServer>, path: string, headers = buildHeaders()) {
  const address = server.address() as AddressInfo;
  const response = await fetch(`http://127.0.0.1:${address.port}${path}`, {
    headers
  });

  return response;
}

async function requestJson(server: ReturnType<typeof createServer>, path: string, headers = buildHeaders()) {
  const response = await request(server, path, headers);
  const body = await response.json();

  return { response, body };
}

async function postJson(
  server: ReturnType<typeof createServer>,
  path: string,
  payload: Record<string, unknown>,
  headers = buildHeaders()
) {
  const address = server.address() as AddressInfo;
  const response = await fetch(`http://127.0.0.1:${address.port}${path}`, {
    method: 'POST',
    headers: {
      ...headers,
      'content-type': 'application/json'
    },
    body: JSON.stringify(payload)
  });
  const body = await response.json();

  return { response, body };
}

type GateQueryMode = 'clean' | 'changed' | 'blocked';
type TestAuthUser = {
  email: string;
  isAdmin?: boolean;
  id?: number;
};

function installReadinessGateQueryMock(getMode: () => GateQueryMode = () => 'clean', authUser?: TestAuthUser) {
  let gate: Record<string, unknown> | null = null;

  function buildGateRow(params: unknown[]) {
    const now = new Date('2026-04-04T10:00:00.000Z');

    return {
      id: '11111111-1111-4111-8111-111111111111',
      gate_version: 'mmm_readiness_gate_v1',
      start_date: params[0],
      end_date: params[1],
      mart_row_type: params[2],
      attribution_model: params[3],
      platform: params[4],
      source: params[5],
      campaign: params[6],
      evidence_payload: JSON.parse(String(params[7])),
      checklist_statuses: JSON.parse(String(params[8])),
      owner_approvals: JSON.parse(String(params[9])),
      waivers: [],
      unresolved_critical_issue_count: params[10],
      evidence_hash: params[11],
      gate_status: params[12],
      final_state: params[13],
      decision_reason: params[14],
      decided_by: params[15],
      decided_at: params[12] === 'approved' ? now : null,
      created_by: params[16],
      updated_by: params[16],
      created_at: now,
      updated_at: now
    };
  }

  pool.query = (async (text: string, params?: unknown[]) => {
    if (text.includes('FROM app_sessions s')) {
      return {
        rows: authUser
          ? [
              {
                session_id: 7,
                user_id: authUser.id ?? 42,
                email: authUser.email,
                display_name: authUser.email,
                is_admin: authUser.isAdmin ?? false,
                status: 'active',
                last_login_at: new Date('2026-04-04T09:00:00.000Z'),
                created_at: new Date('2026-04-01T00:00:00.000Z'),
                expires_at: new Date('2026-05-01T00:00:00.000Z')
              }
            ]
          : []
      };
    }

    if (text.includes('FROM mmm_readiness_gates') && text.includes('WHERE start_date = $1::date')) {
      return { rows: gate ? [gate] : [] };
    }

    if (text.includes('WITH requested_dates')) {
      return {
        rows: [
          {
            metric_date: '2026-04-01',
            matching_row_count: getMode() === 'blocked' ? '0' : '2',
            mart_row_count: '2',
            generation_timestamp: getMode() === 'blocked' ? null : new Date('2026-04-04T08:00:00.000Z')
          }
        ]
      };
    }

    if (text.includes('COUNT(*)::bigint AS total_rows') && text.includes('FROM mmm_daily_input_mart_v1')) {
      return {
        rows: [
          {
            total_rows: getMode() === 'changed' ? '3' : '2',
            paid_media_rows: '1',
            attribution_rows: '1',
            total_spend: '100',
            total_impressions: '1000',
            total_clicks: '50',
            total_shopify_orders: '4',
            total_shopify_revenue: '500',
            total_attribution_credit_orders: '3',
            total_attribution_credit_revenue: '450',
            latest_spend_last_synced_at: new Date('2026-04-04T07:00:00.000Z'),
            latest_shopify_last_ingested_at: new Date('2026-04-04T07:30:00.000Z'),
            latest_attribution_last_computed_at: new Date('2026-04-04T08:00:00.000Z'),
            latest_last_computed_at: new Date('2026-04-04T08:01:00.000Z'),
            unresolved_metadata_rows: getMode() === 'blocked' ? '2' : '0'
          }
        ]
      };
    }

    if (text.includes('FROM ad_exposure_events')) {
      return {
        rows: [
          {
            total_exposures: '10',
            valid_exposures: '10',
            identity_resolved_exposures: '9',
            campaign_joinable_exposures: '10',
            campaign_metadata_resolved_exposures: '9',
            latest_exposure_at: new Date('2026-04-04T08:00:00.000Z')
          }
        ]
      };
    }

    if (text.includes('WITH filtered_mart AS')) {
      return {
        rows: [
          {
            metric_date: null,
            total_rows: '2',
            unknown_source_rows: '0',
            unmapped_source_rows: '0',
            unknown_or_unmapped_source_rows: '0',
            unknown_medium_rows: '0',
            unmapped_medium_rows: '0',
            unknown_or_unmapped_medium_rows: '0',
            unresolved_campaign_metadata_rows: getMode() === 'blocked' ? '2' : '0',
            stale_campaign_metadata_rows: '0',
            native_id_eligible_rows: '2',
            account_id_rows: '2',
            campaign_id_rows: '2',
            adset_id_rows: '2',
            ad_id_rows: '2',
            creative_id_rows: '2',
            platform_native_id_rows: '2'
          }
        ]
      };
    }

    if (text.includes('FROM data_quality_check_runs')) {
      return { rows: [] };
    }

    if (text.includes('FROM mmm_model_runs')) {
      return {
        rows: [
          {
            id: 'run-1',
            model_type: 'baseline_linear_mmm',
            model_version: 'baseline_linear_mmm_v1',
            mart_version: 'mmm_daily_input_mart_v1',
            attribution_model: 'last_touch',
            run_status: 'completed',
            training_start_date: '2026-04-01',
            training_end_date: '2026-04-01',
            holdout_start_date: null,
            holdout_end_date: null,
            run_config: {},
            input_summary: {},
            model_artifact: {},
            calibration_report: {},
            validation_report: {},
            error_code: null,
            error_message: null,
            created_at: new Date('2026-04-04T08:00:00.000Z'),
            started_at: new Date('2026-04-04T08:01:00.000Z'),
            completed_at: new Date('2026-04-04T08:02:00.000Z')
          }
        ]
      };
    }

    if (text.includes('FROM mmm_baseline_calibration_freezes')) {
      return {
        rows: [
          {
            id: '22222222-2222-4222-8222-222222222222',
            freeze_schema_version: 'mmm_baseline_calibration_freeze_v1',
            mart_version: 'mmm_weekly_channel_input_mart_v1',
            snapshot_version: 'mmm_weekly_channel_snapshot_v1',
            freeze_status: 'approved',
            generation_timestamp: new Date('2026-04-04T09:00:00.000Z'),
            calibration_start_date: '2026-04-01',
            calibration_end_date: '2026-04-01',
            attribution_model: 'last_touch',
            evidence_hash: 'a'.repeat(64),
            approved_by: 'analytics@example.com',
            approved_at: new Date('2026-04-04T09:30:00.000Z')
          }
        ]
      };
    }

    if (text.includes('INSERT INTO mmm_readiness_gates')) {
      const nextGate = buildGateRow(params ?? []);

      if (gate && gate.evidence_hash === nextGate.evidence_hash) {
        gate = {
          ...nextGate,
          waivers: gate.waivers
        };
      } else {
        gate = nextGate;
      }

      return { rows: [gate] };
    }

    if (text.includes('UPDATE mmm_readiness_gates') && text.includes('owner_approvals = $2::jsonb')) {
      gate = {
        ...(gate ?? {}),
        owner_approvals: JSON.parse(String(params?.[1])),
        gate_status: params?.[2],
        final_state: params?.[3],
        decision_reason: params?.[4],
        decided_by: params?.[2] === 'approved' ? params?.[5] : gate?.decided_by,
        decided_at: params?.[2] === 'approved' ? new Date('2026-04-04T10:05:00.000Z') : gate?.decided_at,
        updated_by: params?.[5],
        updated_at: new Date('2026-04-04T10:05:00.000Z')
      };

      return { rows: [gate] };
    }

    if (text.includes('UPDATE mmm_readiness_gates') && text.includes('waivers = $2::jsonb')) {
      gate = {
        ...(gate ?? {}),
        waivers: JSON.parse(String(params?.[1])),
        checklist_statuses: JSON.parse(String(params?.[2])),
        unresolved_critical_issue_count: params?.[3],
        owner_approvals: JSON.parse(String(params?.[4])),
        gate_status: params?.[5],
        final_state: params?.[6],
        updated_by: params?.[7],
        updated_at: new Date('2026-04-04T10:10:00.000Z')
      };

      return { rows: [gate] };
    }

    throw new Error(`Unexpected readiness gate query: ${text.slice(0, 120)}`);
  }) as typeof pool.query;

  return {
    get gate() {
      return gate;
    }
  };
}

test('MMM read API requires authentication', async () => {
  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/reporting/mmm?startDate=2026-04-01&endDate=2026-04-02',
      {}
    );

    assert.equal(response.status, 401);
    assert.equal(body.error, 'unauthorized');
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('MMM read API returns schema-versioned rows with readiness exclusions', async () => {
  const calls: string[] = [];
  pool.query = (async (text: string, params?: unknown[]) => {
    calls.push(text);

    if (text.includes('WITH requested_dates')) {
      assert.match(text, /FROM mmm_daily_input_mart_v1/);
      assert.match(text, /mart_row_type = \$3/);
      assert.match(text, /attribution_model = \$4/);
      assert.deepEqual(params, ['2026-04-01', '2026-04-03', 'attribution', 'last_touch']);

      return {
        rows: [
          {
            metric_date: '2026-04-01',
            matching_row_count: '2',
            mart_row_count: '4',
            generation_timestamp: new Date('2026-04-04T08:00:00.000Z')
          },
          {
            metric_date: '2026-04-02',
            matching_row_count: '0',
            mart_row_count: '3',
            generation_timestamp: null
          },
          {
            metric_date: '2026-04-03',
            matching_row_count: '0',
            mart_row_count: '0',
            generation_timestamp: null
          }
        ]
      };
    }

    assert.match(text, /ORDER BY metric_date ASC, mart_row_type ASC/);
    assert.deepEqual(params, ['2026-04-01', '2026-04-03', 'attribution', 'last_touch', 5, 0]);

    return {
      rows: [
        {
          metric_date: '2026-04-01',
          mart_version: 'v1',
          mart_row_type: 'attribution',
          attribution_model: 'last_touch',
          platform: 'taxonomy',
          platform_connection_id: null,
          granularity: 'taxonomy',
          entity_key: 'google|cpc|spring-sale|unknown|unknown',
          account_id: null,
          account_name: null,
          campaign_id: null,
          campaign_name: null,
          adset_id: null,
          adset_name: null,
          ad_id: null,
          ad_name: null,
          creative_id: null,
          creative_name: null,
          source: 'google',
          medium: 'cpc',
          campaign: 'spring-sale',
          content: 'unknown',
          term: 'unknown',
          currency: null,
          spend: '0.00',
          impressions: '0',
          clicks: '0',
          shopify_orders: '7',
          shopify_revenue: '810.50',
          attribution_credit_orders: '6.50000000',
          attribution_credit_revenue: '790.25',
          new_customer_credit_orders: '4.00000000',
          returning_customer_credit_orders: '2.50000000',
          new_customer_credit_revenue: '500.00',
          returning_customer_credit_revenue: '290.25',
          match_source_coverage: { first_party: 6.5 },
          confidence_label_coverage: { high: 6.5 },
          spend_last_synced_at: null,
          shopify_last_ingested_at: new Date('2026-04-04T07:00:00.000Z'),
          attribution_last_computed_at: new Date('2026-04-04T08:00:00.000Z'),
          last_computed_at: new Date('2026-04-04T08:01:00.000Z')
        }
      ]
    };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/reporting/mmm?startDate=2026-04-01&endDate=2026-04-03&martRowType=attribution&attributionModel=last_touch&limit=5'
    );

    assert.equal(response.status, 200);
    assert.equal(response.headers.get('x-roas-radar-mmm-schema'), MMM_SCHEMA_VERSION);
    assert.equal(calls.length, 2);
    assert.deepEqual(body.readiness, {
      status: 'partial',
      generationTimestamp: '2026-04-04T08:00:00.000Z',
      includedDateCount: 1,
      excludedDateWindows: [
        {
          startDate: '2026-04-02',
          endDate: '2026-04-02',
          reason: 'no_rows_matching_filters'
        },
        {
          startDate: '2026-04-03',
          endDate: '2026-04-03',
          reason: 'no_mmm_mart_rows'
        }
      ]
    });
    assert.deepEqual(body.pagination, {
      limit: 5,
      offset: 0,
      returned: 1,
      totalRows: 2,
      hasMore: true
    });
    assert.deepEqual(body.rows[0], {
      date: '2026-04-01',
      martVersion: 'v1',
      martRowType: 'attribution',
      attributionModel: 'last_touch',
      platform: 'taxonomy',
      platformConnectionId: null,
      granularity: 'taxonomy',
      entityKey: 'google|cpc|spring-sale|unknown|unknown',
      accountId: null,
      accountName: null,
      campaignId: null,
      campaignName: null,
      adsetId: null,
      adsetName: null,
      adId: null,
      adName: null,
      creativeId: null,
      creativeName: null,
      source: 'google',
      medium: 'cpc',
      campaign: 'spring-sale',
      content: 'unknown',
      term: 'unknown',
      currency: null,
      spend: 0,
      impressions: 0,
      clicks: 0,
      shopifyOrders: 7,
      shopifyRevenue: 810.5,
      attributionCreditOrders: 6.5,
      attributionCreditRevenue: 790.25,
      newCustomerCreditOrders: 4,
      returningCustomerCreditOrders: 2.5,
      newCustomerCreditRevenue: 500,
      returningCustomerCreditRevenue: 290.25,
      matchSourceCoverage: { first_party: 6.5 },
      confidenceLabelCoverage: { high: 6.5 },
      spendLastSyncedAt: null,
      shopifyLastIngestedAt: '2026-04-04T07:00:00.000Z',
      attributionLastComputedAt: '2026-04-04T08:00:00.000Z',
      lastComputedAt: '2026-04-04T08:01:00.000Z'
    });
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('MMM taxonomy drift API reports taxonomy, metadata, and native id coverage rates', async () => {
  pool.query = (async (text: string, params?: unknown[]) => {
    assert.match(text, /WITH filtered_mart AS/);
    assert.match(text, /ad_platform_entity_metadata/);
    assert.match(text, /mart\.platform = \$3/);
    assert.match(text, /mart\.source = \$4/);
    assert.match(text, /mart\.campaign = \$5/);

    if (text.includes('daily_summary')) {
      assert.deepEqual(params, ['2026-04-01', '2026-04-02', 'meta', 'facebook', 'prospecting', 30]);

      return {
        rows: [
          {
            metric_date: null,
            total_rows: '10',
            unknown_source_rows: '1',
            unmapped_source_rows: '2',
            unknown_or_unmapped_source_rows: '3',
            unknown_medium_rows: '2',
            unmapped_medium_rows: '1',
            unknown_or_unmapped_medium_rows: '2',
            unresolved_campaign_metadata_rows: '4',
            stale_campaign_metadata_rows: '1',
            native_id_eligible_rows: '8',
            account_id_rows: '7',
            campaign_id_rows: '6',
            adset_id_rows: '5',
            ad_id_rows: '4',
            creative_id_rows: '3',
            platform_native_id_rows: '6'
          },
          {
            metric_date: '2026-04-01',
            total_rows: '4',
            unknown_source_rows: '0',
            unmapped_source_rows: '1',
            unknown_or_unmapped_source_rows: '1',
            unknown_medium_rows: '1',
            unmapped_medium_rows: '0',
            unknown_or_unmapped_medium_rows: '1',
            unresolved_campaign_metadata_rows: '2',
            stale_campaign_metadata_rows: '0',
            native_id_eligible_rows: '3',
            account_id_rows: '3',
            campaign_id_rows: '2',
            adset_id_rows: '2',
            ad_id_rows: '1',
            creative_id_rows: '1',
            platform_native_id_rows: '2'
          }
        ]
      };
    }

    assert.match(text, /sample_candidates/);
    assert.deepEqual(params, ['2026-04-01', '2026-04-02', 'meta', 'facebook', 'prospecting', 30, 3]);
    return {
      rows: [
        {
          sample_type: 'unresolved_campaign_metadata',
          row_count: '4',
          source: 'facebook',
          medium: 'paid_social',
          campaign: 'prospecting',
          platform: 'meta',
          mart_row_type: 'paid_media',
          attribution_model: 'none',
          account_id: 'act_1',
          campaign_id: 'cmp_1',
          metadata_last_seen_at: new Date('2026-03-01T00:00:00.000Z'),
          metadata_updated_at: new Date('2026-03-01T01:00:00.000Z')
        }
      ]
    };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/reporting/mmm/taxonomy-drift?startDate=2026-04-01&endDate=2026-04-02&platform=meta&source=facebook&campaign=prospecting&staleAfterDays=30&sampleLimit=3'
    );

    assert.equal(response.status, 200);
    assert.equal(response.headers.get('x-roas-radar-mmm-schema'), MMM_SCHEMA_VERSION);
    assert.equal(body.schemaVersion, 'mmm_taxonomy_drift_report_v1');
    assert.deepEqual(body.filters, {
      martRowType: null,
      attributionModel: null,
      platform: 'meta',
      source: 'facebook',
      campaign: 'prospecting',
      staleAfterDays: 30,
      sampleLimit: 3
    });
    assert.equal(body.overall.unknownOrUnmappedSourceRows, 3);
    assert.equal(body.overall.unknownOrUnmappedSourceRate, 0.3);
    assert.equal(body.overall.unresolvedCampaignMetadataRate, 0.4);
    assert.equal(body.overall.nativeIdCoverage.platformNativeIdRate, 0.75);
    assert.equal(body.daily[0].date, '2026-04-01');
    assert.equal(body.daily[0].nativeIdCoverage.accountIdRate, 1);
    assert.deepEqual(body.samples[0], {
      sampleType: 'unresolved_campaign_metadata',
      rowCount: 4,
      source: 'facebook',
      medium: 'paid_social',
      campaign: 'prospecting',
      platform: 'meta',
      martRowType: 'paid_media',
      attributionModel: 'none',
      accountId: 'act_1',
      campaignId: 'cmp_1',
      metadataLastSeenAt: '2026-03-01T00:00:00.000Z',
      metadataUpdatedAt: '2026-03-01T01:00:00.000Z'
    });
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('MMM export API can render CSV for model training pipelines', async () => {
  pool.query = (async (text: string, params?: unknown[]) => {
    if (text.includes('WITH requested_dates')) {
      assert.deepEqual(params, ['2026-04-01', '2026-04-01']);
      return {
        rows: [
          {
            metric_date: '2026-04-01',
            matching_row_count: '1',
            mart_row_count: '1',
            generation_timestamp: new Date('2026-04-02T09:00:00.000Z')
          }
        ]
      };
    }

    assert.deepEqual(params, ['2026-04-01', '2026-04-01', 1000, 0]);
    return {
      rows: [
        {
          metric_date: '2026-04-01',
          mart_version: 'v1',
          mart_row_type: 'paid_media',
          attribution_model: 'none',
          platform: 'meta',
          platform_connection_id: '42',
          granularity: 'creative',
          entity_key: 'creative-1',
          account_id: 'act_1',
          account_name: 'Meta Account',
          campaign_id: 'cmp_1',
          campaign_name: 'Prospecting',
          adset_id: 'set_1',
          adset_name: 'US',
          ad_id: 'ad_1',
          ad_name: 'Hero',
          creative_id: 'creative_1',
          creative_name: 'Hero Creative',
          source: 'meta',
          medium: 'paid_social',
          campaign: 'prospecting',
          content: 'hero',
          term: 'unknown',
          currency: 'USD',
          spend: '123.45',
          impressions: '1000',
          clicks: '50',
          shopify_orders: '0',
          shopify_revenue: '0',
          attribution_credit_orders: '0',
          attribution_credit_revenue: '0',
          new_customer_credit_orders: '0',
          returning_customer_credit_orders: '0',
          new_customer_credit_revenue: '0',
          returning_customer_credit_revenue: '0',
          match_source_coverage: {},
          confidence_label_coverage: {},
          spend_last_synced_at: new Date('2026-04-02T08:00:00.000Z'),
          shopify_last_ingested_at: null,
          attribution_last_computed_at: null,
          last_computed_at: new Date('2026-04-02T09:00:00.000Z')
        }
      ]
    };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const response = await request(server, '/api/reporting/mmm?startDate=2026-04-01&endDate=2026-04-01&format=csv');
    const body = await response.text();

    assert.equal(response.status, 200);
    assert.equal(response.headers.get('x-roas-radar-mmm-schema'), MMM_SCHEMA_VERSION);
    assert.match(response.headers.get('content-type') ?? '', /text\/csv/);
    assert.match(body, /^schemaVersion,generationTimestamp,readinessStatus,date,/);
    assert.match(body, /mmm_daily_input_mart_v1,2026-04-02T09:00:00.000Z,ready,2026-04-01,v1,paid_media,none,meta,42/);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('MMM model runs API exposes normalized Bayesian run outputs', async () => {
  pool.query = (async (text: string, params?: unknown[]) => {
    assert.match(text, /FROM mmm_model_runs/);
    assert.match(text, /training_end_date >= \$1::date/);
    assert.match(text, /training_start_date <= \$2::date/);
    assert.match(text, /attribution_model = \$3/);
    assert.deepEqual(params, ['2026-04-01', '2026-05-31', 'last_touch', 10]);

    return {
      rows: [
        {
          id: 'run-bayes-1',
          model_type: 'bayesian_hierarchical_mmm',
          model_version: 'bayesian_hierarchical_mmm_v1',
          mart_version: 'mmm_weekly_channel_input_mart_v1',
          attribution_model: 'last_touch',
          run_status: 'completed',
          training_start_date: '2026-04-06',
          training_end_date: '2026-05-17',
          holdout_start_date: '2026-05-11',
          holdout_end_date: '2026-05-17',
          run_config: {
            inputContractVersion: 'bayesian_hierarchical_mmm_v1',
            posteriorEngine: 'gibbs_sampler_conjugate_gaussian_hierarchical_v1',
            responseVariable: 'weekly_total_shopify_revenue_from_channel_mart_outcomes',
            calibrationUse: 'hierarchical_priors_and_calibration_diagnostics'
          },
          input_summary: {
            rowCount: 24,
            warnCount: 1,
            failCount: 0,
            observationCount: 6,
            trainingObservationCount: 5,
            holdoutObservationCount: 1,
            selectedChannels: ['meta|paid_social|prospecting'],
            otherPaidChannelCount: 1,
            inputContractVersion: 'bayesian_hierarchical_mmm_v1',
            snapshotVersion: 'mmm_weekly_channel_snapshot_v1',
            snapshotRowCount: 24,
            snapshotHash: 'snapshot-hash'
          },
          model_artifact: {
            contributionOutputs: {
              totalMediaContribution: {
                mean: 700,
                credibleInterval80: { lower: 600, upper: 800 },
                credibleInterval95: { lower: 500, upper: 900 }
              },
              channels: [
                {
                  key: 'meta|paid_social|prospecting',
                  source: 'meta',
                  medium: 'paid_social',
                  campaign: 'prospecting',
                  channel: 'Paid Social',
                  channelGroup: 'Paid',
                  spend: 300,
                  impressions: 12000,
                  clicks: 340,
                  attributedOrders: 12,
                  attributedRevenue: 650,
                  contribution: {
                    mean: 720,
                    credibleInterval80: { lower: 660, upper: 780 },
                    credibleInterval95: { lower: 600, upper: 840 }
                  },
                  contributionShare: {
                    mean: 0.72,
                    credibleInterval80: { lower: 0.66, upper: 0.78 },
                    credibleInterval95: { lower: 0.6, upper: 0.84 }
                  },
                  posteriorProbabilityPositive: 0.99
                }
              ],
              weeklyChannels: [
                {
                  weekStartDate: '2026-04-06',
                  key: 'meta|paid_social|prospecting',
                  source: 'meta',
                  medium: 'paid_social',
                  campaign: 'prospecting',
                  channel: 'Paid Social',
                  channelGroup: 'Paid',
                  transformedMediaFeature: 0.48,
                  contribution: {
                    mean: 120,
                    credibleInterval80: { lower: 90, upper: 150 },
                    credibleInterval95: { lower: 70, upper: 170 }
                  },
                  contributionShare: {
                    mean: 0.6,
                    credibleInterval80: { lower: 0.45, upper: 0.75 },
                    credibleInterval95: { lower: 0.35, upper: 0.85 }
                  },
                  posteriorProbabilityPositive: 0.98
                }
              ]
            }
          },
          calibration_report: {
            reportVersion: 'mmm_calibration_report_v1',
            governanceStatus: 'passed',
            deterministicAttributionUsage: 'hierarchical_priors_and_calibration_diagnostics',
            deterministicBaseline: {
              version: 'mmm_deterministic_baseline_30d_click_7d_view_v1'
            },
            totalDeterministicRevenue: 650,
            totalPosteriorMediaContribution: 720,
            segments: [
              {
                key: 'meta|paid_social|prospecting',
                source: 'meta',
                medium: 'paid_social',
                campaign: 'prospecting',
                channel: 'Paid Social',
                channelGroup: 'Paid',
                deterministicRevenue: 650,
                posteriorContributionMean: 720,
                deterministicContributionShare: 1,
                posteriorContributionShareMean: 0.72,
                productionContributionShare: 1,
                posteriorProbabilityPositive: 0.99,
                trustWeights: { posteriorCalibration: 1, production: 1 }
              }
            ]
          },
          validation_report: {
            train: { rmse: 10, mape: 0.08 },
            holdout: { rmse: 12, mape: 0.1 },
            posteriorDiagnostics: {
              maxRhat: 1.02,
              minEffectiveSampleSize: 180,
              totalDraws: 400,
              byParameter: {
                trend: { rhat: 1.01, effectiveSampleSize: 220 }
              }
            },
            posteriorSanityChecks: {
              status: 'pass',
              maxRhat: 1.02,
              maxAllowedRhat: 1.1,
              minEffectiveSampleSize: 180,
              minRequiredEffectiveSampleSize: 100
            }
          },
          error_code: null,
          error_message: null,
          created_at: new Date('2026-05-18T00:00:00.000Z'),
          started_at: new Date('2026-05-18T00:01:00.000Z'),
          completed_at: new Date('2026-05-18T00:05:00.000Z')
        }
      ]
    };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(
      server,
      '/api/reporting/mmm/model-runs?startDate=2026-04-01&endDate=2026-05-31&attributionModel=last_touch'
    );

    assert.equal(response.status, 200);
    assert.equal(response.headers.get('x-roas-radar-mmm-schema'), MMM_SCHEMA_VERSION);
    assert.equal(body.schemaVersion, 'mmm_model_runs_v1');
    assert.equal(body.rows[0].runSummary.modelType, 'bayesian_hierarchical_mmm');
    assert.equal(body.rows[0].runSummary.snapshotHash, 'snapshot-hash');
    assert.equal(body.rows[0].posteriorContributionIntervals.totalMediaContribution.mean, 700);
    assert.deepEqual(body.rows[0].posteriorContributionIntervals.channels[0].contribution.credibleInterval95, {
      lower: 600,
      upper: 840
    });
    assert.equal(body.rows[0].posteriorContributionIntervals.channels[0].posteriorProbabilityPositive, 0.99);
    assert.equal(body.rows[0].posteriorContributionIntervals.weeklyChannels[0].weekStartDate, '2026-04-06');
    assert.deepEqual(body.rows[0].posteriorContributionIntervals.weeklyChannels[0].contribution.credibleInterval95, {
      lower: 70,
      upper: 170
    });
    assert.equal(body.rows[0].calibrationDeltas.deltaRevenue, 70);
    assert.equal(body.rows[0].calibrationDeltas.segments[0].deltaPct, 70 / 650);
    assert.equal(body.rows[0].validationDiagnostics.posteriorDiagnostics.maxRhat, 1.02);
    assert.deepEqual(body.rows[0].readiness, {
      status: 'partial',
      blockers: [],
      warnings: ['warning_input_rows'],
      completedAt: '2026-05-18T00:05:00.000Z'
    });
    assert.equal(body.rows[0].modelArtifact.contributionOutputs.channels.length, 1);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('MMM readiness gate requires every production owner to approve the current evidence hash', async () => {
  installReadinessGateQueryMock();
  const server = createServer();
  const payload = {
    startDate: '2026-04-01',
    endDate: '2026-04-01',
    attributionModel: 'last_touch'
  };

  try {
    const created = await postJson(server, '/api/reporting/mmm/readiness-gate/refresh', payload);
    assert.equal(created.response.status, 200);
    assert.equal(created.body.gate.gateStatus, 'pending');
    assert.equal(created.body.gate.finalState, 'blocked');
    assert.deepEqual(
      created.body.gate.ownerApprovals.map((approval: { owner: string; status: string }) => [approval.owner, approval.status]),
      [
        ['Product', 'pending'],
        ['Analytics', 'pending'],
        ['Backend', 'pending'],
        ['Data Platform', 'pending']
      ]
    );

    const firstApproval = await postJson(server, '/api/reporting/mmm/readiness-gate/approve', {
      ...payload,
      owner: 'Product',
      reason: 'Product reviewed the evidence.'
    });
    assert.equal(firstApproval.response.status, 200);
    assert.equal(firstApproval.body.gate.gateStatus, 'pending');
    assert.equal(firstApproval.body.gate.finalState, 'blocked');
    assert.equal(firstApproval.body.gate.ownerApprovals[0].evidenceHash, created.body.gate.evidenceHash);

    let latest = firstApproval;
    for (const owner of ['Analytics', 'Backend', 'Data Platform']) {
      latest = await postJson(server, '/api/reporting/mmm/readiness-gate/approve', {
        ...payload,
        owner
      });
      assert.equal(latest.response.status, 200);
    }

    assert.equal(latest.body.gate.gateStatus, 'approved');
    assert.equal(latest.body.gate.finalState, 'approved');
    assert.equal(latest.body.gate.decisionReason, 'All required owners approved the current readiness evidence.');
    assert.deepEqual(
      latest.body.gate.ownerApprovals.map((approval: { owner: string; status: string; evidenceHash: string }) => [
        approval.owner,
        approval.status,
        approval.evidenceHash
      ]),
      [
        ['Product', 'pass', created.body.gate.evidenceHash],
        ['Analytics', 'pass', created.body.gate.evidenceHash],
        ['Backend', 'pass', created.body.gate.evidenceHash],
        ['Data Platform', 'pass', created.body.gate.evidenceHash]
      ]
    );
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('MMM readiness gate rejects authenticated users without owner authorization', async () => {
  const originalOwnerApprovers = process.env.MMM_READINESS_OWNER_APPROVERS;
  process.env.MMM_READINESS_OWNER_APPROVERS = 'Product:product@example.com';
  installReadinessGateQueryMock(() => 'clean', { email: 'viewer@example.com' });
  const server = createServer();

  try {
    const response = await postJson(
      server,
      '/api/reporting/mmm/readiness-gate/approve',
      {
        startDate: '2026-04-01',
        endDate: '2026-04-01',
        attributionModel: 'last_touch'
      },
      { authorization: 'Bearer viewer-session-token' }
    );

    assert.equal(response.response.status, 403);
    assert.equal(response.body.error, 'mmm_readiness_owner_forbidden');
  } finally {
    process.env.MMM_READINESS_OWNER_APPROVERS = originalOwnerApprovers;
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('MMM readiness gate prevents users from approving the wrong owner role', async () => {
  const originalOwnerApprovers = process.env.MMM_READINESS_OWNER_APPROVERS;
  process.env.MMM_READINESS_OWNER_APPROVERS = 'Analytics:analytics-owner@example.com';
  installReadinessGateQueryMock(() => 'clean', { email: 'analytics-owner@example.com' });
  const server = createServer();

  try {
    const response = await postJson(
      server,
      '/api/reporting/mmm/readiness-gate/approve',
      {
        startDate: '2026-04-01',
        endDate: '2026-04-01',
        attributionModel: 'last_touch',
        owner: 'Product'
      },
      { authorization: 'Bearer analytics-session-token' }
    );

    assert.equal(response.response.status, 403);
    assert.equal(response.body.error, 'mmm_readiness_owner_forbidden');
    assert.equal(response.body.details.requestedOwner, 'Product');
    assert.deepEqual(response.body.details.allowedOwners, ['Analytics']);
  } finally {
    process.env.MMM_READINESS_OWNER_APPROVERS = originalOwnerApprovers;
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('MMM readiness gate derives valid owner approval from authenticated user mapping', async () => {
  const originalOwnerApprovers = process.env.MMM_READINESS_OWNER_APPROVERS;
  process.env.MMM_READINESS_OWNER_APPROVERS = 'Analytics:analytics-owner@example.com';
  installReadinessGateQueryMock(() => 'clean', { email: 'analytics-owner@example.com', id: 77 });
  const server = createServer();
  const payload = {
    startDate: '2026-04-01',
    endDate: '2026-04-01',
    attributionModel: 'last_touch'
  };

  try {
    const created = await postJson(server, '/api/reporting/mmm/readiness-gate/refresh', payload);
    const approved = await postJson(
      server,
      '/api/reporting/mmm/readiness-gate/approve',
      {
        ...payload,
        evidenceHash: created.body.gate.evidenceHash
      },
      { authorization: 'Bearer analytics-session-token' }
    );

    assert.equal(approved.response.status, 200);
    const analyticsApproval = approved.body.gate.ownerApprovals.find((approval: { owner: string }) => approval.owner === 'Analytics');
    assert.equal(analyticsApproval.status, 'pass');
    assert.equal(analyticsApproval.approvedBy, 'analytics-owner@example.com');
    assert.equal(analyticsApproval.approvedByUserId, 77);
    assert.equal(analyticsApproval.approvedByEmail, 'analytics-owner@example.com');
    assert.equal(analyticsApproval.sourceAction, 'approve');
  } finally {
    process.env.MMM_READINESS_OWNER_APPROVERS = originalOwnerApprovers;
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('MMM readiness gate rejects stale evidence hash and duplicate approvals', async () => {
  installReadinessGateQueryMock();
  const server = createServer();
  const payload = {
    startDate: '2026-04-01',
    endDate: '2026-04-01',
    attributionModel: 'last_touch',
    owner: 'Product'
  };

  try {
    const stale = await postJson(server, '/api/reporting/mmm/readiness-gate/approve', {
      ...payload,
      evidenceHash: '0'.repeat(64)
    });
    assert.equal(stale.response.status, 409);
    assert.equal(stale.body.error, 'stale_evidence_hash');

    const created = await postJson(server, '/api/reporting/mmm/readiness-gate/refresh', {
      startDate: '2026-04-01',
      endDate: '2026-04-01',
      attributionModel: 'last_touch'
    });
    const approved = await postJson(server, '/api/reporting/mmm/readiness-gate/approve', {
      ...payload,
      evidenceHash: created.body.gate.evidenceHash
    });
    assert.equal(approved.response.status, 200);

    const duplicate = await postJson(server, '/api/reporting/mmm/readiness-gate/approve', {
      ...payload,
      evidenceHash: created.body.gate.evidenceHash
    });
    assert.equal(duplicate.response.status, 409);
    assert.equal(duplicate.body.error, 'duplicate_owner_approval');
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('MMM readiness gate audit report returns final gate evidence and approved baseline freeze', async () => {
  installReadinessGateQueryMock();
  const server = createServer();
  const payload = {
    startDate: '2026-04-01',
    endDate: '2026-04-01',
    attributionModel: 'last_touch'
  };

  try {
    for (const owner of ['Product', 'Analytics', 'Backend', 'Data Platform']) {
      await postJson(server, '/api/reporting/mmm/readiness-gate/approve', {
        ...payload,
        owner
      });
    }

    const { response, body } = await requestJson(
      server,
      '/api/reporting/mmm/readiness-gate/audit-report?startDate=2026-04-01&endDate=2026-04-01&attributionModel=last_touch'
    );

    assert.equal(response.status, 200);
    assert.equal(body.schemaVersion, 'mmm_readiness_gate_operational_audit_v1');
    assert.equal(body.auditStatus, 'complete');
    assert.equal(body.productionGateRecord.finalState, 'approved');
    assert.match(body.evidenceHash, /^[0-9a-f]{64}$/);
    assert.equal(body.unresolvedCriticalIssueCount, 0);
    assert.equal(body.approvedBaselineFreezeId, '22222222-2222-4222-8222-222222222222');
    assert.deepEqual(
      body.ownerApprovals.map((approval: { owner: string; status: string }) => [approval.owner, approval.status]),
      [
        ['Product', 'approved'],
        ['Analytics', 'approved'],
        ['Backend', 'approved'],
        ['Data Platform', 'approved']
      ]
    );
    assert.equal(body.checklistStatuses.every((item: { status: string }) => item.status === 'pass' || item.status === 'warn'), true);
    assert.deepEqual(body.waivers, []);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('MMM readiness gate refresh resets approvals and waivers when evidence hash changes', async () => {
  let mode: GateQueryMode = 'clean';
  installReadinessGateQueryMock(() => mode);
  const server = createServer();
  const payload = {
    startDate: '2026-04-01',
    endDate: '2026-04-01',
    attributionModel: 'last_touch'
  };

  try {
    const initial = await postJson(server, '/api/reporting/mmm/readiness-gate/refresh', payload);
    const firstHash = initial.body.gate.evidenceHash;

    for (const owner of ['Product', 'Analytics', 'Backend', 'Data Platform']) {
      await postJson(server, '/api/reporting/mmm/readiness-gate/approve', {
        ...payload,
        owner
      });
    }

    mode = 'changed';
    const refreshed = await postJson(server, '/api/reporting/mmm/readiness-gate/refresh', payload);
    assert.equal(refreshed.response.status, 200);
    assert.notEqual(refreshed.body.gate.evidenceHash, firstHash);
    assert.equal(refreshed.body.gate.gateStatus, 'pending');
    assert.equal(refreshed.body.gate.finalState, 'blocked');
    assert.deepEqual(
      refreshed.body.gate.ownerApprovals.map((approval: { owner: string; status: string; approvedBy: string | null }) => [
        approval.owner,
        approval.status,
        approval.approvedBy
      ]),
      [
        ['Product', 'pending', null],
        ['Analytics', 'pending', null],
        ['Backend', 'pending', null],
        ['Data Platform', 'pending', null]
      ]
    );
    assert.deepEqual(refreshed.body.gate.waivers, []);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('MMM readiness gate waivers clear checklist failures without bypassing required owner approvals', async () => {
  installReadinessGateQueryMock(() => 'blocked');
  const server = createServer();
  const payload = {
    startDate: '2026-04-01',
    endDate: '2026-04-01',
    attributionModel: 'last_touch'
  };

  try {
    const created = await postJson(server, '/api/reporting/mmm/readiness-gate/refresh', payload);
    assert.equal(created.body.gate.unresolvedCriticalIssueCount > 0, true);

    const waived = await postJson(server, '/api/reporting/mmm/readiness-gate/waive', {
      ...payload,
      owner: 'Data Platform',
      waiver: {
        checklistKey: 'campaign_resolver_coverage',
        reason: 'Accepted for one dry run while metadata backfill completes.',
        reviewBy: '2026-04-08T00:00:00.000Z'
      }
    });
    assert.equal(waived.response.status, 200);
    assert.equal(
      waived.body.gate.checklistStatuses.find((item: { key: string }) => item.key === 'campaign_resolver_coverage').status,
      'waived'
    );
    assert.equal(waived.body.gate.waivers[0].evidenceHash, created.body.gate.evidenceHash);
    assert.equal(waived.body.gate.gateStatus, 'pending');
    assert.equal(waived.body.gate.finalState, 'blocked');
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('MMM readiness gate stays blocked when all owners approve unresolved evidence', async () => {
  installReadinessGateQueryMock(() => 'blocked');
  const server = createServer();
  const payload = {
    startDate: '2026-04-01',
    endDate: '2026-04-01',
    attributionModel: 'last_touch'
  };

  try {
    let latest = await postJson(server, '/api/reporting/mmm/readiness-gate/refresh', payload);
    for (const owner of ['Product', 'Analytics', 'Backend']) {
      latest = await postJson(server, '/api/reporting/mmm/readiness-gate/approve', {
        ...payload,
        owner
      });
    }

    const blockedFinal = await postJson(server, '/api/reporting/mmm/readiness-gate/approve', {
      ...payload,
      owner: 'Data Platform'
    });

    assert.equal(blockedFinal.response.status, 409);
    assert.equal(blockedFinal.body.error, 'blocked_final_gate_state');
    assert.equal(latest.body.gate.gateStatus, 'pending');
    assert.equal(latest.body.gate.finalState, 'blocked');
    assert.equal(latest.body.gate.ownerApprovals.filter((approval: { status: string }) => approval.status === 'pass').length, 3);
    assert.equal(latest.body.gate.unresolvedCriticalIssueCount > 0, true);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});

test('MMM read API rejects invalid date ranges before querying', async () => {
  let queryCalls = 0;
  pool.query = (async () => {
    queryCalls += 1;
    return { rows: [] };
  }) as typeof pool.query;

  const server = createServer();

  try {
    const { response, body } = await requestJson(server, '/api/reporting/mmm?startDate=2026-04-03&endDate=2026-04-01');

    assert.equal(response.status, 400);
    assert.equal(body.error, 'invalid_request');
    assert.equal(queryCalls, 0);
    assert.deepEqual(body.details.fieldErrors.startDate, ['startDate must be on or before endDate']);
  } finally {
    pool.query = originalPoolQuery as typeof pool.query;
    await closeServer(server);
  }
});
