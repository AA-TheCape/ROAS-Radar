import assert from 'node:assert/strict';
import test from 'node:test';

import type { PoolClient } from 'pg';

import {
  BAYESIAN_HIERARCHICAL_MMM_INPUT_CONTRACT_VERSION,
  fetchBayesianHierarchicalMmmV1FeatureRowsWithClient,
  refreshWeeklyMmmChannelInputMartWithClient
} from '../src/modules/mmm/weekly-mart.js';

type QueryCall = {
  text: string;
  params?: unknown[];
};

function buildFakeClient(onQuery?: (call: QueryCall) => { rows: unknown[] } | undefined): PoolClient {
  const calls: QueryCall[] = [];

  return {
    calls,
    query: async (text: string, params?: unknown[]) => {
      const call = { text, params };
      calls.push(call);

      return onQuery?.(call) ?? { rows: [] };
    }
  } as unknown as PoolClient & { calls: QueryCall[] };
}

test('weekly MMM refresh emits bayesian_hierarchical_mmm_v1 metadata and filters to complete weeks', async () => {
  const client = buildFakeClient((call) => {
    if (call.text.includes('COUNT(*)::text AS row_count')) {
      assert.deepEqual(call.params, ['2026-04-02', '2026-04-30', ['last_touch']]);

      return {
        rows: [
          {
            row_count: '0',
            fail_count: '0',
            warn_count: '0',
            unknown_dimension_row_count: '0',
            future_dated_source_row_count: '0'
          }
        ]
      };
    }

    return { rows: [] };
  }) as PoolClient & { calls: QueryCall[] };

  await refreshWeeklyMmmChannelInputMartWithClient(client, {
    startDate: '2026-04-02',
    endDate: '2026-04-30',
    attributionModels: ['last_touch']
  });

  const insertQuery = client.calls.find((call) => call.text.includes('INSERT INTO mmm_weekly_channel_input_mart_v1'));
  assert.ok(insertQuery);
  assert.match(insertQuery.text, /eligible_weeks AS/);
  assert.match(insertQuery.text, /week_start_date >= \$1::date/);
  assert.match(insertQuery.text, /\(week_start_date::date \+ 6\) <= \$2::date/);
  assert.match(insertQuery.text, /inputContractVersion', 'bayesian_hierarchical_mmm_v1'/);
  assert.match(insertQuery.text, /clickLookbackWindowDays', 30/);
  assert.match(insertQuery.text, /viewLookbackWindowDays', 7/);
  assert.match(insertQuery.text, /maxSpendLastSyncedAt/);
  assert.match(insertQuery.text, /maxAttributionLastComputedAt/);
});

test('bayesian hierarchical MMM feature fetch stamps finalized input contract version', async () => {
  const client = buildFakeClient((call) => {
    if (call.text.includes('FROM mmm_weekly_channel_input_mart_v1')) {
      return {
        rows: [
          {
            week_start_date: '2026-04-06',
            week_end_date: '2026-04-12',
            mart_version: 'mmm_weekly_channel_input_mart_v1',
            source_mart_version: 'mmm_daily_input_mart_v1',
            attribution_model: 'last_touch',
            channel_key: 'meta|paid_social|prospecting|paid_social|paid',
            source: 'meta',
            medium: 'paid_social',
            campaign: 'prospecting',
            channel: 'paid_social',
            channel_group: 'paid',
            spend: '100.00',
            impressions: '1000',
            clicks: '50',
            shopify_orders: '4',
            shopify_revenue: '400.00',
            attribution_credit_orders: '3.50000000',
            attribution_credit_revenue: '350.00',
            new_customer_credit_orders: '2.00000000',
            returning_customer_credit_orders: '1.50000000',
            new_customer_credit_revenue: '225.00',
            returning_customer_credit_revenue: '125.00',
            match_source_coverage: { first_party: 3.5 },
            confidence_label_coverage: { high: 3.5 },
            controls: { inputContractVersion: BAYESIAN_HIERARCHICAL_MMM_INPUT_CONTRACT_VERSION },
            deterministic_anchors: {
              inputContractVersion: BAYESIAN_HIERARCHICAL_MMM_INPUT_CONTRACT_VERSION,
              clickLookbackWindowDays: 30,
              viewLookbackWindowDays: 7
            },
            missingness_report: { missingDimensions: [] },
            leakage_report: {
              inputContractVersion: BAYESIAN_HIERARCHICAL_MMM_INPUT_CONTRACT_VERSION,
              isCompleteWeek: true,
              calibrationMetadata: {
                clickLookbackWindowDays: 30,
                viewLookbackWindowDays: 7
              }
            },
            dq_status: 'pass',
            source_row_count: '7',
            generated_at: '2026-04-13T00:00:00.000Z'
          }
        ]
      };
    }

    return { rows: [] };
  }) as PoolClient;

  const rows = await fetchBayesianHierarchicalMmmV1FeatureRowsWithClient(client, {
    startDate: '2026-04-06',
    endDate: '2026-04-12',
    attributionModels: ['last_touch']
  });

  assert.equal(rows[0]?.input_contract_version, BAYESIAN_HIERARCHICAL_MMM_INPUT_CONTRACT_VERSION);
  assert.equal(rows[0]?.leakage_report && typeof rows[0].leakage_report, 'object');
});
