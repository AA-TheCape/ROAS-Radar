import assert from 'node:assert/strict';
import test from 'node:test';

import { click, h, loadDashboardModule, mountUi, tick } from './dashboard-ui-test-helpers';

const baseResultRow = {
  record: {
    run_id: '11111111-1111-4111-8111-111111111111',
    attribution_spec_version: 'v1' as const,
    order_id: 'order-2001',
    model_key: 'last_non_direct' as const,
    allocation_status: 'attributed' as const,
    winner_touchpoint_id: 'tp-1',
    winner_session_id: '22222222-2222-4222-8222-222222222222',
    winner_evidence_source: 'landing_session_id' as const,
    winner_attribution_reason: 'matched_by_landing_session',
    total_credit_weight: '1.00000000',
    total_revenue_credited: '120.00',
    touchpoint_count_considered: 2,
    eligible_click_count: 2,
    eligible_view_count: 0,
    lookback_rule_applied: '28d_click' as const,
    winner_selection_rule: 'last_non_direct' as const,
    direct_suppression_applied: true,
    deterministic_block_applied: false,
    normalization_failures_count: 0,
    generated_at_utc: '2026-04-30T12:05:00.000Z'
  },
  orderOccurredAtUtc: '2026-04-30T12:00:00.000Z',
  run: {
    id: '11111111-1111-4111-8111-111111111111',
    status: 'completed',
    triggerSource: 'manual',
    submittedBy: 'internal',
    windowStartUtc: '2026-04-01T00:00:00.000Z',
    windowEndUtc: '2026-04-30T23:59:59.000Z',
    lookbackClickWindowDays: 28,
    lookbackViewWindowDays: 7,
    createdAtUtc: '2026-04-30T12:00:00.000Z',
    completedAtUtc: '2026-04-30T12:05:00.000Z'
  },
  model: {
    key: 'last_non_direct' as const,
    winnerSelectionRule: 'last_non_direct' as const,
    lookbackRuleApplied: '28d_click' as const
  },
  primaryTouchpoint: {
    run_id: '11111111-1111-4111-8111-111111111111',
    attribution_spec_version: 'v1' as const,
    order_id: 'order-2001',
    model_key: 'last_non_direct' as const,
    touchpoint_id: 'tp-1',
    session_id: '22222222-2222-4222-8222-222222222222',
    touchpoint_position: 1,
    occurred_at_utc: '2026-04-28T10:00:00.000Z',
    source: 'google',
    medium: 'cpc',
    campaign: 'spring-sale',
    content: 'hero',
    term: 'widget',
    click_id_type: 'gclid' as const,
    click_id_value: 'ABC123',
    touch_type: 'click' as const,
    is_direct: false,
    evidence_source: 'landing_session_id' as const,
    is_synthetic: false,
    attribution_reason: 'matched_by_landing_session',
    credit_weight: '1.00000000',
    revenue_credit: '120.00',
    is_primary: true
  }
};

test('attribution dashboard keeps model comparison and per-order rationale explicit', async () => {
  const { default: AttributionDashboard } = await loadDashboardModule<
    typeof import('../dashboard/src/components/AttributionDashboard')
  >('dashboard/src/components/AttributionDashboard.tsx');

  let inspectedOrder: { orderId: string; runId: string } | null = null;
  const mounted = await mountUi(
    h(AttributionDashboard, {
      filters: {
        startDate: '2026-04-01',
        endDate: '2026-04-30',
        source: '',
        medium: '',
        campaign: '',
        orderId: ''
      },
      onFiltersChange: () => undefined,
      onClearFilters: () => undefined,
      activeModel: 'last_non_direct',
      onActiveModelChange: () => undefined,
      reportingTimezone: 'America/Los_Angeles',
      resultsSection: {
        data: [baseResultRow],
        loading: false,
        error: null
      },
      channelTotalsSection: {
        data: {
          rows: [
            {
              modelKey: 'last_non_direct',
              source: 'google',
              medium: 'cpc',
              orderCount: 1,
              revenueCredited: '120.00',
              creditWeightTotal: '1.00000000'
            },
            {
              modelKey: 'linear',
              source: 'google',
              medium: 'cpc',
              orderCount: 1,
              revenueCredited: '60.00',
              creditWeightTotal: '0.50000000'
            }
          ],
          lookbackClickWindowDays: 28,
          lookbackViewWindowDays: 7
        },
        loading: false,
        error: null
      },
      explainabilitySection: {
        data: null,
        loading: false,
        error: null
      },
      selectedOrderId: null,
      onInspectOrder: (orderId: string, runId: string) => {
        inspectedOrder = { orderId, runId };
      }
    })
  );

  try {
    const text = mounted.container.textContent ?? '';
    assert.match(text, /Alternative views, not one universal truth/);
    assert.match(text, /28-day click lookback/);
    assert.match(text, /Last non-direct/);
    assert.match(text, /Channel comparison/);
    assert.match(text, /google \/ cpc/i);

    const inspectButton = mounted.container.querySelector(
      'button[aria-label="Inspect rationale for order order-2001"]'
    );
    assert.ok(inspectButton);

    click(inspectButton);
    await tick();

    assert.deepEqual(inspectedOrder, {
      orderId: 'order-2001',
      runId: '11111111-1111-4111-8111-111111111111'
    });
  } finally {
    mounted.cleanup();
  }
});

test('attribution dashboard renders touchpoints, credits, and explainability when an order is selected', async () => {
  const { default: AttributionDashboard } = await loadDashboardModule<
    typeof import('../dashboard/src/components/AttributionDashboard')
  >('dashboard/src/components/AttributionDashboard.tsx');

  const mounted = await mountUi(
    h(AttributionDashboard, {
      filters: {
        startDate: '2026-04-01',
        endDate: '2026-04-30',
        source: '',
        medium: '',
        campaign: '',
        orderId: ''
      },
      onFiltersChange: () => undefined,
      onClearFilters: () => undefined,
      activeModel: 'last_non_direct',
      onActiveModelChange: () => undefined,
      reportingTimezone: 'America/Los_Angeles',
      resultsSection: {
        data: [baseResultRow],
        loading: false,
        error: null
      },
      channelTotalsSection: {
        data: {
          rows: [
            {
              modelKey: 'last_non_direct',
              source: 'google',
              medium: 'cpc',
              orderCount: 1,
              revenueCredited: '120.00',
              creditWeightTotal: '1.00000000'
            }
          ],
          lookbackClickWindowDays: 28,
          lookbackViewWindowDays: 7
        },
        loading: false,
        error: null
      },
      explainabilitySection: {
        data: {
          orderId: 'order-2001',
          selectedRunReason: 'explicit_run_id',
          run: {
            id: '11111111-1111-4111-8111-111111111111',
            attributionSpecVersion: 'v1',
            status: 'completed',
            triggerSource: 'manual',
            submittedBy: 'internal',
            windowStartUtc: '2026-04-01T00:00:00.000Z',
            windowEndUtc: '2026-04-30T23:59:59.000Z',
            lookbackClickWindowDays: 28,
            lookbackViewWindowDays: 7,
            createdAtUtc: '2026-04-30T12:00:00.000Z',
            completedAtUtc: '2026-04-30T12:05:00.000Z'
          },
          summaries: [baseResultRow.record],
          touchpoints: [
            {
              runId: '11111111-1111-4111-8111-111111111111',
              orderId: 'order-2001',
              touchpointId: 'tp-1',
              sessionId: '22222222-2222-4222-8222-222222222222',
              identityJourneyId: null,
              touchpointOccurredAtUtc: '2026-04-28T10:00:00.000Z',
              touchpointCapturedAtUtc: '2026-04-28T10:00:01.000Z',
              touchpointSourceKind: 'session_event',
              ingestionSource: 'browser',
              source: 'google',
              medium: 'cpc',
              campaign: 'spring-sale',
              content: 'hero',
              term: 'widget',
              clickIdType: 'gclid',
              clickIdValue: 'ABC123',
              evidenceSource: 'landing_session_id',
              isDirect: false,
              engagementType: 'click',
              isSynthetic: false,
              isEligible: true,
              ineligibilityReason: null,
              attributionReason: 'matched_by_landing_session',
              attributionHint: {}
            }
          ],
          credits: [baseResultRow.primaryTouchpoint],
          explainability: [
            {
              run_id: '11111111-1111-4111-8111-111111111111',
              order_id: 'order-2001',
              touchpoint_id: 'tp-1',
              model_key: 'last_non_direct',
              explain_stage: 'model_scoring',
              decision: 'winner',
              decision_reason: 'matched_by_landing_session',
              details_json: { creditWeight: 1 },
              order_occurred_at_utc: '2026-04-30T12:00:00.000Z',
              created_at_utc: '2026-04-30T12:05:01.000Z'
            }
          ]
        },
        loading: false,
        error: null
      },
      selectedOrderId: 'order-2001',
      onInspectOrder: () => undefined
    })
  );

  try {
    const text = mounted.container.textContent ?? '';
    assert.match(text, /Selected order/);
    assert.match(text, /order-2001/);
    assert.match(text, /Eligible touchpoints/);
    assert.match(text, /Credited rows/);
    assert.match(text, /Explainability audit trail/);
  } finally {
    mounted.cleanup();
  }
});
