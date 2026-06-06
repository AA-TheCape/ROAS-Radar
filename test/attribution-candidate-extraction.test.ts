import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';

async function getTestUtils() {
  const attributionModule = await import('../src/modules/attribution/index.js');
  return attributionModule.__attributionTestUtils;
}

function buildFakeClient(rows: unknown[] = []) {
  return {
    query: async () => ({
      rows,
      rowCount: rows.length
    })
  } as never;
}

test('extractAttributionCandidatesForOrder groups first-party, Shopify hint, and GA4 candidates with normalized metadata', async () => {
  const testUtils = await getTestUtils();

  const fakeClient = buildFakeClient();
  const result = await testUtils.extractAttributionCandidatesForOrder(
    fakeClient,
    {
      shopifyOrderId: 'order-1',
      processedAt: new Date('2026-04-02T14:00:00.000Z'),
      createdAtShopify: null,
      ingestedAt: new Date('2026-04-02T14:05:00.000Z'),
      landingSessionId: null,
      checkoutToken: 'checkout-1',
      cartToken: null,
      rawPayload: {
        landing_site: 'https://store.example/products/widget?utm_source=Google&utm_medium=Paid_Social&fbclid=FB-CLICK-123'
      }
    },
    {
      loadDeterministicFirstPartyCandidates: async () => [
        {
          sourceClass: 'deterministic_first_party',
          sourceKey: 'session-a',
          sessionId: 'session-a',
          sourceTouchEventId: 'touch-a',
          ingestionSource: 'checkout_token',
          occurredAtUtc: new Date('2026-04-01T12:00:00.000Z'),
          source: 'google',
          medium: 'cpc',
          campaign: 'spring-search',
          content: null,
          term: null,
          clickIdType: 'gclid',
          clickIdValue: 'gclid-123',
          attributionReason: 'matched_by_checkout_token',
          confidenceScore: 1,
          isDirect: false,
          isSynthetic: false
        }
      ],
      loadGa4Candidates: async () => [
        {
          stableIdentifier: 'ga4-session-1',
          occurredAt: '2026-04-02T13:45:00.000Z',
          source: 'Google',
          medium: 'CPC',
          campaign: 'Brand Search',
          clickIdType: 'gclid',
          clickIdValue: 'gclid-ga4-1'
        }
      ]
    }
  );

  assert.equal(result.orderTimestampSource, 'processed_at');
  assert.equal(result.deterministicFirstParty.length, 1);
  assert.equal(result.shopifyHint.length, 1);
  assert.equal(result.ga4Fallback.length, 1);
  assert.equal(result.deterministicFirstParty[0].confidenceScore, 1);
  assert.equal(result.shopifyHint[0].attributionReason, 'shopify_hint_derived');
  assert.equal(result.shopifyHint[0].source, 'google');
  assert.equal(result.shopifyHint[0].medium, 'paid_social');
  assert.equal(result.ga4Fallback[0].source, 'google');
  assert.equal(result.ga4Fallback[0].sourceClass, 'ga4_fallback');
  assert.equal(result.ga4Fallback[0].ingestionSource, 'ga4_fallback');
  assert.equal(result.ga4Fallback[0].campaign, 'brand search');
  assert.equal(result.ga4Fallback[0].confidenceScore, 0.35);
  assert.deepEqual(result.normalizationFailures, []);
});

test('extractAttributionCandidatesForOrder records timestamp failures and drops invalid GA4 candidates', async () => {
  const testUtils = await getTestUtils();

  const fakeClient = buildFakeClient();
  const result = await testUtils.extractAttributionCandidatesForOrder(
    fakeClient,
    {
      shopifyOrderId: 'order-2',
      processedAt: null,
      createdAtShopify: '2026-04-02T14:00:00',
      ingestedAt: null,
      landingSessionId: null,
      checkoutToken: null,
      cartToken: null,
      rawPayload: 'not-an-object'
    },
    {
      loadDeterministicFirstPartyCandidates: async () => [],
      loadGa4Candidates: async () => [
        {
          stableIdentifier: 'ga4-invalid-time',
          occurredAt: '2026-04-02T13:45:00',
          source: 'google',
          medium: 'cpc'
        }
      ]
    }
  );

  assert.equal(result.orderOccurredAtUtc, null);
  assert.equal(result.deterministicFirstParty.length, 0);
  assert.equal(result.shopifyHint.length, 0);
  assert.equal(result.ga4Fallback.length, 0);
  assert.deepEqual(result.normalizationFailures, [
    {
      scope: 'order',
      reason: 'missing_order_timestamp',
      sourceKey: 'order-2'
    }
  ]);
});

test('extractAttributionCandidatesForOrder records non-order normalization failures and dedupes GA4 candidates by stable key', async () => {
  const testUtils = await getTestUtils();

  const fakeClient = buildFakeClient();
  const result = await testUtils.extractAttributionCandidatesForOrder(
    fakeClient,
    {
      shopifyOrderId: 'order-3',
      processedAt: '2026-04-02T14:00:00.000Z',
      createdAtShopify: null,
      ingestedAt: '2026-04-02T14:05:00.000Z',
      landingSessionId: null,
      checkoutToken: null,
      cartToken: null,
      rawPayload: 'not-an-object'
    },
    {
      loadDeterministicFirstPartyCandidates: async () => [],
      loadGa4Candidates: async () => [
        {
          stableIdentifier: 'ga4-dup',
          occurredAt: '2026-04-02T13:30:00.000Z',
          source: 'google',
          medium: 'cpc'
        },
        {
          stableIdentifier: 'ga4-dup',
          occurredAt: '2026-04-02T13:45:00.000Z',
          source: 'google',
          medium: 'cpc',
          clickIdType: 'gclid',
          clickIdValue: 'gclid-1'
        },
        {
          stableIdentifier: 'ga4-future',
          occurredAt: '2026-04-02T14:30:00.000Z',
          source: 'google',
          medium: 'cpc'
        }
      ]
    }
  );

  assert.equal(result.orderOccurredAtUtc?.toISOString(), '2026-04-02T14:00:00.000Z');
  assert.equal(result.ga4Fallback.length, 1);
  assert.equal(result.ga4Fallback[0].sourceKey, 'ga4-dup');
  assert.equal(result.ga4Fallback[0].sourceClass, 'ga4_fallback');
  assert.equal(result.ga4Fallback[0].ingestionSource, 'ga4_fallback');
  assert.equal(result.ga4Fallback[0].clickIdValue, 'gclid-1');
  assert.deepEqual(result.normalizationFailures, [
    {
      scope: 'shopify_hint',
      reason: 'invalid_shopify_payload_shape',
      sourceKey: 'order-3'
    },
    {
      scope: 'ga4_fallback',
      reason: 'future_dated_candidate',
      sourceKey: 'ga4-future'
    }
  ]);
});

test('extractAttributionCandidatesForOrder labels platform-reported Meta evidence separately from GA4 fallback', async () => {
  const testUtils = await getTestUtils();

  const fakeClient = buildFakeClient([
    {
      id: 'meta-evidence-1',
      meta_signal_id: 'signal-1',
      meta_touchpoint_occurred_at_utc: new Date('2026-04-02T13:40:00.000Z'),
      campaign_id: 'campaign-1',
      campaign_name: 'Prospecting',
      ad_id: 'ad-1',
      match_basis: 'fbclid',
      confidence_score: '0.62',
      eligibility_outcome: 'eligible_canonical',
      is_click_through: true,
      is_view_through: false
    }
  ]);

  const result = await testUtils.extractAttributionCandidatesForOrder(
    fakeClient,
    {
      shopifyOrderId: 'order-4',
      processedAt: '2026-04-02T14:00:00.000Z',
      createdAtShopify: null,
      ingestedAt: null,
      landingSessionId: null,
      checkoutToken: null,
      cartToken: null,
      rawPayload: null
    },
    {
      loadDeterministicFirstPartyCandidates: async () => [],
      loadGa4Candidates: async () => [
        {
          stableIdentifier: 'ga4-session-4',
          occurredAt: '2026-04-02T13:50:00.000Z',
          source: 'google',
          medium: 'cpc',
          campaign: 'brand'
        }
      ]
    }
  );

  assert.equal(result.platformReportedMeta.length, 1);
  assert.equal(result.platformReportedMeta[0].sourceClass, 'platform_reported_meta');
  assert.equal(result.platformReportedMeta[0].ingestionSource, 'meta_platform_reported');
  assert.equal(result.platformReportedMeta[0].sourceKey, 'meta:meta-evidence-1');
  assert.equal(result.platformReportedMeta[0].metaAttributionEvidenceId, 'meta-evidence-1');
  assert.equal(result.platformReportedMeta[0].metaEligibilityOutcome, 'eligible_canonical');

  assert.equal(result.ga4Fallback.length, 1);
  assert.equal(result.ga4Fallback[0].sourceClass, 'ga4_fallback');
  assert.equal(result.ga4Fallback[0].ingestionSource, 'ga4_fallback');
  assert.equal(result.ga4Fallback[0].sourceKey, 'ga4-session-4');
  assert.deepEqual(result.normalizationFailures, []);
});

test('normalizeTimestampToUtc rejects naive timestamp strings and accepts zoned timestamps', async () => {
  const candidateModule = await import('../src/modules/attribution/candidate-extraction.js');
  assert.equal(candidateModule.normalizeTimestampToUtc('2026-04-02T14:00:00'), null);
  assert.equal(
    candidateModule.normalizeTimestampToUtc('2026-04-02T14:00:00-05:00')?.toISOString(),
    '2026-04-02T19:00:00.000Z'
  );
});

test('collectDeterministicFirstPartyCandidates preserves identity-journey attribution reasons', async () => {
  const candidateModule = await import('../src/modules/attribution/candidate-extraction.js');

  const fakeClient = {
    query: async (_text: string, params: unknown[]) => {
      assert.equal(params[0], '123e4567-e89b-42d3-a456-426614174900');
      assert.equal(params[1], null);
      assert.equal(params[2], 'order-identity-journey');

      return {
        rows: [
          {
            session_id: '123e4567-e89b-42d3-a456-426614174901',
            source_touch_event_id: 'evt-identity-journey',
            occurred_at: new Date('2026-04-01T12:00:00.000Z'),
            attribution_reason: 'matched_by_identity_journey',
            source: 'meta',
            medium: 'paid_social',
            campaign: 'retargeting',
            content: null,
            term: null,
            click_id_type: 'fbclid',
            click_id_value: 'fbclid-123'
          }
        ],
        rowCount: 1
      };
    }
  } as never;

  const result = await candidateModule.collectDeterministicFirstPartyCandidates(fakeClient, {
    shopifyOrderId: 'order-identity-journey',
    processedAt: '2026-04-02T14:00:00.000Z',
    createdAtShopify: null,
    ingestedAt: '2026-04-02T14:05:00.000Z',
    landingSessionId: null,
    checkoutToken: null,
    cartToken: null,
    customerIdentityId: null,
    identityJourneyId: '123e4567-e89b-42d3-a456-426614174900',
    rawPayload: null
  });

  assert.equal(result.length, 1);
  assert.equal(result[0].ingestionSource, 'customer_identity');
  assert.equal(result[0].attributionReason, 'matched_by_identity_journey');
});
