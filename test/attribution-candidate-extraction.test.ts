import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';

async function getTestUtils() {
  const attributionModule = await import('../src/modules/attribution/index.js');
  return attributionModule.__attributionTestUtils;
}

test('extractAttributionCandidatesForOrder groups first-party, Shopify hint, and GA4 candidates with normalized metadata', async () => {
  const testUtils = await getTestUtils();

  const fakeClient = {} as never;
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
  assert.equal(result.ga4Fallback[0].campaign, 'brand search');
  assert.equal(result.ga4Fallback[0].confidenceScore, 0.35);
  assert.deepEqual(result.normalizationFailures, []);
});

test('extractAttributionCandidatesForOrder records timestamp failures and drops invalid GA4 candidates', async () => {
  const testUtils = await getTestUtils();

  const fakeClient = {} as never;
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

test('extractAttributionCandidatesForOrder keeps GA4 Google campaign metadata when campaign name is absent', async () => {
  const testUtils = await getTestUtils();

  const fakeClient = {} as never;
  const result = await testUtils.extractAttributionCandidatesForOrder(
    fakeClient,
    {
      shopifyOrderId: 'order-google-campaign-metadata',
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
          stableIdentifier: 'ga4-google-campaign-metadata',
          occurredAt: '2026-04-02T13:30:00.000Z',
          source: 'google',
          medium: 'cpc',
          campaignId: '987654321',
          campaign: null,
          accountId: '1234567890',
          accountName: 'Cape Google Ads',
          channelType: 'SEARCH',
          channelSubtype: 'SEARCH_STANDARD',
          campaignMetadataSource: 'google_ads_transfer',
          accountMetadataSource: 'google_ads_transfer',
          channelMetadataSource: 'google_ads_transfer'
        }
      ]
    }
  );

  assert.equal(result.ga4Fallback.length, 1);
  assert.equal(result.ga4Fallback[0].source, 'google');
  assert.equal(result.ga4Fallback[0].medium, 'cpc');
  assert.equal(result.ga4Fallback[0].campaign, '987654321');
  assert.equal(result.ga4Fallback[0].campaignId, '987654321');
  assert.equal(result.ga4Fallback[0].accountId, '1234567890');
  assert.equal(result.ga4Fallback[0].accountName, 'Cape Google Ads');
  assert.equal(result.ga4Fallback[0].channelType, 'SEARCH');
  assert.equal(result.ga4Fallback[0].channelSubtype, 'SEARCH_STANDARD');
  assert.equal(result.ga4Fallback[0].campaignMetadataSource, 'google_ads_transfer');
  assert.equal(result.ga4Fallback[0].accountMetadataSource, 'google_ads_transfer');
  assert.equal(result.ga4Fallback[0].channelMetadataSource, 'google_ads_transfer');
});

test('extractAttributionCandidatesForOrder preserves campaign id from default GA4 fallback rows', async () => {
  const testUtils = await getTestUtils();

  const fakeClient = {
    query: async () => ({
      rows: [
        {
          candidate_key: 'ga4-default-google-campaign-metadata',
          occurred_at: new Date('2026-04-02T13:30:00.000Z'),
          ga4_user_key: 'client:123.456',
          ga4_client_id: '123.456',
          ga4_session_id: '9001',
          transaction_id: 'order-default-google-campaign-metadata',
          email_hash: null,
          customer_identity_id: null,
          source: 'google',
          medium: 'cpc',
          campaign: null,
          campaign_id: '987654321',
          content: null,
          term: null,
          click_id_type: null,
          click_id_value: null,
          account_id: '1234567890',
          account_name: 'Cape Google Ads',
          channel_type: 'SEARCH',
          channel_subtype: 'SEARCH_STANDARD',
          campaign_metadata_source: 'google_ads_transfer',
          account_metadata_source: 'google_ads_transfer',
          channel_metadata_source: 'google_ads_transfer',
          session_has_required_fields: true,
          source_export_hour: new Date('2026-04-02T13:00:00.000Z'),
          source_dataset: 'analytics_123',
          source_table_type: 'events',
          retained_until: new Date('2026-05-07T13:30:00.000Z'),
          matched_on: 'transaction_id'
        }
      ]
    })
  } as never;

  const result = await testUtils.extractAttributionCandidatesForOrder(
    fakeClient,
    {
      shopifyOrderId: 'order-default-google-campaign-metadata',
      processedAt: '2026-04-02T14:00:00.000Z',
      createdAtShopify: null,
      ingestedAt: '2026-04-02T14:05:00.000Z',
      landingSessionId: null,
      checkoutToken: null,
      cartToken: null,
      rawPayload: 'not-an-object'
    },
    {
      loadDeterministicFirstPartyCandidates: async () => []
    }
  );

  assert.equal(result.ga4Fallback.length, 1);
  assert.equal(result.ga4Fallback[0].source, 'google');
  assert.equal(result.ga4Fallback[0].medium, 'cpc');
  assert.equal(result.ga4Fallback[0].campaign, '987654321');
  assert.equal(result.ga4Fallback[0].campaignId, '987654321');
  assert.equal(result.ga4Fallback[0].accountId, '1234567890');
  assert.equal(result.ga4Fallback[0].accountName, 'Cape Google Ads');
  assert.equal(result.ga4Fallback[0].campaignMetadataSource, 'google_ads_transfer');
});

test('extractAttributionCandidatesForOrder records non-order normalization failures and dedupes GA4 candidates by stable key', async () => {
  const testUtils = await getTestUtils();

  const fakeClient = {} as never;
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
