import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';

async function getPreprocessing() {
  const attributionModule = await import('../src/modules/attribution/index.js');
  return attributionModule.__attributionTestUtils.preprocessAttributionSnapshot;
}

test('preprocessAttributionSnapshot builds deterministic normalized orders and touchpoints', async () => {
  const preprocessAttributionSnapshot = await getPreprocessing();
  const failures: Array<{ reasonCode: string }> = [];

  const result = preprocessAttributionSnapshot(
    {
      orders: [
        {
          shopifyOrderId: 'order-1',
          processedAt: '2026-04-20T18:00:00.000Z',
          createdAtShopify: null,
          ingestedAt: '2026-04-20T18:02:00.000Z',
          currencyCode: 'usd',
          subtotalAmount: '120.00',
          totalAmount: '150.00',
          landingSessionId: '123e4567-e89b-42d3-a456-426614174001',
          checkoutToken: 'checkout-1',
          cartToken: 'cart-1',
          shopifyCustomerId: 'customer-1',
          emailHash: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
          sourceName: 'web',
          identityJourneyId: '123e4567-e89b-42d3-a456-426614174099',
          rawPayload: {
            landing_site: 'https://shop.example/products/widget?utm_source=Google&utm_medium=CPC&utm_campaign=Brand&gclid=G-123'
          }
        }
      ],
      sessionIdentities: [
        {
          sessionId: '123e4567-e89b-42d3-a456-426614174001',
          customerIdentityId: 'customer-1',
          identityJourneyId: '123e4567-e89b-42d3-a456-426614174099',
          emailHash: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
          firstCapturedAt: '2026-04-10T09:00:00.000Z',
          lastCapturedAt: '2026-04-10T09:30:00.000Z',
          initialUtmSource: 'Google',
          initialUtmMedium: 'CPC',
          initialUtmCampaign: 'Spring Search',
          initialUtmContent: 'Hero',
          initialUtmTerm: 'Widget',
          initialGclid: 'G-123'
        }
      ],
      touchEvents: [
        {
          touchEventId: 'evt-1',
          sessionId: '123e4567-e89b-42d3-a456-426614174001',
          occurredAt: '2026-04-19T16:00:00.000Z',
          capturedAt: '2026-04-19T16:00:01.000Z',
          eventType: 'ad_click',
          ingestionSource: 'browser',
          utmSource: 'Google',
          utmMedium: 'CPC',
          utmCampaign: 'Brand Search',
          gclid: 'G-CLICK-1',
          shopifyCheckoutToken: 'checkout-1',
          rawPayload: {
            engagement_type: 'click'
          }
        }
      ],
      journeySessions: []
    },
    {
      logger: (failure) => failures.push({ reasonCode: failure.reasonCode })
    }
  );

  assert.equal(result.orders.length, 1);
  assert.equal(result.orders[0].currency_code, 'USD');
  assert.equal(result.touchpoints.length, 3);
  assert.deepEqual(
    result.touchpoints.map((touchpoint) => touchpoint.touchpoint_id),
    ['session:123e4567-e89b-42d3-a456-426614174001:first_touch', 'event:evt-1', 'shopify_hint:order-1']
  );
  assert.equal(result.touchpoints[1].engagement_type, 'click');
  assert.equal(result.touchpoints[2].is_synthetic, true);
  assert.deepEqual(failures, []);
});

test('preprocessAttributionSnapshot records missing sessions, future touchpoints, and duplicate event drops', async () => {
  const preprocessAttributionSnapshot = await getPreprocessing();

  const result = preprocessAttributionSnapshot({
    orders: [
      {
        shopifyOrderId: 'order-2',
        processedAt: '2026-04-20T18:00:00.000Z',
        createdAtShopify: null,
        ingestedAt: '2026-04-20T18:02:00.000Z',
        currencyCode: 'USD',
        subtotalAmount: '100.00',
        totalAmount: '100.00',
        landingSessionId: '123e4567-e89b-42d3-a456-426614174002',
        checkoutToken: 'checkout-2',
        cartToken: null,
        shopifyCustomerId: null,
        emailHash: null,
        sourceName: 'web',
        identityJourneyId: null,
        rawPayload: null
      }
    ],
    sessionIdentities: [],
    touchEvents: [
      {
        touchEventId: 'evt-dup',
        sessionId: '123e4567-e89b-42d3-a456-426614174003',
        occurredAt: '2026-04-21T01:00:00.000Z',
        capturedAt: '2026-04-21T01:00:00.000Z',
        eventType: 'ad_click',
        ingestionSource: 'browser',
        utmSource: 'Google',
        utmMedium: 'CPC',
        gclid: 'future-click',
        shopifyCheckoutToken: 'checkout-2'
      },
      {
        touchEventId: 'evt-dup',
        sessionId: '123e4567-e89b-42d3-a456-426614174003',
        occurredAt: '2026-04-21T01:00:00.000Z',
        capturedAt: '2026-04-21T01:00:00.000Z',
        eventType: 'ad_click',
        ingestionSource: 'browser',
        utmSource: 'Google',
        utmMedium: 'CPC',
        utmCampaign: 'Brand',
        gclid: 'future-click',
        shopifyCheckoutToken: 'checkout-2'
      }
    ],
    journeySessions: []
  });

  assert.equal(result.touchpoints.length, 1);
  assert.equal(result.touchpoints[0].is_eligible, false);
  assert.equal(result.touchpoints[0].ineligibility_reason, 'future_touchpoint');
  assert.equal(
    result.failures.filter((failure) => failure.reasonCode === 'duplicate_touchpoint_dropped').length,
    1
  );
  assert.ok(result.failures.some((failure) => failure.reasonCode === 'missing_session_identity'));
});

test('preprocessAttributionSnapshot excludes unknown touch types and keeps 28d click versus 7d view windows', async () => {
  const preprocessAttributionSnapshot = await getPreprocessing();

  const result = preprocessAttributionSnapshot({
    orders: [
      {
        shopifyOrderId: 'order-3',
        processedAt: '2026-04-30T12:00:00.000Z',
        createdAtShopify: null,
        ingestedAt: '2026-04-30T12:01:00.000Z',
        currencyCode: 'USD',
        subtotalAmount: '80.00',
        totalAmount: '80.00',
        landingSessionId: null,
        checkoutToken: null,
        cartToken: null,
        shopifyCustomerId: 'customer-3',
        emailHash: null,
        sourceName: 'web',
        identityJourneyId: null,
        rawPayload: null
      }
    ],
    sessionIdentities: [
      {
        sessionId: '123e4567-e89b-42d3-a456-426614174004',
        customerIdentityId: 'customer-3',
        firstCapturedAt: '2026-04-01T11:00:00.000Z',
        lastCapturedAt: '2026-04-01T11:15:00.000Z',
        initialUtmSource: null,
        initialUtmMedium: null
      }
    ],
    touchEvents: [
      {
        touchEventId: 'evt-click',
        sessionId: '123e4567-e89b-42d3-a456-426614174004',
        occurredAt: '2026-04-05T10:00:00.000Z',
        capturedAt: '2026-04-05T10:00:00.000Z',
        eventType: 'page_view',
        gclid: 'CLICK-28D'
      },
      {
        touchEventId: 'evt-view',
        sessionId: '123e4567-e89b-42d3-a456-426614174004',
        occurredAt: '2026-04-20T10:00:00.000Z',
        capturedAt: '2026-04-20T10:00:00.000Z',
        eventType: 'impression'
      },
      {
        touchEventId: 'evt-unknown',
        sessionId: '123e4567-e89b-42d3-a456-426614174004',
        occurredAt: '2026-04-29T10:00:00.000Z',
        capturedAt: '2026-04-29T10:00:00.000Z',
        eventType: 'mystery'
      }
    ],
    journeySessions: []
  });

  const firstTouch = result.touchpoints.find((touchpoint) => touchpoint.touchpoint_id.includes('first_touch'));
  const clickTouchpoint = result.touchpoints.find((touchpoint) => touchpoint.touchpoint_id === 'event:evt-click');
  const viewTouchpoint = result.touchpoints.find((touchpoint) => touchpoint.touchpoint_id === 'event:evt-view');
  const unknownTouchpoint = result.touchpoints.find((touchpoint) => touchpoint.touchpoint_id === 'event:evt-unknown');

  assert.equal(firstTouch?.is_eligible, false);
  assert.equal(firstTouch?.ineligibility_reason, 'outside_click_lookback_window');
  assert.equal(clickTouchpoint?.is_eligible, true);
  assert.equal(viewTouchpoint?.is_eligible, false);
  assert.equal(viewTouchpoint?.ineligibility_reason, 'outside_view_lookback_window');
  assert.equal(unknownTouchpoint?.is_eligible, false);
  assert.equal(unknownTouchpoint?.ineligibility_reason, 'unknown_engagement_type');
});
