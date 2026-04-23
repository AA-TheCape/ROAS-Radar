import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';

async function getTestUtils() {
  const attributionModule = await import('../src/modules/attribution/index.js');
  return attributionModule.__attributionTestUtils;
}

type TestUtils = Awaited<ReturnType<typeof getTestUtils>>;
type TestTouchpoint = Parameters<TestUtils['dedupeDeterministicCandidates']>[0][number];

function buildTouchpoint(
  sessionId: string,
  occurredAt: string,
  overrides: Partial<TestTouchpoint> = {}
): TestTouchpoint {
  return {
    sessionId,
    sourceTouchEventId: `${sessionId}-event`,
    occurredAt: new Date(occurredAt),
    source: 'google',
    medium: 'cpc',
    campaign: `campaign-${sessionId}`,
    content: null,
    term: null,
    clickIdType: null,
    clickIdValue: null,
    attributionReason: 'matched_by_customer_identity',
    ingestionSource: 'customer_identity',
    isDirect: false,
    isForced: false,
    ...overrides
  };
}

test('last non-direct winner ignores a later direct revisit', async () => {
  const testUtils = await getTestUtils();
  const touchpoints = testUtils.dedupeDeterministicCandidates([
    buildTouchpoint('session-paid', '2026-04-01T10:00:00.000Z', {
      clickIdType: 'gclid',
      clickIdValue: 'gclid-123'
    }),
    buildTouchpoint('session-direct', '2026-04-03T09:00:00.000Z', {
      source: null,
      medium: null,
      campaign: null,
      isDirect: true
    })
  ]);

  const winner = testUtils.selectLastNonDirectWinner(touchpoints);

  assert.equal(winner?.sessionId, 'session-paid');
});

test('direct-only timelines resolve to the latest direct touch', async () => {
  const testUtils = await getTestUtils();
  const touchpoints = testUtils.dedupeDeterministicCandidates([
    buildTouchpoint('session-a', '2026-04-01T10:00:00.000Z', {
      source: null,
      medium: null,
      campaign: null,
      isDirect: true
    }),
    buildTouchpoint('session-b', '2026-04-02T11:00:00.000Z', {
      source: null,
      medium: null,
      campaign: null,
      isDirect: true
    })
  ]);

  const winner = testUtils.selectLastNonDirectWinner(touchpoints);

  assert.equal(winner?.sessionId, 'session-b');
});

test('dedupe keeps the strongest evidence when the same session is visible multiple ways', async () => {
  const testUtils = await getTestUtils();
  const deduped = testUtils.dedupeDeterministicCandidates([
    buildTouchpoint('session-a', '2026-04-01T10:00:00.000Z', {
      ingestionSource: 'customer_identity',
      attributionReason: 'matched_by_customer_identity'
    }),
    buildTouchpoint('session-a', '2026-04-01T10:00:00.000Z', {
      ingestionSource: 'landing_session_id',
      attributionReason: 'matched_by_landing_session'
    })
  ]);

  assert.equal(deduped.length, 1);
  assert.equal(deduped[0].ingestionSource, 'landing_session_id');
  assert.equal(deduped[0].attributionReason, 'matched_by_landing_session');
});

test('same-timestamp winner selection prefers stronger source, then click id, then lexical session id', async () => {
  const testUtils = await getTestUtils();
  const winnerFromSourcePrecedence = testUtils.selectLastNonDirectWinner(
    testUtils.dedupeDeterministicCandidates([
      buildTouchpoint('session-cart', '2026-04-05T15:00:00.000Z', {
        ingestionSource: 'cart_token',
        attributionReason: 'matched_by_cart_token'
      }),
      buildTouchpoint('session-checkout', '2026-04-05T15:00:00.000Z', {
        ingestionSource: 'checkout_token',
        attributionReason: 'matched_by_checkout_token'
      })
    ])
  );

  assert.equal(winnerFromSourcePrecedence?.sessionId, 'session-checkout');

  const winnerFromClickId = testUtils.selectLastNonDirectWinner(
    testUtils.dedupeDeterministicCandidates([
      buildTouchpoint('session-a', '2026-04-05T15:00:00.000Z'),
      buildTouchpoint('session-b', '2026-04-05T15:00:00.000Z', {
        clickIdType: 'fbclid',
        clickIdValue: 'fbclid-123'
      })
    ])
  );

  assert.equal(winnerFromClickId?.sessionId, 'session-b');

  const winnerFromLexicalSession = testUtils.selectLastNonDirectWinner(
    testUtils.dedupeDeterministicCandidates([
      buildTouchpoint('session-b', '2026-04-05T15:00:00.000Z'),
      buildTouchpoint('session-a', '2026-04-05T15:00:00.000Z')
    ])
  );

  assert.equal(winnerFromLexicalSession?.sessionId, 'session-a');
});
