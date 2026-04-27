import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@localhost:5432/roas_radar_test';

async function getTestUtils() {
  const attributionModule = await import('../src/modules/attribution/index.js');
  return attributionModule.__attributionTestUtils;
}

type TestUtils = Awaited<ReturnType<typeof getTestUtils>>;
type TestTouchpoint = Parameters<TestUtils['dedupeDeterministicCandidates']>[0][number];
type TestGa4Candidate = Parameters<TestUtils['selectGa4FallbackWinner']>[0][number];

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

function buildGa4Candidate(
  occurredAt: string,
  overrides: Partial<TestGa4Candidate> = {}
): TestGa4Candidate {
  return {
    candidateKey: `candidate-${occurredAt}`,
    occurredAt,
    ga4UserKey: 'user-1',
    ga4ClientId: 'client-1',
    ga4SessionId: 'session-1',
    transactionId: null,
    emailHash: null,
    customerIdentityId: null,
    source: 'google',
    medium: 'cpc',
    campaign: 'spring-search',
    content: null,
    term: null,
    clickIdType: null,
    clickIdValue: null,
    sessionHasRequiredFields: true,
    sourceExportHour: '2026-04-01T10:00:00.000Z',
    sourceDataset: 'ga4_export',
    sourceTableType: 'events',
    retainedUntil: '2026-05-01T10:00:00.000Z',
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

test('winner selection returns null when there are no deterministic candidates', async () => {
  const testUtils = await getTestUtils();

  assert.equal(testUtils.selectLastNonDirectWinner([]), null);
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

test('dedupe breaks same-session ties by later occurredAt, then click id presence', async () => {
  const testUtils = await getTestUtils();
  const winnerFromLaterOccurredAt = testUtils.dedupeDeterministicCandidates([
    buildTouchpoint('session-a', '2026-04-01T10:00:00.000Z'),
    buildTouchpoint('session-a', '2026-04-01T11:00:00.000Z')
  ]);

  assert.equal(winnerFromLaterOccurredAt.length, 1);
  assert.equal(winnerFromLaterOccurredAt[0].occurredAt.toISOString(), '2026-04-01T11:00:00.000Z');

  const winnerFromClickId = testUtils.dedupeDeterministicCandidates([
    buildTouchpoint('session-a', '2026-04-01T10:00:00.000Z'),
    buildTouchpoint('session-a', '2026-04-01T10:00:00.000Z', {
      clickIdType: 'gclid',
      clickIdValue: 'gclid-123'
    })
  ]);

  assert.equal(winnerFromClickId.length, 1);
  assert.equal(winnerFromClickId[0].clickIdType, 'gclid');
  assert.equal(winnerFromClickId[0].clickIdValue, 'gclid-123');
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

test('click-id-only touches remain non-direct and beat later direct revisits', async () => {
  const testUtils = await getTestUtils();
  const winner = testUtils.selectLastNonDirectWinner(
    testUtils.dedupeDeterministicCandidates([
      buildTouchpoint('session-paid', '2026-04-02T14:00:00.000Z', {
        source: null,
        medium: null,
        campaign: null,
        clickIdType: 'fbclid',
        clickIdValue: 'fbclid-abc',
        isDirect: false
      }),
      buildTouchpoint('session-direct', '2026-04-03T11:00:00.000Z', {
        source: null,
        medium: null,
        campaign: null,
        clickIdType: null,
        clickIdValue: null,
        isDirect: true
      })
    ])
  );

  assert.equal(winner?.sessionId, 'session-paid');
});

test('latest non-direct wins even when the newer touch has UTMs and no click id', async () => {
  const testUtils = await getTestUtils();
  const winner = testUtils.selectLastNonDirectWinner(
    testUtils.dedupeDeterministicCandidates([
      buildTouchpoint('session-older-click-id', '2026-04-01T10:00:00.000Z', {
        clickIdType: 'gclid',
        clickIdValue: 'gclid-123'
      }),
      buildTouchpoint('session-newer-utm', '2026-04-04T08:00:00.000Z', {
        clickIdType: null,
        clickIdValue: null,
        source: 'google',
        medium: 'cpc',
        campaign: 'spring-search',
        isDirect: false
      })
    ])
  );

  assert.equal(winner?.sessionId, 'session-newer-utm');
});

test('GA4 fallback chooses the latest eligible candidate and keeps low confidence buckets', async () => {
  const testUtils = await getTestUtils();
  const winner = testUtils.selectGa4FallbackWinner(
    [
      buildGa4Candidate('2026-04-01T10:00:00.000Z', {
        clickIdType: 'gclid',
        clickIdValue: 'gclid-older'
      }),
      buildGa4Candidate('2026-04-02T10:00:00.000Z', {
        candidateKey: 'latest-utm-only',
        ga4SessionId: 'session-2',
        clickIdType: null,
        clickIdValue: null
      })
    ],
    new Date('2026-04-03T00:00:00.000Z')
  );

  assert.equal(winner?.candidateKey, 'latest-utm-only');
  assert.equal(testUtils.confidenceLabelForScore(0.35), 'low');
  assert.equal(testUtils.confidenceLabelForScore(0.25), 'low');
});

test('GA4 fallback same-timestamp ties prefer click ids, then richer dimensions, then lexical identifiers', async () => {
  const testUtils = await getTestUtils();
  const clickIdWinner = testUtils.selectGa4FallbackWinner(
    [
      buildGa4Candidate('2026-04-02T10:00:00.000Z', {
        candidateKey: 'utm-only',
        ga4SessionId: 'session-b',
        clickIdType: null,
        clickIdValue: null
      }),
      buildGa4Candidate('2026-04-02T10:00:00.000Z', {
        candidateKey: 'click-id',
        ga4SessionId: 'session-a',
        clickIdType: 'fbclid',
        clickIdValue: 'FB-CLICK-1'
      })
    ],
    new Date('2026-04-03T00:00:00.000Z')
  );
  assert.equal(clickIdWinner?.candidateKey, 'click-id');

  const richerDimensionsWinner = testUtils.selectGa4FallbackWinner(
    [
      buildGa4Candidate('2026-04-02T10:00:00.000Z', {
        candidateKey: 'one-dimension',
        ga4SessionId: 'session-b',
        clickIdType: null,
        clickIdValue: null,
        medium: null,
        campaign: null
      }),
      buildGa4Candidate('2026-04-02T10:00:00.000Z', {
        candidateKey: 'more-dimensions',
        ga4SessionId: 'session-c',
        clickIdType: null,
        clickIdValue: null,
        campaign: 'retargeting'
      })
    ],
    new Date('2026-04-03T00:00:00.000Z')
  );
  assert.equal(richerDimensionsWinner?.candidateKey, 'more-dimensions');

  const lexicalWinner = testUtils.selectGa4FallbackWinner(
    [
      buildGa4Candidate('2026-04-02T10:00:00.000Z', {
        candidateKey: 'session-b',
        ga4SessionId: 'session-b',
        clickIdType: null,
        clickIdValue: null,
        campaign: null
      }),
      buildGa4Candidate('2026-04-02T10:00:00.000Z', {
        candidateKey: 'session-a',
        ga4SessionId: 'session-a',
        clickIdType: null,
        clickIdValue: null,
        campaign: null
      })
    ],
    new Date('2026-04-03T00:00:00.000Z')
  );
  assert.equal(lexicalWinner?.candidateKey, 'session-a');
});

test('GA4 fallback same-timestamp ties fall through to client id, transaction id, then stable input order', async () => {
  const testUtils = await getTestUtils();
  const clientIdWinner = testUtils.selectGa4FallbackWinner(
    [
      buildGa4Candidate('2026-04-02T10:00:00.000Z', {
        candidateKey: 'client-b',
        ga4SessionId: null,
        ga4ClientId: 'client-b',
        campaign: null
      }),
      buildGa4Candidate('2026-04-02T10:00:00.000Z', {
        candidateKey: 'client-a',
        ga4SessionId: null,
        ga4ClientId: 'client-a',
        campaign: null
      })
    ],
    new Date('2026-04-03T00:00:00.000Z')
  );
  assert.equal(clientIdWinner?.candidateKey, 'client-a');

  const transactionIdWinner = testUtils.selectGa4FallbackWinner(
    [
      buildGa4Candidate('2026-04-02T10:00:00.000Z', {
        candidateKey: 'transaction-b',
        ga4SessionId: null,
        ga4ClientId: null,
        transactionId: 'transaction-b',
        campaign: null
      }),
      buildGa4Candidate('2026-04-02T10:00:00.000Z', {
        candidateKey: 'transaction-a',
        ga4SessionId: null,
        ga4ClientId: null,
        transactionId: 'transaction-a',
        campaign: null
      })
    ],
    new Date('2026-04-03T00:00:00.000Z')
  );
  assert.equal(transactionIdWinner?.candidateKey, 'transaction-a');

  const stableOrderWinner = testUtils.selectGa4FallbackWinner(
    [
      buildGa4Candidate('2026-04-02T10:00:00.000Z', {
        candidateKey: 'first-input',
        ga4SessionId: null,
        ga4ClientId: null,
        transactionId: 'same-transaction',
        campaign: null,
        source: 'google'
      }),
      buildGa4Candidate('2026-04-02T10:00:00.000Z', {
        candidateKey: 'second-input',
        ga4SessionId: null,
        ga4ClientId: null,
        transactionId: 'same-transaction',
        campaign: null,
        source: 'google'
      })
    ],
    new Date('2026-04-03T00:00:00.000Z')
  );

  assert.equal(stableOrderWinner?.candidateKey, 'first-input');
});

test('GA4 fallback rejects future-dated and empty candidates', async () => {
  const testUtils = await getTestUtils();
  const winner = testUtils.selectGa4FallbackWinner(
    [
      buildGa4Candidate('2026-04-04T10:00:00.000Z', {
        candidateKey: 'future-candidate'
      }),
      buildGa4Candidate('2026-04-02T10:00:00.000Z', {
        candidateKey: 'empty-candidate',
        source: null,
        medium: null,
        campaign: null,
        content: null,
        term: null,
        clickIdType: null,
        clickIdValue: null
      })
    ],
    new Date('2026-04-03T00:00:00.000Z')
  );

  assert.equal(winner, null);
});

test('GA4 fallback can use a transaction-only candidate when attribution dimensions are present', async () => {
  const testUtils = await getTestUtils();
  const winner = testUtils.selectGa4FallbackWinner(
    [
      buildGa4Candidate('2026-04-02T10:00:00.000Z', {
        candidateKey: 'transaction-only',
        ga4SessionId: null,
        ga4ClientId: null,
        transactionId: 'transaction-only-1',
        source: 'google',
        medium: 'cpc',
        campaign: 'transaction-only'
      })
    ],
    new Date('2026-04-03T00:00:00.000Z')
  );

  assert.equal(winner?.candidateKey, 'transaction-only');
});
