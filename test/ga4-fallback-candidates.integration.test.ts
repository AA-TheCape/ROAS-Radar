import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ??= 'postgres://postgres:postgres@127.0.0.1:5432/roas_radar';

async function resetGa4FallbackFixtures(): Promise<void> {
  const { pool } = await import('../src/db/pool.js');
  await pool.query(`
    TRUNCATE TABLE
      ga4_fallback_candidates,
      customer_identities
    RESTART IDENTITY CASCADE
  `);
}

test.beforeEach(async () => {
  await resetGa4FallbackFixtures();
});

test.after(async () => {
  await resetGa4FallbackFixtures();
  const { pool } = await import('../src/db/pool.js');
  await pool.end();
});

test('GA4 fallback candidate store upserts normalized rows and resolves keyed lookup order', { concurrency: false }, async () => {
  const emailHash = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';

  const [{ upsertGa4FallbackCandidates, lookupGa4FallbackCandidates, listGa4FallbackCandidates }, { pool }] =
    await Promise.all([
      import('../src/modules/attribution/ga4-fallback-candidates.js'),
      import('../src/db/pool.js')
    ]);

  const insertedCount = await upsertGa4FallbackCandidates([
    {
      occurredAt: '2026-04-26T11:00:00.000Z',
      ga4UserKey: 'user-1',
      ga4ClientId: 'client-1',
      ga4SessionId: 'session-1',
      transactionId: 'shopify-order-77',
      emailHash,
      customerIdentityId: null,
      source: 'Google',
      medium: 'CPC',
      campaign: 'Brand',
      content: 'Hero',
      term: 'boots',
      clickIdType: 'gclid',
      clickIdValue: 'gclid-1',
      sessionHasRequiredFields: true,
      sourceExportHour: '2026-04-26T11:00:00.000Z',
      sourceDataset: 'ga4_export',
      sourceTableType: 'events'
    },
    {
      occurredAt: '2026-04-26T11:00:00.000Z',
      ga4UserKey: 'user-1',
      ga4ClientId: 'client-1',
      ga4SessionId: 'session-1',
      transactionId: 'shopify-order-77',
      emailHash,
      customerIdentityId: null,
      source: 'google',
      medium: 'cpc',
      campaign: 'Brand',
      content: 'Retargeting',
      term: 'boots',
      clickIdType: 'gclid',
      clickIdValue: 'gclid-1',
      sessionHasRequiredFields: true,
      sourceExportHour: '2026-04-26T12:00:00.000Z',
      sourceDataset: 'ga4_export',
      sourceTableType: 'intraday'
    },
    {
      occurredAt: '2026-04-26T12:00:00.000Z',
      ga4UserKey: 'user-2',
      ga4ClientId: 'client-2',
      ga4SessionId: 'session-2',
      transactionId: null,
      emailHash,
      customerIdentityId: null,
      source: 'Email',
      medium: 'Newsletter',
      campaign: 'Spring',
      content: null,
      term: null,
      clickIdType: null,
      clickIdValue: null,
      sessionHasRequiredFields: true,
      sourceExportHour: '2026-04-26T12:00:00.000Z',
      sourceDataset: 'ga4_export',
      sourceTableType: 'events'
    }
  ]);

  assert.equal(insertedCount, 3);

  const persisted = await listGa4FallbackCandidates(pool);
  assert.equal(persisted.length, 2);
  assert.deepEqual(
    persisted.map((row) => ({
      ga4SessionId: row.ga4SessionId,
      sourceTableType: row.sourceTableType,
      content: row.content,
      sourceExportHour: row.sourceExportHour
    })),
    [
      {
        ga4SessionId: 'session-2',
        sourceTableType: 'events',
        content: null,
        sourceExportHour: '2026-04-26T12:00:00.000Z'
      },
      {
        ga4SessionId: 'session-1',
        sourceTableType: 'intraday',
        content: 'Retargeting',
        sourceExportHour: '2026-04-26T12:00:00.000Z'
      }
    ]
  );

  const resolvedCandidates = await lookupGa4FallbackCandidates(
    {
      orderOccurredAt: '2026-04-27T12:00:00.000Z',
      customerIdentityId: null,
      emailHash,
      transactionId: 'shopify-order-77'
    },
    pool
  );

  assert.deepEqual(
    resolvedCandidates.map((row) => ({
      ga4SessionId: row.ga4SessionId,
      transactionId: row.transactionId,
      clickIdType: row.clickIdType,
      source: row.source,
      medium: row.medium
    })),
    [
      {
        ga4SessionId: 'session-2',
        transactionId: null,
        clickIdType: null,
        source: 'email',
        medium: 'newsletter'
      },
      {
        ga4SessionId: 'session-1',
        transactionId: 'shopify-order-77',
        clickIdType: 'gclid',
        source: 'google',
        medium: 'cpc'
      }
    ]
  );
});

test('GA4 fallback candidate store deduplicates repeated ingestion and keeps coherent attribution bundles', { concurrency: false }, async () => {
  const emailHash = 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';

  const [{ upsertGa4FallbackCandidates, listGa4FallbackCandidates, lookupGa4FallbackCandidates }, { pool }] =
    await Promise.all([
      import('../src/modules/attribution/ga4-fallback-candidates.js'),
      import('../src/db/pool.js')
    ]);

  const insertedCount = await upsertGa4FallbackCandidates([
    {
      occurredAt: '2026-04-26T11:00:00.000Z',
      ga4UserKey: 'user-repeat',
      ga4ClientId: 'client-repeat',
      ga4SessionId: 'session-repeat',
      transactionId: 'shopify-order-repeat',
      emailHash: null,
      customerIdentityId: null,
      source: 'google',
      medium: 'cpc',
      campaign: 'brand',
      content: 'hero',
      term: 'boots',
      clickIdType: 'gclid',
      clickIdValue: 'gclid-older',
      sessionHasRequiredFields: true,
      sourceExportHour: '2026-04-26T10:00:00.000Z',
      sourceDataset: 'ga4_export',
      sourceTableType: 'intraday'
    },
    {
      occurredAt: '2026-04-26T11:00:00.000Z',
      ga4UserKey: 'user-repeat',
      ga4ClientId: 'client-repeat',
      ga4SessionId: 'session-repeat',
      transactionId: 'shopify-order-repeat',
      emailHash,
      customerIdentityId: null,
      source: 'meta',
      medium: 'paid_social',
      campaign: 'retargeting',
      content: 'story',
      term: 'sandals',
      clickIdType: 'fbclid',
      clickIdValue: 'fbclid-newer',
      sessionHasRequiredFields: true,
      sourceExportHour: '2026-04-26T12:00:00.000Z',
      sourceDataset: 'ga4_export',
      sourceTableType: 'events'
    },
    {
      occurredAt: '2026-04-26T11:00:00.000Z',
      ga4UserKey: 'user-repeat',
      ga4ClientId: 'client-repeat',
      ga4SessionId: 'session-repeat',
      transactionId: 'shopify-order-repeat',
      emailHash,
      customerIdentityId: null,
      source: null,
      medium: null,
      campaign: 'conflict-should-not-stick',
      content: null,
      term: null,
      clickIdType: null,
      clickIdValue: null,
      sessionHasRequiredFields: false,
      sourceExportHour: '2026-04-26T13:00:00.000Z',
      sourceDataset: 'ga4_export',
      sourceTableType: 'events'
    }
  ]);

  assert.equal(insertedCount, 3);

  const persisted = await listGa4FallbackCandidates(pool);
  assert.equal(persisted.length, 1);
  assert.deepEqual(persisted[0], {
    candidateKey: persisted[0]?.candidateKey,
    occurredAt: '2026-04-26T11:00:00.000Z',
    ga4UserKey: 'user-repeat',
    ga4ClientId: 'client-repeat',
    ga4SessionId: 'session-repeat',
    transactionId: 'shopify-order-repeat',
    emailHash,
    customerIdentityId: null,
    source: 'meta',
    medium: 'paid_social',
    campaign: 'retargeting',
    content: 'story',
    term: 'sandals',
    clickIdType: 'fbclid',
    clickIdValue: 'fbclid-newer',
    sessionHasRequiredFields: true,
    sourceExportHour: '2026-04-26T12:00:00.000Z',
    sourceDataset: 'ga4_export',
    sourceTableType: 'events',
    retainedUntil: persisted[0]?.retainedUntil
  });

  const resolvedCandidates = await lookupGa4FallbackCandidates(
    {
      orderOccurredAt: '2026-04-27T12:00:00.000Z',
      customerIdentityId: null,
      emailHash,
      transactionId: 'shopify-order-repeat'
    },
    pool
  );

  assert.equal(resolvedCandidates.length, 1);
  assert.equal(resolvedCandidates[0]?.campaign, 'retargeting');
  assert.equal(resolvedCandidates[0]?.content, 'story');
  assert.equal(resolvedCandidates[0]?.clickIdValue, 'fbclid-newer');
});
