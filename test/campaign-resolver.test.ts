import assert from 'node:assert/strict';
import test from 'node:test';

import {
  backfillMmmCampaignMetadata,
  DEFAULT_CAMPAIGN_RESOLVER_VERSION,
  resolveCampaignMetadata
} from '../src/modules/campaign-resolver/index.js';

type MockQueryCall = {
  text: string;
  params?: unknown[];
};

function createMockClient(responses: Array<{ rows: unknown[] }>) {
  const calls: MockQueryCall[] = [];

  return {
    calls,
    client: {
      async query(text: string, params?: unknown[]) {
        calls.push({ text, params });
        const response = responses.shift();

        if (!response) {
          throw new Error(`Unexpected query: ${text}`);
        }

        return response;
      }
    }
  };
}

function buildRule(overrides: Partial<Record<string, unknown>> = {}) {
  return {
    id: 'rule-1',
    resolver_version: DEFAULT_CAMPAIGN_RESOLVER_VERSION,
    rule_kind: 'rule',
    priority: 50,
    match_platform: null,
    match_source: 'google',
    match_medium: 'cpc',
    match_campaign: 'spring-sale',
    match_content: null,
    match_term: null,
    match_account_id: null,
    match_campaign_id: null,
    match_adset_id: null,
    match_ad_id: null,
    match_expression: {},
    canonical_campaign_id: 'canonical-spring',
    canonical_campaign_name: 'Spring Sale',
    canonical_source: 'google',
    canonical_medium: 'cpc',
    canonical_channel: 'paid_search',
    canonical_channel_group: 'paid_media',
    hierarchy_metadata: { funnel: 'prospecting' },
    confidence: '0.9500',
    source_label: 'rule',
    ...overrides
  };
}

test('campaign resolver returns override metadata with confidence and provenance', async () => {
  const override = buildRule({
    id: 'override-1',
    rule_kind: 'override',
    priority: 1,
    canonical_campaign_name: 'Spring Sale Override',
    confidence: '1.0000'
  });
  const rule = buildRule();
  const { client } = createMockClient([{ rows: [override, rule] }]);

  const resolution = await resolveCampaignMetadata(
    {
      source: 'Google',
      medium: 'CPC',
      campaign: 'Spring-Sale',
      enqueueUnmapped: false
    },
    client
  );

  assert.equal(resolution.status, 'resolved');
  assert.equal(resolution.source, 'override');
  assert.equal(resolution.confidence, 1);
  assert.equal(resolution.ruleId, 'override-1');
  assert.deepEqual(resolution.canonical, {
    campaignId: 'canonical-spring',
    campaignName: 'Spring Sale Override',
    source: 'google',
    medium: 'cpc',
    channel: 'paid_search',
    channelGroup: 'paid_media',
    hierarchy: { funnel: 'prospecting' }
  });
});

test('campaign resolver enqueues unmapped records for QA', async () => {
  const { client, calls } = createMockClient([{ rows: [] }, { rows: [{ id: 'qa-1' }] }]);

  const resolution = await resolveCampaignMetadata(
    {
      source: 'unknown_network',
      medium: 'mystery',
      campaign: null,
      accountId: 'acct-1',
      campaignId: 'cmp-1',
      samplePayload: { row: 'sample' }
    },
    client
  );

  assert.equal(resolution.status, 'unmapped');
  assert.equal(resolution.source, 'unmapped');
  assert.equal(resolution.confidence, 0);
  assert.equal(resolution.qaQueueId, 'qa-1');
  assert.match(calls[1].text, /campaign_metadata_qa_queue/);
  assert.deepEqual(calls[1].params?.slice(0, 4), [
    DEFAULT_CAMPAIGN_RESOLVER_VERSION,
    'no_matching_campaign_metadata_rule',
    null,
    'unknown_network'
  ]);
});

test('MMM campaign metadata backfill enriches rows and reports QA counts', async () => {
  const { client, calls } = createMockClient([
    {
      rows: [
        {
          metric_date: '2026-04-01',
          mart_version: 'v1',
          mart_row_type: 'paid_media',
          attribution_model: 'none',
          platform: 'google',
          granularity: 'creative',
          entity_key: 'ad-1',
          source: 'google',
          medium: 'cpc',
          campaign: 'spring-sale',
          content: 'unknown',
          term: 'unknown',
          account_id: 'acct-1',
          campaign_id: 'cmp-1',
          adset_id: null,
          ad_id: 'ad-1'
        },
        {
          metric_date: '2026-04-01',
          mart_version: 'v1',
          mart_row_type: 'attribution',
          attribution_model: 'last_touch',
          platform: 'taxonomy',
          granularity: 'taxonomy',
          entity_key: 'unknown|unknown|unknown|unknown|unknown',
          source: 'unknown',
          medium: 'unknown',
          campaign: 'unknown',
          content: 'unknown',
          term: 'unknown',
          account_id: null,
          campaign_id: null,
          adset_id: null,
          ad_id: null
        }
      ]
    },
    { rows: [buildRule()] },
    { rows: [] },
    { rows: [] },
    { rows: [{ id: 'qa-2' }] },
    { rows: [] }
  ]);

  const report = await backfillMmmCampaignMetadata(
    {
      startDate: '2026-04-01',
      endDate: '2026-04-01'
    },
    client
  );

  assert.deepEqual(report, {
    resolverVersion: DEFAULT_CAMPAIGN_RESOLVER_VERSION,
    processedRows: 2,
    resolvedRows: 1,
    fallbackRows: 0,
    unmappedRows: 1,
    qaQueuedRows: 1
  });
  assert.equal(calls.filter((call) => /UPDATE mmm_daily_input_mart_v1/.test(call.text)).length, 2);
});
