import React, { useCallback, useEffect, useState, type FormEvent } from 'react';

import {
  type AdminDebugAuditResponse,
  type AdminDebugJourneyResponse,
  type AdminDebugReplayResponse,
  type CampaignResolverDebugResponse,
  debugCampaignResolver,
  fetchAdminDebugAudit,
  fetchAdminDebugJourney,
  triggerAdminDebugRecompute,
  triggerAdminDebugReplay
} from '../lib/api';
import {
  formatCurrency,
  formatDateTimeLabel,
  formatNumber,
  formatPercent
} from '../lib/format';
import {
  Badge,
  Banner,
  Button,
  ButtonRow,
  Card,
  CardDescription,
  CardHeader,
  CardTitle,
  EmptyState,
  Eyebrow,
  Field,
  FieldGrid,
  Form,
  Input,
  Panel,
  SectionState,
  Select
} from './AuthenticatedUi';

type AsyncSection<T> = {
  data: T | null;
  loading: boolean;
  error: string | null;
};

type Feedback = {
  tone: 'success' | 'error' | 'warning';
  message: string;
} | null;

function idleSection<T>(): AsyncSection<T> {
  return {
    data: null,
    loading: false,
    error: null
  };
}

function loadingSection<T>(current: AsyncSection<T>): AsyncSection<T> {
  return {
    data: current.data,
    loading: true,
    error: null
  };
}

function stringifyJson(value: unknown): string {
  return JSON.stringify(value ?? {}, null, 2);
}

function titleize(value: string | null | undefined): string {
  if (!value) {
    return 'None';
  }

  return value
    .split('_')
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function MetricTile({ label, value, detail }: { label: string; value: string; detail: string }) {
  return (
    <Card padding="compact" className="min-h-[8.5rem]">
      <Eyebrow>{label}</Eyebrow>
      <p className="mt-3 font-display text-title text-ink">{value}</p>
      <p className="mt-2 text-body text-ink-muted">{detail}</p>
    </Card>
  );
}

function KeyValueGrid({ rows }: { rows: Array<[string, string | null | undefined]> }) {
  return (
    <dl className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
      {rows.map(([label, value]) => (
        <div key={label} className="rounded-card border border-line/60 bg-surface-alt/55 p-4">
          <dt className="text-caption font-semibold uppercase tracking-[0.12em] text-ink-muted">{label}</dt>
          <dd className="mt-2 break-words text-body font-semibold text-ink">{value || 'None'}</dd>
        </div>
      ))}
    </dl>
  );
}

function JourneyPanel({
  journey,
  reportingTimezone
}: {
  journey: AdminDebugJourneyResponse;
  reportingTimezone: string;
}) {
  const primarySummary = journey.attribution.modelSummaries.find((row) => row.winnerTouchpointId) ?? null;

  return (
    <div className="grid gap-6">
      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricTile
          label="Order value"
          value={formatCurrency(journey.order.totalPrice)}
          detail={`${journey.order.currencyCode} subtotal ${formatCurrency(journey.order.subtotalPrice)}`}
        />
        <MetricTile
          label="Touchpoints"
          value={formatNumber(journey.attribution.touchpoints.length)}
          detail={`${formatNumber(journey.events.length)} raw events returned`}
        />
        <MetricTile
          label="Identity"
          value={journey.identity.journey ? titleize(journey.identity.journey.status) : 'Unlinked'}
          detail={`${formatNumber(journey.identity.edges.length)} graph edges`}
        />
        <MetricTile
          label="Merge audits"
          value={formatNumber(journey.identity.mergeAudits.length)}
          detail="Winner, loser, and candidate score records"
        />
      </div>

      <Panel title="Order and current attribution" wide>
        <KeyValueGrid
          rows={[
            ['Shopify order', journey.order.shopifyOrderId],
            ['Order number', journey.order.shopifyOrderNumber],
            ['Customer', journey.order.shopifyCustomerId],
            ['Processed', journey.order.processedAt ? formatDateTimeLabel(journey.order.processedAt, reportingTimezone) : null],
            ['Landing session', journey.order.landingSessionId],
            ['Identity journey', journey.order.identityJourneyId],
            ['Current source', journey.order.currentAttribution.source],
            ['Current medium', journey.order.currentAttribution.medium],
            ['Current campaign', journey.order.currentAttribution.campaign],
            ['Reason', journey.order.currentAttribution.attributionReason],
            ['Confidence', journey.order.currentAttribution.confidenceScore == null ? null : formatPercent(journey.order.currentAttribution.confidenceScore)],
            ['Latest run', journey.run?.runId]
          ]}
        />
      </Panel>

      <Panel
        title="Attribution decisions"
        description="Model summaries, credited touchpoints, and explain records from the latest attribution run."
        wide
      >
        {journey.run ? (
          <div className="grid gap-5">
            <div className="flex flex-wrap items-center gap-3">
              <Badge tone={journey.run.status === 'completed' ? 'success' : 'warning'}>{journey.run.status}</Badge>
              <span className="text-body text-ink-muted">
                {journey.run.triggerSource} run created {formatDateTimeLabel(journey.run.createdAt, reportingTimezone)}
              </span>
            </div>
            <div className="overflow-auto rounded-card border border-line/60 bg-surface/65">
              <table className="min-w-[58rem] border-collapse [&_td]:border-b [&_td]:border-line/50 [&_td]:px-4 [&_td]:py-4 [&_td]:text-left [&_td]:align-top [&_th]:border-b [&_th]:border-line/50 [&_th]:px-4 [&_th]:py-4 [&_th]:text-left [&_th]:text-caption [&_th]:font-semibold [&_th]:uppercase [&_th]:tracking-[0.14em] [&_th]:text-ink-muted">
                <thead>
                  <tr>
                    <th>Model</th>
                    <th>Status</th>
                    <th>Winner</th>
                    <th>Credit</th>
                    <th>Rule</th>
                    <th>Flags</th>
                  </tr>
                </thead>
                <tbody>
                  {journey.attribution.modelSummaries.map((row) => (
                    <tr key={row.modelKey}>
                      <td className="font-semibold text-ink">{titleize(row.modelKey)}</td>
                      <td>{titleize(row.allocationStatus)}</td>
                      <td>{row.winnerTouchpointId || row.winnerAttributionReason || 'None'}</td>
                      <td>{formatCurrency(row.totalRevenueCredited)} / {formatPercent(row.totalCreditWeight)}</td>
                      <td>{titleize(row.winnerSelectionRule)}</td>
                      <td>
                        {row.directSuppressionApplied ? 'direct suppressed' : 'direct allowed'}
                        {row.deterministicBlockApplied ? ', deterministic block' : ''}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {primarySummary ? (
              <Banner tone="success">
                Primary winner: {primarySummary.winnerTouchpointId} via {titleize(primarySummary.winnerEvidenceSource)} because {primarySummary.winnerAttributionReason || 'no reason recorded'}.
              </Banner>
            ) : null}
          </div>
        ) : (
          <EmptyState
            title="No attribution run found"
            description="This order exists, but there are no v1 attribution run inputs for it yet."
            compact
          />
        )}
      </Panel>

      <Panel title="Touchpoint path" wide>
        <SectionState
          loading={false}
          error={null}
          empty={journey.attribution.touchpoints.length === 0}
          emptyLabel="No attribution touchpoints were captured for the latest run."
        >
          <div className="grid gap-3">
            {journey.attribution.touchpoints.map((touchpoint) => (
              <Card key={touchpoint.touchpointId} padding="compact">
                <CardHeader>
                  <div>
                    <CardTitle>{touchpoint.source || 'direct'} / {touchpoint.medium || 'none'}</CardTitle>
                    <CardDescription>
                      {formatDateTimeLabel(touchpoint.occurredAt, reportingTimezone)} · {touchpoint.campaign || 'No campaign'}
                    </CardDescription>
                  </div>
                  <Badge tone={touchpoint.isEligible ? 'success' : 'warning'}>
                    {touchpoint.isEligible ? 'Eligible' : 'Excluded'}
                  </Badge>
                </CardHeader>
                <KeyValueGrid
                  rows={[
                    ['Touchpoint', touchpoint.touchpointId],
                    ['Evidence', titleize(touchpoint.evidenceSource)],
                    ['Source kind', titleize(touchpoint.sourceKind)],
                    ['Engagement', titleize(touchpoint.engagementType)],
                    ['Reason', touchpoint.attributionReason ?? touchpoint.ineligibilityReason],
                    ['Session', touchpoint.sessionId]
                  ]}
                />
              </Card>
            ))}
          </div>
        </SectionState>
      </Panel>

      <Panel title="Identity merge explainability" wide>
        <div className="grid gap-5">
          {journey.identity.journey ? (
            <KeyValueGrid
              rows={[
                ['Journey', journey.identity.journey.id],
                ['Status', titleize(journey.identity.journey.status)],
                ['Merge version', String(journey.identity.journey.mergeVersion)],
                ['Authoritative customer', journey.identity.journey.authoritativeShopifyCustomerId],
                ['Merged into', journey.identity.journey.mergedIntoJourneyId],
                ['Updated', formatDateTimeLabel(journey.identity.journey.updatedAt, reportingTimezone)]
              ]}
            />
          ) : (
            <EmptyState title="No identity journey" description="The order is not linked to an identity journey." compact />
          )}
          <div className="grid gap-3">
            {journey.identity.mergeAudits.map((audit) => (
              <Card key={audit.id} padding="compact">
                <CardHeader>
                  <div>
                    <CardTitle>{titleize(audit.mergeReasonCode)}</CardTitle>
                    <CardDescription>
                      {audit.loserJourneyId} merged into {audit.winnerJourneyId}
                    </CardDescription>
                  </div>
                  <Badge tone="teal">{formatNumber(audit.rehomedNodes)} rehomed</Badge>
                </CardHeader>
                <pre className="max-h-72 overflow-auto rounded-card bg-canvas-tint p-4 text-xs text-ink-soft">
                  {stringifyJson({
                    winnerScore: audit.winnerScore,
                    loserScore: audit.loserScore,
                    candidateScores: audit.candidateScores
                  })}
                </pre>
              </Card>
            ))}
          </div>
        </div>
      </Panel>
    </div>
  );
}

export default function AdminDebugToolsView({ reportingTimezone }: { reportingTimezone: string }) {
  const [orderId, setOrderId] = useState('');
  const [journeySection, setJourneySection] = useState<AsyncSection<AdminDebugJourneyResponse>>(idleSection());
  const [resolverForm, setResolverForm] = useState({
    platform: '',
    source: '',
    medium: '',
    campaign: '',
    campaignId: '',
    accountId: ''
  });
  const [resolverSection, setResolverSection] = useState<AsyncSection<CampaignResolverDebugResponse>>(idleSection());
  const [replayForm, setReplayForm] = useState({
    eventType: '',
    sourceTable: '',
    limit: '25',
    dryRun: true
  });
  const [replaySection, setReplaySection] = useState<AsyncSection<AdminDebugReplayResponse>>(idleSection());
  const [recomputeForm, setRecomputeForm] = useState({
    startDate: '',
    endDate: '',
    dryRun: true
  });
  const [auditSection, setAuditSection] = useState<AsyncSection<AdminDebugAuditResponse>>(idleSection());
  const [feedback, setFeedback] = useState<Feedback>(null);

  const latestAuditRows = auditSection.data?.rows ?? [];
  const recomputeLimit = 100;

  const loadAudit = useCallback(async () => {
    setAuditSection((current) => loadingSection(current));

    try {
      setAuditSection({
        data: await fetchAdminDebugAudit(25),
        loading: false,
        error: null
      });
    } catch (error) {
      setAuditSection({
        data: null,
        loading: false,
        error: error instanceof Error ? error.message : 'Failed to load audit log'
      });
    }
  }, []);

  useEffect(() => {
    void loadAudit();
  }, [loadAudit]);

  async function handleJourneySubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const normalizedOrderId = orderId.trim();
    if (!normalizedOrderId) {
      return;
    }

    setJourneySection((current) => loadingSection(current));
    setFeedback(null);

    try {
      setJourneySection({
        data: await fetchAdminDebugJourney(normalizedOrderId),
        loading: false,
        error: null
      });
      await loadAudit();
    } catch (error) {
      setJourneySection({
        data: null,
        loading: false,
        error: error instanceof Error ? error.message : 'Failed to inspect conversion journey'
      });
    }
  }

  async function handleResolverSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setResolverSection((current) => loadingSection(current));
    setFeedback(null);

    try {
      const response = await debugCampaignResolver({
        platform: resolverForm.platform.trim() || null,
        source: resolverForm.source.trim() || null,
        medium: resolverForm.medium.trim() || null,
        campaign: resolverForm.campaign.trim() || null,
        campaignId: resolverForm.campaignId.trim() || null,
        accountId: resolverForm.accountId.trim() || null,
        enqueueUnmapped: false
      });
      setResolverSection({
        data: response,
        loading: false,
        error: null
      });
      await loadAudit();
    } catch (error) {
      setResolverSection({
        data: null,
        loading: false,
        error: error instanceof Error ? error.message : 'Failed to debug campaign resolver'
      });
    }
  }

  async function handleReplaySubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setReplaySection((current) => loadingSection(current));
    setFeedback(null);

    try {
      const response = await triggerAdminDebugReplay({
        eventType: replayForm.eventType.trim() || undefined,
        sourceTable: replayForm.sourceTable.trim() || undefined,
        limit: Number(replayForm.limit || '25'),
        dryRun: replayForm.dryRun
      });
      setReplaySection({
        data: response,
        loading: false,
        error: null
      });
      setFeedback({
        tone: replayForm.dryRun ? 'warning' : 'success',
        message: replayForm.dryRun
          ? `Dry run inspected ${formatNumber(response.replay.candidateCount)} pending events.`
          : `Replay run ${response.replay.replayRunId} requeued ${formatNumber(response.replay.replayedCount)} events.`
      });
      await loadAudit();
    } catch (error) {
      setReplaySection({
        data: null,
        loading: false,
        error: error instanceof Error ? error.message : 'Failed to trigger replay'
      });
    }
  }

  async function handleRecomputeSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setFeedback(null);

    try {
      if (!recomputeForm.startDate || !recomputeForm.endDate) {
        throw new Error('Start and end dates are required.');
      }

      const response = await triggerAdminDebugRecompute({
        startDate: recomputeForm.startDate,
        endDate: recomputeForm.endDate,
        dryRun: recomputeForm.dryRun,
        limit: recomputeLimit,
        webOrdersOnly: false,
        skipShopifyWriteback: true
      });
      setFeedback({
        tone: 'success',
        message: `Queued recompute job ${response.jobId}.`
      });
      await loadAudit();
    } catch (error) {
      setFeedback({
        tone: 'error',
        message: error instanceof Error ? error.message : 'Failed to queue recompute job'
      });
    }
  }

  return (
    <section className="grid gap-section">
      <Panel
        title="Admin debug tools"
        description="Inspect conversion journeys, explain merge and attribution decisions, debug campaign metadata resolution, and trigger guarded replay or recompute jobs."
        wide
      >
        <div className="grid gap-5">
          {feedback ? <Banner tone={feedback.tone}>{feedback.message}</Banner> : null}
          <Form onSubmit={handleJourneySubmit}>
            <FieldGrid>
              <Field label="Shopify order ID" required>
                <Input
                  value={orderId}
                  onChange={(event) => setOrderId(event.target.value)}
                  placeholder="gid://shopify/Order/123 or 123"
                />
              </Field>
              <div className="flex items-end">
                <Button type="submit" disabled={journeySection.loading}>
                  {journeySection.loading ? 'Inspecting…' : 'Inspect journey'}
                </Button>
              </div>
            </FieldGrid>
          </Form>
        </div>
      </Panel>

      {journeySection.loading || journeySection.error || journeySection.data ? (
        <SectionState
          loading={journeySection.loading}
          error={journeySection.error}
          empty={!journeySection.data}
          emptyLabel="Enter a Shopify order ID to inspect a journey."
        >
          <JourneyPanel journey={journeySection.data as AdminDebugJourneyResponse} reportingTimezone={reportingTimezone} />
        </SectionState>
      ) : null}

      <div className="grid gap-section xl:grid-cols-2">
        <Panel title="Campaign resolver debugger" description="Run the resolver without writing QA queue records.">
          <Form onSubmit={handleResolverSubmit}>
            <FieldGrid>
              <Field label="Platform">
                <Select
                  value={resolverForm.platform}
                  onChange={(event) => setResolverForm((current) => ({ ...current, platform: event.target.value }))}
                >
                  <option value="">Unknown</option>
                  <option value="google_ads">Google Ads</option>
                  <option value="meta_ads">Meta Ads</option>
                </Select>
              </Field>
              <Field label="Source">
                <Input value={resolverForm.source} onChange={(event) => setResolverForm((current) => ({ ...current, source: event.target.value }))} />
              </Field>
              <Field label="Medium">
                <Input value={resolverForm.medium} onChange={(event) => setResolverForm((current) => ({ ...current, medium: event.target.value }))} />
              </Field>
              <Field label="Campaign">
                <Input value={resolverForm.campaign} onChange={(event) => setResolverForm((current) => ({ ...current, campaign: event.target.value }))} />
              </Field>
              <Field label="Campaign ID">
                <Input value={resolverForm.campaignId} onChange={(event) => setResolverForm((current) => ({ ...current, campaignId: event.target.value }))} />
              </Field>
              <Field label="Account ID">
                <Input value={resolverForm.accountId} onChange={(event) => setResolverForm((current) => ({ ...current, accountId: event.target.value }))} />
              </Field>
            </FieldGrid>
            <ButtonRow>
              <Button type="submit" disabled={resolverSection.loading}>
                {resolverSection.loading ? 'Resolving…' : 'Explain resolution'}
              </Button>
            </ButtonRow>
          </Form>
          <SectionState
            loading={resolverSection.loading}
            error={resolverSection.error}
            empty={!resolverSection.data}
            emptyLabel="No resolver debug result has been requested yet."
            compact
          >
            <div className="mt-5 grid gap-3">
              <Badge tone={resolverSection.data?.resolution.status === 'resolved' ? 'success' : 'warning'}>
                {resolverSection.data?.resolution.status}
              </Badge>
              <KeyValueGrid
                rows={[
                  ['Source', titleize(resolverSection.data?.resolution.source)],
                  ['Rule ID', resolverSection.data?.resolution.ruleId],
                  ['Campaign', resolverSection.data?.resolution.canonical.campaignName],
                  ['Channel', resolverSection.data?.resolution.canonical.channel],
                  ['Confidence', resolverSection.data ? formatPercent(resolverSection.data.resolution.confidence) : null],
                  ['QA queue', resolverSection.data?.resolution.qaQueueId]
                ]}
              />
            </div>
          </SectionState>
        </Panel>

        <Panel title="Replay and recompute triggers" description="Actions are admin-gated and recorded in the audit log.">
          <div className="grid gap-7">
            <Form onSubmit={handleReplaySubmit}>
              <FieldGrid>
                <Field label="Event type">
                  <Input value={replayForm.eventType} onChange={(event) => setReplayForm((current) => ({ ...current, eventType: event.target.value }))} />
                </Field>
                <Field label="Source table">
                  <Input value={replayForm.sourceTable} onChange={(event) => setReplayForm((current) => ({ ...current, sourceTable: event.target.value }))} />
                </Field>
                <Field label="Limit">
                  <Input type="number" min="1" max="500" value={replayForm.limit} onChange={(event) => setReplayForm((current) => ({ ...current, limit: event.target.value }))} />
                </Field>
                <Field label="Mode">
                  <Select
                    value={replayForm.dryRun ? 'dry_run' : 'replay'}
                    onChange={(event) => setReplayForm((current) => ({ ...current, dryRun: event.target.value === 'dry_run' }))}
                  >
                    <option value="dry_run">Dry run</option>
                    <option value="replay">Replay</option>
                  </Select>
                </Field>
              </FieldGrid>
              <ButtonRow>
                <Button type="submit" disabled={replaySection.loading}>
                  {replaySection.loading ? 'Submitting…' : 'Trigger replay'}
                </Button>
              </ButtonRow>
            </Form>

            <Form onSubmit={handleRecomputeSubmit}>
              <FieldGrid>
                <Field label="Start date" required>
                  <Input type="date" value={recomputeForm.startDate} onChange={(event) => setRecomputeForm((current) => ({ ...current, startDate: event.target.value }))} />
                </Field>
                <Field label="End date" required>
                  <Input type="date" value={recomputeForm.endDate} onChange={(event) => setRecomputeForm((current) => ({ ...current, endDate: event.target.value }))} />
                </Field>
                <Field label="Mode">
                  <Select
                    value={recomputeForm.dryRun ? 'dry_run' : 'recompute'}
                    onChange={(event) => setRecomputeForm((current) => ({ ...current, dryRun: event.target.value === 'dry_run' }))}
                  >
                    <option value="dry_run">Dry run</option>
                    <option value="recompute">Recompute</option>
                  </Select>
                </Field>
              </FieldGrid>
              <ButtonRow>
                <Button type="submit">Queue recompute</Button>
              </ButtonRow>
            </Form>
          </div>
        </Panel>
      </div>

      <Panel title="Recent admin debug audit" wide>
        <SectionState
          loading={auditSection.loading}
          error={auditSection.error}
          empty={latestAuditRows.length === 0}
          emptyLabel="No admin debug actions have been recorded yet."
        >
          <div className="overflow-auto rounded-card border border-line/60 bg-surface/65">
            <table className="min-w-[58rem] border-collapse [&_td]:border-b [&_td]:border-line/50 [&_td]:px-4 [&_td]:py-4 [&_td]:text-left [&_td]:align-top [&_th]:border-b [&_th]:border-line/50 [&_th]:px-4 [&_th]:py-4 [&_th]:text-left [&_th]:text-caption [&_th]:font-semibold [&_th]:uppercase [&_th]:tracking-[0.14em] [&_th]:text-ink-muted">
              <thead>
                <tr>
                  <th>Time</th>
                  <th>Actor</th>
                  <th>Action</th>
                  <th>Target</th>
                  <th>Result</th>
                </tr>
              </thead>
              <tbody>
                {latestAuditRows.map((row) => (
                  <tr key={row.id}>
                    <td>{formatDateTimeLabel(row.createdAt, reportingTimezone)}</td>
                    <td>{row.actorEmail}</td>
                    <td>{titleize(row.action)}</td>
                    <td>{row.targetType}{row.targetId ? ` · ${row.targetId}` : ''}</td>
                    <td>
                      <pre className="max-h-32 overflow-auto text-xs text-ink-soft">{stringifyJson(row.resultSummary)}</pre>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </SectionState>
      </Panel>
    </section>
  );
}
