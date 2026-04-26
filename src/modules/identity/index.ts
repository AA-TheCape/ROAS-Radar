import { randomUUID } from 'node:crypto';

import type { PoolClient } from 'pg';

import { logInfo, logWarning } from '../../observability/index.js';
import {
  hashEmailAddress,
  hashPhoneNumber,
  normalizeEmailAddress,
  normalizePhoneNumber
} from '../../shared/privacy.js';
import { refreshCustomerJourneyForJourneys } from './customer-journey.js';

type DbClient = PoolClient;

type IdentityRecord = {
  id: string;
  hashed_email: string | null;
  shopify_customer_id: string | null;
};

type IdentityStitchInput = {
  shopifyOrderId: string;
  shopifyCustomerId: string | null;
  email: string | null;
  phoneHash?: string | null;
  landingSessionId: string | null;
  checkoutToken: string | null;
  cartToken: string | null;
  sourceTimestamp?: string | Date | null;
  evidenceSource?: string;
  sourceTable?: string | null;
  sourceRecordId?: string | null;
  idempotencyKey?: string | null;
};

type IdentityStitchDecision =
  | {
      outcome: 'skipped';
      reason: 'missing_identifiers';
      emailHash: string | null;
    }
  | {
      outcome: 'conflict';
      reason:
        | 'customer_id_conflicts_with_existing_email'
        | 'email_hash_conflicts_with_existing_customer_id'
        | 'identifiers_resolve_to_different_identities';
      emailHash: string | null;
    }
  | {
      outcome: 'linked';
      identityId: string | null;
      emailHash: string | null;
      shopifyCustomerId: string | null;
      operation: 'create' | 'reuse';
    };

export type IdentityStitchResult = {
  outcome: IdentityStitchDecision['outcome'];
  reason:
    | 'missing_identifiers'
    | 'customer_id_conflicts_with_existing_email'
    | 'email_hash_conflicts_with_existing_customer_id'
    | 'identifiers_resolve_to_different_identities'
    | 'create_identity'
    | 'reuse_identity'
    | 'authoritative_shopify_customer_conflict';
  identityId: string | null;
  emailHash: string | null;
  linkedSessionIds: string[];
};

type IdentityNodeType =
  | 'session_id'
  | 'checkout_token'
  | 'cart_token'
  | 'shopify_customer_id'
  | 'hashed_email'
  | 'phone_hash';

type IdentityNodeInput = {
  nodeType: IdentityNodeType;
  nodeKey: string;
};

type IdentityNodeRow = {
  id: string;
  node_type: IdentityNodeType;
  node_key: string;
  is_authoritative: boolean;
  is_ambiguous: boolean;
};

type IdentityJourneyCandidateRow = {
  node_id: string;
  node_type: IdentityNodeType;
  journey_id: string | null;
  edge_id: string | null;
  edge_type: string | null;
  precedence_rank: number | null;
  authoritative_shopify_customer_id: string | null;
  last_observed_at: Date | null;
};

type IdentityJourneyScoreRow = {
  journey_id: string;
  max_precedence_rank: number;
  latest_observed_at: Date;
};

type IdentityJourneyRow = {
  id: string;
  authoritative_shopify_customer_id: string | null;
  primary_email_hash: string | null;
  primary_phone_hash: string | null;
};

type IdentityJourneyLookbackWindow = {
  lookbackWindowStartedAt: Date;
  lookbackWindowExpiresAt: Date;
  lastTouchEligibleAt: Date;
};

type RegisteredIngestion = {
  deduplicated: boolean;
  existingJourneyId: string | null;
  existingOutcome: string | null;
};

type IdentityEdgeIngestionRunStatus = 'started' | 'completed' | 'conflicted';

type IdentityIngestionInput = {
  sourceTimestamp: string | Date;
  evidenceSource: string;
  sourceTable: string | null;
  sourceRecordId: string | null;
  idempotencyKey: string;
  sessionId?: string | null;
  checkoutToken?: string | null;
  cartToken?: string | null;
  shopifyCustomerId?: string | null;
  email?: string | null;
  hashedEmail?: string | null;
  phone?: string | null;
  phoneHash?: string | null;
};

type IdentityGraphIngestionResult = {
  outcome: 'linked' | 'skipped' | 'conflict';
  reason: 'linked' | 'missing_identifiers' | 'authoritative_shopify_customer_conflict';
  journeyId: string | null;
  deduplicated: boolean;
  emailHash: string | null;
  linkedSessionIds: string[];
  metrics: {
    processedNodes: number;
    attachedNodes: number;
    rehomedNodes: number;
    quarantinedNodes: number;
    deduplicated: boolean;
    outcome: 'linked' | 'skipped' | 'conflict';
  };
};

type IdentityMergeReasonCode =
  | 'created_new_journey'
  | 'reused_existing_journey'
  | 'shopify_customer_id_authoritative_winner'
  | 'non_authoritative_precedence_winner';

const IDENTITY_PRECEDENCE: Record<IdentityNodeType, number> = {
  shopify_customer_id: 100,
  hashed_email: 70,
  phone_hash: 60,
  checkout_token: 40,
  cart_token: 30,
  session_id: 20
};

const ACTIVE_IDENTITY_STATUSES = new Set(['active', 'quarantined']);
const AUTHORITATIVE_CONFLICT = 'authoritative_shopify_customer_conflict';
const HISTORICAL_IDENTITY_LOOKBACK_DAYS = 30;
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export const normalizeIdentityEmail = normalizeEmailAddress;
export const hashIdentityEmail = hashEmailAddress;
export const hashIdentityPhone = hashPhoneNumber;
export const normalizeIdentityPhone = normalizePhoneNumber;

export function resolveIdentityStitch(
  existingIdentities: IdentityRecord[],
  input: Pick<IdentityStitchInput, 'shopifyCustomerId' | 'email'> & { emailHash?: string | null }
): IdentityStitchDecision {
  const shopifyCustomerId = normalizeNullableString(input.shopifyCustomerId);
  const emailHash = input.emailHash ?? hashIdentityEmail(input.email);

  if (!shopifyCustomerId && !emailHash) {
    return {
      outcome: 'skipped',
      reason: 'missing_identifiers',
      emailHash
    };
  }

  const identityByCustomerId = shopifyCustomerId
    ? existingIdentities.find((identity) => identity.shopify_customer_id === shopifyCustomerId) ?? null
    : null;
  const identityByEmailHash = emailHash
    ? existingIdentities.find((identity) => identity.hashed_email === emailHash) ?? null
    : null;

  if (identityByCustomerId && emailHash && identityByCustomerId.hashed_email && identityByCustomerId.hashed_email !== emailHash) {
    return {
      outcome: 'conflict',
      reason: 'customer_id_conflicts_with_existing_email',
      emailHash
    };
  }

  if (identityByEmailHash && shopifyCustomerId && identityByEmailHash.shopify_customer_id && identityByEmailHash.shopify_customer_id !== shopifyCustomerId) {
    return {
      outcome: 'conflict',
      reason: 'email_hash_conflicts_with_existing_customer_id',
      emailHash
    };
  }

  if (identityByCustomerId && identityByEmailHash && identityByCustomerId.id !== identityByEmailHash.id) {
    return {
      outcome: 'conflict',
      reason: 'identifiers_resolve_to_different_identities',
      emailHash
    };
  }

  const reusedIdentity = identityByCustomerId ?? identityByEmailHash;

  if (reusedIdentity) {
    return {
      outcome: 'linked',
      identityId: reusedIdentity.id,
      emailHash,
      shopifyCustomerId,
      operation: 'reuse'
    };
  }

  return {
    outcome: 'linked',
    identityId: null,
    emailHash,
    shopifyCustomerId,
    operation: 'create'
  };
}

export function buildIdentityEdgeIngestionMetricsLog(result: {
  sourceTable: string | null;
  evidenceSource: string;
  outcome: 'linked' | 'skipped' | 'conflict';
  deduplicated: boolean;
  processedNodes: number;
  attachedNodes: number;
  rehomedNodes: number;
  quarantinedNodes: number;
  journeyId: string | null;
}): string {
  return JSON.stringify({
    severity: 'INFO',
    event: 'identity_edge_ingestion_processed',
    message: 'identity_edge_ingestion_processed',
    timestamp: new Date().toISOString(),
    service: process.env.K_SERVICE ?? 'roas-radar-api',
    ...result
  });
}

export async function stitchKnownCustomerIdentity(
  client: DbClient,
  input: IdentityStitchInput
): Promise<IdentityStitchResult> {
  const emailHash = input.email ? hashIdentityEmail(input.email) : null;
  const graphResult = await ingestIdentityEdges(client, {
    sourceTimestamp: input.sourceTimestamp ?? new Date(),
    evidenceSource: input.evidenceSource ?? 'shopify_order_webhook',
    sourceTable: input.sourceTable ?? 'shopify_orders',
    sourceRecordId: input.sourceRecordId ?? input.shopifyOrderId,
    idempotencyKey:
      input.idempotencyKey ??
      [
        'shopify_order_identity',
        input.shopifyOrderId,
        normalizeNullableString(input.shopifyCustomerId) ?? 'anonymous',
        emailHash ?? 'no_email',
        normalizeNullableString(input.checkoutToken) ?? 'no_checkout',
        normalizeNullableString(input.cartToken) ?? 'no_cart',
        normalizeSessionIdentifier(input.landingSessionId) ?? 'no_session',
        input.phoneHash ?? 'no_phone'
      ].join(':'),
    sessionId: input.landingSessionId,
    checkoutToken: input.checkoutToken,
    cartToken: input.cartToken,
    shopifyCustomerId: input.shopifyCustomerId,
    email: input.email,
    phoneHash: input.phoneHash ?? null
  });

  return {
    outcome: graphResult.outcome,
    reason:
      graphResult.reason === 'linked'
        ? graphResult.deduplicated
          ? 'reuse_identity'
          : 'create_identity'
        : graphResult.reason,
    identityId: graphResult.journeyId,
    emailHash: graphResult.emailHash,
    linkedSessionIds: graphResult.linkedSessionIds
  };
}

export async function ingestIdentityEdges(
  client: DbClient,
  input: IdentityIngestionInput
): Promise<IdentityGraphIngestionResult> {
  const sourceTimestamp = normalizeSourceTimestamp(input.sourceTimestamp);
  const normalizedNodes = buildNormalizedIdentityNodes(input);

  if (normalizedNodes.length === 0) {
    const metrics = {
      processedNodes: 0,
      attachedNodes: 0,
      rehomedNodes: 0,
      quarantinedNodes: 0,
      deduplicated: false,
      outcome: 'skipped' as const
    };

    emitIdentityIngestionMetrics({
      sourceTable: input.sourceTable,
      evidenceSource: input.evidenceSource,
      outcome: 'skipped',
      deduplicated: false,
      processedNodes: 0,
      attachedNodes: 0,
      rehomedNodes: 0,
      quarantinedNodes: 0,
      journeyId: null
    });

    return {
      outcome: 'skipped',
      reason: 'missing_identifiers',
      journeyId: null,
      deduplicated: false,
      emailHash: normalizedNodes.find((node) => node.nodeType === 'hashed_email')?.nodeKey ?? null,
      linkedSessionIds: [],
      metrics
    };
  }

  const registeredIngestion = await registerIdentityEdgeIngestion(client, {
    idempotencyKey: input.idempotencyKey,
    evidenceSource: input.evidenceSource,
    sourceTable: input.sourceTable,
    sourceRecordId: input.sourceRecordId,
    sourceTimestamp
  });

  if (registeredIngestion.deduplicated) {
    const linkedSessionIds = registeredIngestion.existingJourneyId
      ? await collectJourneySessionIds(client, registeredIngestion.existingJourneyId)
      : [];
    const metrics = {
      processedNodes: normalizedNodes.length,
      attachedNodes: 0,
      rehomedNodes: 0,
      quarantinedNodes: 0,
      deduplicated: true,
      outcome: (registeredIngestion.existingOutcome === 'conflict' ? 'conflict' : 'linked') as 'linked' | 'conflict'
    };

    emitIdentityIngestionMetrics({
      sourceTable: input.sourceTable,
      evidenceSource: input.evidenceSource,
      outcome: metrics.outcome,
      deduplicated: true,
      processedNodes: normalizedNodes.length,
      attachedNodes: 0,
      rehomedNodes: 0,
      quarantinedNodes: 0,
      journeyId: registeredIngestion.existingJourneyId
    });

    return {
      outcome: metrics.outcome,
      reason: metrics.outcome === 'conflict' ? AUTHORITATIVE_CONFLICT : 'linked',
      journeyId: registeredIngestion.existingJourneyId,
      deduplicated: true,
      emailHash: normalizedNodes.find((node) => node.nodeType === 'hashed_email')?.nodeKey ?? null,
      linkedSessionIds,
      metrics
    };
  }

  const nodeRows = await upsertAndLockIdentityNodes(client, normalizedNodes, sourceTimestamp);
  const qualifyingIdentityObservedAt = hasQualifyingIdentityNodes(normalizedNodes) ? sourceTimestamp : null;
  const candidateRows = await loadActiveJourneyCandidates(client, nodeRows.map((row) => row.id));
  const journeyScores = await loadJourneyScores(
    client,
    [...new Set(candidateRows.map((row) => row.journey_id).filter((value): value is string => Boolean(value)))]
  );

  const quarantinedNodeIds = new Set<string>();
  const candidateRowsForResolution = candidateRows.filter((row) => {
    const node = nodeRows.find((nodeRow) => nodeRow.id === row.node_id);
    return row.journey_id && !node?.is_ambiguous;
  });

  const journeyResolution = await resolveWinningJourney(client, {
    nodeRows,
    candidateRows: candidateRowsForResolution,
    journeyScores,
    incomingShopifyCustomerId:
      normalizedNodes.find((node) => node.nodeType === 'shopify_customer_id')?.nodeKey ?? null,
    evidenceSource: input.evidenceSource,
    sourceTable: input.sourceTable,
    sourceRecordId: input.sourceRecordId,
    sourceTimestamp,
    quarantinedNodeIds
  });

  if (journeyResolution.outcome === 'conflict') {
    await completeIdentityEdgeIngestion(client, {
      idempotencyKey: input.idempotencyKey,
      status: 'conflicted',
      journeyId: null,
      outcomeReason: AUTHORITATIVE_CONFLICT,
      metrics: {
        processedNodes: normalizedNodes.length,
        attachedNodes: 0,
        rehomedNodes: 0,
        quarantinedNodes: quarantinedNodeIds.size
      }
    });

    emitIdentityIngestionMetrics({
      sourceTable: input.sourceTable,
      evidenceSource: input.evidenceSource,
      outcome: 'conflict',
      deduplicated: false,
      processedNodes: normalizedNodes.length,
      attachedNodes: 0,
      rehomedNodes: 0,
      quarantinedNodes: quarantinedNodeIds.size,
      journeyId: null
    });

    return {
      outcome: 'conflict',
      reason: AUTHORITATIVE_CONFLICT,
      journeyId: null,
      deduplicated: false,
      emailHash: normalizedNodes.find((node) => node.nodeType === 'hashed_email')?.nodeKey ?? null,
      linkedSessionIds: [],
      metrics: {
        processedNodes: normalizedNodes.length,
        attachedNodes: 0,
        rehomedNodes: 0,
        quarantinedNodes: quarantinedNodeIds.size,
        deduplicated: false,
        outcome: 'conflict'
      }
    };
  }

  const winnerJourneyId = journeyResolution.journeyId;
  let attachedNodes = 0;
  let rehomedNodes = 0;

  for (const node of nodeRows) {
    if (quarantinedNodeIds.has(node.id) || node.is_ambiguous) {
      continue;
    }

    const activeCandidate = candidateRows.find((row) => row.node_id === node.id && row.edge_id);
    const desiredEdgeType = node.node_type === 'shopify_customer_id' ? 'authoritative' : activeCandidate?.journey_id && activeCandidate.journey_id !== winnerJourneyId ? 'promoted' : 'deterministic';

    if (!activeCandidate?.edge_id) {
      await insertIdentityEdge(client, {
        nodeId: node.id,
        journeyId: winnerJourneyId,
        edgeType: desiredEdgeType,
        evidenceSource: input.evidenceSource,
        sourceTable: input.sourceTable,
        sourceRecordId: input.sourceRecordId,
        sourceTimestamp,
        conflictCode: null
      });
      attachedNodes += 1;
      continue;
    }

    if (activeCandidate.journey_id === winnerJourneyId) {
      await touchActiveIdentityEdge(client, activeCandidate.edge_id, sourceTimestamp);
      continue;
    }

    const newEdgeId = randomUUID();
    await deactivateIdentityEdge(client, activeCandidate.edge_id);
    await insertIdentityEdge(client, {
      id: newEdgeId,
      nodeId: node.id,
      journeyId: winnerJourneyId,
      edgeType: 'promoted',
      evidenceSource: input.evidenceSource,
      sourceTable: input.sourceTable,
      sourceRecordId: input.sourceRecordId,
      sourceTimestamp,
      conflictCode: null
    });
    await linkSupersededIdentityEdge(client, activeCandidate.edge_id, newEdgeId);
    rehomedNodes += 1;
  }

  await refreshJourneyConvenienceFields(client, winnerJourneyId, sourceTimestamp, {
    authoritativeShopifyCustomerId: normalizedNodes.find((node) => node.nodeType === 'shopify_customer_id')?.nodeKey ?? null,
    emailHash: normalizedNodes.find((node) => node.nodeType === 'hashed_email')?.nodeKey ?? null,
    phoneHash: normalizedNodes.find((node) => node.nodeType === 'phone_hash')?.nodeKey ?? null,
    graphChanged: journeyResolution.created || attachedNodes > 0 || rehomedNodes > 0 || quarantinedNodeIds.size > 0,
    qualifyingIdentityObservedAt
  });

  for (const row of candidateRows) {
    if (!row.journey_id || row.journey_id === winnerJourneyId) {
      continue;
    }

    await markJourneyMergedIfEmpty(client, row.journey_id, winnerJourneyId, sourceTimestamp);
  }

  await upsertCompatibilityCustomerIdentity(client, winnerJourneyId, {
    emailHash: normalizedNodes.find((node) => node.nodeType === 'hashed_email')?.nodeKey ?? null,
    shopifyCustomerId: normalizedNodes.find((node) => node.nodeType === 'shopify_customer_id')?.nodeKey ?? null,
    sourceTimestamp
  });

  const linkedSessionIds = await syncIdentityReferences(client, {
    journeyId: winnerJourneyId,
    sessionId: normalizedNodes.find((node) => node.nodeType === 'session_id')?.nodeKey ?? null,
    checkoutToken: normalizedNodes.find((node) => node.nodeType === 'checkout_token')?.nodeKey ?? null,
    cartToken: normalizedNodes.find((node) => node.nodeType === 'cart_token')?.nodeKey ?? null,
    shopifyCustomerId: normalizedNodes.find((node) => node.nodeType === 'shopify_customer_id')?.nodeKey ?? null,
    emailHash: normalizedNodes.find((node) => node.nodeType === 'hashed_email')?.nodeKey ?? null,
    phoneHash: normalizedNodes.find((node) => node.nodeType === 'phone_hash')?.nodeKey ?? null,
    qualifyingIdentityObservedAt
  });

  await refreshCustomerJourneyForJourneys(client, [
    winnerJourneyId,
    ...candidateRows.map((row) => row.journey_id).filter((journeyId): journeyId is string => Boolean(journeyId))
  ]);

  await completeIdentityEdgeIngestion(client, {
    idempotencyKey: input.idempotencyKey,
    status: 'completed',
    journeyId: winnerJourneyId,
    outcomeReason: journeyResolution.reasonCode,
    metrics: {
      processedNodes: normalizedNodes.length,
      attachedNodes,
      rehomedNodes,
      quarantinedNodes: quarantinedNodeIds.size
    }
  });

  emitIdentityIngestionMetrics({
    sourceTable: input.sourceTable,
    evidenceSource: input.evidenceSource,
    outcome: 'linked',
    deduplicated: false,
    processedNodes: normalizedNodes.length,
    attachedNodes,
    rehomedNodes,
    quarantinedNodes: quarantinedNodeIds.size,
    journeyId: winnerJourneyId
  });

  return {
    outcome: 'linked',
    reason: 'linked',
    journeyId: winnerJourneyId,
    deduplicated: false,
    emailHash: normalizedNodes.find((node) => node.nodeType === 'hashed_email')?.nodeKey ?? null,
    linkedSessionIds,
    metrics: {
      processedNodes: normalizedNodes.length,
      attachedNodes,
      rehomedNodes,
      quarantinedNodes: quarantinedNodeIds.size,
      deduplicated: false,
      outcome: 'linked'
    }
  };
}

async function findExistingIdentities(
  client: DbClient,
  shopifyCustomerId: string | null,
  emailHash: string | null
): Promise<IdentityRecord[]> {
  const result = await client.query<IdentityRecord>(
    `
      SELECT
        id,
        hashed_email,
        shopify_customer_id
      FROM customer_identities
      WHERE ($1::text IS NOT NULL AND shopify_customer_id = $1)
         OR ($2::text IS NOT NULL AND hashed_email = $2)
      ORDER BY created_at ASC
    `,
    [normalizeNullableString(shopifyCustomerId), emailHash]
  );

  return result.rows;
}

async function registerIdentityEdgeIngestion(
  client: DbClient,
  input: {
    idempotencyKey: string;
    evidenceSource: string;
    sourceTable: string | null;
    sourceRecordId: string | null;
    sourceTimestamp: Date;
  }
): Promise<RegisteredIngestion> {
  const insertResult = await client.query(
    `
      INSERT INTO identity_edge_ingestion_runs (
        idempotency_key,
        evidence_source,
        source_table,
        source_record_id,
        source_timestamp,
        status,
        created_at,
        updated_at
      )
      VALUES ($1, $2, $3, $4, $5, 'started', now(), now())
      ON CONFLICT (idempotency_key) DO NOTHING
      RETURNING id
    `,
    [input.idempotencyKey, input.evidenceSource, input.sourceTable, input.sourceRecordId, input.sourceTimestamp]
  );

  if (insertResult.rowCount) {
    return {
      deduplicated: false,
      existingJourneyId: null,
      existingOutcome: null
    };
  }

  const existingResult = await client.query<{
    journey_id: string | null;
    status: IdentityEdgeIngestionRunStatus;
    outcome_reason: string | null;
  }>(
    `
      SELECT
        journey_id::text AS journey_id,
        status,
        outcome_reason
      FROM identity_edge_ingestion_runs
      WHERE idempotency_key = $1
      FOR UPDATE
      LIMIT 1
    `,
    [input.idempotencyKey]
  );

  const existingRun = existingResult.rows[0] ?? null;

  if (!existingRun) {
    throw new Error(`identity edge ingestion run disappeared for idempotency key ${input.idempotencyKey}`);
  }

  if (existingRun.status === 'completed' || existingRun.status === 'conflicted') {
    return {
      deduplicated: true,
      existingJourneyId: existingRun.journey_id,
      existingOutcome: existingRun.outcome_reason
    };
  }

  await client.query(
    `
      UPDATE identity_edge_ingestion_runs
      SET
        evidence_source = $2,
        source_table = $3,
        source_record_id = $4,
        source_timestamp = $5,
        status = 'started',
        journey_id = NULL,
        outcome_reason = NULL,
        processed_nodes = 0,
        attached_nodes = 0,
        rehomed_nodes = 0,
        quarantined_nodes = 0,
        processed_at = NULL,
        updated_at = now()
      WHERE idempotency_key = $1
    `,
    [input.idempotencyKey, input.evidenceSource, input.sourceTable, input.sourceRecordId, input.sourceTimestamp]
  );

  return {
    deduplicated: false,
    existingJourneyId: null,
    existingOutcome: null
  };
}

async function completeIdentityEdgeIngestion(
  client: DbClient,
  input: {
    idempotencyKey: string;
    status: 'completed' | 'conflicted';
    journeyId: string | null;
    outcomeReason: string;
    metrics: {
      processedNodes: number;
      attachedNodes: number;
      rehomedNodes: number;
      quarantinedNodes: number;
    };
  }
): Promise<void> {
  await client.query(
    `
      UPDATE identity_edge_ingestion_runs
      SET
        status = $2,
        journey_id = $3::uuid,
        outcome_reason = $4,
        processed_nodes = $5,
        attached_nodes = $6,
        rehomed_nodes = $7,
        quarantined_nodes = $8,
        processed_at = now(),
        updated_at = now()
      WHERE idempotency_key = $1
    `,
    [
      input.idempotencyKey,
      input.status,
      input.journeyId,
      input.outcomeReason,
      input.metrics.processedNodes,
      input.metrics.attachedNodes,
      input.metrics.rehomedNodes,
      input.metrics.quarantinedNodes
    ]
  );
}

function buildNormalizedIdentityNodes(input: IdentityIngestionInput): IdentityNodeInput[] {
  const sessionId = normalizeSessionIdentifier(input.sessionId);
  const checkoutToken = normalizeToken(input.checkoutToken);
  const cartToken = normalizeToken(input.cartToken);
  const shopifyCustomerId = normalizeNullableString(input.shopifyCustomerId);
  const hashedEmail = input.hashedEmail ?? hashIdentityEmail(input.email);
  const phoneHash = input.phoneHash ?? hashIdentityPhone(input.phone);

  const candidates: IdentityNodeInput[] = [];

  if (sessionId) {
    candidates.push({ nodeType: 'session_id', nodeKey: sessionId });
  }

  if (checkoutToken) {
    candidates.push({ nodeType: 'checkout_token', nodeKey: checkoutToken });
  }

  if (cartToken) {
    candidates.push({ nodeType: 'cart_token', nodeKey: cartToken });
  }

  if (shopifyCustomerId) {
    candidates.push({ nodeType: 'shopify_customer_id', nodeKey: shopifyCustomerId });
  }

  if (hashedEmail) {
    candidates.push({ nodeType: 'hashed_email', nodeKey: hashedEmail });
  }

  if (phoneHash) {
    candidates.push({ nodeType: 'phone_hash', nodeKey: phoneHash });
  }

  return dedupeIdentityNodes(candidates);
}

function dedupeIdentityNodes(nodes: IdentityNodeInput[]): IdentityNodeInput[] {
  const seen = new Set<string>();
  const deduped: IdentityNodeInput[] = [];

  for (const node of nodes) {
    const key = `${node.nodeType}:${node.nodeKey}`;
    if (seen.has(key)) {
      continue;
    }

    seen.add(key);
    deduped.push(node);
  }

  return deduped;
}

async function upsertAndLockIdentityNodes(
  client: DbClient,
  nodes: IdentityNodeInput[],
  sourceTimestamp: Date
): Promise<IdentityNodeRow[]> {
  for (const node of nodes) {
    await client.query(
      `
        INSERT INTO identity_nodes (
          node_type,
          node_key,
          is_authoritative,
          is_ambiguous,
          first_seen_at,
          last_seen_at,
          created_at,
          updated_at
        )
        VALUES (
          $1,
          $2,
          $3,
          false,
          $4,
          $4,
          now(),
          now()
        )
        ON CONFLICT (node_type, node_key)
        DO UPDATE SET
          first_seen_at = LEAST(identity_nodes.first_seen_at, EXCLUDED.first_seen_at),
          last_seen_at = GREATEST(identity_nodes.last_seen_at, EXCLUDED.last_seen_at),
          updated_at = now()
      `,
      [node.nodeType, node.nodeKey, node.nodeType === 'shopify_customer_id', sourceTimestamp]
    );
  }

  const result = await client.query<IdentityNodeRow>(
    `
      SELECT
        id::text AS id,
        node_type,
        node_key,
        is_authoritative,
        is_ambiguous
      FROM identity_nodes
      WHERE (node_type, node_key) IN (${nodes.map((_, index) => `($${index * 2 + 1}, $${index * 2 + 2})`).join(', ')})
      ORDER BY node_type ASC, node_key ASC
      FOR UPDATE
    `,
    nodes.flatMap((node) => [node.nodeType, node.nodeKey])
  );

  return result.rows;
}

async function loadActiveJourneyCandidates(client: DbClient, nodeIds: string[]): Promise<IdentityJourneyCandidateRow[]> {
  if (nodeIds.length === 0) {
    return [];
  }

  const result = await client.query<IdentityJourneyCandidateRow>(
    `
      SELECT
        n.id::text AS node_id,
        n.node_type,
        e.journey_id::text AS journey_id,
        e.id::text AS edge_id,
        e.edge_type,
        e.precedence_rank,
        j.authoritative_shopify_customer_id,
        e.last_observed_at
      FROM identity_nodes n
      LEFT JOIN identity_edges e
        ON e.node_id = n.id
       AND e.is_active = true
      LEFT JOIN identity_journeys j
        ON j.id = e.journey_id
      WHERE n.id = ANY($1::uuid[])
    `,
    [nodeIds]
  );

  return result.rows.filter((row) => row.journey_id === null || ACTIVE_IDENTITY_STATUSES.has(row.edge_type === 'quarantined' ? 'quarantined' : 'active'));
}

async function loadJourneyScores(client: DbClient, journeyIds: string[]): Promise<Map<string, IdentityJourneyScoreRow>> {
  if (journeyIds.length === 0) {
    return new Map();
  }

  const result = await client.query<IdentityJourneyScoreRow>(
    `
      SELECT
        e.journey_id::text AS journey_id,
        MAX(e.precedence_rank)::int AS max_precedence_rank,
        MAX(e.last_observed_at) AS latest_observed_at
      FROM identity_edges e
      WHERE e.is_active = true
        AND e.journey_id = ANY($1::uuid[])
      GROUP BY e.journey_id
    `,
    [journeyIds]
  );

  return new Map(result.rows.map((row) => [row.journey_id, row]));
}

async function resolveWinningJourney(
  client: DbClient,
  input: {
    nodeRows: IdentityNodeRow[];
    candidateRows: IdentityJourneyCandidateRow[];
    journeyScores: Map<string, IdentityJourneyScoreRow>;
    incomingShopifyCustomerId: string | null;
    evidenceSource: string;
    sourceTable: string | null;
    sourceRecordId: string | null;
    sourceTimestamp: Date;
    quarantinedNodeIds: Set<string>;
  }
): Promise<{ outcome: 'linked'; journeyId: string; created: boolean; reasonCode: IdentityMergeReasonCode } | { outcome: 'conflict' }> {
  const candidateJourneyIds = [...new Set(input.candidateRows.map((row) => row.journey_id).filter((value): value is string => Boolean(value)))];

  if (candidateJourneyIds.length === 0) {
    const journeyId = await createJourney(client, {
      authoritativeShopifyCustomerId: input.incomingShopifyCustomerId,
      emailHash: input.nodeRows.find((row) => row.node_type === 'hashed_email')?.node_key ?? null,
      phoneHash: input.nodeRows.find((row) => row.node_type === 'phone_hash')?.node_key ?? null,
      sourceTimestamp: input.sourceTimestamp,
      lookbackAnchorTimestamp: input.sourceTimestamp,
      evidenceSource: input.evidenceSource,
      sourceTable: input.sourceTable,
      sourceRecordId: input.sourceRecordId
    });

    return {
      outcome: 'linked',
      journeyId,
      created: true,
      reasonCode: 'created_new_journey'
    };
  }

  let workingCandidates = input.candidateRows.slice();
  let authoritativeIds = distinctAuthoritativeShopifyIds(workingCandidates);
  const incomingAuthoritativeJourney = input.incomingShopifyCustomerId
    ? workingCandidates.find((row) => row.authoritative_shopify_customer_id === input.incomingShopifyCustomerId)?.journey_id ?? null
    : null;

  if (authoritativeIds.length > 1 && incomingAuthoritativeJourney) {
    for (const row of workingCandidates) {
      if (!row.edge_id || row.journey_id === incomingAuthoritativeJourney) {
        continue;
      }

      if (row.authoritative_shopify_customer_id === null || row.authoritative_shopify_customer_id === input.incomingShopifyCustomerId) {
        continue;
      }

      if (row.node_type !== 'hashed_email' && row.node_type !== 'phone_hash') {
        continue;
      }

      const conflictCode =
        row.node_type === 'phone_hash'
          ? 'phone_hash_conflicts_across_authoritative_customers'
          : 'hashed_email_conflicts_across_authoritative_customers';

      await quarantineIdentityNode(client, {
        nodeId: row.node_id,
        currentEdgeId: row.edge_id,
        currentJourneyId: row.journey_id as string,
        sourceTimestamp: input.sourceTimestamp,
        evidenceSource: input.evidenceSource,
        sourceTable: input.sourceTable,
        sourceRecordId: input.sourceRecordId,
        conflictCode
      });
      input.quarantinedNodeIds.add(row.node_id);
    }

    workingCandidates = workingCandidates.filter((row) => !input.quarantinedNodeIds.has(row.node_id));
    authoritativeIds = distinctAuthoritativeShopifyIds(workingCandidates);
  }

  if (authoritativeIds.length > 1) {
    logWarning('identity_edge_ingestion_conflict', {
      reason: AUTHORITATIVE_CONFLICT,
      authoritativeShopifyCustomerIds: authoritativeIds
    });
    return {
      outcome: 'conflict'
    };
  }

  if (incomingAuthoritativeJourney) {
    return {
      outcome: 'linked',
      journeyId: incomingAuthoritativeJourney,
      created: false,
      reasonCode: 'shopify_customer_id_authoritative_winner'
    };
  }

  if (candidateJourneyIds.length === 1) {
    return {
      outcome: 'linked',
      journeyId: candidateJourneyIds[0],
      created: false,
      reasonCode: 'reused_existing_journey'
    };
  }

  const selectedJourneyId = selectBestJourneyId(workingCandidates, input.journeyScores);

  if (selectedJourneyId) {
    return {
      outcome: 'linked',
      journeyId: selectedJourneyId,
      created: false,
      reasonCode: input.incomingShopifyCustomerId
        ? 'shopify_customer_id_authoritative_winner'
        : 'non_authoritative_precedence_winner'
    };
  }

  const fallbackJourneyId = await createJourney(client, {
    authoritativeShopifyCustomerId: input.incomingShopifyCustomerId,
    emailHash: input.nodeRows.find((row) => row.node_type === 'hashed_email')?.node_key ?? null,
    phoneHash: input.nodeRows.find((row) => row.node_type === 'phone_hash')?.node_key ?? null,
    sourceTimestamp: input.sourceTimestamp,
    lookbackAnchorTimestamp: input.sourceTimestamp,
    evidenceSource: input.evidenceSource,
    sourceTable: input.sourceTable,
    sourceRecordId: input.sourceRecordId
  });

  return {
    outcome: 'linked',
    journeyId: fallbackJourneyId,
    created: true,
    reasonCode: 'created_new_journey'
  };
}

function distinctAuthoritativeShopifyIds(rows: IdentityJourneyCandidateRow[]): string[] {
  return [...new Set(rows.map((row) => row.authoritative_shopify_customer_id).filter((value): value is string => Boolean(value)))];
}

function hasQualifyingIdentityNodes(nodes: IdentityNodeInput[]): boolean {
  return nodes.some((node) => isQualifyingIdentityNodeType(node.nodeType));
}

function isQualifyingIdentityNodeType(nodeType: IdentityNodeType): boolean {
  return nodeType === 'shopify_customer_id' || nodeType === 'hashed_email' || nodeType === 'phone_hash';
}

function selectBestJourneyId(
  rows: IdentityJourneyCandidateRow[],
  journeyScores: Map<string, IdentityJourneyScoreRow>
): string | null {
  const candidates = [...new Set(rows.map((row) => row.journey_id).filter((value): value is string => Boolean(value)))];

  if (candidates.length === 0) {
    return null;
  }

  return candidates.sort((left, right) => {
    const leftScore = journeyScores.get(left);
    const rightScore = journeyScores.get(right);
    const leftRank = leftScore?.max_precedence_rank ?? 0;
    const rightRank = rightScore?.max_precedence_rank ?? 0;

    if (leftRank !== rightRank) {
      return rightRank - leftRank;
    }

    const leftObserved = leftScore?.latest_observed_at?.getTime() ?? 0;
    const rightObserved = rightScore?.latest_observed_at?.getTime() ?? 0;

    if (leftObserved !== rightObserved) {
      return rightObserved - leftObserved;
    }

    return left.localeCompare(right);
  })[0];
}

async function createJourney(
  client: DbClient,
  input: {
    authoritativeShopifyCustomerId: string | null;
    emailHash: string | null;
    phoneHash: string | null;
    sourceTimestamp: Date;
    lookbackAnchorTimestamp: Date;
    evidenceSource: string;
    sourceTable: string | null;
    sourceRecordId: string | null;
  }
): Promise<string> {
  const journeyId = randomUUID();
  const lookbackWindow = buildHistoricalLookbackWindow(input.lookbackAnchorTimestamp);
  await client.query(
    `
      INSERT INTO identity_journeys (
        id,
        authoritative_shopify_customer_id,
        primary_email_hash,
        primary_phone_hash,
        status,
        merge_version,
        lookback_window_started_at,
        lookback_window_expires_at,
        last_touch_eligible_at,
        first_source_system,
        first_source_table,
        first_source_record_id,
        last_source_system,
        last_source_table,
        last_source_record_id,
        created_at,
        updated_at,
        last_resolved_at
      )
      VALUES (
        $1::uuid,
        $2,
        $3,
        $4,
        'active',
        1,
        $5::timestamptz,
        $6::timestamptz,
        $7::timestamptz,
        $8,
        $9,
        $10,
        $8,
        $9,
        $10,
        now(),
        now(),
        $11::timestamptz
      )
    `,
    [
      journeyId,
      input.authoritativeShopifyCustomerId,
      input.emailHash,
      input.phoneHash,
      lookbackWindow.lookbackWindowStartedAt,
      lookbackWindow.lookbackWindowExpiresAt,
      lookbackWindow.lastTouchEligibleAt,
      input.evidenceSource,
      input.sourceTable,
      input.sourceRecordId,
      input.sourceTimestamp
    ]
  );

  return journeyId;
}

async function quarantineIdentityNode(
  client: DbClient,
  input: {
    nodeId: string;
    currentEdgeId: string;
    currentJourneyId: string;
    sourceTimestamp: Date;
    evidenceSource: string;
    sourceTable: string | null;
    sourceRecordId: string | null;
    conflictCode: string;
  }
): Promise<void> {
  const newEdgeId = randomUUID();
  await client.query(
    `
      UPDATE identity_nodes
      SET
        is_ambiguous = true,
        updated_at = now()
      WHERE id = $1::uuid
    `,
    [input.nodeId]
  );
  await deactivateIdentityEdge(client, input.currentEdgeId);
  await insertIdentityEdge(client, {
    id: newEdgeId,
    nodeId: input.nodeId,
    journeyId: input.currentJourneyId,
    edgeType: 'quarantined',
    evidenceSource: input.evidenceSource,
    sourceTable: input.sourceTable,
    sourceRecordId: input.sourceRecordId,
    sourceTimestamp: input.sourceTimestamp,
    conflictCode: input.conflictCode
  });
  await linkSupersededIdentityEdge(client, input.currentEdgeId, newEdgeId);
}

async function insertIdentityEdge(
  client: DbClient,
  input: {
    id?: string;
    nodeId: string;
    journeyId: string;
    edgeType: 'authoritative' | 'deterministic' | 'promoted' | 'quarantined';
    evidenceSource: string;
    sourceTable: string | null;
    sourceRecordId: string | null;
    sourceTimestamp: Date;
    conflictCode: string | null;
  }
): Promise<void> {
  const nodeTypeResult = await client.query<{ node_type: IdentityNodeType }>(
    `
      SELECT node_type
      FROM identity_nodes
      WHERE id = $1::uuid
    `,
    [input.nodeId]
  );

  const nodeType = nodeTypeResult.rows[0]?.node_type;
  if (!nodeType) {
    throw new Error(`Identity node ${input.nodeId} was not found while inserting edge`);
  }

  await client.query(
    `
      INSERT INTO identity_edges (
        id,
        node_id,
        journey_id,
        edge_type,
        precedence_rank,
        evidence_source,
        source_table,
        source_record_id,
        is_active,
        conflict_code,
        first_observed_at,
        last_observed_at,
        created_at,
        updated_at
      )
      VALUES (
        $1::uuid,
        $2::uuid,
        $3::uuid,
        $4,
        $5,
        $6,
        $7,
        $8,
        true,
        $9,
        $10,
        $10,
        now(),
        now()
      )
    `,
    [
      input.id ?? randomUUID(),
      input.nodeId,
      input.journeyId,
      input.edgeType,
      IDENTITY_PRECEDENCE[nodeType],
      input.evidenceSource,
      input.sourceTable,
      input.sourceRecordId,
      input.conflictCode,
      input.sourceTimestamp
    ]
  );
}

async function touchActiveIdentityEdge(client: DbClient, edgeId: string, sourceTimestamp: Date): Promise<void> {
  await client.query(
    `
      UPDATE identity_edges
      SET
        first_observed_at = LEAST(first_observed_at, $2),
        last_observed_at = GREATEST(last_observed_at, $2),
        updated_at = now()
      WHERE id = $1::uuid
    `,
    [edgeId, sourceTimestamp]
  );
}

async function deactivateIdentityEdge(client: DbClient, edgeId: string): Promise<void> {
  await client.query(
    `
      UPDATE identity_edges
      SET
        is_active = false,
        updated_at = now()
      WHERE id = $1::uuid
    `,
    [edgeId]
  );
}

async function linkSupersededIdentityEdge(client: DbClient, previousEdgeId: string, supersedingEdgeId: string): Promise<void> {
  await client.query(
    `
      UPDATE identity_edges
      SET
        superseded_by_edge_id = $2::uuid,
        updated_at = now()
      WHERE id = $1::uuid
    `,
    [previousEdgeId, supersedingEdgeId]
  );
}

async function refreshJourneyConvenienceFields(
  client: DbClient,
  journeyId: string,
  sourceTimestamp: Date,
  input: {
    authoritativeShopifyCustomerId: string | null;
    emailHash: string | null;
    phoneHash: string | null;
    graphChanged: boolean;
    qualifyingIdentityObservedAt: Date | null;
  }
): Promise<void> {
  await client.query(
    `
      UPDATE identity_journeys
      SET
        authoritative_shopify_customer_id = COALESCE($2, authoritative_shopify_customer_id),
        primary_email_hash = COALESCE($3, primary_email_hash),
        primary_phone_hash = COALESCE($4, primary_phone_hash),
        merge_version = CASE WHEN $5 THEN merge_version + 1 ELSE merge_version END,
        lookback_window_started_at = CASE
          WHEN $6::timestamptz IS NULL THEN lookback_window_started_at
          ELSE GREATEST(last_touch_eligible_at, $6::timestamptz) - ($7::text || ' days')::interval
        END,
        lookback_window_expires_at = CASE
          WHEN $6::timestamptz IS NULL THEN lookback_window_expires_at
          ELSE GREATEST(last_touch_eligible_at, $6::timestamptz)
        END,
        last_touch_eligible_at = CASE
          WHEN $6::timestamptz IS NULL THEN last_touch_eligible_at
          ELSE GREATEST(last_touch_eligible_at, $6::timestamptz)
        END,
        updated_at = now(),
        last_resolved_at = GREATEST(last_resolved_at, $8)
      WHERE id = $1::uuid
    `,
    [
      journeyId,
      input.authoritativeShopifyCustomerId,
      input.emailHash,
      input.phoneHash,
      input.graphChanged,
      input.qualifyingIdentityObservedAt,
      String(HISTORICAL_IDENTITY_LOOKBACK_DAYS),
      sourceTimestamp
    ]
  );
}

async function markJourneyMergedIfEmpty(
  client: DbClient,
  journeyId: string,
  mergedIntoJourneyId: string,
  sourceTimestamp: Date
): Promise<void> {
  const result = await client.query<{ has_active_edges: boolean }>(
    `
      SELECT EXISTS (
        SELECT 1
        FROM identity_edges
        WHERE journey_id = $1::uuid
          AND is_active = true
      ) AS has_active_edges
    `,
    [journeyId]
  );

  if (result.rows[0]?.has_active_edges) {
    return;
  }

  await client.query(
    `
      UPDATE identity_journeys
      SET
        status = 'merged',
        merged_into_journey_id = $2::uuid,
        updated_at = now(),
        last_resolved_at = GREATEST(last_resolved_at, $3)
      WHERE id = $1::uuid
    `,
    [journeyId, mergedIntoJourneyId, sourceTimestamp]
  );
}

async function upsertCompatibilityCustomerIdentity(
  client: DbClient,
  journeyId: string,
  input: {
    emailHash: string | null;
    shopifyCustomerId: string | null;
    sourceTimestamp: Date;
  }
): Promise<void> {
  if (!input.emailHash && !input.shopifyCustomerId) {
    return;
  }

  const safelyAssignableFields = await resolveCompatibilityIdentityAssignments(client, journeyId, {
    emailHash: input.emailHash,
    shopifyCustomerId: input.shopifyCustomerId
  });

  const existingJourneyRow = await client.query<{ id: string }>(
    `
      SELECT id::text AS id
      FROM customer_identities
      WHERE id = $1::uuid
      LIMIT 1
    `,
    [journeyId]
  );

  if (existingJourneyRow.rowCount) {
    await client.query(
      `
        UPDATE customer_identities
        SET
          hashed_email = COALESCE(hashed_email, $2),
          shopify_customer_id = COALESCE(shopify_customer_id, $3),
          updated_at = now(),
          last_stitched_at = GREATEST(last_stitched_at, $4)
        WHERE id = $1::uuid
      `,
      [journeyId, safelyAssignableFields.emailHash, safelyAssignableFields.shopifyCustomerId, input.sourceTimestamp]
    );
    return;
  }

  const matchingIdentity = (
    await findExistingIdentities(client, safelyAssignableFields.shopifyCustomerId, safelyAssignableFields.emailHash)
  )[0] ?? null;
  if (matchingIdentity) {
    await client.query(
      `
        UPDATE customer_identities
        SET
          hashed_email = COALESCE(hashed_email, $2),
          shopify_customer_id = COALESCE(shopify_customer_id, $3),
          updated_at = now(),
          last_stitched_at = GREATEST(last_stitched_at, $4)
        WHERE id = $1::uuid
      `,
      [matchingIdentity.id, safelyAssignableFields.emailHash, safelyAssignableFields.shopifyCustomerId, input.sourceTimestamp]
    );
    return;
  }

  await client.query(
    `
      INSERT INTO customer_identities (
        id,
        hashed_email,
        shopify_customer_id,
        created_at,
        updated_at,
        last_stitched_at
      )
      VALUES ($1::uuid, $2, $3, now(), now(), $4)
    `,
    [journeyId, safelyAssignableFields.emailHash, safelyAssignableFields.shopifyCustomerId, input.sourceTimestamp]
  );
}

async function resolveCompatibilityIdentityAssignments(
  client: DbClient,
  journeyId: string,
  input: {
    emailHash: string | null;
    shopifyCustomerId: string | null;
  }
): Promise<{ emailHash: string | null; shopifyCustomerId: string | null }> {
  const emailOwnerPromise = input.emailHash
    ? client.query<{ id: string }>(
        `
          SELECT id::text AS id
          FROM customer_identities
          WHERE hashed_email = $1
          LIMIT 1
        `,
        [input.emailHash]
      )
    : Promise.resolve({ rows: [] } as { rows: Array<{ id: string }> });
  const customerOwnerPromise = input.shopifyCustomerId
    ? client.query<{ id: string }>(
        `
          SELECT id::text AS id
          FROM customer_identities
          WHERE shopify_customer_id = $1
          LIMIT 1
        `,
        [input.shopifyCustomerId]
      )
    : Promise.resolve({ rows: [] } as { rows: Array<{ id: string }> });

  const [emailOwnerResult, customerOwnerResult] = await Promise.all([emailOwnerPromise, customerOwnerPromise]);
  const emailOwnerId = emailOwnerResult.rows[0]?.id ?? null;
  const customerOwnerId = customerOwnerResult.rows[0]?.id ?? null;

  return {
    emailHash: emailOwnerId && emailOwnerId !== journeyId ? null : input.emailHash,
    shopifyCustomerId: customerOwnerId && customerOwnerId !== journeyId ? null : input.shopifyCustomerId
  };
}

async function syncIdentityReferences(
  client: DbClient,
  input: {
    journeyId: string;
    sessionId: string | null;
    checkoutToken: string | null;
    cartToken: string | null;
    shopifyCustomerId: string | null;
    emailHash: string | null;
    phoneHash?: string | null;
    qualifyingIdentityObservedAt: Date | null;
  }
): Promise<string[]> {
  const compatibilityExists = await customerIdentityExists(client, input.journeyId);
  const journeyId = input.journeyId;
  const lookbackWindow = input.qualifyingIdentityObservedAt
    ? await loadJourneyLookbackWindow(client, input.journeyId)
    : null;
  const safeOrderRewriteContactHashes = await resolveSafeOrderRewriteContactHashes(client, input.journeyId, {
    emailHash: input.emailHash,
    phoneHash: input.phoneHash ?? null
  });

  const sessionResult = await client.query<{ session_id: string }>(
    `
      WITH candidate_sessions AS (
        SELECT $1::uuid AS session_id
        WHERE $1::uuid IS NOT NULL

        UNION

        SELECT DISTINCT e.session_id
        FROM tracking_events e
        WHERE ($2::text IS NOT NULL AND e.shopify_checkout_token = $2)
           OR ($3::text IS NOT NULL AND e.shopify_cart_token = $3)
      )
      SELECT DISTINCT s.id::text AS session_id
      FROM candidate_sessions c
      INNER JOIN tracking_sessions s ON s.id = c.session_id
      WHERE (
        $4::timestamptz IS NULL
        OR (
          s.first_seen_at >= $4::timestamptz
          AND s.first_seen_at <= $5::timestamptz
        )
      )
    `,
    [
      input.sessionId,
      input.checkoutToken,
      input.cartToken,
      lookbackWindow?.lookbackWindowStartedAt ?? null,
      lookbackWindow?.lookbackWindowExpiresAt ?? null
    ]
  );

  const sessionIds = sessionResult.rows.map((row) => row.session_id);

  if (sessionIds.length > 0) {
    await client.query(
      `
        UPDATE tracking_sessions
        SET
          identity_journey_id = $1::uuid,
          customer_identity_id = CASE
            WHEN $3::boolean THEN $1::uuid
            ELSE customer_identity_id
          END,
          updated_at = now()
        WHERE id = ANY($2::uuid[])
      `,
      [journeyId, sessionIds, compatibilityExists]
    );

    await client.query(
      `
        UPDATE tracking_events
        SET
          identity_journey_id = $1::uuid,
          customer_identity_id = CASE
            WHEN $3::boolean THEN $1::uuid
            ELSE customer_identity_id
          END
        WHERE session_id = ANY($2::uuid[])
      `,
      [journeyId, sessionIds, compatibilityExists]
    );

    await client.query(
      `
        UPDATE session_attribution_identities
        SET
          identity_journey_id = $1::uuid,
          customer_identity_id = CASE
            WHEN $3::boolean THEN $1::uuid
            ELSE customer_identity_id
          END,
          updated_at = now()
        WHERE roas_radar_session_id = ANY($2::uuid[])
      `,
      [journeyId, sessionIds, compatibilityExists]
    );
  }

  await client.query(
    `
      UPDATE shopify_orders
      SET
        identity_journey_id = $1::uuid,
        customer_identity_id = CASE
          WHEN $7::boolean THEN $1::uuid
          ELSE customer_identity_id
        END
      WHERE (
        ($2::uuid IS NOT NULL AND landing_session_id = $2::uuid)
        OR ($3::text IS NOT NULL AND checkout_token = $3)
        OR ($4::text IS NOT NULL AND cart_token = $4)
        OR ($5::text IS NOT NULL AND shopify_customer_id = $5)
        OR ($6::text IS NOT NULL AND email_hash = $6)
      )
        AND (
          $8::timestamptz IS NULL
          OR (
            COALESCE(processed_at, created_at_shopify, ingested_at) >= $8::timestamptz
            AND COALESCE(processed_at, created_at_shopify, ingested_at) <= $9::timestamptz
          )
        )
    `,
    [
      journeyId,
      input.sessionId,
      input.checkoutToken,
      input.cartToken,
      input.shopifyCustomerId,
      safeOrderRewriteContactHashes.emailHash,
      compatibilityExists,
      lookbackWindow?.lookbackWindowStartedAt ?? null,
      lookbackWindow?.lookbackWindowExpiresAt ?? null
    ]
  );

  if (input.shopifyCustomerId) {
    await client.query(
      `
        UPDATE shopify_customers
        SET
          identity_journey_id = $2::uuid,
          customer_identity_id = CASE
            WHEN $3::boolean THEN $2::uuid
            ELSE customer_identity_id
          END,
          updated_at = now()
        WHERE shopify_customer_id = $1
      `,
      [input.shopifyCustomerId, journeyId, compatibilityExists]
    );
  }

  return sessionIds;
}

async function resolveSafeOrderRewriteContactHashes(
  client: DbClient,
  journeyId: string,
  input: {
    emailHash: string | null;
    phoneHash: string | null;
  }
): Promise<{ emailHash: string | null; phoneHash: string | null }> {
  const hashesByType = new Map<IdentityNodeType, string>();

  if (input.emailHash) {
    hashesByType.set('hashed_email', input.emailHash);
  }

  if (input.phoneHash) {
    hashesByType.set('phone_hash', input.phoneHash);
  }

  if (hashesByType.size === 0) {
    return {
      emailHash: null,
      phoneHash: null
    };
  }

  const nodeTypes = [...hashesByType.keys()];
  const nodeKeys = nodeTypes.map((nodeType) => hashesByType.get(nodeType) as string);
  const result = await client.query<{
    node_type: IdentityNodeType;
    node_key: string;
  }>(
    `
      SELECT
        n.node_type,
        n.node_key
      FROM identity_nodes n
      INNER JOIN identity_edges e ON e.node_id = n.id
      WHERE e.journey_id = $1::uuid
        AND e.is_active = true
        AND e.edge_type <> 'quarantined'
        AND n.is_ambiguous = false
        AND n.node_type = ANY($2::text[])
        AND n.node_key = ANY($3::text[])
    `,
    [journeyId, nodeTypes, nodeKeys]
  );

  const safeHashes = new Map(result.rows.map((row) => [row.node_type, row.node_key]));

  return {
    emailHash: safeHashes.get('hashed_email') ?? null,
    phoneHash: safeHashes.get('phone_hash') ?? null
  };
}

async function collectJourneySessionIds(client: DbClient, journeyId: string): Promise<string[]> {
  const result = await client.query<{ session_id: string }>(
    `
      SELECT id::text AS session_id
      FROM tracking_sessions
      WHERE identity_journey_id = $1::uuid
      ORDER BY id ASC
    `,
    [journeyId]
  );

  return result.rows.map((row) => row.session_id);
}

async function loadJourneyLookbackWindow(client: DbClient, journeyId: string): Promise<IdentityJourneyLookbackWindow | null> {
  const result = await client.query<{
    lookback_window_started_at: Date;
    lookback_window_expires_at: Date;
    last_touch_eligible_at: Date;
  }>(
    `
      SELECT
        lookback_window_started_at,
        lookback_window_expires_at,
        last_touch_eligible_at
      FROM identity_journeys
      WHERE id = $1::uuid
      LIMIT 1
    `,
    [journeyId]
  );

  const row = result.rows[0];
  if (!row) {
    return null;
  }

  return {
    lookbackWindowStartedAt: row.lookback_window_started_at,
    lookbackWindowExpiresAt: row.lookback_window_expires_at,
    lastTouchEligibleAt: row.last_touch_eligible_at
  };
}

async function customerIdentityExists(client: DbClient, journeyId: string): Promise<boolean> {
  const result = await client.query<{ exists: boolean }>(
    `
      SELECT EXISTS (
        SELECT 1
        FROM customer_identities
        WHERE id = $1::uuid
      ) AS exists
    `,
    [journeyId]
  );

  return result.rows[0]?.exists ?? false;
}

function emitIdentityIngestionMetrics(input: {
  sourceTable: string | null;
  evidenceSource: string;
  outcome: 'linked' | 'skipped' | 'conflict';
  deduplicated: boolean;
  processedNodes: number;
  attachedNodes: number;
  rehomedNodes: number;
  quarantinedNodes: number;
  journeyId: string | null;
}): void {
  const payload = buildIdentityEdgeIngestionMetricsLog(input);
  logInfo('identity_edge_ingestion_processed', JSON.parse(payload) as Record<string, unknown>);
}

function normalizeNullableString(value: string | null | undefined): string | null {
  const normalized = value?.trim();
  return normalized ? normalized : null;
}

function normalizeToken(value: string | null | undefined): string | null {
  return normalizeNullableString(value);
}

function normalizeSessionIdentifier(value: string | null | undefined): string | null {
  const normalized = normalizeNullableString(value);
  return normalized && UUID_PATTERN.test(normalized) ? normalized : null;
}

function normalizeSourceTimestamp(value: string | Date): Date {
  const date = value instanceof Date ? value : new Date(value);

  if (Number.isNaN(date.getTime())) {
    throw new Error(`Invalid identity source timestamp: ${String(value)}`);
  }

  return date;
}

function buildHistoricalLookbackWindow(anchorTimestamp: Date): IdentityJourneyLookbackWindow {
  return {
    lookbackWindowStartedAt: new Date(
      anchorTimestamp.getTime() - HISTORICAL_IDENTITY_LOOKBACK_DAYS * 24 * 60 * 60 * 1000
    ),
    lookbackWindowExpiresAt: anchorTimestamp,
    lastTouchEligibleAt: anchorTimestamp
  };
}

export const __identityTestUtils = {
  buildNormalizedIdentityNodes,
  selectBestJourneyId
};
