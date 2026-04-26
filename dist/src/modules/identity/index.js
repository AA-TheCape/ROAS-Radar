import { randomUUID } from 'node:crypto';
import { logInfo, logWarning } from '../../observability/index.js';
import { hashEmailAddress, hashPhoneNumber, normalizeEmailAddress, normalizePhoneNumber } from '../../shared/privacy.js';
const IDENTITY_PRECEDENCE = {
    shopify_customer_id: 100,
    hashed_email: 70,
    phone_hash: 60,
    checkout_token: 40,
    cart_token: 30,
    session_id: 20
};
const ACTIVE_IDENTITY_STATUSES = new Set(['active', 'quarantined']);
const AUTHORITATIVE_CONFLICT = 'authoritative_shopify_customer_conflict';
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
export const normalizeIdentityEmail = normalizeEmailAddress;
export const hashIdentityEmail = hashEmailAddress;
export const hashIdentityPhone = hashPhoneNumber;
export const normalizeIdentityPhone = normalizePhoneNumber;
export function resolveIdentityStitch(existingIdentities, input) {
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
export function buildIdentityEdgeIngestionMetricsLog(result) {
    return JSON.stringify({
        severity: 'INFO',
        event: 'identity_edge_ingestion_processed',
        message: 'identity_edge_ingestion_processed',
        timestamp: new Date().toISOString(),
        service: process.env.K_SERVICE ?? 'roas-radar-api',
        ...result
    });
}
export async function stitchKnownCustomerIdentity(client, input) {
    const emailHash = input.email ? hashIdentityEmail(input.email) : null;
    const graphResult = await ingestIdentityEdges(client, {
        sourceTimestamp: input.sourceTimestamp ?? new Date(),
        evidenceSource: input.evidenceSource ?? 'shopify_order_webhook',
        sourceTable: input.sourceTable ?? 'shopify_orders',
        sourceRecordId: input.sourceRecordId ?? input.shopifyOrderId,
        idempotencyKey: input.idempotencyKey ??
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
        reason: graphResult.reason === 'linked'
            ? graphResult.deduplicated
                ? 'reuse_identity'
                : 'create_identity'
            : graphResult.reason,
        identityId: graphResult.journeyId,
        emailHash: graphResult.emailHash,
        linkedSessionIds: graphResult.linkedSessionIds
    };
}
export async function ingestIdentityEdges(client, input) {
    const sourceTimestamp = normalizeSourceTimestamp(input.sourceTimestamp);
    const normalizedNodes = buildNormalizedIdentityNodes(input);
    if (normalizedNodes.length === 0) {
        const metrics = {
            processedNodes: 0,
            attachedNodes: 0,
            rehomedNodes: 0,
            quarantinedNodes: 0,
            deduplicated: false,
            outcome: 'skipped'
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
            outcome: (registeredIngestion.existingOutcome === 'conflict' ? 'conflict' : 'linked')
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
    const candidateRows = await loadActiveJourneyCandidates(client, nodeRows.map((row) => row.id));
    const journeyScores = await loadJourneyScores(client, [...new Set(candidateRows.map((row) => row.journey_id).filter((value) => Boolean(value)))]);
    const quarantinedNodeIds = new Set();
    const candidateRowsForResolution = candidateRows.filter((row) => {
        const node = nodeRows.find((nodeRow) => nodeRow.id === row.node_id);
        return row.journey_id && !node?.is_ambiguous;
    });
    const journeyResolution = await resolveWinningJourney(client, {
        nodeRows,
        candidateRows: candidateRowsForResolution,
        journeyScores,
        incomingShopifyCustomerId: normalizedNodes.find((node) => node.nodeType === 'shopify_customer_id')?.nodeKey ?? null,
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
        await supersedeIdentityEdge(client, activeCandidate.edge_id, newEdgeId);
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
        rehomedNodes += 1;
    }
    await refreshJourneyConvenienceFields(client, winnerJourneyId, sourceTimestamp, {
        authoritativeShopifyCustomerId: normalizedNodes.find((node) => node.nodeType === 'shopify_customer_id')?.nodeKey ?? null,
        emailHash: normalizedNodes.find((node) => node.nodeType === 'hashed_email')?.nodeKey ?? null,
        phoneHash: normalizedNodes.find((node) => node.nodeType === 'phone_hash')?.nodeKey ?? null,
        graphChanged: journeyResolution.created || attachedNodes > 0 || rehomedNodes > 0 || quarantinedNodeIds.size > 0
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
        emailHash: normalizedNodes.find((node) => node.nodeType === 'hashed_email')?.nodeKey ?? null
    });
    await completeIdentityEdgeIngestion(client, {
        idempotencyKey: input.idempotencyKey,
        status: 'completed',
        journeyId: winnerJourneyId,
        outcomeReason: 'linked',
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
async function findExistingIdentities(client, shopifyCustomerId, emailHash) {
    const result = await client.query(`
      SELECT
        id,
        hashed_email,
        shopify_customer_id
      FROM customer_identities
      WHERE ($1::text IS NOT NULL AND shopify_customer_id = $1)
         OR ($2::text IS NOT NULL AND hashed_email = $2)
      ORDER BY created_at ASC
    `, [normalizeNullableString(shopifyCustomerId), emailHash]);
    return result.rows;
}
async function registerIdentityEdgeIngestion(client, input) {
    const insertResult = await client.query(`
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
    `, [input.idempotencyKey, input.evidenceSource, input.sourceTable, input.sourceRecordId, input.sourceTimestamp]);
    if (insertResult.rowCount) {
        return {
            deduplicated: false,
            existingJourneyId: null,
            existingOutcome: null
        };
    }
    const existingResult = await client.query(`
      SELECT
        journey_id::text AS journey_id,
        status,
        outcome_reason
      FROM identity_edge_ingestion_runs
      WHERE idempotency_key = $1
      LIMIT 1
    `, [input.idempotencyKey]);
    return {
        deduplicated: true,
        existingJourneyId: existingResult.rows[0]?.journey_id ?? null,
        existingOutcome: existingResult.rows[0]?.outcome_reason ?? null
    };
}
async function completeIdentityEdgeIngestion(client, input) {
    await client.query(`
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
    `, [
        input.idempotencyKey,
        input.status,
        input.journeyId,
        input.outcomeReason,
        input.metrics.processedNodes,
        input.metrics.attachedNodes,
        input.metrics.rehomedNodes,
        input.metrics.quarantinedNodes
    ]);
}
function buildNormalizedIdentityNodes(input) {
    const sessionId = normalizeSessionIdentifier(input.sessionId);
    const checkoutToken = normalizeToken(input.checkoutToken);
    const cartToken = normalizeToken(input.cartToken);
    const shopifyCustomerId = normalizeNullableString(input.shopifyCustomerId);
    const hashedEmail = input.hashedEmail ?? hashIdentityEmail(input.email);
    const phoneHash = input.phoneHash ?? hashIdentityPhone(input.phone);
    const candidates = [];
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
function dedupeIdentityNodes(nodes) {
    const seen = new Set();
    const deduped = [];
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
async function upsertAndLockIdentityNodes(client, nodes, sourceTimestamp) {
    for (const node of nodes) {
        await client.query(`
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
      `, [node.nodeType, node.nodeKey, node.nodeType === 'shopify_customer_id', sourceTimestamp]);
    }
    const result = await client.query(`
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
    `, nodes.flatMap((node) => [node.nodeType, node.nodeKey]));
    return result.rows;
}
async function loadActiveJourneyCandidates(client, nodeIds) {
    if (nodeIds.length === 0) {
        return [];
    }
    const result = await client.query(`
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
    `, [nodeIds]);
    return result.rows.filter((row) => row.journey_id === null || ACTIVE_IDENTITY_STATUSES.has(row.edge_type === 'quarantined' ? 'quarantined' : 'active'));
}
async function loadJourneyScores(client, journeyIds) {
    if (journeyIds.length === 0) {
        return new Map();
    }
    const result = await client.query(`
      SELECT
        e.journey_id::text AS journey_id,
        MAX(e.precedence_rank)::int AS max_precedence_rank,
        MAX(e.last_observed_at) AS latest_observed_at
      FROM identity_edges e
      WHERE e.is_active = true
        AND e.journey_id = ANY($1::uuid[])
      GROUP BY e.journey_id
    `, [journeyIds]);
    return new Map(result.rows.map((row) => [row.journey_id, row]));
}
async function resolveWinningJourney(client, input) {
    const candidateJourneyIds = [...new Set(input.candidateRows.map((row) => row.journey_id).filter((value) => Boolean(value)))];
    if (candidateJourneyIds.length === 0) {
        const journeyId = await createJourney(client, {
            authoritativeShopifyCustomerId: input.incomingShopifyCustomerId,
            emailHash: input.nodeRows.find((row) => row.node_type === 'hashed_email')?.node_key ?? null,
            phoneHash: input.nodeRows.find((row) => row.node_type === 'phone_hash')?.node_key ?? null,
            sourceTimestamp: input.sourceTimestamp,
            evidenceSource: input.evidenceSource,
            sourceTable: input.sourceTable,
            sourceRecordId: input.sourceRecordId
        });
        return {
            outcome: 'linked',
            journeyId,
            created: true
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
            const conflictCode = row.node_type === 'phone_hash'
                ? 'phone_hash_conflicts_across_authoritative_customers'
                : 'hashed_email_conflicts_across_authoritative_customers';
            await quarantineIdentityNode(client, {
                nodeId: row.node_id,
                currentEdgeId: row.edge_id,
                currentJourneyId: row.journey_id,
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
            created: false
        };
    }
    if (candidateJourneyIds.length === 1) {
        return {
            outcome: 'linked',
            journeyId: candidateJourneyIds[0],
            created: false
        };
    }
    const selectedJourneyId = selectBestJourneyId(workingCandidates, input.journeyScores);
    if (selectedJourneyId) {
        return {
            outcome: 'linked',
            journeyId: selectedJourneyId,
            created: false
        };
    }
    const fallbackJourneyId = await createJourney(client, {
        authoritativeShopifyCustomerId: input.incomingShopifyCustomerId,
        emailHash: input.nodeRows.find((row) => row.node_type === 'hashed_email')?.node_key ?? null,
        phoneHash: input.nodeRows.find((row) => row.node_type === 'phone_hash')?.node_key ?? null,
        sourceTimestamp: input.sourceTimestamp,
        evidenceSource: input.evidenceSource,
        sourceTable: input.sourceTable,
        sourceRecordId: input.sourceRecordId
    });
    return {
        outcome: 'linked',
        journeyId: fallbackJourneyId,
        created: true
    };
}
function distinctAuthoritativeShopifyIds(rows) {
    return [...new Set(rows.map((row) => row.authoritative_shopify_customer_id).filter((value) => Boolean(value)))];
}
function selectBestJourneyId(rows, journeyScores) {
    const candidates = [...new Set(rows.map((row) => row.journey_id).filter((value) => Boolean(value)))];
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
async function createJourney(client, input) {
    const journeyId = randomUUID();
    await client.query(`
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
        $5::timestamptz + interval '30 days',
        $5::timestamptz,
        $6,
        $7,
        $8,
        $6,
        $7,
        $8,
        now(),
        now(),
        $5::timestamptz
      )
    `, [
        journeyId,
        input.authoritativeShopifyCustomerId,
        input.emailHash,
        input.phoneHash,
        input.sourceTimestamp,
        input.evidenceSource,
        input.sourceTable,
        input.sourceRecordId
    ]);
    return journeyId;
}
async function quarantineIdentityNode(client, input) {
    const newEdgeId = randomUUID();
    await client.query(`
      UPDATE identity_nodes
      SET
        is_ambiguous = true,
        updated_at = now()
      WHERE id = $1::uuid
    `, [input.nodeId]);
    await supersedeIdentityEdge(client, input.currentEdgeId, newEdgeId);
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
}
async function insertIdentityEdge(client, input) {
    const nodeTypeResult = await client.query(`
      SELECT node_type
      FROM identity_nodes
      WHERE id = $1::uuid
    `, [input.nodeId]);
    const nodeType = nodeTypeResult.rows[0]?.node_type;
    if (!nodeType) {
        throw new Error(`Identity node ${input.nodeId} was not found while inserting edge`);
    }
    await client.query(`
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
    `, [
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
    ]);
}
async function touchActiveIdentityEdge(client, edgeId, sourceTimestamp) {
    await client.query(`
      UPDATE identity_edges
      SET
        first_observed_at = LEAST(first_observed_at, $2),
        last_observed_at = GREATEST(last_observed_at, $2),
        updated_at = now()
      WHERE id = $1::uuid
    `, [edgeId, sourceTimestamp]);
}
async function supersedeIdentityEdge(client, previousEdgeId, supersedingEdgeId) {
    await client.query(`
      UPDATE identity_edges
      SET
        is_active = false,
        superseded_by_edge_id = $2::uuid,
        updated_at = now()
      WHERE id = $1::uuid
    `, [previousEdgeId, supersedingEdgeId]);
}
async function refreshJourneyConvenienceFields(client, journeyId, sourceTimestamp, input) {
    await client.query(`
      UPDATE identity_journeys
      SET
        authoritative_shopify_customer_id = COALESCE($2, authoritative_shopify_customer_id),
        primary_email_hash = COALESCE($3, primary_email_hash),
        primary_phone_hash = COALESCE($4, primary_phone_hash),
        merge_version = CASE WHEN $5 THEN merge_version + 1 ELSE merge_version END,
        updated_at = now(),
        last_resolved_at = GREATEST(last_resolved_at, $6)
      WHERE id = $1::uuid
    `, [journeyId, input.authoritativeShopifyCustomerId, input.emailHash, input.phoneHash, input.graphChanged, sourceTimestamp]);
}
async function markJourneyMergedIfEmpty(client, journeyId, mergedIntoJourneyId, sourceTimestamp) {
    const result = await client.query(`
      SELECT EXISTS (
        SELECT 1
        FROM identity_edges
        WHERE journey_id = $1::uuid
          AND is_active = true
      ) AS has_active_edges
    `, [journeyId]);
    if (result.rows[0]?.has_active_edges) {
        return;
    }
    await client.query(`
      UPDATE identity_journeys
      SET
        status = 'merged',
        merged_into_journey_id = $2::uuid,
        updated_at = now(),
        last_resolved_at = GREATEST(last_resolved_at, $3)
      WHERE id = $1::uuid
    `, [journeyId, mergedIntoJourneyId, sourceTimestamp]);
}
async function upsertCompatibilityCustomerIdentity(client, journeyId, input) {
    if (!input.emailHash && !input.shopifyCustomerId) {
        return;
    }
    try {
        await client.query(`
        INSERT INTO customer_identities (
          id,
          hashed_email,
          shopify_customer_id,
          created_at,
          updated_at,
          last_stitched_at
        )
        VALUES ($1::uuid, $2, $3, now(), now(), $4)
        ON CONFLICT (id)
        DO UPDATE SET
          hashed_email = COALESCE(customer_identities.hashed_email, EXCLUDED.hashed_email),
          shopify_customer_id = COALESCE(customer_identities.shopify_customer_id, EXCLUDED.shopify_customer_id),
          updated_at = now(),
          last_stitched_at = GREATEST(customer_identities.last_stitched_at, EXCLUDED.last_stitched_at)
      `, [journeyId, input.emailHash, input.shopifyCustomerId, input.sourceTimestamp]);
    }
    catch (error) {
        if (typeof error === 'object' && error !== null && 'code' in error && error.code === '23505') {
            const existingIdentities = await findExistingIdentities(client, input.shopifyCustomerId, input.emailHash);
            const existingIdentityId = existingIdentities[0]?.id;
            if (existingIdentityId) {
                await client.query(`
            UPDATE customer_identities
            SET
              hashed_email = COALESCE(hashed_email, $2),
              shopify_customer_id = COALESCE(shopify_customer_id, $3),
              updated_at = now(),
              last_stitched_at = GREATEST(last_stitched_at, $4)
            WHERE id = $1::uuid
          `, [existingIdentityId, input.emailHash, input.shopifyCustomerId, input.sourceTimestamp]);
                return;
            }
        }
        throw error;
    }
}
async function syncIdentityReferences(client, input) {
    const compatibilityExists = await customerIdentityExists(client, input.journeyId);
    const journeyId = input.journeyId;
    const sessionResult = await client.query(`
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
    `, [input.sessionId, input.checkoutToken, input.cartToken]);
    const sessionIds = sessionResult.rows.map((row) => row.session_id);
    if (sessionIds.length > 0) {
        await client.query(`
        UPDATE tracking_sessions
        SET
          identity_journey_id = $1::uuid,
          customer_identity_id = CASE
            WHEN $3::boolean THEN $1::uuid
            ELSE customer_identity_id
          END,
          updated_at = now()
        WHERE id = ANY($2::uuid[])
      `, [journeyId, sessionIds, compatibilityExists]);
        await client.query(`
        UPDATE tracking_events
        SET
          identity_journey_id = $1::uuid,
          customer_identity_id = CASE
            WHEN $3::boolean THEN $1::uuid
            ELSE customer_identity_id
          END
        WHERE session_id = ANY($2::uuid[])
      `, [journeyId, sessionIds, compatibilityExists]);
        await client.query(`
        UPDATE session_attribution_identities
        SET
          identity_journey_id = $1::uuid,
          customer_identity_id = CASE
            WHEN $3::boolean THEN $1::uuid
            ELSE customer_identity_id
          END,
          updated_at = now()
        WHERE roas_radar_session_id = ANY($2::uuid[])
      `, [journeyId, sessionIds, compatibilityExists]);
    }
    await client.query(`
      UPDATE shopify_orders
      SET
        identity_journey_id = $1::uuid,
        customer_identity_id = CASE
          WHEN $7::boolean THEN $1::uuid
          ELSE customer_identity_id
        END
      WHERE ($2::uuid IS NOT NULL AND landing_session_id = $2::uuid)
         OR ($3::text IS NOT NULL AND checkout_token = $3)
         OR ($4::text IS NOT NULL AND cart_token = $4)
         OR ($5::text IS NOT NULL AND shopify_customer_id = $5)
         OR ($6::text IS NOT NULL AND email_hash = $6)
    `, [journeyId, input.sessionId, input.checkoutToken, input.cartToken, input.shopifyCustomerId, input.emailHash, compatibilityExists]);
    if (input.shopifyCustomerId) {
        await client.query(`
        UPDATE shopify_customers
        SET
          identity_journey_id = $2::uuid,
          customer_identity_id = CASE
            WHEN $3::boolean THEN $2::uuid
            ELSE customer_identity_id
          END,
          updated_at = now()
        WHERE shopify_customer_id = $1
      `, [input.shopifyCustomerId, journeyId, compatibilityExists]);
    }
    return sessionIds;
}
async function collectJourneySessionIds(client, journeyId) {
    const result = await client.query(`
      SELECT id::text AS session_id
      FROM tracking_sessions
      WHERE identity_journey_id = $1::uuid
      ORDER BY id ASC
    `, [journeyId]);
    return result.rows.map((row) => row.session_id);
}
async function customerIdentityExists(client, journeyId) {
    const result = await client.query(`
      SELECT EXISTS (
        SELECT 1
        FROM customer_identities
        WHERE id = $1::uuid
      ) AS exists
    `, [journeyId]);
    return result.rows[0]?.exists ?? false;
}
function emitIdentityIngestionMetrics(input) {
    const payload = buildIdentityEdgeIngestionMetricsLog(input);
    logInfo('identity_edge_ingestion_processed', JSON.parse(payload));
}
function normalizeNullableString(value) {
    const normalized = value?.trim();
    return normalized ? normalized : null;
}
function normalizeToken(value) {
    return normalizeNullableString(value);
}
function normalizeSessionIdentifier(value) {
    const normalized = normalizeNullableString(value);
    return normalized && UUID_PATTERN.test(normalized) ? normalized : null;
}
function normalizeSourceTimestamp(value) {
    const date = value instanceof Date ? value : new Date(value);
    if (Number.isNaN(date.getTime())) {
        throw new Error(`Invalid identity source timestamp: ${String(value)}`);
    }
    return date;
}
