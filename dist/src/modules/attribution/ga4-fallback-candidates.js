import { createHash } from 'node:crypto';
import { env } from '../../config/env.js';
import { query } from '../../db/pool.js';
const ATTRIBUTION_WINDOW_DAYS = 7;
const DEFAULT_RETENTION_DAYS = 35;
function normalizeNullableString(value, { lowerCase = false } = {}) {
    if (typeof value !== 'string') {
        return null;
    }
    const trimmed = value.trim();
    if (!trimmed) {
        return null;
    }
    return lowerCase ? trimmed.toLowerCase() : trimmed;
}
function normalizeEmailHash(value) {
    const normalized = normalizeNullableString(value, { lowerCase: true });
    if (!normalized) {
        return null;
    }
    return /^[0-9a-f]{64}$/.test(normalized) ? normalized : null;
}
function normalizePositiveInteger(value, fallback) {
    if (!Number.isFinite(value)) {
        return fallback;
    }
    return Math.max(Math.trunc(value ?? fallback), 1);
}
function buildRetentionTimestamp(occurredAtIso) {
    const occurredAt = new Date(occurredAtIso);
    const retainedUntil = new Date(occurredAt);
    retainedUntil.setUTCDate(retainedUntil.getUTCDate() + (env.GA4_FALLBACK_RETENTION_DAYS || DEFAULT_RETENTION_DAYS));
    return retainedUntil.toISOString();
}
export function buildGa4FallbackCandidateKey(input) {
    const digest = createHash('sha256');
    digest.update(JSON.stringify({
        occurredAt: input.occurredAt,
        ga4UserKey: input.ga4UserKey,
        ga4SessionId: normalizeNullableString(input.ga4SessionId),
        ga4ClientId: normalizeNullableString(input.ga4ClientId),
        transactionId: normalizeNullableString(input.transactionId)
    }));
    return digest.digest('hex');
}
function mapPersistedCandidate(row) {
    return {
        candidateKey: row.candidate_key,
        occurredAt: row.occurred_at.toISOString(),
        ga4UserKey: row.ga4_user_key,
        ga4ClientId: row.ga4_client_id,
        ga4SessionId: row.ga4_session_id,
        transactionId: row.transaction_id,
        emailHash: row.email_hash,
        customerIdentityId: row.customer_identity_id,
        source: row.source,
        medium: row.medium,
        campaign: row.campaign,
        content: row.content,
        term: row.term,
        clickIdType: row.click_id_type,
        clickIdValue: row.click_id_value,
        sessionHasRequiredFields: row.session_has_required_fields,
        sourceExportHour: row.source_export_hour.toISOString(),
        sourceDataset: row.source_dataset,
        sourceTableType: row.source_table_type,
        retainedUntil: row.retained_until.toISOString()
    };
}
async function ensureTouchedPartitions(db, occurredAtValues) {
    const months = Array.from(new Set(occurredAtValues.map((value) => {
        const occurredAt = new Date(value);
        return new Date(Date.UTC(occurredAt.getUTCFullYear(), occurredAt.getUTCMonth(), 1)).toISOString().slice(0, 10);
    })));
    for (const month of months) {
        await executeQuery(db, 'SELECT ensure_ga4_fallback_candidate_partition($1::date)', [month]);
    }
}
function buildGa4SignalSql(alias) {
    return `(${alias}.click_id_value IS NOT NULL
    OR ${alias}.source IS NOT NULL
    OR ${alias}.medium IS NOT NULL
    OR ${alias}.campaign IS NOT NULL
    OR ${alias}.content IS NOT NULL
    OR ${alias}.term IS NOT NULL)`;
}
function buildGa4DimensionCountSql(alias) {
    return `(
    (${alias}.source IS NOT NULL)::int
    + (${alias}.medium IS NOT NULL)::int
    + (${alias}.campaign IS NOT NULL)::int
    + (${alias}.content IS NOT NULL)::int
    + (${alias}.term IS NOT NULL)::int
  )`;
}
function buildShouldReplaceGa4BundleSql(currentAlias, incomingAlias) {
    const currentHasSignal = buildGa4SignalSql(currentAlias);
    const incomingHasSignal = buildGa4SignalSql(incomingAlias);
    const currentDimensionCount = buildGa4DimensionCountSql(currentAlias);
    const incomingDimensionCount = buildGa4DimensionCountSql(incomingAlias);
    return `(
    CASE
      WHEN NOT ${currentHasSignal} AND ${incomingHasSignal} THEN true
      WHEN ${currentHasSignal} AND NOT ${incomingHasSignal} THEN false
      WHEN (${incomingAlias}.click_id_value IS NOT NULL)::int <> (${currentAlias}.click_id_value IS NOT NULL)::int
      THEN ${incomingAlias}.click_id_value IS NOT NULL
      WHEN ${incomingDimensionCount} <> ${currentDimensionCount}
      THEN ${incomingDimensionCount} > ${currentDimensionCount}
      WHEN ${incomingAlias}.source_export_hour <> ${currentAlias}.source_export_hour
      THEN ${incomingAlias}.source_export_hour > ${currentAlias}.source_export_hour
      WHEN ${incomingAlias}.source_table_type <> ${currentAlias}.source_table_type
      THEN ${incomingAlias}.source_table_type = 'events'
      ELSE false
    END
  )`;
}
async function executeQuery(client, text, params) {
    if (client) {
        return client.query(text, params);
    }
    return query(text, params);
}
export async function upsertGa4FallbackCandidates(candidates, client) {
    if (candidates.length === 0) {
        return 0;
    }
    const db = client;
    await ensureTouchedPartitions(db, candidates.map((candidate) => candidate.occurredAt));
    let upsertedRows = 0;
    const shouldReplaceBundleSql = buildShouldReplaceGa4BundleSql('ga4_fallback_candidates', 'EXCLUDED');
    for (const candidate of candidates) {
        const normalized = {
            candidateKey: buildGa4FallbackCandidateKey(candidate),
            occurredAt: new Date(candidate.occurredAt).toISOString(),
            ga4UserKey: normalizeNullableString(candidate.ga4UserKey),
            ga4ClientId: normalizeNullableString(candidate.ga4ClientId),
            ga4SessionId: normalizeNullableString(candidate.ga4SessionId),
            transactionId: normalizeNullableString(candidate.transactionId),
            emailHash: normalizeEmailHash(candidate.emailHash),
            customerIdentityId: normalizeNullableString(candidate.customerIdentityId),
            source: normalizeNullableString(candidate.source, { lowerCase: true }),
            medium: normalizeNullableString(candidate.medium, { lowerCase: true }),
            campaign: normalizeNullableString(candidate.campaign),
            content: normalizeNullableString(candidate.content),
            term: normalizeNullableString(candidate.term),
            clickIdType: normalizeNullableString(candidate.clickIdType, { lowerCase: true }),
            clickIdValue: normalizeNullableString(candidate.clickIdValue),
            sessionHasRequiredFields: Boolean(candidate.sessionHasRequiredFields),
            sourceExportHour: new Date(candidate.sourceExportHour).toISOString(),
            sourceDataset: normalizeNullableString(candidate.sourceDataset),
            sourceTableType: candidate.sourceTableType,
            retainedUntil: candidate.retainedUntil
                ? new Date(candidate.retainedUntil).toISOString()
                : buildRetentionTimestamp(candidate.occurredAt)
        };
        if (!normalized.ga4UserKey) {
            throw new Error('GA4 fallback candidate requires ga4UserKey');
        }
        if (!normalized.sourceDataset) {
            throw new Error('GA4 fallback candidate requires sourceDataset');
        }
        if (!normalized.customerIdentityId &&
            !normalized.emailHash &&
            !normalized.transactionId &&
            !normalized.ga4ClientId &&
            !normalized.ga4SessionId) {
            throw new Error('GA4 fallback candidate requires at least one lookup key');
        }
        const result = await executeQuery(db, `
        INSERT INTO ga4_fallback_candidates (
          candidate_key,
          occurred_at,
          ga4_user_key,
          ga4_client_id,
          ga4_session_id,
          transaction_id,
          email_hash,
          customer_identity_id,
          source,
          medium,
          campaign,
          content,
          term,
          click_id_type,
          click_id_value,
          session_has_required_fields,
          source_export_hour,
          source_dataset,
          source_table_type,
          retained_until,
          updated_at
        )
        VALUES (
          $1,
          $2::timestamptz,
          $3,
          $4,
          $5,
          $6,
          $7,
          $8::uuid,
          $9,
          $10,
          $11,
          $12,
          $13,
          $14,
          $15,
          $16,
          $17::timestamptz,
          $18,
          $19,
          $20::timestamptz,
          now()
        )
        ON CONFLICT (candidate_key, occurred_at)
        DO UPDATE SET
          ga4_user_key = EXCLUDED.ga4_user_key,
          ga4_client_id = COALESCE(EXCLUDED.ga4_client_id, ga4_fallback_candidates.ga4_client_id),
          ga4_session_id = COALESCE(EXCLUDED.ga4_session_id, ga4_fallback_candidates.ga4_session_id),
          transaction_id = COALESCE(EXCLUDED.transaction_id, ga4_fallback_candidates.transaction_id),
          email_hash = COALESCE(EXCLUDED.email_hash, ga4_fallback_candidates.email_hash),
          customer_identity_id = COALESCE(EXCLUDED.customer_identity_id, ga4_fallback_candidates.customer_identity_id),
          source = CASE WHEN ${shouldReplaceBundleSql} THEN EXCLUDED.source ELSE ga4_fallback_candidates.source END,
          medium = CASE WHEN ${shouldReplaceBundleSql} THEN EXCLUDED.medium ELSE ga4_fallback_candidates.medium END,
          campaign = CASE WHEN ${shouldReplaceBundleSql} THEN EXCLUDED.campaign ELSE ga4_fallback_candidates.campaign END,
          content = CASE WHEN ${shouldReplaceBundleSql} THEN EXCLUDED.content ELSE ga4_fallback_candidates.content END,
          term = CASE WHEN ${shouldReplaceBundleSql} THEN EXCLUDED.term ELSE ga4_fallback_candidates.term END,
          click_id_type = CASE
            WHEN ${shouldReplaceBundleSql}
            THEN EXCLUDED.click_id_type
            ELSE ga4_fallback_candidates.click_id_type
          END,
          click_id_value = CASE
            WHEN ${shouldReplaceBundleSql}
            THEN EXCLUDED.click_id_value
            ELSE ga4_fallback_candidates.click_id_value
          END,
          session_has_required_fields = ga4_fallback_candidates.session_has_required_fields OR EXCLUDED.session_has_required_fields,
          source_export_hour = CASE
            WHEN ${shouldReplaceBundleSql}
            THEN EXCLUDED.source_export_hour
            ELSE ga4_fallback_candidates.source_export_hour
          END,
          source_dataset = CASE
            WHEN ${shouldReplaceBundleSql}
            THEN EXCLUDED.source_dataset
            ELSE ga4_fallback_candidates.source_dataset
          END,
          source_table_type = CASE
            WHEN ${shouldReplaceBundleSql}
            THEN EXCLUDED.source_table_type
            ELSE ga4_fallback_candidates.source_table_type
          END,
          retained_until = GREATEST(ga4_fallback_candidates.retained_until, EXCLUDED.retained_until),
          updated_at = now()
      `, [
            normalized.candidateKey,
            normalized.occurredAt,
            normalized.ga4UserKey,
            normalized.ga4ClientId,
            normalized.ga4SessionId,
            normalized.transactionId,
            normalized.emailHash,
            normalized.customerIdentityId,
            normalized.source,
            normalized.medium,
            normalized.campaign,
            normalized.content,
            normalized.term,
            normalized.clickIdType,
            normalized.clickIdValue,
            normalized.sessionHasRequiredFields,
            normalized.sourceExportHour,
            normalized.sourceDataset,
            normalized.sourceTableType,
            normalized.retainedUntil
        ]);
        upsertedRows += result.rowCount ?? 0;
    }
    return upsertedRows;
}
function countPopulatedDimensions(row) {
    return [row.source, row.medium, row.campaign, row.content, row.term].filter(Boolean).length;
}
function compareSourceTableType(left, right) {
    const precedence = {
        events: 0,
        intraday: 1
    };
    return precedence[left] - precedence[right];
}
function compareLexical(left, right) {
    return (left ?? '').localeCompare(right ?? '');
}
function compareLookupRows(left, right) {
    const occurredAtComparison = right.occurred_at.getTime() - left.occurred_at.getTime();
    if (occurredAtComparison !== 0) {
        return occurredAtComparison;
    }
    const clickComparison = Number(Boolean(right.click_id_value)) - Number(Boolean(left.click_id_value));
    if (clickComparison !== 0) {
        return clickComparison;
    }
    const exportFreshnessComparison = right.source_export_hour.getTime() - left.source_export_hour.getTime();
    if (exportFreshnessComparison !== 0) {
        return exportFreshnessComparison;
    }
    const sourceTableTypeComparison = compareSourceTableType(left.source_table_type, right.source_table_type);
    if (sourceTableTypeComparison !== 0) {
        return sourceTableTypeComparison;
    }
    const dimensionComparison = countPopulatedDimensions(right) - countPopulatedDimensions(left);
    if (dimensionComparison !== 0) {
        return dimensionComparison;
    }
    const sessionComparison = compareLexical(left.ga4_session_id, right.ga4_session_id);
    if (sessionComparison !== 0) {
        return sessionComparison;
    }
    const clientComparison = compareLexical(left.ga4_client_id, right.ga4_client_id);
    if (clientComparison !== 0) {
        return clientComparison;
    }
    const transactionComparison = compareLexical(left.transaction_id, right.transaction_id);
    if (transactionComparison !== 0) {
        return transactionComparison;
    }
    return compareLexical(left.candidate_key, right.candidate_key);
}
export async function lookupGa4FallbackCandidates(input, client) {
    const orderOccurredAt = new Date(input.orderOccurredAt).toISOString();
    const lookbackDays = normalizePositiveInteger(input.lookbackDays, ATTRIBUTION_WINDOW_DAYS);
    const perKeyLimit = normalizePositiveInteger(input.limit, 25);
    const customerIdentityId = normalizeNullableString(input.customerIdentityId);
    const emailHash = normalizeEmailHash(input.emailHash);
    const transactionId = normalizeNullableString(input.transactionId);
    if (!customerIdentityId && !emailHash && !transactionId) {
        return [];
    }
    const db = client;
    const result = await executeQuery(db, `
      WITH candidate_pool AS (
        SELECT *
        FROM (
          SELECT
            candidate_key,
            occurred_at,
            ga4_user_key,
            ga4_client_id,
            ga4_session_id,
            transaction_id,
            email_hash,
            customer_identity_id::text AS customer_identity_id,
            source,
            medium,
            campaign,
            content,
            term,
            click_id_type,
            click_id_value,
            session_has_required_fields,
            source_export_hour,
            source_dataset,
            source_table_type,
            retained_until,
            created_at,
            updated_at,
            'customer_identity_id'::text AS matched_on
          FROM ga4_fallback_candidates
          WHERE $1::uuid IS NOT NULL
            AND customer_identity_id = $1::uuid
            AND occurred_at <= $4::timestamptz
            AND occurred_at >= $4::timestamptz - ($5::int || ' days')::interval
          ORDER BY occurred_at DESC, ga4_session_id ASC
          LIMIT $6
        ) customer_identity_matches

        UNION ALL

        SELECT *
        FROM (
          SELECT
            candidate_key,
            occurred_at,
            ga4_user_key,
            ga4_client_id,
            ga4_session_id,
            transaction_id,
            email_hash,
            customer_identity_id::text AS customer_identity_id,
            source,
            medium,
            campaign,
            content,
            term,
            click_id_type,
            click_id_value,
            session_has_required_fields,
            source_export_hour,
            source_dataset,
            source_table_type,
            retained_until,
            created_at,
            updated_at,
            'email_hash'::text AS matched_on
          FROM ga4_fallback_candidates
          WHERE $2::text IS NOT NULL
            AND email_hash = $2
            AND occurred_at <= $4::timestamptz
            AND occurred_at >= $4::timestamptz - ($5::int || ' days')::interval
          ORDER BY occurred_at DESC, ga4_session_id ASC
          LIMIT $6
        ) email_hash_matches

        UNION ALL

        SELECT *
        FROM (
          SELECT
            candidate_key,
            occurred_at,
            ga4_user_key,
            ga4_client_id,
            ga4_session_id,
            transaction_id,
            email_hash,
            customer_identity_id::text AS customer_identity_id,
            source,
            medium,
            campaign,
            content,
            term,
            click_id_type,
            click_id_value,
            session_has_required_fields,
            source_export_hour,
            source_dataset,
            source_table_type,
            retained_until,
            created_at,
            updated_at,
            'transaction_id'::text AS matched_on
          FROM ga4_fallback_candidates
          WHERE $3::text IS NOT NULL
            AND transaction_id = $3
            AND occurred_at <= $4::timestamptz
            AND occurred_at >= $4::timestamptz - ($5::int || ' days')::interval
          ORDER BY occurred_at DESC, ga4_session_id ASC
          LIMIT $6
        ) transaction_matches
      )
      SELECT DISTINCT ON (candidate_key, occurred_at)
        candidate_key,
        occurred_at,
        ga4_user_key,
        ga4_client_id,
        ga4_session_id,
        transaction_id,
        email_hash,
        customer_identity_id,
        source,
        medium,
        campaign,
        content,
        term,
        click_id_type,
        click_id_value,
        session_has_required_fields,
        source_export_hour,
        source_dataset,
        source_table_type,
        retained_until,
        matched_on
      FROM candidate_pool
      WHERE session_has_required_fields = true
        AND (
          click_id_value IS NOT NULL
          OR source IS NOT NULL
          OR medium IS NOT NULL
          OR campaign IS NOT NULL
          OR content IS NOT NULL
          OR term IS NOT NULL
        )
      ORDER BY candidate_key ASC, occurred_at ASC, matched_on ASC
    `, [customerIdentityId, emailHash, transactionId, orderOccurredAt, lookbackDays, perKeyLimit]);
    return result.rows.sort(compareLookupRows).map(mapPersistedCandidate);
}
export async function listGa4FallbackCandidates(client) {
    const db = client;
    const result = await executeQuery(db, `
      SELECT
        candidate_key,
        occurred_at,
        ga4_user_key,
        ga4_client_id,
        ga4_session_id,
        transaction_id,
        email_hash,
        customer_identity_id::text AS customer_identity_id,
        source,
        medium,
        campaign,
        content,
        term,
        click_id_type,
        click_id_value,
        session_has_required_fields,
        source_export_hour,
        source_dataset,
        source_table_type,
        retained_until
      FROM ga4_fallback_candidates
      ORDER BY occurred_at DESC, candidate_key ASC
    `);
    return result.rows.map(mapPersistedCandidate);
}
