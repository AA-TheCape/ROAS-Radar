import { createHash } from 'node:crypto';

import type { PoolClient } from 'pg';

import { env } from '../../config/env.js';
import { query } from '../../db/pool.js';

const ATTRIBUTION_WINDOW_DAYS = 7;
const DEFAULT_RETENTION_DAYS = 35;

type PersistedGa4FallbackCandidateRow = {
  candidate_key: string;
  occurred_at: Date;
  ga4_user_key: string;
  ga4_client_id: string | null;
  ga4_session_id: string | null;
  transaction_id: string | null;
  email_hash: string | null;
  customer_identity_id: string | null;
  source: string | null;
  medium: string | null;
  campaign: string | null;
  content: string | null;
  term: string | null;
  click_id_type: string | null;
  click_id_value: string | null;
  session_has_required_fields: boolean;
  source_export_hour: Date;
  source_dataset: string;
  source_table_type: 'events' | 'intraday';
  retained_until: Date;
};

type LookupRow = PersistedGa4FallbackCandidateRow & {
  matched_on: 'customer_identity_id' | 'email_hash' | 'transaction_id';
};

export type Ga4FallbackCandidateInput = {
  occurredAt: string;
  ga4UserKey: string;
  ga4ClientId?: string | null;
  ga4SessionId?: string | null;
  transactionId?: string | null;
  emailHash?: string | null;
  customerIdentityId?: string | null;
  source?: string | null;
  medium?: string | null;
  campaign?: string | null;
  content?: string | null;
  term?: string | null;
  clickIdType?: string | null;
  clickIdValue?: string | null;
  sessionHasRequiredFields: boolean;
  sourceExportHour: string;
  sourceDataset: string;
  sourceTableType: 'events' | 'intraday';
  retainedUntil?: string | null;
};

export type PersistedGa4FallbackCandidate = {
  candidateKey: string;
  occurredAt: string;
  ga4UserKey: string;
  ga4ClientId: string | null;
  ga4SessionId: string | null;
  transactionId: string | null;
  emailHash: string | null;
  customerIdentityId: string | null;
  source: string | null;
  medium: string | null;
  campaign: string | null;
  content: string | null;
  term: string | null;
  clickIdType: string | null;
  clickIdValue: string | null;
  sessionHasRequiredFields: boolean;
  sourceExportHour: string;
  sourceDataset: string;
  sourceTableType: 'events' | 'intraday';
  retainedUntil: string;
};

export type Ga4FallbackLookupInput = {
  orderOccurredAt: string;
  customerIdentityId?: string | null;
  emailHash?: string | null;
  transactionId?: string | null;
  lookbackDays?: number;
  limit?: number;
};

function normalizeNullableString(value: string | null | undefined, { lowerCase = false } = {}): string | null {
  if (typeof value !== 'string') {
    return null;
  }

  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }

  return lowerCase ? trimmed.toLowerCase() : trimmed;
}

function normalizeEmailHash(value: string | null | undefined): string | null {
  const normalized = normalizeNullableString(value, { lowerCase: true });
  if (!normalized) {
    return null;
  }

  return /^[0-9a-f]{64}$/.test(normalized) ? normalized : null;
}

function normalizePositiveInteger(value: number | undefined, fallback: number): number {
  if (!Number.isFinite(value)) {
    return fallback;
  }

  return Math.max(Math.trunc(value ?? fallback), 1);
}

function buildRetentionTimestamp(occurredAtIso: string): string {
  const occurredAt = new Date(occurredAtIso);
  const retainedUntil = new Date(occurredAt);
  retainedUntil.setUTCDate(retainedUntil.getUTCDate() + (env.GA4_FALLBACK_RETENTION_DAYS || DEFAULT_RETENTION_DAYS));
  return retainedUntil.toISOString();
}

export function buildGa4FallbackCandidateKey(input: {
  occurredAt: string;
  ga4UserKey: string;
  ga4SessionId?: string | null;
  ga4ClientId?: string | null;
  transactionId?: string | null;
  emailHash?: string | null;
  customerIdentityId?: string | null;
}): string {
  const digest = createHash('sha256');
  digest.update(
    JSON.stringify({
      occurredAt: input.occurredAt,
      ga4UserKey: input.ga4UserKey,
      ga4SessionId: normalizeNullableString(input.ga4SessionId),
      ga4ClientId: normalizeNullableString(input.ga4ClientId),
      transactionId: normalizeNullableString(input.transactionId),
      emailHash: normalizeEmailHash(input.emailHash),
      customerIdentityId: normalizeNullableString(input.customerIdentityId)
    })
  );

  return digest.digest('hex');
}

function mapPersistedCandidate(row: PersistedGa4FallbackCandidateRow): PersistedGa4FallbackCandidate {
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

async function ensureTouchedPartitions(db: QueryExecutor, occurredAtValues: string[]): Promise<void> {
  const months = Array.from(
    new Set(
      occurredAtValues.map((value) => {
        const occurredAt = new Date(value);
        return new Date(Date.UTC(occurredAt.getUTCFullYear(), occurredAt.getUTCMonth(), 1)).toISOString().slice(0, 10);
      })
    )
  );

  for (const month of months) {
    await executeQuery(db, 'SELECT ensure_ga4_fallback_candidate_partition($1::date)', [month]);
  }
}

type QueryExecutor = PoolClient | null | undefined;

async function executeQuery<TResult extends Record<string, unknown> = Record<string, unknown>>(
  client: QueryExecutor,
  text: string,
  params?: unknown[]
) {
  if (client) {
    return client.query<TResult>(text, params);
  }

  return query<TResult>(text, params);
}

export async function upsertGa4FallbackCandidates(
  candidates: Ga4FallbackCandidateInput[],
  client?: PoolClient
): Promise<number> {
  if (candidates.length === 0) {
    return 0;
  }

  const db: QueryExecutor = client;
  await ensureTouchedPartitions(
    db,
    candidates.map((candidate) => candidate.occurredAt)
  );

  let upsertedRows = 0;

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

    if (
      !normalized.customerIdentityId &&
      !normalized.emailHash &&
      !normalized.transactionId &&
      !normalized.ga4ClientId &&
      !normalized.ga4SessionId
    ) {
      throw new Error('GA4 fallback candidate requires at least one lookup key');
    }

    const result = await executeQuery(
      db,
      `
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
          source = COALESCE(EXCLUDED.source, ga4_fallback_candidates.source),
          medium = COALESCE(EXCLUDED.medium, ga4_fallback_candidates.medium),
          campaign = COALESCE(EXCLUDED.campaign, ga4_fallback_candidates.campaign),
          content = COALESCE(EXCLUDED.content, ga4_fallback_candidates.content),
          term = COALESCE(EXCLUDED.term, ga4_fallback_candidates.term),
          click_id_type = COALESCE(EXCLUDED.click_id_type, ga4_fallback_candidates.click_id_type),
          click_id_value = COALESCE(EXCLUDED.click_id_value, ga4_fallback_candidates.click_id_value),
          session_has_required_fields = EXCLUDED.session_has_required_fields,
          source_export_hour = GREATEST(ga4_fallback_candidates.source_export_hour, EXCLUDED.source_export_hour),
          source_dataset = EXCLUDED.source_dataset,
          source_table_type = EXCLUDED.source_table_type,
          retained_until = GREATEST(ga4_fallback_candidates.retained_until, EXCLUDED.retained_until),
          updated_at = now()
      `,
      [
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
      ]
    );

    upsertedRows += result.rowCount ?? 0;
  }

  return upsertedRows;
}

function countPopulatedDimensions(row: Pick<LookupRow, 'source' | 'medium' | 'campaign' | 'content' | 'term'>): number {
  return [row.source, row.medium, row.campaign, row.content, row.term].filter(Boolean).length;
}

function compareLexical(left: string | null, right: string | null): number {
  return (left ?? '').localeCompare(right ?? '');
}

function compareLookupRows(left: LookupRow, right: LookupRow): number {
  const occurredAtComparison = right.occurred_at.getTime() - left.occurred_at.getTime();
  if (occurredAtComparison !== 0) {
    return occurredAtComparison;
  }

  const clickComparison = Number(Boolean(right.click_id_value)) - Number(Boolean(left.click_id_value));
  if (clickComparison !== 0) {
    return clickComparison;
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

export async function lookupGa4FallbackCandidates(
  input: Ga4FallbackLookupInput,
  client?: PoolClient
): Promise<PersistedGa4FallbackCandidate[]> {
  const orderOccurredAt = new Date(input.orderOccurredAt).toISOString();
  const lookbackDays = normalizePositiveInteger(input.lookbackDays, ATTRIBUTION_WINDOW_DAYS);
  const perKeyLimit = normalizePositiveInteger(input.limit, 25);
  const customerIdentityId = normalizeNullableString(input.customerIdentityId);
  const emailHash = normalizeEmailHash(input.emailHash);
  const transactionId = normalizeNullableString(input.transactionId);

  if (!customerIdentityId && !emailHash && !transactionId) {
    return [];
  }

  const db: QueryExecutor = client;
  const result = await executeQuery<LookupRow>(
    db,
    `
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
    `,
    [customerIdentityId, emailHash, transactionId, orderOccurredAt, lookbackDays, perKeyLimit]
  );

  return result.rows.sort(compareLookupRows).map(mapPersistedCandidate);
}

export async function listGa4FallbackCandidates(client?: PoolClient): Promise<PersistedGa4FallbackCandidate[]> {
  const db: QueryExecutor = client;
  const result = await executeQuery<PersistedGa4FallbackCandidateRow>(
    db,
    `
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
    `
  );

  return result.rows.map(mapPersistedCandidate);
}
