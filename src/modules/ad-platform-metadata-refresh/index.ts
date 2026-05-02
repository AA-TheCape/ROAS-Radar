import { randomUUID } from 'node:crypto';

import { withTransaction, query } from '../../db/pool.js';
import { logError, logInfo } from '../../observability/index.js';

export type MetadataPlatform = 'google_ads' | 'meta_ads';
export type MetadataEntityType = 'campaign' | 'adset' | 'ad';
export type CampaignMetadataBackfillRunStatus = 'processing' | 'completed' | 'failed';

type CoverageRow = {
  platform: MetadataPlatform;
  entity_type: MetadataEntityType;
  total_entities: string | number;
  named_entities_in_history: string | number;
  resolved_entities: string | number;
};

type MergePlanRow = {
  to_insert: string | number;
  to_update: string | number;
};

type UnresolvedSampleRow = {
  platform: MetadataPlatform;
  account_id: string;
  entity_type: MetadataEntityType;
  entity_id: string;
  last_seen_at: Date;
  had_name_in_history: boolean;
};

export type CampaignMetadataCoverageBreakdown = {
  platform: MetadataPlatform;
  entityType: MetadataEntityType;
  totalEntities: number;
  namedEntitiesInHistory: number;
  resolvedEntities: number;
  unresolvedEntities: number;
  resolvedRate: number;
  unresolvedRate: number;
};

export type CampaignMetadataUnresolvedSample = {
  platform: MetadataPlatform;
  accountId: string;
  entityType: MetadataEntityType;
  entityId: string;
  lastSeenAt: string;
  hadNameInHistory: boolean;
};

export type CampaignMetadataBackfillReport = {
  runId: string;
  status: CampaignMetadataBackfillRunStatus;
  requestedBy: string;
  workerId: string;
  windowStart: string;
  windowEnd: string;
  dryRun: boolean;
  plannedInserts: number;
  plannedUpdates: number;
  coverageBefore: CampaignMetadataCoverageBreakdown[];
  coverageAfter: CampaignMetadataCoverageBreakdown[];
  campaignCoverageBefore: {
    totalEntities: number;
    resolvedEntities: number;
    resolvedRate: number;
  };
  campaignCoverageAfter: {
    totalEntities: number;
    resolvedEntities: number;
    resolvedRate: number;
  };
  unresolvedRate: {
    totalEntities: number;
    unresolvedEntities: number;
    unresolvedRate: number;
  };
  unresolvedSamples: CampaignMetadataUnresolvedSample[];
  startedAt: string;
  completedAt: string | null;
};

export type CampaignMetadataBackfillOptions = {
  requestedBy: string;
  workerId: string;
  startDate: string;
  endDate: string;
  dryRun?: boolean;
  unresolvedSampleLimit?: number;
  runId?: string | null;
};

const DEFAULT_UNRESOLVED_SAMPLE_LIMIT = 25;
const VALID_DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

const SOURCE_ENTITIES_CTE = `
  WITH source_entities AS (
    SELECT
      'meta_ads'::text AS platform,
      NULLIF(btrim(account_id), '') AS account_id,
      'campaign'::text AS entity_type,
      NULLIF(btrim(campaign_id), '') AS entity_id,
      NULLIF(regexp_replace(btrim(campaign_name), '\\s+', ' ', 'g'), '') AS latest_name,
      (report_date::timestamptz + interval '1 day' - interval '1 second') AS last_seen_at
    FROM meta_ads_daily_spend
    WHERE report_date BETWEEN $1::date AND $2::date
      AND granularity IN ('campaign', 'adset', 'ad', 'creative')

    UNION ALL

    SELECT
      'meta_ads'::text AS platform,
      NULLIF(btrim(account_id), '') AS account_id,
      'adset'::text AS entity_type,
      NULLIF(btrim(adset_id), '') AS entity_id,
      NULLIF(regexp_replace(btrim(adset_name), '\\s+', ' ', 'g'), '') AS latest_name,
      (report_date::timestamptz + interval '1 day' - interval '1 second') AS last_seen_at
    FROM meta_ads_daily_spend
    WHERE report_date BETWEEN $1::date AND $2::date
      AND granularity IN ('adset', 'ad', 'creative')

    UNION ALL

    SELECT
      'meta_ads'::text AS platform,
      NULLIF(btrim(account_id), '') AS account_id,
      'ad'::text AS entity_type,
      NULLIF(btrim(ad_id), '') AS entity_id,
      NULLIF(regexp_replace(btrim(ad_name), '\\s+', ' ', 'g'), '') AS latest_name,
      (report_date::timestamptz + interval '1 day' - interval '1 second') AS last_seen_at
    FROM meta_ads_daily_spend
    WHERE report_date BETWEEN $1::date AND $2::date
      AND granularity IN ('ad', 'creative')

    UNION ALL

    SELECT
      'google_ads'::text AS platform,
      NULLIF(btrim(account_id), '') AS account_id,
      'campaign'::text AS entity_type,
      NULLIF(btrim(campaign_id), '') AS entity_id,
      NULLIF(regexp_replace(btrim(campaign_name), '\\s+', ' ', 'g'), '') AS latest_name,
      (report_date::timestamptz + interval '1 day' - interval '1 second') AS last_seen_at
    FROM google_ads_daily_spend
    WHERE report_date BETWEEN $1::date AND $2::date
      AND granularity IN ('campaign', 'adset', 'ad', 'creative')

    UNION ALL

    SELECT
      'google_ads'::text AS platform,
      NULLIF(btrim(account_id), '') AS account_id,
      'adset'::text AS entity_type,
      NULLIF(btrim(adset_id), '') AS entity_id,
      NULLIF(regexp_replace(btrim(adset_name), '\\s+', ' ', 'g'), '') AS latest_name,
      (report_date::timestamptz + interval '1 day' - interval '1 second') AS last_seen_at
    FROM google_ads_daily_spend
    WHERE report_date BETWEEN $1::date AND $2::date
      AND granularity IN ('adset', 'ad', 'creative')

    UNION ALL

    SELECT
      'google_ads'::text AS platform,
      NULLIF(btrim(account_id), '') AS account_id,
      'ad'::text AS entity_type,
      NULLIF(btrim(ad_id), '') AS entity_id,
      NULLIF(regexp_replace(btrim(ad_name), '\\s+', ' ', 'g'), '') AS latest_name,
      (report_date::timestamptz + interval '1 day' - interval '1 second') AS last_seen_at
    FROM google_ads_daily_spend
    WHERE report_date BETWEEN $1::date AND $2::date
      AND granularity IN ('ad', 'creative')
  ),
  distinct_entities AS (
    SELECT DISTINCT
      platform,
      account_id,
      entity_type,
      entity_id
    FROM source_entities
    WHERE account_id IS NOT NULL
      AND entity_id IS NOT NULL
  ),
  latest_named_entities AS (
    SELECT DISTINCT ON (platform, account_id, entity_type, entity_id)
      platform,
      account_id,
      entity_type,
      entity_id,
      latest_name,
      last_seen_at
    FROM source_entities
    WHERE account_id IS NOT NULL
      AND entity_id IS NOT NULL
      AND latest_name IS NOT NULL
    ORDER BY platform, account_id, entity_type, entity_id, last_seen_at DESC, latest_name ASC
  )
`;

function normalizeRequiredString(value: string, fieldName: string): string {
  const normalized = value.trim();

  if (!normalized) {
    throw new Error(`Campaign metadata backfill requires ${fieldName}`);
  }

  return normalized;
}

function normalizeDateString(value: string, fieldName: string): string {
  const normalized = normalizeRequiredString(value, fieldName);

  if (!VALID_DATE_PATTERN.test(normalized)) {
    throw new Error(`Campaign metadata backfill ${fieldName} must use YYYY-MM-DD format`);
  }

  return normalized;
}

function normalizeUnresolvedSampleLimit(value?: number): number {
  const normalized = Math.floor(value ?? DEFAULT_UNRESOLVED_SAMPLE_LIMIT);

  if (!Number.isFinite(normalized) || normalized <= 0) {
    throw new Error('Campaign metadata backfill unresolvedSampleLimit must be a positive integer');
  }

  return normalized;
}

function toInteger(value: string | number): number {
  return typeof value === 'number' ? value : Number.parseInt(value, 10);
}

function buildCoverageBreakdown(rows: CoverageRow[]): CampaignMetadataCoverageBreakdown[] {
  return rows.map((row) => {
    const totalEntities = toInteger(row.total_entities);
    const resolvedEntities = toInteger(row.resolved_entities);
    const namedEntitiesInHistory = toInteger(row.named_entities_in_history);
    const unresolvedEntities = Math.max(totalEntities - resolvedEntities, 0);
    const resolvedRate = totalEntities > 0 ? resolvedEntities / totalEntities : 0;
    const unresolvedRate = totalEntities > 0 ? unresolvedEntities / totalEntities : 0;

    return {
      platform: row.platform,
      entityType: row.entity_type,
      totalEntities,
      namedEntitiesInHistory,
      resolvedEntities,
      unresolvedEntities,
      resolvedRate,
      unresolvedRate
    };
  });
}

function summarizeCampaignCoverage(rows: CampaignMetadataCoverageBreakdown[]) {
  const campaignRows = rows.filter((row) => row.entityType === 'campaign');
  const totalEntities = campaignRows.reduce((sum, row) => sum + row.totalEntities, 0);
  const resolvedEntities = campaignRows.reduce((sum, row) => sum + row.resolvedEntities, 0);

  return {
    totalEntities,
    resolvedEntities,
    resolvedRate: totalEntities > 0 ? resolvedEntities / totalEntities : 0
  };
}

function summarizeUnresolvedRate(rows: CampaignMetadataCoverageBreakdown[]) {
  const totalEntities = rows.reduce((sum, row) => sum + row.totalEntities, 0);
  const unresolvedEntities = rows.reduce((sum, row) => sum + row.unresolvedEntities, 0);

  return {
    totalEntities,
    unresolvedEntities,
    unresolvedRate: totalEntities > 0 ? unresolvedEntities / totalEntities : 0
  };
}

function buildBackfillReport(input: {
  runId: string;
  requestedBy: string;
  workerId: string;
  startDate: string;
  endDate: string;
  dryRun: boolean;
  plannedInserts: number;
  plannedUpdates: number;
  coverageBefore: CoverageRow[];
  coverageAfter: CoverageRow[];
  unresolvedSamples: UnresolvedSampleRow[];
  startedAt: string;
  completedAt: string | null;
  status: CampaignMetadataBackfillRunStatus;
}): CampaignMetadataBackfillReport {
  const coverageBefore = buildCoverageBreakdown(input.coverageBefore);
  const coverageAfter = buildCoverageBreakdown(input.coverageAfter);

  return {
    runId: input.runId,
    status: input.status,
    requestedBy: input.requestedBy,
    workerId: input.workerId,
    windowStart: input.startDate,
    windowEnd: input.endDate,
    dryRun: input.dryRun,
    plannedInserts: input.plannedInserts,
    plannedUpdates: input.plannedUpdates,
    coverageBefore,
    coverageAfter,
    campaignCoverageBefore: summarizeCampaignCoverage(coverageBefore),
    campaignCoverageAfter: summarizeCampaignCoverage(coverageAfter),
    unresolvedRate: summarizeUnresolvedRate(coverageAfter),
    unresolvedSamples: input.unresolvedSamples.map((row) => ({
      platform: row.platform,
      accountId: row.account_id,
      entityType: row.entity_type,
      entityId: row.entity_id,
      lastSeenAt: row.last_seen_at.toISOString(),
      hadNameInHistory: row.had_name_in_history
    })),
    startedAt: input.startedAt,
    completedAt: input.completedAt
  };
}

async function loadCoverageRows(client: { query: typeof query }, startDate: string, endDate: string): Promise<CoverageRow[]> {
  const result = await client.query<CoverageRow>(
    `
      ${SOURCE_ENTITIES_CTE}
      SELECT
        entities.platform,
        entities.entity_type,
        COUNT(*)::int AS total_entities,
        COUNT(*) FILTER (
          WHERE EXISTS (
            SELECT 1
            FROM source_entities named
            WHERE named.platform = entities.platform
              AND named.account_id = entities.account_id
              AND named.entity_type = entities.entity_type
              AND named.entity_id = entities.entity_id
              AND named.latest_name IS NOT NULL
          )
        )::int AS named_entities_in_history,
        COUNT(*) FILTER (
          WHERE EXISTS (
            SELECT 1
            FROM ad_platform_entity_metadata metadata
            WHERE metadata.platform = entities.platform
              AND metadata.account_id = entities.account_id
              AND metadata.entity_type = entities.entity_type
              AND metadata.entity_id = entities.entity_id
              AND metadata.tenant_id IS NULL
              AND metadata.workspace_id IS NULL
          )
        )::int AS resolved_entities
      FROM distinct_entities entities
      GROUP BY entities.platform, entities.entity_type
      ORDER BY entities.platform ASC, entities.entity_type ASC
    `,
    [startDate, endDate]
  );

  return result.rows;
}

async function loadMergePlan(client: { query: typeof query }, startDate: string, endDate: string): Promise<MergePlanRow> {
  const result = await client.query<MergePlanRow>(
    `
      ${SOURCE_ENTITIES_CTE}
      SELECT
        COUNT(*) FILTER (WHERE metadata.id IS NULL)::int AS to_insert,
        COUNT(*) FILTER (
          WHERE metadata.id IS NOT NULL
            AND (
              latest_named_entities.last_seen_at > metadata.last_seen_at
              OR latest_named_entities.latest_name IS DISTINCT FROM metadata.latest_name
            )
        )::int AS to_update
      FROM latest_named_entities
      LEFT JOIN ad_platform_entity_metadata metadata
        ON metadata.platform = latest_named_entities.platform
        AND metadata.account_id = latest_named_entities.account_id
        AND metadata.entity_type = latest_named_entities.entity_type
        AND metadata.entity_id = latest_named_entities.entity_id
        AND metadata.tenant_id IS NULL
        AND metadata.workspace_id IS NULL
    `,
    [startDate, endDate]
  );

  return result.rows[0] ?? { to_insert: 0, to_update: 0 };
}

async function upsertLatestNames(client: { query: typeof query }, startDate: string, endDate: string): Promise<void> {
  await client.query(
    `
      ${SOURCE_ENTITIES_CTE}
      MERGE INTO ad_platform_entity_metadata AS metadata
      USING latest_named_entities
      ON metadata.platform = latest_named_entities.platform
        AND metadata.account_id = latest_named_entities.account_id
        AND metadata.entity_type = latest_named_entities.entity_type
        AND metadata.entity_id = latest_named_entities.entity_id
        AND metadata.tenant_id IS NULL
        AND metadata.workspace_id IS NULL
      WHEN MATCHED AND (
        latest_named_entities.last_seen_at > metadata.last_seen_at
        OR latest_named_entities.latest_name IS DISTINCT FROM metadata.latest_name
      ) THEN
        UPDATE SET
          latest_name = latest_named_entities.latest_name,
          last_seen_at = GREATEST(metadata.last_seen_at, latest_named_entities.last_seen_at),
          updated_at = now()
      WHEN NOT MATCHED THEN
        INSERT (
          tenant_id,
          workspace_id,
          platform,
          account_id,
          entity_type,
          entity_id,
          latest_name,
          last_seen_at,
          updated_at
        )
        VALUES (
          NULL,
          NULL,
          latest_named_entities.platform,
          latest_named_entities.account_id,
          latest_named_entities.entity_type,
          latest_named_entities.entity_id,
          latest_named_entities.latest_name,
          latest_named_entities.last_seen_at,
          now()
        )
    `,
    [startDate, endDate]
  );
}

async function loadUnresolvedSamples(
  client: { query: typeof query },
  startDate: string,
  endDate: string,
  unresolvedSampleLimit: number
): Promise<UnresolvedSampleRow[]> {
  const result = await client.query<UnresolvedSampleRow>(
    `
      ${SOURCE_ENTITIES_CTE}
      SELECT
        entities.platform,
        entities.account_id,
        entities.entity_type,
        entities.entity_id,
        MAX(source_entities.last_seen_at) AS last_seen_at,
        BOOL_OR(source_entities.latest_name IS NOT NULL) AS had_name_in_history
      FROM distinct_entities entities
      JOIN source_entities
        ON source_entities.platform = entities.platform
        AND source_entities.account_id = entities.account_id
        AND source_entities.entity_type = entities.entity_type
        AND source_entities.entity_id = entities.entity_id
      LEFT JOIN ad_platform_entity_metadata metadata
        ON metadata.platform = entities.platform
        AND metadata.account_id = entities.account_id
        AND metadata.entity_type = entities.entity_type
        AND metadata.entity_id = entities.entity_id
        AND metadata.tenant_id IS NULL
        AND metadata.workspace_id IS NULL
      WHERE metadata.id IS NULL
      GROUP BY entities.platform, entities.account_id, entities.entity_type, entities.entity_id
      ORDER BY MAX(source_entities.last_seen_at) DESC, entities.platform ASC, entities.entity_type ASC, entities.entity_id ASC
      LIMIT $3
    `,
    [startDate, endDate, unresolvedSampleLimit]
  );

  return result.rows;
}

export async function getCampaignMetadataBackfillRun(runId: string): Promise<CampaignMetadataBackfillReport | null> {
  const normalizedRunId = normalizeRequiredString(runId, 'runId');
  const result = await query<{ report: CampaignMetadataBackfillReport | null }>(
    `
      SELECT report
      FROM campaign_metadata_backfill_runs
      WHERE id = $1::uuid
    `,
    [normalizedRunId]
  );

  return result.rows[0]?.report ?? null;
}

export async function backfillCampaignMetadataHistory(
  options: CampaignMetadataBackfillOptions
): Promise<CampaignMetadataBackfillReport> {
  const requestedBy = normalizeRequiredString(options.requestedBy, 'requestedBy');
  const workerId = normalizeRequiredString(options.workerId, 'workerId');
  const startDate = normalizeDateString(options.startDate, 'startDate');
  const endDate = normalizeDateString(options.endDate, 'endDate');
  const dryRun = options.dryRun ?? false;
  const unresolvedSampleLimit = normalizeUnresolvedSampleLimit(options.unresolvedSampleLimit);
  const runId = options.runId?.trim() || randomUUID();

  if (startDate > endDate) {
    throw new Error('Campaign metadata backfill startDate must be on or before endDate');
  }

  const startedAt = new Date().toISOString();

  await query(
    `
      INSERT INTO campaign_metadata_backfill_runs (
        id,
        status,
        requested_by,
        worker_id,
        started_at,
        window_start,
        window_end,
        dry_run,
        report,
        updated_at
      )
      VALUES (
        $1::uuid,
        'processing',
        $2,
        $3,
        $4::timestamptz,
        $5::date,
        $6::date,
        $7::boolean,
        '{}'::jsonb,
        now()
      )
    `,
    [runId, requestedBy, workerId, startedAt, startDate, endDate, dryRun]
  );

  logInfo('campaign_metadata_backfill_started', {
    runId,
    requestedBy,
    workerId,
    startDate,
    endDate,
    dryRun,
    unresolvedSampleLimit
  });

  try {
    const report = await withTransaction(async (client) => {
      const coverageBefore = await loadCoverageRows(client, startDate, endDate);
      const mergePlan = await loadMergePlan(client, startDate, endDate);

      if (!dryRun) {
        await upsertLatestNames(client, startDate, endDate);
      }

      const coverageAfter = await loadCoverageRows(client, startDate, endDate);
      const unresolvedSamples = await loadUnresolvedSamples(client, startDate, endDate, unresolvedSampleLimit);
      const completedAt = new Date().toISOString();

      const builtReport = buildBackfillReport({
        runId,
        requestedBy,
        workerId,
        startDate,
        endDate,
        dryRun,
        plannedInserts: toInteger(mergePlan.to_insert),
        plannedUpdates: toInteger(mergePlan.to_update),
        coverageBefore,
        coverageAfter,
        unresolvedSamples,
        startedAt,
        completedAt,
        status: 'completed'
      });

      await client.query(
        `
          UPDATE campaign_metadata_backfill_runs
          SET
            status = 'completed',
            completed_at = $2::timestamptz,
            report = $3::jsonb,
            updated_at = now()
          WHERE id = $1::uuid
        `,
        [runId, completedAt, JSON.stringify(builtReport)]
      );

      return builtReport;
    });

    logInfo('campaign_metadata_backfill_completed', report);
    return report;
  } catch (error) {
    const completedAt = new Date().toISOString();
    const failureMessage = error instanceof Error ? error.message : String(error);
    const failureCode = error instanceof Error && error.name.trim() ? error.name.trim() : 'campaign_metadata_backfill_failed';

    logError('campaign_metadata_backfill_failed', error, {
      runId,
      requestedBy,
      workerId,
      startDate,
      endDate,
      dryRun
    });

    await query(
      `
        UPDATE campaign_metadata_backfill_runs
        SET
          status = 'failed',
          completed_at = $2::timestamptz,
          error_code = $3,
          error_message = $4,
          updated_at = now()
        WHERE id = $1::uuid
      `,
      [runId, completedAt, failureCode, failureMessage]
    ).catch(() => undefined);

    throw error;
  }
}
