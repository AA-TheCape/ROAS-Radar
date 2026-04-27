import { query, withTransaction } from '../../db/pool.js';
import { resolveGa4BigQueryIngestionConfig } from './ga4-bigquery-config.js';
const GA4_SESSION_ATTRIBUTION_PIPELINE = 'ga4_session_attribution';
const HOUR_IN_MILLISECONDS = 60 * 60 * 1000;
export async function createDefaultGa4BigQueryExecutor(config) {
    const importer = new Function('specifier', 'return import(specifier)');
    const { BigQuery } = await importer('@google-cloud/bigquery').catch((error) => {
        throw new Error(`GA4 BigQuery ingestion requires the optional @google-cloud/bigquery dependency: ${error instanceof Error ? error.message : String(error)}`);
    });
    const client = new BigQuery({
        projectId: config.ga4.projectId,
        location: config.ga4.location
    });
    return {
        async runQuery(input) {
            const [rows] = await client.query({
                query: input.query,
                params: input.params,
                location: input.location,
                useLegacySql: false
            });
            return rows;
        }
    };
}
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
function normalizeRequiredString(value, fieldName) {
    const normalized = normalizeNullableString(value);
    if (!normalized) {
        throw new Error(`GA4 row is missing required ${fieldName}`);
    }
    return normalized;
}
function normalizeDate(value, fieldName) {
    if (value instanceof Date) {
        if (Number.isNaN(value.getTime())) {
            throw new Error(`GA4 row has invalid ${fieldName}`);
        }
        return value.toISOString();
    }
    if (typeof value === 'string' || typeof value === 'number') {
        const normalized = new Date(value);
        if (Number.isNaN(normalized.getTime())) {
            throw new Error(`GA4 row has invalid ${fieldName}`);
        }
        return normalized.toISOString();
    }
    throw new Error(`GA4 row is missing required ${fieldName}`);
}
function floorToHour(date) {
    return new Date(Math.floor(date.getTime() / HOUR_IN_MILLISECONDS) * HOUR_IN_MILLISECONDS);
}
function addHours(date, hours) {
    return new Date(date.getTime() + hours * HOUR_IN_MILLISECONDS);
}
function compareIsoAscending(left, right) {
    return left.localeCompare(right);
}
function compareIsoDescending(left, right) {
    return right.localeCompare(left);
}
function selectPreferredClickId(row) {
    const clickIdPreference = [
        { type: 'gclid', value: row.gclid ?? null },
        { type: 'gbraid', value: row.gbraid ?? null },
        { type: 'wbraid', value: row.wbraid ?? null },
        { type: 'fbclid', value: row.fbclid ?? null },
        { type: 'ttclid', value: row.ttclid ?? null },
        { type: 'msclkid', value: row.msclkid ?? null }
    ];
    for (const candidate of clickIdPreference) {
        if (candidate.value) {
            return candidate;
        }
    }
    return { type: null, value: null };
}
function mapBigQueryRowToNormalizedRow(row) {
    const sourceTableType = normalizeRequiredString(row.source_table_type, 'source_table_type');
    if (sourceTableType !== 'events' && sourceTableType !== 'intraday') {
        throw new Error(`GA4 row has unsupported source_table_type: ${sourceTableType}`);
    }
    return {
        ga4SessionKey: normalizeRequiredString(row.ga4_session_key, 'ga4_session_key'),
        ga4UserKey: normalizeRequiredString(row.ga4_user_key, 'ga4_user_key'),
        ga4ClientId: normalizeNullableString(row.ga4_client_id),
        ga4SessionId: normalizeRequiredString(row.ga4_session_id, 'ga4_session_id'),
        sessionStartedAt: normalizeDate(row.session_started_at, 'session_started_at'),
        lastEventAt: normalizeDate(row.last_event_at, 'last_event_at'),
        source: normalizeNullableString(row.source, { lowerCase: true }),
        medium: normalizeNullableString(row.medium, { lowerCase: true }),
        campaign: normalizeNullableString(row.campaign),
        content: normalizeNullableString(row.content),
        term: normalizeNullableString(row.term),
        clickIdType: normalizeNullableString(row.click_id_type, { lowerCase: true }),
        clickIdValue: normalizeNullableString(row.click_id_value),
        sourceExportHour: normalizeDate(row.source_export_hour, 'source_export_hour'),
        sourceDataset: normalizeRequiredString(row.source_dataset, 'source_dataset'),
        sourceTableType
    };
}
function mapNormalizedRowForPersistence(row) {
    return {
        ga4_session_key: row.ga4SessionKey,
        ga4_user_key: row.ga4UserKey,
        ga4_client_id: row.ga4ClientId,
        ga4_session_id: row.ga4SessionId,
        session_started_at: new Date(row.sessionStartedAt),
        last_event_at: new Date(row.lastEventAt),
        source: row.source,
        medium: row.medium,
        campaign: row.campaign,
        content: row.content,
        term: row.term,
        click_id_type: row.clickIdType,
        click_id_value: row.clickIdValue,
        source_export_hour: new Date(row.sourceExportHour),
        source_dataset: row.sourceDataset,
        source_table_type: row.sourceTableType
    };
}
function executeQuery(executor, sql, params) {
    if (typeof executor === 'function') {
        return executor(sql, params);
    }
    return executor.query(sql, params);
}
async function ensureIngestionStateRow(executor) {
    await executeQuery(executor, `
      INSERT INTO ga4_bigquery_ingestion_state (
        pipeline_name,
        last_run_status
      )
      VALUES ($1, 'idle')
      ON CONFLICT (pipeline_name) DO NOTHING
    `, [GA4_SESSION_ATTRIBUTION_PIPELINE]);
}
async function readWatermarkHour(executor = query) {
    await ensureIngestionStateRow(executor);
    const result = await executeQuery(executor, `
      SELECT watermark_hour
      FROM ga4_bigquery_ingestion_state
      WHERE pipeline_name = $1
      LIMIT 1
    `, [GA4_SESSION_ATTRIBUTION_PIPELINE]);
    return result.rows[0]?.watermark_hour ?? null;
}
function normalizeEnabledConfig(config) {
    const resolved = config ?? resolveGa4BigQueryIngestionConfig();
    if (!resolved.enabled) {
        throw new Error('GA4 BigQuery ingestion is disabled');
    }
    return resolved;
}
export function planGa4SessionAttributionHourlyWindows(input) {
    const enabledConfig = normalizeEnabledConfig(input.config);
    const now = input.now ?? new Date();
    const latestCompleteHour = addHours(floorToHour(now), -1);
    if (latestCompleteHour.getTime() < 0) {
        return [];
    }
    const startHour = input.watermarkHour
        ? addHours(floorToHour(input.watermarkHour), -(enabledConfig.ga4.backfillHours - 1))
        : addHours(latestCompleteHour, -(enabledConfig.ga4.lookbackHours - 1));
    const hours = [];
    for (let cursor = floorToHour(startHour); cursor.getTime() <= latestCompleteHour.getTime(); cursor = addHours(cursor, 1)) {
        hours.push({
            hourStart: cursor.toISOString(),
            hourEndExclusive: addHours(cursor, 1).toISOString()
        });
    }
    return hours;
}
export function buildGa4SessionAttributionHourlyQuery(input) {
    const config = normalizeEnabledConfig(input.config);
    const hourStart = normalizeDate(input.hourStart, 'hourStart');
    const hourEndExclusive = normalizeDate(input.hourEndExclusive, 'hourEndExclusive');
    const startDateSuffix = hourStart.slice(0, 10).replaceAll('-', '');
    const endDateSuffix = hourEndExclusive.slice(0, 10).replaceAll('-', '');
    return {
        location: config.ga4.location,
        params: {
            window_start: hourStart,
            window_end: hourEndExclusive,
            start_date_suffix: startDateSuffix,
            end_date_suffix: endDateSuffix,
            source_dataset: config.ga4.dataset,
            source_export_hour: hourStart
        },
        query: `
      WITH exported_events AS (
        SELECT
          'events' AS source_table_type,
          TIMESTAMP_MICROS(event_timestamp) AS occurred_at,
          user_pseudo_id,
          NULLIF(user_id, '') AS user_id,
          event_name,
          (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_id' LIMIT 1) AS ga_session_id,
          NULLIF(collected_traffic_source.manual_source, '') AS source,
          NULLIF(collected_traffic_source.manual_medium, '') AS medium,
          NULLIF(collected_traffic_source.manual_campaign_name, '') AS campaign,
          NULLIF(collected_traffic_source.manual_content, '') AS content,
          NULLIF(collected_traffic_source.manual_term, '') AS term,
          NULLIF((SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'gclid' LIMIT 1), '') AS gclid,
          NULLIF((SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'gbraid' LIMIT 1), '') AS gbraid,
          NULLIF((SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'wbraid' LIMIT 1), '') AS wbraid,
          NULLIF((SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'fbclid' LIMIT 1), '') AS fbclid,
          NULLIF((SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'ttclid' LIMIT 1), '') AS ttclid,
          NULLIF((SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'msclkid' LIMIT 1), '') AS msclkid
        FROM ${config.ga4.eventsTableExpression}
        WHERE NOT STARTS_WITH(_TABLE_SUFFIX, 'intraday_')
          AND _TABLE_SUFFIX BETWEEN @start_date_suffix AND @end_date_suffix
          AND TIMESTAMP_MICROS(event_timestamp) >= TIMESTAMP(@window_start)
          AND TIMESTAMP_MICROS(event_timestamp) < TIMESTAMP(@window_end)

        UNION ALL

        SELECT
          'intraday' AS source_table_type,
          TIMESTAMP_MICROS(event_timestamp) AS occurred_at,
          user_pseudo_id,
          NULLIF(user_id, '') AS user_id,
          event_name,
          (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_id' LIMIT 1) AS ga_session_id,
          NULLIF(collected_traffic_source.manual_source, '') AS source,
          NULLIF(collected_traffic_source.manual_medium, '') AS medium,
          NULLIF(collected_traffic_source.manual_campaign_name, '') AS campaign,
          NULLIF(collected_traffic_source.manual_content, '') AS content,
          NULLIF(collected_traffic_source.manual_term, '') AS term,
          NULLIF((SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'gclid' LIMIT 1), '') AS gclid,
          NULLIF((SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'gbraid' LIMIT 1), '') AS gbraid,
          NULLIF((SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'wbraid' LIMIT 1), '') AS wbraid,
          NULLIF((SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'fbclid' LIMIT 1), '') AS fbclid,
          NULLIF((SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'ttclid' LIMIT 1), '') AS ttclid,
          NULLIF((SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key = 'msclkid' LIMIT 1), '') AS msclkid
        FROM ${config.ga4.intradayTableExpression}
        WHERE _TABLE_SUFFIX BETWEEN @start_date_suffix AND @end_date_suffix
          AND TIMESTAMP_MICROS(event_timestamp) >= TIMESTAMP(@window_start)
          AND TIMESTAMP_MICROS(event_timestamp) < TIMESTAMP(@window_end)
      ),
      deduplicated_events AS (
        SELECT *
        FROM exported_events
        WHERE user_pseudo_id IS NOT NULL
          AND ga_session_id IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY user_pseudo_id, ga_session_id, occurred_at, event_name
          ORDER BY CASE source_table_type WHEN 'events' THEN 0 ELSE 1 END
        ) = 1
      ),
      session_rollup AS (
        SELECT
          CONCAT(user_pseudo_id, ':', CAST(ga_session_id AS STRING)) AS ga4_session_key,
          COALESCE(user_id, user_pseudo_id) AS ga4_user_key,
          user_pseudo_id AS ga4_client_id,
          CAST(ga_session_id AS STRING) AS ga4_session_id,
          MIN(occurred_at) AS session_started_at,
          MAX(occurred_at) AS last_event_at,
          ARRAY_AGG(source IGNORE NULLS ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS source,
          ARRAY_AGG(medium IGNORE NULLS ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS medium,
          ARRAY_AGG(campaign IGNORE NULLS ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS campaign,
          ARRAY_AGG(content IGNORE NULLS ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS content,
          ARRAY_AGG(term IGNORE NULLS ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS term,
          ARRAY_AGG(gclid IGNORE NULLS ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS gclid,
          ARRAY_AGG(gbraid IGNORE NULLS ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS gbraid,
          ARRAY_AGG(wbraid IGNORE NULLS ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS wbraid,
          ARRAY_AGG(fbclid IGNORE NULLS ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS fbclid,
          ARRAY_AGG(ttclid IGNORE NULLS ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS ttclid,
          ARRAY_AGG(msclkid IGNORE NULLS ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS msclkid,
          ARRAY_AGG(source_table_type ORDER BY occurred_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS source_table_type
        FROM deduplicated_events
        GROUP BY 1, 2, 3, 4
      )
      SELECT
        ga4_session_key,
        ga4_user_key,
        ga4_client_id,
        ga4_session_id,
        session_started_at,
        last_event_at,
        source,
        medium,
        campaign,
        content,
        term,
        CASE
          WHEN gclid IS NOT NULL THEN 'gclid'
          WHEN gbraid IS NOT NULL THEN 'gbraid'
          WHEN wbraid IS NOT NULL THEN 'wbraid'
          WHEN fbclid IS NOT NULL THEN 'fbclid'
          WHEN ttclid IS NOT NULL THEN 'ttclid'
          WHEN msclkid IS NOT NULL THEN 'msclkid'
          ELSE NULL
        END AS click_id_type,
        COALESCE(gclid, gbraid, wbraid, fbclid, ttclid, msclkid) AS click_id_value,
        TIMESTAMP(@source_export_hour) AS source_export_hour,
        @source_dataset AS source_dataset,
        source_table_type
      FROM session_rollup
      ORDER BY last_event_at DESC, ga4_session_key ASC
    `
    };
}
export async function extractGa4SessionAttributionForHour(input) {
    const config = normalizeEnabledConfig(input.config);
    const hourStartIso = normalizeDate(input.hourStart, 'hourStart');
    const hourEndIso = addHours(new Date(hourStartIso), 1).toISOString();
    const queryInput = buildGa4SessionAttributionHourlyQuery({
        config,
        hourStart: hourStartIso,
        hourEndExclusive: hourEndIso
    });
    const rows = await input.executor.runQuery(queryInput);
    return {
        hourStart: hourStartIso,
        rows: rows.map((row) => {
            const normalizedRow = mapBigQueryRowToNormalizedRow(row);
            const clickId = selectPreferredClickId({
                gclid: normalizeNullableString(row.gclid),
                gbraid: normalizeNullableString(row.gbraid),
                wbraid: normalizeNullableString(row.wbraid),
                fbclid: normalizeNullableString(row.fbclid),
                ttclid: normalizeNullableString(row.ttclid),
                msclkid: normalizeNullableString(row.msclkid)
            });
            return {
                ...normalizedRow,
                clickIdType: normalizedRow.clickIdType ?? clickId.type,
                clickIdValue: normalizedRow.clickIdValue ?? clickId.value
            };
        })
    };
}
async function upsertGa4SessionAttributionRow(client, row) {
    await client.query(`
      INSERT INTO ga4_session_attribution (
        ga4_session_key,
        ga4_user_key,
        ga4_client_id,
        ga4_session_id,
        session_started_at,
        last_event_at,
        source,
        medium,
        campaign,
        content,
        term,
        click_id_type,
        click_id_value,
        source_export_hour,
        source_dataset,
        source_table_type,
        updated_at
      )
      VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, now()
      )
      ON CONFLICT (ga4_session_key)
      DO UPDATE SET
        ga4_user_key = EXCLUDED.ga4_user_key,
        ga4_client_id = COALESCE(EXCLUDED.ga4_client_id, ga4_session_attribution.ga4_client_id),
        ga4_session_id = EXCLUDED.ga4_session_id,
        session_started_at = LEAST(ga4_session_attribution.session_started_at, EXCLUDED.session_started_at),
        last_event_at = GREATEST(ga4_session_attribution.last_event_at, EXCLUDED.last_event_at),
        source = COALESCE(EXCLUDED.source, ga4_session_attribution.source),
        medium = COALESCE(EXCLUDED.medium, ga4_session_attribution.medium),
        campaign = COALESCE(EXCLUDED.campaign, ga4_session_attribution.campaign),
        content = COALESCE(EXCLUDED.content, ga4_session_attribution.content),
        term = COALESCE(EXCLUDED.term, ga4_session_attribution.term),
        click_id_type = COALESCE(EXCLUDED.click_id_type, ga4_session_attribution.click_id_type),
        click_id_value = COALESCE(EXCLUDED.click_id_value, ga4_session_attribution.click_id_value),
        source_export_hour = GREATEST(ga4_session_attribution.source_export_hour, EXCLUDED.source_export_hour),
        source_dataset = EXCLUDED.source_dataset,
        source_table_type = CASE
          WHEN EXCLUDED.source_export_hour >= ga4_session_attribution.source_export_hour
            THEN EXCLUDED.source_table_type
          ELSE ga4_session_attribution.source_table_type
        END,
        updated_at = now()
    `, [
        row.ga4_session_key,
        row.ga4_user_key,
        row.ga4_client_id,
        row.ga4_session_id,
        row.session_started_at,
        row.last_event_at,
        row.source,
        row.medium,
        row.campaign,
        row.content,
        row.term,
        row.click_id_type,
        row.click_id_value,
        row.source_export_hour,
        row.source_dataset,
        row.source_table_type
    ]);
}
async function markRunStarted(client, startedAt) {
    await ensureIngestionStateRow(client);
    await client.query(`
      UPDATE ga4_bigquery_ingestion_state
      SET
        last_run_started_at = $2,
        last_run_status = 'running',
        last_error = NULL,
        updated_at = $2
      WHERE pipeline_name = $1
    `, [GA4_SESSION_ATTRIBUTION_PIPELINE, startedAt]);
}
async function markRunCompleted(client, completedAt, watermarkAfter) {
    await client.query(`
      UPDATE ga4_bigquery_ingestion_state
      SET
        watermark_hour = $2::timestamptz,
        last_run_completed_at = $3,
        last_run_status = 'completed',
        last_error = NULL,
        updated_at = $3
      WHERE pipeline_name = $1
    `, [GA4_SESSION_ATTRIBUTION_PIPELINE, watermarkAfter, completedAt]);
}
export async function markGa4SessionAttributionRunFailed(error, completedAt = new Date()) {
    await ensureIngestionStateRow(query);
    await query(`
      UPDATE ga4_bigquery_ingestion_state
      SET
        last_run_completed_at = $2,
        last_run_status = 'failed',
        last_error = $3,
        updated_at = $2
      WHERE pipeline_name = $1
    `, [
        GA4_SESSION_ATTRIBUTION_PIPELINE,
        completedAt,
        error instanceof Error ? error.message : String(error)
    ]);
}
export async function ingestGa4SessionAttribution(input) {
    const config = normalizeEnabledConfig(input.config);
    const now = input.now ?? new Date();
    const watermarkBefore = await readWatermarkHour();
    const windows = planGa4SessionAttributionHourlyWindows({
        now,
        watermarkHour: watermarkBefore,
        config
    });
    const hourlyResults = [];
    for (const window of windows) {
        hourlyResults.push(await extractGa4SessionAttributionForHour({
            config,
            executor: input.executor,
            hourStart: window.hourStart
        }));
    }
    const rowsToPersist = hourlyResults.flatMap((result) => result.rows).map(mapNormalizedRowForPersistence);
    const watermarkAfter = windows.length > 0 ? windows[windows.length - 1]?.hourStart ?? null : watermarkBefore?.toISOString() ?? null;
    const upsertedRows = await withTransaction(async (client) => {
        const startedAt = new Date();
        await markRunStarted(client, startedAt);
        for (const row of rowsToPersist) {
            await upsertGa4SessionAttributionRow(client, row);
        }
        if (input.beforeCommit) {
            await input.beforeCommit(client);
        }
        await markRunCompleted(client, new Date(), watermarkAfter);
        return rowsToPersist.length;
    });
    return {
        watermarkBefore: watermarkBefore?.toISOString() ?? null,
        watermarkAfter,
        processedHours: windows.map((window) => window.hourStart).sort(compareIsoAscending),
        extractedRows: hourlyResults.reduce((sum, result) => sum + result.rows.length, 0),
        upsertedRows
    };
}
export async function listGa4SessionAttributionRows(executor = query) {
    const result = await executeQuery(executor, `
      SELECT
        ga4_session_key,
        ga4_user_key,
        ga4_client_id,
        ga4_session_id,
        session_started_at,
        last_event_at,
        source,
        medium,
        campaign,
        content,
        term,
        click_id_type,
        click_id_value,
        source_export_hour,
        source_dataset,
        source_table_type
      FROM ga4_session_attribution
      ORDER BY last_event_at DESC, ga4_session_key ASC
    `);
    return result.rows
        .map((row) => ({
        ga4SessionKey: row.ga4_session_key,
        ga4UserKey: row.ga4_user_key,
        ga4ClientId: row.ga4_client_id,
        ga4SessionId: row.ga4_session_id,
        sessionStartedAt: row.session_started_at.toISOString(),
        lastEventAt: row.last_event_at.toISOString(),
        source: row.source,
        medium: row.medium,
        campaign: row.campaign,
        content: row.content,
        term: row.term,
        clickIdType: row.click_id_type,
        clickIdValue: row.click_id_value,
        sourceExportHour: row.source_export_hour.toISOString(),
        sourceDataset: row.source_dataset,
        sourceTableType: row.source_table_type === 'intraday' ? 'intraday' : 'events'
    }))
        .sort((left, right) => {
        const eventComparison = compareIsoDescending(left.lastEventAt, right.lastEventAt);
        if (eventComparison !== 0) {
            return eventComparison;
        }
        return left.ga4SessionKey.localeCompare(right.ga4SessionKey);
    });
}
export async function getGa4SessionAttributionWatermark(executor = query) {
    const watermark = await readWatermarkHour(executor);
    return watermark?.toISOString() ?? null;
}
