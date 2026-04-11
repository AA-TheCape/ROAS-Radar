import { env } from '../../config/env.js';
import { query, withTransaction } from '../../db/pool.js';
function toDateString(value) {
    return value.toISOString().slice(0, 10);
}
function addUtcDays(date, days) {
    const next = new Date(date);
    next.setUTCDate(next.getUTCDate() + days);
    return next;
}
function toNumber(value) {
    return typeof value === 'number' ? value : Number(value);
}
export function resolveRunDate(now = new Date()) {
    const target = addUtcDays(new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate())), -env.DATA_QUALITY_TARGET_LAG_DAYS);
    return toDateString(target);
}
export function buildLookbackDates(runDate, lookbackDays) {
    const end = new Date(`${runDate}T00:00:00.000Z`);
    const days = Math.max(lookbackDays, 1);
    const dates = [];
    for (let index = days - 1; index >= 0; index -= 1) {
        dates.push(toDateString(addUtcDays(end, -index)));
    }
    return dates;
}
export function detectAnomalyFlags(rows, runDate) {
    const current = rows.find((row) => row.metric_date === runDate);
    if (!current) {
        return [];
    }
    const baselineRows = rows.filter((row) => row.metric_date !== runDate);
    if (baselineRows.length === 0) {
        return [];
    }
    const metrics = ['visits', 'orders', 'revenue', 'spend'];
    return metrics
        .map((metric) => {
        const baselineValue = baselineRows.reduce((sum, row) => sum + toNumber(row[metric]), 0) / baselineRows.length;
        const currentValue = toNumber(current[metric]);
        const absoluteDelta = baselineValue - currentValue;
        const relativeDelta = baselineValue <= 0 ? null : absoluteDelta / baselineValue;
        return {
            metric,
            currentValue,
            baselineValue,
            absoluteDelta,
            relativeDelta
        };
    })
        .filter((flag) => flag.baselineValue >= env.DATA_QUALITY_ANOMALY_MIN_BASELINE &&
        flag.absoluteDelta > 0 &&
        (flag.relativeDelta ?? 0) >= env.DATA_QUALITY_ANOMALY_THRESHOLD_RATIO);
}
export async function fetchDataQualityReport(runDate) {
    const result = await query(`
      SELECT
        run_date::text,
        check_key,
        status,
        severity,
        discrepancy_count,
        summary,
        details,
        checked_at,
        alert_emitted_at
      FROM data_quality_check_runs
      WHERE run_date = $1::date
      ORDER BY
        CASE status
          WHEN 'failed' THEN 0
          WHEN 'warning' THEN 1
          ELSE 2
        END,
        check_key ASC
    `, [runDate]);
    const checks = result.rows.map((row) => ({
        checkKey: row.check_key,
        status: row.status,
        severity: row.severity,
        discrepancyCount: row.discrepancy_count,
        summary: row.summary,
        details: row.details,
        checkedAt: row.checked_at.toISOString(),
        alertEmittedAt: row.alert_emitted_at?.toISOString() ?? null
    }));
    return {
        runDate,
        totals: {
            totalChecks: checks.length,
            failedChecks: checks.filter((check) => check.status === 'failed').length,
            warningChecks: checks.filter((check) => check.status === 'warning').length,
            totalDiscrepancies: checks.reduce((sum, check) => sum + check.discrepancyCount, 0)
        },
        checks
    };
}
export async function runDailyDataQualityChecks(runDate = resolveRunDate()) {
    const lookbackDates = buildLookbackDates(runDate, env.DATA_QUALITY_ANOMALY_LOOKBACK_DAYS + 1);
    const metricsResult = await query(`
      SELECT
        metric_date::text,
        COALESCE(SUM(visits), 0)::text AS visits,
        COALESCE(SUM(attributed_orders), 0)::text AS orders,
        COALESCE(SUM(attributed_revenue), 0)::text AS revenue,
        COALESCE(SUM(spend), 0)::text AS spend
      FROM daily_reporting_metrics
      WHERE metric_date = ANY($1::date[])
      GROUP BY metric_date
      ORDER BY metric_date ASC
    `, [lookbackDates]);
    const anomalyFlags = detectAnomalyFlags(metricsResult.rows, runDate);
    await withTransaction(async (client) => {
        const discrepancyCount = anomalyFlags.length;
        const status = discrepancyCount > 0 ? 'warning' : 'healthy';
        const summary = discrepancyCount > 0
            ? `${discrepancyCount} reporting metrics deviated from the trailing baseline.`
            : 'No reporting anomalies detected for the run date.';
        await client.query(`
        INSERT INTO data_quality_check_runs (
          run_date,
          check_key,
          status,
          severity,
          discrepancy_count,
          summary,
          details,
          checked_at,
          updated_at
        )
        VALUES (
          $1::date,
          'reporting_anomaly_check',
          $2,
          CASE WHEN $2 = 'warning' THEN 'warning' ELSE 'info' END,
          $3,
          $4,
          $5::jsonb,
          now(),
          now()
        )
        ON CONFLICT (run_date, check_key)
        DO UPDATE SET
          status = EXCLUDED.status,
          severity = EXCLUDED.severity,
          discrepancy_count = EXCLUDED.discrepancy_count,
          summary = EXCLUDED.summary,
          details = EXCLUDED.details,
          checked_at = now(),
          updated_at = now()
      `, [runDate, status, discrepancyCount, summary, JSON.stringify({ anomalyFlags })]);
    });
    const report = await fetchDataQualityReport(runDate);
    return {
        runDate: report.runDate,
        totals: report.totals
    };
}
export const __dataQualityTestUtils = {
    resolveRunDate,
    buildLookbackDates,
    detectAnomalyFlags
};
