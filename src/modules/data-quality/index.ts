import { env } from '../../config/env.js';
import { query } from '../../db/pool.js';
import { ATTRIBUTION_MODELS } from '../attribution/engine.js';

const DATA_QUALITY_CHECK_KEYS = [
  'shopify_webhook_gaps',
  'spend_ingestion_gaps',
  'attribution_conservation',
  'day_over_day_anomalies'
] as const;

type DataQualityCheckKey = (typeof DATA_QUALITY_CHECK_KEYS)[number];
type DataQualityStatus = 'healthy' | 'warning' | 'failed';
type DataQualitySeverity = 'info' | 'warning' | 'critical';

type DataQualityCheckResult = {
  checkKey: DataQualityCheckKey;
  status: DataQualityStatus;
  severity: DataQualitySeverity;
  discrepancyCount: number;
  summary: string;
  details: Record<string, unknown>;
  alertable: boolean;
};

export type DataQualityReport = {
  runDate: string | null;
  checks: Array<{
    checkKey: DataQualityCheckKey;
    status: DataQualityStatus;
    severity: DataQualitySeverity;
    discrepancyCount: number;
    summary: string;
    details: Record<string, unknown>;
    checkedAt: string;
    alertEmittedAt: string | null;
  }>;
  totals: {
    healthyChecks: number;
    warningChecks: number;
    failedChecks: number;
    totalDiscrepancies: number;
  };
};

function formatDateOnly(value: Date): string {
  return value.toISOString().slice(0, 10);
}

function addDays(value: Date, days: number): Date {
  const nextValue = new Date(value);
  nextValue.setUTCDate(nextValue.getUTCDate() + days);
  return nextValue;
}

function listDateRangeInclusive(startDate: string, endDate: string): string[] {
  const dates: string[] = [];
  let cursor = new Date(`${startDate}T00:00:00.000Z`);
  const end = new Date(`${endDate}T00:00:00.000Z`);

  while (cursor <= end) {
    dates.push(formatDateOnly(cursor));
    cursor = addDays(cursor, 1);
  }

  return dates;
}

function buildLookbackDates(runDate: string, lookbackDays: number): string[] {
  const end = new Date(`${runDate}T00:00:00.000Z`);
  const start = addDays(end, -(lookbackDays - 1));
  return listDateRangeInclusive(formatDateOnly(start), formatDateOnly(end));
}

function toNumber(value: number | string | null | undefined): number {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : 0;
  }

  if (typeof value !== 'string') {
    return 0;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function resolveRunDate(now = new Date()): string {
  return formatDateOnly(addDays(new Date(`${formatDateOnly(now)}T00:00:00.000Z`), -env.DATA_QUALITY_TARGET_LAG_DAYS));
}

function buildAlertLog(runDate: string, check: DataQualityCheckResult): string {
  return JSON.stringify({
    event: 'data_quality_alert',
    runDate,
    checkKey: check.checkKey,
    status: check.status,
    severity: check.severity,
    discrepancyCount: check.discrepancyCount,
    summary: check.summary,
    details: check.details
  });
}

async function persistCheck(runDate: string, check: DataQualityCheckResult): Promise<void> {
  await query(
    `
      INSERT INTO data_quality_check_runs (
        run_date,
        check_key,
        status,
        severity,
        discrepancy_count,
        summary,
        details,
        checked_at,
        alert_emitted_at,
        updated_at
      )
      VALUES (
        $1::date,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7::jsonb,
        now(),
        CASE WHEN $8 THEN now() ELSE NULL END,
        now()
      )
      ON CONFLICT (run_date, check_key)
      DO UPDATE SET
        status = EXCLUDED.status,
        severity = EXCLUDED.severity,
        discrepancy_count = EXCLUDED.discrepancy_count,
        summary = EXCLUDED.summary,
        details = EXCLUDED.details,
        checked_at = EXCLUDED.checked_at,
        alert_emitted_at = CASE
          WHEN $8 THEN now()
          ELSE data_quality_check_runs.alert_emitted_at
        END,
        updated_at = now()
    `,
    [
      runDate,
      check.checkKey,
      check.status,
      check.severity,
      check.discrepancyCount,
      check.summary,
      JSON.stringify(check.details),
      check.alertable
    ]
  );
}

async function runWebhookGapCheck(runDate: string): Promise<DataQualityCheckResult> {
  // Checks Shopify orders for missing webhook receipts, failed receipts, and duplicate delivery groups.
  // Returns actionable sample order ids when gaps are present.
}

async function runSpendIngestionGapCheck(runDate: string): Promise<DataQualityCheckResult> {
  // Checks active Meta and Google Ads connections for expected trailing dates
  // without a completed sync job, then returns provider/connection/date context.
}

async function runAttributionConservationCheck(runDate: string): Promise<DataQualityCheckResult> {
  // Cross-joins scoped orders against every supported attribution model and verifies:
  // - credit weights sum to 1
  // - revenue credits sum to order total
  // - missing credit rows are surfaced explicitly
}

function detectAnomalyFlags(rows: DailyMetricRow[], runDate: string): AnomalyFlag[] {
  // Compares the run date against the trailing baseline in daily_reporting_metrics
  // and flags visits/orders/revenue/spend deviations above the configured threshold.
}

async function runDayOverDayAnomalyCheck(runDate: string): Promise<DataQualityCheckResult> {
  // Produces warning-level anomaly flags with baseline averages and day-over-day deltas.
}

export async function runDailyDataQualityChecks(options: { runDate?: string; now?: Date } = {}) {
  const runDate = options.runDate ?? resolveRunDate(options.now);
  const checks = await Promise.all([
    runWebhookGapCheck(runDate),
    runSpendIngestionGapCheck(runDate),
    runAttributionConservationCheck(runDate),
    runDayOverDayAnomalyCheck(runDate)
  ]);

  for (const check of checks) {
    await persistCheck(runDate, check);

    if (check.alertable) {
      process.stderr.write(`${buildAlertLog(runDate, check)}\n`);
    }
  }

  return {
    runDate,
    checks,
    totals: {
      healthyChecks: checks.filter((check) => check.status === 'healthy').length,
      warningChecks: checks.filter((check) => check.status === 'warning').length,
      failedChecks: checks.filter((check) => check.status === 'failed').length,
      totalDiscrepancies: checks.reduce((sum, check) => sum + check.discrepancyCount, 0)
    }
  };
}

export async function fetchDataQualityReport(requestedRunDate?: string): Promise<DataQualityReport> {
  // Returns the latest persisted run or a requested date with all check rows and totals.
}

export const __dataQualityTestUtils = {
  resolveRunDate,
  buildLookbackDates,
  detectAnomalyFlags
};
