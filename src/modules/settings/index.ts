import { Router } from 'express';
import { z } from 'zod';
import type { QueryResult, QueryResultRow } from 'pg';

import { withTransaction, query } from '../../db/pool.js';
import { attachAuthContext, requireAdmin, requireAuthenticated } from '../auth/index.js';
import { refreshAllDailyReportingMetrics } from '../reporting/aggregates.js';

const REPORTING_TIMEZONE_ALIASES: Record<string, string> = {
  PST: 'America/Los_Angeles',
  PDT: 'America/Los_Angeles',
  PT: 'America/Los_Angeles'
};

export const DEFAULT_REPORTING_TIMEZONE = 'America/Los_Angeles';

type QueryExecutor = typeof query | { query: typeof query };

type AppSettingsRow = {
  reporting_timezone: string;
  updated_at: Date;
};

class SettingsHttpError extends Error {
  statusCode: number;
  code: string;
  details?: unknown;

  constructor(statusCode: number, code: string, message: string, details?: unknown) {
    super(message);
    this.name = 'SettingsHttpError';
    this.statusCode = statusCode;
    this.code = code;
    this.details = details;
  }
}

function normalizeReportingTimezone(input: string): string {
  const trimmed = input.trim();
  const aliased = REPORTING_TIMEZONE_ALIASES[trimmed.toUpperCase()] ?? trimmed;

  try {
    return new Intl.DateTimeFormat('en-US', { timeZone: aliased }).resolvedOptions().timeZone;
  } catch {
    throw new SettingsHttpError(400, 'invalid_timezone', 'Timezone must be a valid IANA timezone or supported alias');
  }
}

const updateSettingsSchema = z.object({
  reportingTimezone: z.string().min(1).transform(normalizeReportingTimezone)
});

function executeQuery<TResult extends QueryResultRow = QueryResultRow>(
  executor: QueryExecutor,
  sql: string,
  params?: unknown[]
): Promise<QueryResult<TResult>> {
  if (typeof executor === 'function') {
    return executor<TResult>(sql, params);
  }

  return executor.query<TResult>(sql, params);
}

async function ensureAppSettingsRow(executor: QueryExecutor): Promise<void> {
  await executeQuery(
    executor,
    `
      INSERT INTO app_settings (singleton, reporting_timezone)
      VALUES (true, $1)
      ON CONFLICT (singleton) DO NOTHING
    `,
    [DEFAULT_REPORTING_TIMEZONE]
  );
}

async function fetchAppSettingsRow(executor: QueryExecutor): Promise<AppSettingsRow> {
  await ensureAppSettingsRow(executor);

  const result = await executeQuery<AppSettingsRow>(
    executor,
    `
      SELECT reporting_timezone, updated_at
      FROM app_settings
      WHERE singleton = true
      LIMIT 1
    `
  );

  if (!result.rowCount) {
    return {
      reporting_timezone: DEFAULT_REPORTING_TIMEZONE,
      updated_at: new Date()
    };
  }

  return result.rows[0];
}

export async function getReportingTimezone(executor: QueryExecutor = query): Promise<string> {
  const row = await fetchAppSettingsRow(executor);
  return normalizeReportingTimezone(row.reporting_timezone || DEFAULT_REPORTING_TIMEZONE);
}

export function formatDateInTimezone(date: Date, reportingTimezone: string): string {
  const formatter = new Intl.DateTimeFormat('en-CA', {
    timeZone: reportingTimezone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  });

  const parts = formatter.formatToParts(date);
  const year = parts.find((part) => part.type === 'year')?.value;
  const month = parts.find((part) => part.type === 'month')?.value;
  const day = parts.find((part) => part.type === 'day')?.value;

  if (!year || !month || !day) {
    throw new Error('Failed to format date for reporting timezone');
  }

  return `${year}-${month}-${day}`;
}

function serializeSettings(row: AppSettingsRow) {
  return {
    reportingTimezone: normalizeReportingTimezone(row.reporting_timezone || DEFAULT_REPORTING_TIMEZONE),
    updatedAt: row.updated_at.toISOString()
  };
}

export function createSettingsRouter(): Router {
  const router = Router();

  router.use(attachAuthContext);
  router.use(requireAuthenticated);

  router.get('/', async (_req, res, next) => {
    try {
      const settings = await fetchAppSettingsRow(query);
      res.json(serializeSettings(settings));
    } catch (error) {
      next(error);
    }
  });

  router.put('/', requireAdmin, async (req, res, next) => {
    try {
      const input = updateSettingsSchema.parse(req.body ?? {});

      const settings = await withTransaction(async (client) => {
        await ensureAppSettingsRow(client);
        const result = await client.query<AppSettingsRow>(
          `
            UPDATE app_settings
            SET
              reporting_timezone = $1,
              updated_at = now()
            WHERE singleton = true
            RETURNING reporting_timezone, updated_at
          `,
          [input.reportingTimezone]
        );

        await refreshAllDailyReportingMetrics(client);
        return result.rows[0];
      });

      res.json({
        ok: true,
        settings: serializeSettings(settings)
      });
    } catch (error) {
      next(error);
    }
  });

  return router;
}
