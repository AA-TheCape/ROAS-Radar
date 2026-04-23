export function formatCurrency(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) {
    return 'N/A';
  }

  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: value >= 100 ? 0 : 2
  }).format(value);
}

export function formatCompactCurrency(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) {
    return 'N/A';
  }

  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    notation: Math.abs(value) >= 1000 ? 'compact' : 'standard',
    maximumFractionDigits: 1
  }).format(value);
}

export function formatNumber(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) {
    return 'N/A';
  }

  return new Intl.NumberFormat('en-US', {
    maximumFractionDigits: value >= 100 ? 0 : 2
  }).format(value);
}

export function formatPercent(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) {
    return 'N/A';
  }

  return `${(value * 100).toFixed(value < 0.1 ? 2 : 1)}%`;
}

const DEFAULT_REPORTING_TIMEZONE = 'America/Los_Angeles';

export function formatDateLabel(
  value: string,
  reportingTimezone = DEFAULT_REPORTING_TIMEZONE
): string {
  const match = value.match(/^(\d{4})-(\d{2})-(\d{2})$/);

  if (!match) {
    return value;
  }

  const [, year, month, day] = match;
  const date = new Date(Date.UTC(Number(year), Number(month) - 1, Number(day), 12));

  return new Intl.DateTimeFormat('en-US', {
    month: 'short',
    day: 'numeric',
    timeZone: reportingTimezone
  }).format(date);
}

export function formatDateTimeLabel(
  value: string | null | undefined,
  reportingTimezone = DEFAULT_REPORTING_TIMEZONE
): string {
  if (!value) {
    return 'N/A';
  }

  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return new Intl.DateTimeFormat('en-US', {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    timeZone: reportingTimezone
  }).format(date);
}

export function formatCurrentTimestamp(
  value: Date,
  options: {
    timeZone?: string;
    includeTimeZoneName?: boolean;
  } = {}
): string {
  const { timeZone, includeTimeZoneName = false } = options;

  return new Intl.DateTimeFormat('en-US', {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    ...(timeZone ? { timeZone } : {}),
    ...(includeTimeZoneName ? { timeZoneName: 'short' as const } : {})
  }).format(value);
}
