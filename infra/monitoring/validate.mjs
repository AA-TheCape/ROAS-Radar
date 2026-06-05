import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(root, '../..');
const metricsDir = path.join(root, 'log-metrics');
const alertsDir = path.join(root, 'alert-policies');
const dashboardPath = path.join(root, 'dashboard.json');
const maxThresholdDurationSeconds = 24 * 60 * 60 + 30 * 60;

function renderPlaceholders(raw) {
  return raw
    .replaceAll('"__ALERT_NOTIFICATION_CHANNELS__"', '[]')
    .replaceAll('__ALERT_NOTIFICATION_CHANNELS__', '[]')
    .replaceAll('__ENVIRONMENT__', 'test')
    .replaceAll('__RUNBOOK_URL_INGESTION__', 'docs/runbooks/ingestion-failures.md')
    .replaceAll('__RUNBOOK_URL_META_ORDER_VALUE__', 'docs/runbooks/meta-order-value-ingestion.md')
    .replaceAll('__RUNBOOK_URL_ATTRIBUTION__', 'docs/runbooks/attribution-worker-backlog.md')
    .replaceAll('__RUNBOOK_URL_ATTRIBUTION_COMPLETENESS__', 'docs/runbooks/attribution-completeness.md')
    .replaceAll('__RUNBOOK_URL_API_LATENCY__', 'docs/runbooks/api-latency.md')
    .replaceAll('__RUNBOOK_URL_DATA_QUALITY__', 'docs/runbooks/identity-data-quality.md')
    .replaceAll('__RUNBOOK_URL_MMM__', 'docs/runbooks/mmm-pipelines.md')
    .replaceAll('__DASHBOARD_DISPLAY_NAME__', 'ROAS Radar Test Pipeline Health');
}

function loadJson(filePath) {
  return JSON.parse(renderPlaceholders(readFileSync(filePath, 'utf8')));
}

function validateMetric(filePath) {
  const data = loadJson(filePath);
  const labels = data.metricDescriptor?.labels?.length ?? 0;
  const extractors = Object.keys(data.labelExtractors ?? {}).length;
  const issues = [];
  if (labels !== extractors) {
    issues.push(`${path.basename(filePath)}: metricDescriptor.labels (${labels}) must match labelExtractors (${extractors})`);
  }
  return issues;
}

function validateAlert(filePath) {
  const data = loadJson(filePath);
  const issues = [];
  const conditions = data.conditions ?? [];
  if (conditions.length === 0) {
    return [`${path.basename(filePath)}: must define at least one condition`];
  }

  let hasLogMatch = false;
  conditions.forEach((condition, index) => {
    const kinds = Object.keys(condition).filter((key) => key.startsWith('condition'));
    if (kinds.length !== 1) {
      issues.push(`${path.basename(filePath)}: conditions[${index}] must define exactly one condition subtype`);
      return;
    }

    if (kinds[0] === 'conditionMatchedLog') {
      hasLogMatch = true;
    }

    const threshold = condition.conditionThreshold;
    if (threshold) {
      const duration = parseDurationSeconds(threshold.duration);
      if (duration > maxThresholdDurationSeconds) {
        issues.push(`${path.basename(filePath)}: conditions[${index}].conditionThreshold.duration must be <= 88200s`);
      }

      const filter = threshold.filter ?? '';
      if (/metric\.labels\.[A-Za-z0-9_]+\s*=\s*\([^)]*\bOR\b/.test(filter)) {
        issues.push(`${path.basename(filePath)}: conditions[${index}].conditionThreshold.filter must repeat metric label comparisons instead of using label=(... OR ...)`);
      }

      if (/metric\.labels\.[A-Za-z0-9_]+\s*!?=\s*"[^"]+"\s+\(metric\.labels\.[^)]*\bOR\b/.test(filter)) {
        issues.push(`${path.basename(filePath)}: conditions[${index}].conditionThreshold.filter must not mix AND and OR metric label restrictions`);
      }
    }
  });

  const rateLimit = data.alertStrategy?.notificationRateLimit?.period;
  if (hasLogMatch && !rateLimit) {
    issues.push(`${path.basename(filePath)}: log-based alert policies require alertStrategy.notificationRateLimit.period`);
  } else if (!hasLogMatch && rateLimit) {
    issues.push(`${path.basename(filePath)}: only log-based alert policies may define alertStrategy.notificationRateLimit.period`);
  }

  return issues;
}

function parseDurationSeconds(duration) {
  const match = /^(\d+)s$/.exec(duration ?? '');
  return match ? Number(match[1]) : 0;
}

function trackedJsonFiles(dir, fallbackDir) {
  try {
    return execFileSync('git', ['-C', repoRoot, 'ls-files', dir], { encoding: 'utf8' })
      .split('\n')
      .filter(Boolean)
      .map((fileName) => path.join(repoRoot, fileName))
      .filter((fileName) => existsSync(fileName));
  } catch {
    return readdirSync(fallbackDir)
      .filter((name) => name.endsWith('.json'))
      .sort()
      .map((name) => path.join(fallbackDir, name));
  }
}

function validateDashboard(filePath) {
  const data = loadJson(filePath);
  const tiles = data.mosaicLayout?.tiles ?? [];
  const issues = [];
  if (tiles.length === 0) {
    return [`${path.basename(filePath)}: must define at least one dashboard tile`];
  }

  tiles.forEach((tile, index) => {
    for (const key of ['xPos', 'yPos', 'width', 'height']) {
      const value = tile[key];
      if (!Number.isInteger(value)) {
        issues.push(`${path.basename(filePath)}: tiles[${index}].${key} must be an integer`);
      } else if ((key === 'xPos' || key === 'yPos') && value < 0) {
        issues.push(`${path.basename(filePath)}: tiles[${index}].${key} must be non-negative`);
      } else if ((key === 'width' || key === 'height') && value < 1) {
        issues.push(`${path.basename(filePath)}: tiles[${index}].${key} must be at least 1`);
      }
    }

    const widget = tile.widget;
    if (!widget || typeof widget !== 'object') {
      issues.push(`${path.basename(filePath)}: tiles[${index}].widget must be an object`);
      return;
    }

    const widgetKinds = ['text', 'scorecard', 'xyChart', 'blank'].filter((key) => key in widget);
    if (widgetKinds.length !== 1) {
      issues.push(`${path.basename(filePath)}: tiles[${index}].widget must define exactly one supported widget kind`);
    }
  });

  return issues;
}

const issues = [];
for (const filePath of trackedJsonFiles('infra/monitoring/log-metrics/*.json', metricsDir)) {
  issues.push(...validateMetric(filePath));
}
for (const filePath of trackedJsonFiles('infra/monitoring/alert-policies/*.json', alertsDir)) {
  issues.push(...validateAlert(filePath));
}
issues.push(...validateDashboard(dashboardPath));

if (issues.length > 0) {
  for (const issue of issues) {
    console.error(issue);
  }
  process.exit(1);
}

console.log('Monitoring templates validated.');
