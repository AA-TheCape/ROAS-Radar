import { randomUUID } from 'node:crypto';
import process from 'node:process';
import { performance } from 'node:perf_hooks';

function readArg(name, fallback) {
  const prefix = `--${name}=`;
  const direct = process.argv.find((arg) => arg.startsWith(prefix));

  if (direct) {
    return direct.slice(prefix.length);
  }

  const index = process.argv.indexOf(`--${name}`);
  if (index >= 0 && index + 1 < process.argv.length) {
    return process.argv[index + 1];
  }

  return fallback;
}

function readNumberArg(name, fallback) {
  const raw = readArg(name, `${fallback}`);
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`Invalid --${name} value: ${raw}`);
  }

  return parsed;
}

function percentile(values, percentileValue) {
  if (!values.length) {
    return 0;
  }

  const sorted = [...values].sort((left, right) => left - right);
  const index = Math.min(sorted.length - 1, Math.ceil((percentileValue / 100) * sorted.length) - 1);
  return sorted[index];
}

function formatBytes(value) {
  if (value >= 1024 * 1024) {
    return `${(value / (1024 * 1024)).toFixed(2)} MiB`;
  }

  if (value >= 1024) {
    return `${(value / 1024).toFixed(2)} KiB`;
  }

  return `${value} B`;
}

function buildPayload(targetBytes, sequence) {
  const sessionId = randomUUID();
  const clientEventId = randomUUID();
  const basePayload = {
    eventType: 'page_view',
    occurredAt: new Date().toISOString(),
    sessionId,
    pageUrl: `https://loadtest.example/products/widget?run=${sequence}`,
    referrerUrl: 'https://www.google.com/search?q=roas+radar',
    shopifyCartToken: null,
    shopifyCheckoutToken: null,
    clientEventId,
    consentState: 'granted',
    context: {
      userAgent: `roas-radar-large-payload-load-test/${sequence}`,
      screen: '1728x1117',
      language: 'en-US'
    },
    rawSourceEnvelope: {
      source: 'staging-load-test',
      batchId: randomUUID(),
      sequence,
      nested: {
        flags: ['large-payload', 'cloud-run', 'jsonb']
      }
    },
    rawBlob: ''
  };

  const seed = JSON.stringify(basePayload);
  const overhead = Buffer.byteLength(seed, 'utf8');
  const rawBlobBytes = Math.max(targetBytes - overhead, 0);
  basePayload.rawBlob = 'x'.repeat(rawBlobBytes);
  return basePayload;
}

async function postTrackingEvent(baseUrl, origin, targetBytes, sequence) {
  const payload = buildPayload(targetBytes, sequence);
  const body = JSON.stringify(payload);
  const startedAt = performance.now();
  const response = await fetch(`${baseUrl.replace(/\/$/, '')}/track`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      accept: 'application/json',
      origin
    },
    body
  });
  const durationMs = performance.now() - startedAt;

  let responseBody = null;
  try {
    responseBody = await response.json();
  } catch {
    responseBody = null;
  }

  return {
    durationMs,
    ok: response.ok,
    status: response.status,
    requestBytes: Buffer.byteLength(body, 'utf8'),
    responseBody
  };
}

async function run() {
  const baseUrl = readArg('base-url', process.env.LOAD_TEST_BASE_URL ?? '');
  const origin = readArg('origin', process.env.LOAD_TEST_ORIGIN ?? '');

  if (!baseUrl) {
    throw new Error('Missing --base-url or LOAD_TEST_BASE_URL');
  }

  if (!origin) {
    throw new Error('Missing --origin or LOAD_TEST_ORIGIN');
  }

  const requests = readNumberArg('requests', 24);
  const concurrency = readNumberArg('concurrency', 4);
  const payloadBytes = readNumberArg('payload-bytes', 6 * 1024 * 1024);
  const maxP95Ms = readNumberArg('max-p95-ms', 4000);

  const pending = new Set();
  const results = [];
  let nextSequence = 0;

  const launchNext = async () => {
    if (nextSequence >= requests) {
      return;
    }

    const sequence = nextSequence;
    nextSequence += 1;

    const task = postTrackingEvent(baseUrl, origin, payloadBytes, sequence)
      .then((result) => {
        results.push(result);
      })
      .finally(() => {
        pending.delete(task);
      });

    pending.add(task);
  };

  while (pending.size < concurrency && nextSequence < requests) {
    await launchNext();
  }

  while (pending.size > 0) {
    await Promise.race(pending);

    while (pending.size < concurrency && nextSequence < requests) {
      await launchNext();
    }
  }

  const latencies = results.map((result) => result.durationMs);
  const failures = results.filter((result) => !result.ok);
  const p95 = percentile(latencies, 95);

  const summary = {
    baseUrl,
    origin,
    requests,
    concurrency,
    payloadBytes,
    payloadSize: formatBytes(payloadBytes),
    succeeded: results.length - failures.length,
    failed: failures.length,
    latencyMs: {
      min: Math.min(...latencies),
      p50: percentile(latencies, 50),
      p95,
      max: Math.max(...latencies),
      average: latencies.reduce((sum, value) => sum + value, 0) / latencies.length
    },
    sampleFailure: failures[0]
      ? {
          status: failures[0].status,
          responseBody: failures[0].responseBody
        }
      : null
  };

  console.log(JSON.stringify(summary, null, 2));

  if (failures.length > 0) {
    process.exitCode = 1;
    return;
  }

  if (p95 > maxP95Ms) {
    console.error(`p95 latency ${p95.toFixed(2)} ms exceeded threshold ${maxP95Ms} ms`);
    process.exitCode = 1;
  }
}

run().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
