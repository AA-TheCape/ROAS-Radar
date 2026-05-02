import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { chmodSync, copyFileSync, mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

function writeExecutable(filePath: string, contents: string) {
  writeFileSync(filePath, contents);
  chmodSync(filePath, 0o755);
}

function createSmokeFixture(mode: 'success' | 'malformed-authenticated-response') {
  const tempDir = mkdtempSync(path.join(os.tmpdir(), 'roas-radar-smoke-test-'));
  const scriptDir = path.join(tempDir, 'infra', 'cloud-run');
  const envDir = path.join(scriptDir, 'environments');
  const binDir = path.join(tempDir, 'bin');
  const curlLogPath = path.join(tempDir, 'curl.log');

  mkdirSync(envDir, { recursive: true });
  mkdirSync(binDir, { recursive: true });

  copyFileSync(path.resolve('infra/cloud-run/smoke-test.sh'), path.join(scriptDir, 'smoke-test.sh'));
  chmodSync(path.join(scriptDir, 'smoke-test.sh'), 0o755);

  writeFileSync(
    path.join(envDir, 'fixture.env'),
    [
      'GCP_PROJECT_ID=test-project',
      'GCP_REGION=us-central1',
      'API_SERVICE_NAME=roas-radar-api',
      'DASHBOARD_SERVICE_NAME=roas-radar-dashboard',
      'WORKER_SERVICE_NAME=roas-radar-worker'
    ].join('\n')
  );

  writeExecutable(
    path.join(binDir, 'gcloud'),
    `#!/bin/sh
set -eu

if [ "$1" = "run" ] && [ "$2" = "services" ] && [ "$3" = "describe" ]; then
  case "$4" in
    roas-radar-api)
      printf '%s\n' 'https://api.example.test'
      exit 0
      ;;
    roas-radar-dashboard)
      printf '%s\n' 'https://dashboard.example.test'
      exit 0
      ;;
    roas-radar-worker)
      printf '%s\n' 'roas-radar-worker-00001'
      exit 0
      ;;
  esac
fi

if [ "$1" = "secrets" ] && [ "$2" = "versions" ] && [ "$3" = "access" ]; then
  printf '%s\n' 'fixture-reporting-token'
  exit 0
fi

echo "unexpected gcloud invocation: $*" >&2
exit 1
`
  );

  writeExecutable(
    path.join(binDir, 'date'),
    `#!/bin/sh
set -eu
printf '%s\n' '2026-04-30'
`
  );

  writeExecutable(
    path.join(binDir, 'curl'),
    `#!/bin/sh
set -eu

OUTPUT_FILE=''
WRITE_OUT=''
AUTH_HEADER=''
URL=''

while [ "$#" -gt 0 ]; do
  case "$1" in
    --output)
      OUTPUT_FILE="$2"
      shift 2
      ;;
    --write-out)
      WRITE_OUT="$2"
      shift 2
      ;;
    -H)
      if [ "$2" = 'Authorization: Bearer fixture-reporting-token' ]; then
        AUTH_HEADER='present'
      fi
      shift 2
      ;;
    --fail|--silent|--show-error)
      shift
      ;;
    *)
      URL="$1"
      shift
      ;;
  esac
done

printf '%s|%s\n' "$AUTH_HEADER" "$URL" >> "$CURL_LOG_PATH"

case "$URL" in
  https://api.example.test/readyz|https://dashboard.example.test/)
    :
    ;;
  https://api.example.test/api/reporting/meta-order-value*)
    if [ -z "$AUTH_HEADER" ]; then
      if [ -n "$WRITE_OUT" ]; then
        printf '401'
      fi
      exit 0
    fi

    if [ "$SMOKE_FIXTURE_MODE" = 'malformed-authenticated-response' ]; then
      printf '%s' '{"scope":{}}'
      exit 0
    fi

    printf '%s' '{"scope":{"organizationId":77},"range":{"startDate":"2026-04-29","endDate":"2026-04-30"},"pagination":{"limit":5,"offset":0,"returned":1,"totalRows":1,"hasMore":false},"totals":{"attributedRevenue":0,"purchaseCount":0,"spend":0,"roas":null},"rows":[]}'
    exit 0
    ;;
  *)
    echo "unexpected curl url: $URL" >&2
    exit 1
    ;;
esac

if [ -n "$OUTPUT_FILE" ]; then
  : > "$OUTPUT_FILE"
fi
`
  );

  writeExecutable(
    path.join(binDir, 'node'),
    `#!/bin/sh
exec "${process.execPath}" "$@"
`
  );

  return {
    cleanup() {
      rmSync(tempDir, { recursive: true, force: true });
    },
    curlLogPath,
    env: {
      ...process.env,
      PATH: `${binDir}:${process.env.PATH ?? ''}`,
      CURL_LOG_PATH: curlLogPath,
      SMOKE_FIXTURE_MODE: mode,
      SMOKE_TEST_START_DATE: '2026-04-29',
      SMOKE_TEST_END_DATE: '2026-04-30'
    },
    scriptPath: path.join(scriptDir, 'smoke-test.sh')
  };
}

test('cloud run smoke test validates unauthenticated and authenticated Meta order value checks', () => {
  const fixture = createSmokeFixture('success');

  try {
    const result = spawnSync('sh', [fixture.scriptPath, 'fixture'], {
      env: fixture.env,
      encoding: 'utf8'
    });

    assert.equal(result.status, 0, result.stderr);

    const curlLog = readFileSync(fixture.curlLogPath, 'utf8');
    assert.match(curlLog, /\|https:\/\/api\.example\.test\/readyz/);
    assert.match(
      curlLog,
      /\|https:\/\/api\.example\.test\/api\/reporting\/meta-order-value\?startDate=2026-04-29&endDate=2026-04-30&limit=5/
    );
    assert.match(
      curlLog,
      /present\|https:\/\/api\.example\.test\/api\/reporting\/meta-order-value\?startDate=2026-04-29&endDate=2026-04-30&limit=5/
    );
  } finally {
    fixture.cleanup();
  }
});

test('cloud run smoke test fails when the authenticated Meta order value response shape is invalid', () => {
  const fixture = createSmokeFixture('malformed-authenticated-response');

  try {
    const result = spawnSync('sh', [fixture.scriptPath, 'fixture'], {
      env: fixture.env,
      encoding: 'utf8'
    });

    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /scope\.organizationId/);
  } finally {
    fixture.cleanup();
  }
});
