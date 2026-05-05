import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.join(__dirname, '..');

function readRepoFile(relativePath: string): string {
  return readFileSync(path.join(repoRoot, relativePath), 'utf8');
}

function extractSection(text: string, startMarker: string, endMarker: string): string {
  const start = text.indexOf(startMarker);
  const end = text.indexOf(endMarker, start);

  assert.notEqual(start, -1, `missing section marker: ${startMarker}`);
  assert.notEqual(end, -1, `missing section marker: ${endMarker}`);

  return text.slice(start, end);
}

test('cloud run deploy script manages metadata refresh jobs and scheduler pause controls', () => {
  const script = readRepoFile('infra/cloud-run/deploy.sh');

  assert.match(script, /configure_metadata_scheduler\(\)/);
  assert.match(script, /META_ADS_METADATA_JOB_NAME/);
  assert.match(script, /GOOGLE_ADS_METADATA_JOB_NAME/);
  assert.match(script, /gcloud run jobs deploy "\$JOB_NAME"/);
  assert.match(script, /gcloud scheduler jobs create http "\$SCHEDULER_NAME"/);
  assert.match(script, /gcloud scheduler jobs pause "\$SCHEDULER_NAME"/);
  assert.match(script, /gcloud scheduler jobs resume "\$SCHEDULER_NAME"/);
});

test('cloud run metadata refresh jobs use provider-specific secret bindings', () => {
  const script = readRepoFile('infra/cloud-run/deploy.sh');
  const metaAdsSection = extractSection(
    script,
    'echo "Deploying Meta Ads metadata refresh job $META_ADS_METADATA_JOB_NAME"',
    'echo "Deploying Google Ads metadata refresh job $GOOGLE_ADS_METADATA_JOB_NAME"'
  );
  const googleAdsSection = extractSection(
    script,
    'echo "Deploying Google Ads metadata refresh job $GOOGLE_ADS_METADATA_JOB_NAME"',
    'echo "Deploying session retention job $RETENTION_JOB_NAME"'
  );

  assert.match(
    metaAdsSection,
    /DATABASE_URL=DATABASE_URL:latest,META_ADS_APP_SECRET=META_ADS_APP_SECRET:latest,META_ADS_ENCRYPTION_KEY=META_ADS_ENCRYPTION_KEY:latest/
  );
  assert.doesNotMatch(metaAdsSection, /GOOGLE_ADS_ENCRYPTION_KEY/);
  assert.match(
    googleAdsSection,
    /DATABASE_URL=DATABASE_URL:latest,GOOGLE_ADS_ENCRYPTION_KEY=GOOGLE_ADS_ENCRYPTION_KEY:latest/
  );
  assert.doesNotMatch(googleAdsSection, /META_ADS_(APP_SECRET|ENCRYPTION_KEY)/);
});

test('cloud run environment templates declare per-platform metadata scheduler controls', () => {
  for (const file of [
    'infra/cloud-run/environments/ENVIRONMENT.env',
    'infra/cloud-run/environments/dev.env',
    'infra/cloud-run/environments/staging.env',
    'infra/cloud-run/environments/production.env'
  ]) {
    const text = readRepoFile(file);

    assert.match(text, /META_ADS_METADATA_SCHEDULER_NAME=/);
    assert.match(text, /META_ADS_METADATA_SCHEDULE=/);
    assert.match(text, /META_ADS_METADATA_SCHEDULER_ENABLED=/);
    assert.match(text, /META_ADS_METADATA_REFRESH_REQUESTED_BY=/);
    assert.match(text, /GOOGLE_ADS_METADATA_SCHEDULER_NAME=/);
    assert.match(text, /GOOGLE_ADS_METADATA_SCHEDULE=/);
    assert.match(text, /GOOGLE_ADS_METADATA_SCHEDULER_ENABLED=/);
    assert.match(text, /GOOGLE_ADS_METADATA_REFRESH_REQUESTED_BY=/);
  }
});

test('cloud run runbooks document metadata scheduler creation and pause or resume controls', () => {
  const cloudRunRunbook = readRepoFile('docs/runbooks/cloud-run-pipelines.md');
  const metadataRunbook = readRepoFile('docs/runbooks/campaign-metadata-resolution.md');

  assert.match(cloudRunRunbook, /Cloud Scheduler/);
  assert.match(cloudRunRunbook, /pause/i);
  assert.match(cloudRunRunbook, /resume/i);
  assert.match(cloudRunRunbook, /META_ADS_METADATA_SCHEDULER_NAME/);
  assert.match(cloudRunRunbook, /GOOGLE_ADS_METADATA_SCHEDULER_NAME/);

  assert.match(metadataRunbook, /campaign_metadata_sync_job_lifecycle/);
  assert.match(metadataRunbook, /META_ADS_METADATA_REFRESH_REQUESTED_BY/);
  assert.match(metadataRunbook, /GOOGLE_ADS_METADATA_REFRESH_REQUESTED_BY/);
});
