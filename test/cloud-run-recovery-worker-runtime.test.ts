import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.join(__dirname, "..");
const deployScript = readFileSync(
	path.join(repoRoot, "infra/cloud-run/deploy.sh"),
	"utf8",
);

const ga4BigQueryEnvDefinition =
	deployScript.match(/GA4_BIGQUERY_ENV_VARS="([^"]+)"/)?.[1] ?? "";

const recoveryWorkerSection =
	deployScript.match(
		/gcloud run deploy "\$WORKER_SERVICE_NAME"[\s\S]*?WORKER_LATEST_REVISION=/,
	)?.[0] ?? "";

test("Cloud Run recovery worker receives GA4 BigQuery runtime config", () => {
	assert.match(recoveryWorkerSection, /--args=run,start:worker-service/);
	assert.match(
		recoveryWorkerSection,
		/--set-env-vars="\$\{COMMON_ENV_VARS\}@DATABASE_POOL_MAX=\$WORKER_DATABASE_POOL_MAX@ATTRIBUTION_WORKER_LOOP=true@\$GA4_BIGQUERY_ENV_VARS"/,
	);
	assert.match(recoveryWorkerSection, /--set-secrets="\$COMMON_SECRET_FLAGS"/);

	for (const key of [
		"GA4_BIGQUERY_ENABLED=true",
		"GA4_BIGQUERY_PROJECT_ID=$GA4_BIGQUERY_PROJECT_ID",
		"GA4_BIGQUERY_LOCATION=$GA4_BIGQUERY_LOCATION",
		"GA4_BIGQUERY_DATASET=$GA4_BIGQUERY_DATASET",
		"GA4_BIGQUERY_EVENTS_TABLE_PATTERN=$GA4_BIGQUERY_EVENTS_TABLE_PATTERN",
		"GA4_BIGQUERY_INTRADAY_TABLE_PATTERN=$GA4_BIGQUERY_INTRADAY_TABLE_PATTERN",
		"GA4_BIGQUERY_LOOKBACK_HOURS=${GA4_BIGQUERY_LOOKBACK_HOURS:-24}",
		"GA4_BIGQUERY_BACKFILL_HOURS=${GA4_BIGQUERY_BACKFILL_HOURS:-168}",
		"GOOGLE_ADS_TRANSFER_BIGQUERY_PROJECT_ID=$GOOGLE_ADS_TRANSFER_BIGQUERY_PROJECT_ID",
		"GOOGLE_ADS_TRANSFER_BIGQUERY_LOCATION=$GOOGLE_ADS_TRANSFER_BIGQUERY_LOCATION",
		"GOOGLE_ADS_TRANSFER_BIGQUERY_DATASET=$GOOGLE_ADS_TRANSFER_BIGQUERY_DATASET",
		"GOOGLE_ADS_TRANSFER_TABLE_PATTERN=$GOOGLE_ADS_TRANSFER_TABLE_PATTERN",
		"GOOGLE_ADS_TRANSFER_LOOKBACK_DAYS=${GOOGLE_ADS_TRANSFER_LOOKBACK_DAYS:-30}",
		"GA4_LINKED_GOOGLE_ADS_CUSTOMER_IDS=${GA4_LINKED_GOOGLE_ADS_CUSTOMER_IDS:-}",
	]) {
		assert.match(ga4BigQueryEnvDefinition, new RegExp(key.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
	}
});
