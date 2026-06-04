import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.join(__dirname, "..");

function readRepoFile(relativePath: string): string {
	return readFileSync(path.join(repoRoot, relativePath), "utf8");
}

test("production manual backfill runbook documents production gates and commands", () => {
	const text = readRepoFile(
		"docs/runbooks/production-manual-backfill-recovery.md",
	);

	assert.match(text, /## Stop Conditions/);
	assert.match(text, /matching staging rehearsal for the same date window/);
	assert.match(text, /## Production Readiness Gate/);
	assert.match(text, /## Handoff Checklist/);
	assert.match(text, /documentation-only walkthrough/);
	assert.match(text, /documentation alone/);
	assert.match(text, /roas-radar-shopify-order-reimport-production/);
	assert.match(text, /roas-radar-shopify-attribution-recovery-production/);
	assert.match(text, /roas-radar-ga4-fallback-recovery-production/);
	assert.match(text, /roas-radar-order-attribution-backfill-production/);
	assert.match(text, /roas-radar-campaign-metadata-backfill-production/);
	assert.match(text, /roas-radar-dead-letter-replay-production/);
	assert.match(text, /--dry-run/);
	assert.match(text, /--apply/);
	assert.match(text, /Validation Queries/);
	assert.match(text, /FROM recovery_job_runs/);
	assert.match(text, /FROM order_attribution_backfill_runs/);
	assert.match(text, /FROM campaign_metadata_backfill_runs/);
	assert.match(text, /ROAS Radar \* Recovery Run Failure Rate/);
});

test("docs indexes link the production manual backfill runbook", () => {
	const docsIndex = readRepoFile("docs/README.md");
	const rootReadme = readRepoFile("README.md");

	assert.match(
		docsIndex,
		/runbooks\/production-manual-backfill-recovery\.md/,
	);
	assert.match(
		rootReadme,
		/docs\/runbooks\/production-manual-backfill-recovery\.md/,
	);
});
