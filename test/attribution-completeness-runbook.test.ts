import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const runbookPath = path.join(
	__dirname,
	"..",
	"docs",
	"runbooks",
	"attribution-completeness.md",
);

test("attribution completeness runbook documents the Shopify recovery order and backfill options", () => {
	const text = readFileSync(runbookPath, "utf8");

	assert.match(text, /## Shopify Recovery Order/);
	assert.match(text, /1\. Import Shopify orders/);
	assert.match(text, /2\. Recover attribution hints/);
	assert.match(text, /3\. Backfill order attribution/);
	assert.match(
		text,
		/Always run a dry run first for the exact same date window before queueing a write-enabled run\./,
	);
	assert.match(text, /`dryRun`: defaults to `true`\./);
	assert.match(text, /`webOrdersOnly`: defaults to `true`\./);
	assert.match(text, /`skipShopifyWriteback`: defaults to `false`\./);
});
