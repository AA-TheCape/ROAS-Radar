import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { resolveMetaAdsRuntimeDescriptor } from "../src/modules/meta-ads/runtime.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.join(__dirname, "..");
const deployScript = readFileSync(path.join(repoRoot, "infra/cloud-run/deploy.sh"), "utf8");
const spendSection =
	deployScript.match(/gcloud run jobs deploy "\$META_ADS_JOB_NAME"[\s\S]*?ensure_job_invoker "\$META_ADS_JOB_NAME"/)?.[0] ?? "";
const orderValueSection =
	deployScript.match(/gcloud run jobs deploy "\$META_ADS_ORDER_VALUE_JOB_NAME"[\s\S]*?ensure_job_invoker "\$META_ADS_ORDER_VALUE_JOB_NAME"/)?.[0] ?? "";
const originalMetaAdsJobMode = process.env.META_ADS_JOB_MODE;

test.afterEach(() => {
	if (originalMetaAdsJobMode === undefined) {
		Reflect.deleteProperty(process.env, "META_ADS_JOB_MODE");
	} else {
		process.env.META_ADS_JOB_MODE = originalMetaAdsJobMode;
	}
});

test("resolveMetaAdsRuntimeDescriptor rejects mixed-mode Cloud Run execution", () => {
	process.env.META_ADS_JOB_MODE = "order_value";

	assert.throws(
		() => resolveMetaAdsRuntimeDescriptor("spend"),
		/Meta Ads runtime mode mismatch: expected spend, received order_value/,
	);
});

test("Cloud Run deploy script keeps spend and order-value job entrypoints isolated", async () => {
	assert.match(spendSection, /--args=run,meta-ads:sync:start/);
	assert.match(spendSection, /META_ADS_JOB_MODE=spend/);
	assert.doesNotMatch(spendSection, /META_ADS_ORDER_VALUE_SYNC_ENABLED=/);

	assert.match(orderValueSection, /--args=run,meta-ads:order-value:start/);
	assert.match(orderValueSection, /META_ADS_JOB_MODE=order_value/);
	assert.match(orderValueSection, /META_ADS_ORDER_VALUE_SYNC_ENABLED=/);
});
