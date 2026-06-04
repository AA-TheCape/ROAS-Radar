import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import {
	chmodSync,
	copyFileSync,
	mkdirSync,
	mkdtempSync,
	readFileSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

function createDeployFixture(freezeId: string) {
	const tempDir = mkdtempSync(path.join(os.tmpdir(), "roas-radar-deploy-"));
	const scriptDir = path.join(tempDir, "infra", "cloud-run");
	const envDir = path.join(scriptDir, "environments");

	mkdirSync(envDir, { recursive: true });
	copyFileSync(
		path.resolve("infra/cloud-run/deploy.sh"),
		path.join(scriptDir, "deploy.sh"),
	);
	chmodSync(path.join(scriptDir, "deploy.sh"), 0o755);

	const productionEnv = readFileSync(
		path.resolve("infra/cloud-run/environments/production.env"),
		"utf8",
	).replace(
		'MMM_BASELINE_FREEZE_ID=""',
		`MMM_BASELINE_FREEZE_ID="${freezeId}"`,
	);
	writeFileSync(path.join(envDir, "fixture.env"), productionEnv);

	return {
		cleanup() {
			rmSync(tempDir, { recursive: true, force: true });
		},
		scriptPath: path.join(scriptDir, "deploy.sh"),
	};
}

test("Cloud Run deploy config validation fails when baseline MMM freeze id is missing", () => {
	const fixture = createDeployFixture("");

	try {
		const result = spawnSync("sh", [fixture.scriptPath, "fixture"], {
			env: {
				...process.env,
				VALIDATE_DEPLOY_CONFIG_ONLY: "true",
			},
			encoding: "utf8",
		});

		assert.notEqual(result.status, 0);
		assert.match(
			result.stderr,
			/missing required variable MMM_BASELINE_FREEZE_ID/,
		);
	} finally {
		fixture.cleanup();
	}
});

test("Cloud Run deploy config validation accepts configured baseline MMM freeze id", () => {
	const fixture = createDeployFixture("22222222-2222-4222-8222-222222222222");

	try {
		const result = spawnSync("sh", [fixture.scriptPath, "fixture"], {
			env: {
				...process.env,
				VALIDATE_DEPLOY_CONFIG_ONLY: "true",
			},
			encoding: "utf8",
		});

		assert.equal(result.status, 0, result.stderr);
		assert.match(
			result.stdout,
			/Cloud Run deployment configuration is valid for fixture/,
		);
	} finally {
		fixture.cleanup();
	}
});
