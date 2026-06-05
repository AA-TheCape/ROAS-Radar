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

function createDeployFixture(input: {
	freezeId: string;
	baselineSchedulerPaused: "true" | "false";
	omitObservabilityDashboardName?: boolean;
}) {
	const tempDir = mkdtempSync(path.join(os.tmpdir(), "roas-radar-deploy-"));
	const scriptDir = path.join(tempDir, "infra", "cloud-run");
	const envDir = path.join(scriptDir, "environments");

	mkdirSync(envDir, { recursive: true });
	copyFileSync(
		path.resolve("infra/cloud-run/deploy.sh"),
		path.join(scriptDir, "deploy.sh"),
	);
	chmodSync(path.join(scriptDir, "deploy.sh"), 0o755);

	let productionEnv = readFileSync(
		path.resolve("infra/cloud-run/environments/production.env"),
		"utf8",
	)
		.replace(
			/MMM_BASELINE_SCHEDULER_PAUSED="[^"]*"/,
			`MMM_BASELINE_SCHEDULER_PAUSED="${input.baselineSchedulerPaused}"`,
		)
		.replace(
			'MMM_BASELINE_FREEZE_ID=""',
			`MMM_BASELINE_FREEZE_ID="${input.freezeId}"`,
		);
	if (input.omitObservabilityDashboardName) {
		productionEnv = productionEnv.replace(
			/^OBSERVABILITY_DASHBOARD_DISPLAY_NAME="[^"]*"\n/m,
			"",
		);
	}
	writeFileSync(path.join(envDir, "fixture.env"), productionEnv);

	return {
		cleanup() {
			rmSync(tempDir, { recursive: true, force: true });
		},
		scriptPath: path.join(scriptDir, "deploy.sh"),
	};
}

test("Cloud Run deploy config validation fails when baseline MMM scheduler is enabled without a freeze id", () => {
	const fixture = createDeployFixture({
		freezeId: "",
		baselineSchedulerPaused: "false",
	});

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
			/MMM_BASELINE_FREEZE_ID is required when MMM_BASELINE_SCHEDULER_PAUSED=false/,
		);
	} finally {
		fixture.cleanup();
	}
});

test("Cloud Run deploy config validation accepts missing baseline MMM freeze id while scheduler is paused", () => {
	const fixture = createDeployFixture({
		freezeId: "",
		baselineSchedulerPaused: "true",
	});

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

test("Cloud Run deploy config validation accepts configured baseline MMM freeze id", () => {
	const fixture = createDeployFixture({
		freezeId: "22222222-2222-4222-8222-222222222222",
		baselineSchedulerPaused: "false",
	});

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

test("Cloud Run deploy config validation requires monitoring dashboard name", () => {
	const fixture = createDeployFixture({
		freezeId: "",
		baselineSchedulerPaused: "true",
		omitObservabilityDashboardName: true,
	});

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
			/missing required variable OBSERVABILITY_DASHBOARD_DISPLAY_NAME/,
		);
	} finally {
		fixture.cleanup();
	}
});
