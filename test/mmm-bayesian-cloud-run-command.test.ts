import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import test from "node:test";

const execFileAsync = promisify(execFile);

const commandEnv = {
	...process.env,
	DATABASE_URL: "postgres://user:pass@127.0.0.1:5432/roas_radar_test",
	MMM_BAYESIAN_LOOKBACK_DAYS: "90",
	MMM_BAYESIAN_LAG_DAYS: "1",
	MMM_BAYESIAN_ATTRIBUTION_MODEL: "last_touch",
	MMM_BAYESIAN_FREEZE_ID: "11111111-1111-4111-8111-111111111111",
};

test("Bayesian MMM Cloud Run command validates config without reading production data", async () => {
	const { stdout } = await execFileAsync(
		process.execPath,
		[
			"--import",
			"tsx",
			"src/admin/train-mmm-bayesian-hierarchical.ts",
			"--validate-config",
		],
		{
			env: commandEnv,
			timeout: 10_000,
		},
	);
	const payload = JSON.parse(stdout) as Record<string, unknown>;

	assert.equal(payload.ok, true);
	assert.equal(payload.command, "mmm:train-bayesian:start");
	assert.equal(payload.modelVersion, "bayesian_hierarchical_mmm_v1");
	assert.equal(payload.attributionModel, "last_touch");
	assert.equal(payload.approvedFreezeId, commandEnv.MMM_BAYESIAN_FREEZE_ID);
});

test("Bayesian MMM Cloud Run command fails validation when freeze id is missing", async () => {
	await assert.rejects(
		execFileAsync(
			process.execPath,
			[
				"--import",
				"tsx",
				"src/admin/train-mmm-bayesian-hierarchical.ts",
				"--validate-config",
			],
			{
				env: {
					...commandEnv,
					MMM_BAYESIAN_FREEZE_ID: "",
				},
				timeout: 10_000,
			},
		),
		/--freeze-id or MMM_BAYESIAN_FREEZE_ID/,
	);
});

test("Bayesian MMM Cloud Run command fails validation when database config is missing", async () => {
	const env = { ...commandEnv };
	delete env.DATABASE_URL;

	await assert.rejects(
		execFileAsync(
			process.execPath,
			[
				"--import",
				"tsx",
				"src/admin/train-mmm-bayesian-hierarchical.ts",
				"--validate-config",
			],
			{
				env,
				timeout: 10_000,
			},
		),
		/requires DATABASE_URL/,
	);
});
