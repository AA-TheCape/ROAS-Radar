import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import test from "node:test";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const commandEnv = {
	...process.env,
	DATABASE_URL: "postgres://user:pass@127.0.0.1:5432/roas_radar_test",
	MMM_BASELINE_LOOKBACK_DAYS: "90",
	MMM_BASELINE_LAG_DAYS: "1",
	MMM_BASELINE_ATTRIBUTION_MODEL: "last_touch",
	MMM_BASELINE_FREEZE_ID: "22222222-2222-4222-8222-222222222222",
};

test("Baseline MMM Cloud Run command validates config without reading production data", async () => {
	const { stdout } = await execFileAsync(
		process.execPath,
		["--import", "tsx", "src/admin/train-mmm-baseline.ts", "--validate-config"],
		{
			env: commandEnv,
			timeout: 10_000,
		},
	);
	const payload = JSON.parse(stdout) as Record<string, unknown>;

	assert.equal(payload.ok, true);
	assert.equal(payload.command, "mmm:train-baseline:start");
	assert.equal(payload.modelVersion, "baseline_linear_mmm_v1");
	assert.equal(payload.attributionModel, "last_touch");
	assert.equal(payload.approvedFreezeId, commandEnv.MMM_BASELINE_FREEZE_ID);
});

test("Baseline MMM Cloud Run command fails validation when freeze id is missing", async () => {
	await assert.rejects(
		execFileAsync(
			process.execPath,
			[
				"--import",
				"tsx",
				"src/admin/train-mmm-baseline.ts",
				"--validate-config",
			],
			{
				env: {
					...commandEnv,
					MMM_BASELINE_FREEZE_ID: "",
				},
				timeout: 10_000,
			},
		),
		/--freeze-id or MMM_BASELINE_FREEZE_ID/,
	);
});

test("Baseline MMM Cloud Run command fails validation when database config is missing", async () => {
	const env = { ...commandEnv };
	env.DATABASE_URL = undefined;

	await assert.rejects(
		execFileAsync(
			process.execPath,
			[
				"--import",
				"tsx",
				"src/admin/train-mmm-baseline.ts",
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
