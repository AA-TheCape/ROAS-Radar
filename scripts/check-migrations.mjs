import { spawnSync } from "node:child_process";
import { readdir } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import pg from "pg";

const { Client } = pg;

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");
const migrationsDir = path.join(repoRoot, "db", "migrations");

function runMigrations() {
	const result = spawnSync("npx", ["tsx", "src/db/migrate.ts"], {
		cwd: repoRoot,
		stdio: "inherit",
		env: process.env,
	});

	if (result.status !== 0) {
		process.exit(result.status ?? 1);
	}
}

async function verifyMigrations() {
	if (!process.env.DATABASE_URL) {
		throw new Error("DATABASE_URL is required for migration verification");
	}

	runMigrations();
	runMigrations();

	const expectedFiles = (await readdir(migrationsDir))
		.filter((file) => file.endsWith(".sql"))
		.sort();
	const client = new Client({ connectionString: process.env.DATABASE_URL });

	await client.connect();

	try {
		const migrationCountResult = await client.query(
			"SELECT COUNT(*)::text AS count FROM schema_migrations",
		);
		const appliedCount = Number(migrationCountResult.rows[0]?.count ?? "0");

		if (appliedCount !== expectedFiles.length) {
			throw new Error(
				`Expected ${expectedFiles.length} applied migrations, found ${appliedCount}`,
			);
		}
	} finally {
		await client.end();
	}
}

verifyMigrations().catch((error) => {
	process.stderr.write(
		`${error instanceof Error ? error.stack : String(error)}\n`,
	);
	process.exit(1);
});
