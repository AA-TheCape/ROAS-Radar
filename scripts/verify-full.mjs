import { spawnSync } from "node:child_process";

const REQUIRED_NODE_MAJOR = 22;
const REQUIRED_NODE_MINOR = 13;

function fail(message) {
	process.stderr.write(`${message}\n`);
	process.exit(1);
}

function assertSupportedNode() {
	const [major = 0, minor = 0] = process.versions.node
		.split(".")
		.map((part) => Number.parseInt(part, 10));

	if (
		major < REQUIRED_NODE_MAJOR ||
		(major === REQUIRED_NODE_MAJOR && minor < REQUIRED_NODE_MINOR)
	) {
		fail(
			`Node ${REQUIRED_NODE_MAJOR}.${REQUIRED_NODE_MINOR}.0 or newer is required; current runtime is ${process.versions.node}. Run nvm use from the repository root.`,
		);
	}
}

function assertDatabaseUrl() {
	if (!process.env.DATABASE_URL?.trim()) {
		fail(
			"DATABASE_URL is required for full verification because migrations, integration tests, and attribution persistence tests use PostgreSQL.",
		);
	}
}

function run(label, command, args, options = {}) {
	process.stdout.write(`\n> ${label}\n`);

	const result = spawnSync(command, args, {
		stdio: "inherit",
		env: process.env,
		...options,
	});

	if (result.error) {
		throw result.error;
	}

	if ((result.status ?? 1) !== 0) {
		process.exit(result.status ?? 1);
	}
}

assertSupportedNode();
assertDatabaseUrl();

run("Install backend dependencies", "npm", ["ci", "--include=dev"]);
run("Build backend", "npm", ["run", "build"]);
run("Lint backend", "npm", ["run", "lint"]);
run("Install dashboard dependencies", "npm", ["ci", "--include=dev"], {
	cwd: "dashboard",
});
run("Build dashboard", "npm", ["run", "build"], { cwd: "dashboard" });
run("Lint dashboard", "npm", ["run", "lint"], { cwd: "dashboard" });
run("Validate database migrations", "npm", ["run", "db:migrate:check"]);
run("Run DB-backed integration tests", "npm", ["run", "test:integration"]);
run("Run attribution persistence tests", "npm", ["run", "test:attribution"]);
