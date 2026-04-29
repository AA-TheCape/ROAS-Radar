import { replayDeadLetters, } from "../modules/dead-letters/index.js";
function readFlag(name) {
    const prefixed = `--${name}`;
    const index = process.argv.indexOf(prefixed);
    if (index === -1) {
        return null;
    }
    return process.argv[index + 1] ?? null;
}
function requireFlag(name) {
    const value = readFlag(name)?.trim();
    if (!value) {
        throw new Error(`Missing required flag --${name}`);
    }
    return value;
}
function optionalDate(value) {
    if (!value) {
        return undefined;
    }
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) {
        throw new Error(`Invalid date value: ${value}`);
    }
    return parsed;
}
function parseLimit(value) {
    if (!value) {
        return undefined;
    }
    const parsed = Number.parseInt(value, 10);
    if (!Number.isFinite(parsed) || parsed <= 0) {
        throw new Error(`Invalid limit value: ${value}`);
    }
    return parsed;
}
function parseStatus(value) {
    if (!value) {
        return undefined;
    }
    if (value === "pending_replay" || value === "replayed") {
        return value;
    }
    throw new Error(`Invalid status value: ${value}`);
}
async function run() {
    const result = await replayDeadLetters({
        requestedBy: requireFlag("requested-by"),
        eventType: readFlag("event-type") ?? undefined,
        sourceTable: readFlag("source-table") ?? undefined,
        status: parseStatus(readFlag("status")),
        fromTime: optionalDate(readFlag("from")),
        toTime: optionalDate(readFlag("to")),
        limit: parseLimit(readFlag("limit")),
        dryRun: process.argv.includes("--dry-run"),
    });
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
}
run().catch((error) => {
    const message = error instanceof Error ? error.message : String(error);
    process.stderr.write(`${message}\n`);
    process.exitCode = 1;
});
