"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const index_js_1 = require("../modules/dead-letters/index.js");
async function run() {
    const result = await (0, index_js_1.replayDeadLetters)({
        requestedBy: requireFlag('requested-by'),
        eventType: readFlag('event-type') ?? undefined,
        sourceTable: readFlag('source-table') ?? undefined,
        windowStart: optionalDate(readFlag('from')),
        windowEnd: optionalDate(readFlag('to')),
        limit: parsedLimit,
        dryRun: process.argv.includes('--dry-run')
    });
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
}
