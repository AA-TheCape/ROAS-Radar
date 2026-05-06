import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import {
	buildHashedContactProfile,
	hashEmailAddress,
	hashPhoneNumber,
	isSha256Hex,
	normalizeEmailAddress,
	normalizePhoneNumber,
} from "../src/shared/privacy.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");

test("email hashing is deterministic across casing and whitespace differences", () => {
	const firstHash = hashEmailAddress(" Buyer@Example.com ");
	const secondHash = hashEmailAddress("buyer@example.com");

	assert.equal(firstHash, secondHash);
	assert.equal(
		normalizeEmailAddress(" Buyer@Example.com "),
		"buyer@example.com",
	);
	assert.equal(isSha256Hex(firstHash), true);
});

test("phone normalization canonicalizes supported formats to E.164 before hashing", () => {
	assert.equal(normalizePhoneNumber("(415) 555-2671"), "+14155552671");
	assert.equal(normalizePhoneNumber("+1 415 555 2671"), "+14155552671");
	assert.equal(normalizePhoneNumber("0044 20 7946 0958"), "+442079460958");
	assert.equal(normalizePhoneNumber("12345"), null);
	assert.equal(
		hashPhoneNumber("(415) 555-2671"),
		hashPhoneNumber("+1 415 555 2671"),
	);
	assert.equal(isSha256Hex(hashPhoneNumber("(415) 555-2671")), true);
});

test("hashed contact helpers never return plaintext fields", () => {
	const profile = buildHashedContactProfile({
		email: "Buyer@example.com",
		phone: "(415) 555-2671",
	});

	assert.deepEqual(Object.keys(profile).sort(), ["emailHash", "phoneHash"]);
	assert.equal(profile.emailHash, hashEmailAddress("buyer@example.com"));
	assert.equal(profile.phoneHash, hashPhoneNumber("+14155552671"));
});

test("shopify and identity logging code avoids raw email or phone fields", async () => {
	const files = [
		path.join(repoRoot, "src/modules/shopify/index.ts"),
		path.join(repoRoot, "src/modules/identity/index.ts"),
	];

	for (const file of files) {
		const source = await readFile(file, "utf8");
		const logCalls =
			source.match(
				/(?:logInfo|logWarning|logError|console\.(?:log|warn|error|info))\([\s\S]*?\n\s*\);/g,
			) ?? [];

		for (const logCall of logCalls) {
			assert.doesNotMatch(logCall, /\bemail\s*:/);
			assert.doesNotMatch(logCall, /\bphone\s*:/);
		}
	}
});
