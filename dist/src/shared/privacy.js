"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.isSha256Hex = isSha256Hex;
exports.normalizeEmailAddress = normalizeEmailAddress;
exports.hashEmailAddress = hashEmailAddress;
exports.normalizePhoneNumber = normalizePhoneNumber;
exports.hashPhoneNumber = hashPhoneNumber;
exports.buildHashedContactProfile = buildHashedContactProfile;
const node_crypto_1 = require("node:crypto");
const SHA256_HEX_PATTERN = /^[0-9a-f]{64}$/;
const E164_DIGIT_COUNT_PATTERN = /^\d{8,15}$/;
function sha256Hex(value) {
    return (0, node_crypto_1.createHash)('sha256').update(value).digest('hex');
}
function isSha256Hex(value) {
    return typeof value === 'string' && SHA256_HEX_PATTERN.test(value);
}
function normalizeEmailAddress(email) {
    const normalized = email?.trim().toLowerCase();
    return normalized ? normalized : null;
}
function hashEmailAddress(email) {
    const normalized = normalizeEmailAddress(email);
    return normalized ? sha256Hex(normalized) : null;
}
function normalizePhoneNumber(phone) {
    const trimmed = phone?.trim();
    if (!trimmed) {
        return null;
    }
    if (trimmed.startsWith('+')) {
        const digits = trimmed.slice(1).replace(/\D/g, '');
        return E164_DIGIT_COUNT_PATTERN.test(digits) ? `+${digits}` : null;
    }
    const digitsOnly = trimmed.replace(/\D/g, '');
    if (!digitsOnly) {
        return null;
    }
    if (digitsOnly.startsWith('00')) {
        const internationalDigits = digitsOnly.slice(2);
        return E164_DIGIT_COUNT_PATTERN.test(internationalDigits) ? `+${internationalDigits}` : null;
    }
    if (digitsOnly.length === 10) {
        return `+1${digitsOnly}`;
    }
    if (digitsOnly.length === 11 && digitsOnly.startsWith('1')) {
        return `+${digitsOnly}`;
    }
    return E164_DIGIT_COUNT_PATTERN.test(digitsOnly) ? `+${digitsOnly}` : null;
}
function hashPhoneNumber(phone) {
    const normalized = normalizePhoneNumber(phone);
    return normalized ? sha256Hex(normalized) : null;
}
function buildHashedContactProfile(input) {
    return {
        emailHash: hashEmailAddress(input.email),
        phoneHash: hashPhoneNumber(input.phone)
    };
}
