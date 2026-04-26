import { createHash } from 'node:crypto';

const SHA256_HEX_PATTERN = /^[0-9a-f]{64}$/;
const E164_DIGIT_COUNT_PATTERN = /^\d{8,15}$/;

function sha256Hex(value: string): string {
  return createHash('sha256').update(value).digest('hex');
}

export function isSha256Hex(value: string | null | undefined): value is string {
  return typeof value === 'string' && SHA256_HEX_PATTERN.test(value);
}

export function normalizeEmailAddress(email: string | null | undefined): string | null {
  const normalized = email?.trim().toLowerCase();
  return normalized ? normalized : null;
}

export function hashEmailAddress(email: string | null | undefined): string | null {
  const normalized = normalizeEmailAddress(email);
  return normalized ? sha256Hex(normalized) : null;
}

export function normalizePhoneNumber(phone: string | null | undefined): string | null {
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

export function hashPhoneNumber(phone: string | null | undefined): string | null {
  const normalized = normalizePhoneNumber(phone);
  return normalized ? sha256Hex(normalized) : null;
}

export function buildHashedContactProfile(input: {
  email?: string | null;
  phone?: string | null;
}): {
  emailHash: string | null;
  phoneHash: string | null;
} {
  return {
    emailHash: hashEmailAddress(input.email),
    phoneHash: hashPhoneNumber(input.phone)
  };
}
