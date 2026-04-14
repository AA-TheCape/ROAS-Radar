const CLICK_ID_TYPES = ['gclid', 'fbclid', 'ttclid', 'msclkid'] as const;

const SOURCE_ALIASES: Record<string, string> = {
  google: 'google',
  google_ads: 'google',
  googleadwords: 'google',
  google_adwords: 'google',
  adwords: 'google',
  youtube: 'google',
  bing: 'microsoft',
  microsoft: 'microsoft',
  microsoft_ads: 'microsoft',
  msn: 'microsoft',
  facebook: 'meta',
  fb: 'meta',
  instagram: 'meta',
  ig: 'meta',
  meta: 'meta',
  meta_ads: 'meta',
  tiktok: 'tiktok',
  tik_tok: 'tiktok',
  tt: 'tiktok',
  email: 'email',
  e_mail: 'email',
  newsletter: 'email',
  klaviyo: 'email',
  mailchimp: 'email',
  direct: 'direct',
  '(direct)': 'direct',
  referral: 'referral',
  organic: 'organic',
  organic_search: 'organic',
  seo: 'organic',
  shopify: 'shopify'
};

const MEDIUM_ALIASES: Record<string, string> = {
  cpc: 'cpc',
  ppc: 'cpc',
  sem: 'cpc',
  paidsearch: 'cpc',
  paid_search: 'cpc',
  paidsocial: 'paid_social',
  paid_social: 'paid_social',
  social_paid: 'paid_social',
  social: 'paid_social',
  email: 'email',
  e_mail: 'email',
  newsletter: 'email',
  organic: 'organic_search',
  organic_search: 'organic_search',
  seo: 'organic_search',
  referral: 'referral',
  affiliate: 'affiliate',
  display: 'display',
  banner: 'display',
  programmatic: 'display',
  sms: 'sms',
  text: 'sms',
  direct: 'direct',
  none: 'direct',
  '(none)': 'direct'
};

const CLICK_ID_SOURCE_MEDIUM_MAP: Record<CanonicalClickIdType, { source: string; medium: string }> = {
  gclid: {
    source: 'google',
    medium: 'cpc'
  },
  fbclid: {
    source: 'meta',
    medium: 'paid_social'
  },
  ttclid: {
    source: 'tiktok',
    medium: 'paid_social'
  },
  msclkid: {
    source: 'microsoft',
    medium: 'cpc'
  }
};

export const CANONICAL_UNKNOWN_VALUE = 'unknown';
export const CANONICAL_UNMAPPED_VALUE = 'unmapped';

export type CanonicalClickIdType = (typeof CLICK_ID_TYPES)[number];

export type CanonicalTouchpointDimensions = {
  source: string | null;
  medium: string | null;
  campaign: string | null;
  content: string | null;
  term: string | null;
  clickIdType: CanonicalClickIdType | null;
  clickIdValue: string | null;
};

export type CanonicalSpendDimensions = {
  source: string;
  medium: string;
  campaign: string;
  content: string;
  term: string;
};

type CanonicalTouchpointInput = {
  source?: string | null;
  medium?: string | null;
  campaign?: string | null;
  content?: string | null;
  term?: string | null;
  clickIdType?: string | null;
  clickIdValue?: string | null;
  gclid?: string | null;
  fbclid?: string | null;
  ttclid?: string | null;
  msclkid?: string | null;
};

type CanonicalSpendInput = {
  source?: string | null;
  medium?: string | null;
  campaign?: string | null;
  content?: string | null;
  term?: string | null;
  clickIdType?: string | null;
  clickIdValue?: string | null;
};

function normalizeNullableString(value: string | null | undefined): string | null {
  const trimmed = value?.trim();
  return trimmed ? trimmed : null;
}

function normalizeLookupKey(value: string | null | undefined): string | null {
  const normalized = normalizeNullableString(value)?.toLowerCase();

  if (!normalized) {
    return null;
  }

  return normalized.replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
}

function normalizeFreeformDimension(value: string | null | undefined): string | null {
  const normalized = normalizeNullableString(value)?.toLowerCase().replace(/\s+/g, ' ');
  return normalized && normalized.length > 0 ? normalized : null;
}

function normalizeClickIdType(value: string | null | undefined): CanonicalClickIdType | null {
  const normalized = normalizeLookupKey(value);

  if (!normalized) {
    return null;
  }

  return CLICK_ID_TYPES.find((candidate) => candidate === normalized) ?? null;
}

function resolveCanonicalClickId(input: CanonicalTouchpointInput | CanonicalSpendInput): {
  clickIdType: CanonicalClickIdType | null;
  clickIdValue: string | null;
} {
  const explicitClickIdType = normalizeClickIdType(input.clickIdType);
  const explicitClickIdValue = normalizeNullableString(input.clickIdValue);

  if (explicitClickIdType && explicitClickIdValue) {
    return {
      clickIdType: explicitClickIdType,
      clickIdValue: explicitClickIdValue
    };
  }

  const gclid = 'gclid' in input ? normalizeNullableString(input.gclid) : null;
  if (gclid) {
    return {
      clickIdType: 'gclid',
      clickIdValue: gclid
    };
  }

  const fbclid = 'fbclid' in input ? normalizeNullableString(input.fbclid) : null;
  if (fbclid) {
    return {
      clickIdType: 'fbclid',
      clickIdValue: fbclid
    };
  }

  const ttclid = 'ttclid' in input ? normalizeNullableString(input.ttclid) : null;
  if (ttclid) {
    return {
      clickIdType: 'ttclid',
      clickIdValue: ttclid
    };
  }

  const msclkid = 'msclkid' in input ? normalizeNullableString(input.msclkid) : null;
  if (msclkid) {
    return {
      clickIdType: 'msclkid',
      clickIdValue: msclkid
    };
  }

  return {
    clickIdType: explicitClickIdType,
    clickIdValue: explicitClickIdValue
  };
}

function canonicalizeSource(rawSource: string | null, clickIdType: CanonicalClickIdType | null): string | null {
  const normalizedKey = normalizeLookupKey(rawSource);

  if (normalizedKey) {
    return SOURCE_ALIASES[normalizedKey] ?? CANONICAL_UNMAPPED_VALUE;
  }

  if (clickIdType) {
    return CLICK_ID_SOURCE_MEDIUM_MAP[clickIdType].source;
  }

  return null;
}

function canonicalizeMedium(rawMedium: string | null, clickIdType: CanonicalClickIdType | null): string | null {
  const normalizedKey = normalizeLookupKey(rawMedium);

  if (normalizedKey) {
    return MEDIUM_ALIASES[normalizedKey] ?? CANONICAL_UNMAPPED_VALUE;
  }

  if (clickIdType) {
    return CLICK_ID_SOURCE_MEDIUM_MAP[clickIdType].medium;
  }

  return null;
}

export function buildCanonicalTouchpointDimensions(input: CanonicalTouchpointInput): CanonicalTouchpointDimensions {
  const resolvedClickId = resolveCanonicalClickId(input);

  return {
    source: canonicalizeSource(normalizeNullableString(input.source), resolvedClickId.clickIdType),
    medium: canonicalizeMedium(normalizeNullableString(input.medium), resolvedClickId.clickIdType),
    campaign: normalizeFreeformDimension(input.campaign),
    content: normalizeFreeformDimension(input.content),
    term: normalizeFreeformDimension(input.term),
    clickIdType: resolvedClickId.clickIdType,
    clickIdValue: resolvedClickId.clickIdValue
  };
}

export function buildCanonicalSpendDimensions(input: CanonicalSpendInput): CanonicalSpendDimensions {
  const touchpointDimensions = buildCanonicalTouchpointDimensions(input);

  return {
    source: touchpointDimensions.source ?? CANONICAL_UNKNOWN_VALUE,
    medium: touchpointDimensions.medium ?? CANONICAL_UNKNOWN_VALUE,
    campaign: touchpointDimensions.campaign ?? CANONICAL_UNKNOWN_VALUE,
    content: touchpointDimensions.content ?? CANONICAL_UNKNOWN_VALUE,
    term: touchpointDimensions.term ?? CANONICAL_UNKNOWN_VALUE
  };
}
