const VARIANT_PATTERN = /\b(ES|CL|MR)\b/i;
const EDITION_TOKENS = [
  'IAE',
  'INVICTUS',
  'WARBOND',
  'SHOWFLOOR',
  'SHOWROOM',
  'FOUNDATION',
  'FOUNDER',
  'PROMO',
  'LIVERY',
  'PAINT',
  'REFERRAL',
  'BUNDLE',
  'PACK',
  'JUBILEE',
  'LIMITED',
  'EDITION'
];

const EDITION_REGEX = new RegExp(`\\b(${EDITION_TOKENS.join('|')})\\b`, 'i');
const LIVERY_REGEX = /\b(LIVERY|PAINT)\b/i;

function sanitizeToken(value: string): string {
  return value.replace(/[^a-z0-9]+/gi, '_').replace(/_+/g, '_').replace(/^_|_$/g, '').toUpperCase();
}

function titleCase(value: string): string {
  return value
    .toLowerCase()
    .split(/\s+/)
    .map((part) => (part ? part[0].toUpperCase() + part.slice(1) : part))
    .join(' ')
    .trim();
}

export const VARIANT_CODES = ['ES', 'CL', 'MR'] as const;
export const VARIANT_CODE_SET: Set<string> = new Set(VARIANT_CODES);
export const EDITION_KEYWORDS = new Set(EDITION_TOKENS);

export function buildHullKey(manufacturer: string | undefined, family: string | undefined): string {
  const manufacturerPart = manufacturer ? sanitizeToken(manufacturer) : 'UNKNOWN';
  const familyPart = family ? sanitizeToken(family) : 'HULL';
  return `${manufacturerPart}_${familyPart}`;
}

export function extractVariantCode(source: string | undefined): 'ES' | 'CL' | 'MR' | 'BASE' {
  if (!source) return 'BASE';
  const match = source.match(VARIANT_PATTERN);
  if (match) {
    return match[1].toUpperCase() as 'ES' | 'CL' | 'MR';
  }
  return 'BASE';
}

export function detectEditionOrLivery(name: string | undefined) {
  if (!name) {
    return {} as { editionCode?: string; livery?: string };
  }

  const tokens = name.split(/[^a-z0-9]+/i).filter(Boolean);
  const editionTokens: string[] = [];
  for (const token of tokens) {
    const upper = token.toUpperCase();
    if (EDITION_KEYWORDS.has(upper)) {
      editionTokens.push(upper);
    }
    if (/^\d{3,4}$/.test(upper) && editionTokens.length) {
      const lastIndex = editionTokens.length - 1;
      editionTokens[lastIndex] = `${editionTokens[lastIndex]}${upper}`;
    }
  }

  if (editionTokens.length) {
    const priority = ['IAE', 'INVICTUS'];
    editionTokens.sort((a, b) => {
      const score = (value: string) => {
        const idx = priority.findIndex((token) => value.startsWith(token));
        return idx === -1 ? priority.length : idx;
      };
      const diff = score(a) - score(b);
      if (diff !== 0) return diff;
      return a.localeCompare(b);
    });
  }

  let livery: string | undefined;
  const liveryMatch = name.match(LIVERY_REGEX);
  if (liveryMatch) {
    const after = name.slice(liveryMatch.index! + liveryMatch[0].length).trim();
    if (after) {
      const tail = after.split(/[^a-z0-9]+/i).filter(Boolean).slice(0, 2);
      if (tail.length) {
        livery = tail.map((part) => titleCase(part)).join(' ');
      }
    }
  }

  return {
    editionCode: editionTokens.length ? sanitizeToken(editionTokens.join('_')) : undefined,
    livery
  } as { editionCode?: string; livery?: string };
}

export function isEditionOnly(name: string | undefined): boolean {
  if (!name) return false;
  const hasVariant = VARIANT_PATTERN.test(name);
  if (hasVariant) return false;
  return EDITION_REGEX.test(name);
}

export function toCanonicalVariantExtId(hullKey: string, code: string): string {
  return `${hullKey}_${sanitizeToken(code)}`;
}

export function cleanFamilyName(input: string, variantCode: string, manufacturer?: string): string {
  const tokens = input.split(/[^a-z0-9]+/i).filter(Boolean);
  const manufacturerToken = manufacturer ? sanitizeToken(manufacturer) : undefined;
  const filtered = tokens.filter((token) => {
    const upper = token.toUpperCase();
    if (VARIANT_CODE_SET.has(upper)) return false;
    if (EDITION_KEYWORDS.has(upper)) return false;
    if (upper === variantCode.toUpperCase()) return false;
    if (manufacturerToken && upper === manufacturerToken) return false;
    return true;
  });
  if (!filtered.length) {
    return sanitizeToken(input);
  }
  return filtered.map((token) => sanitizeToken(token)).join('_');
}

export function canonicalVariantName(baseName: string, variantCode: string): string {
  if (variantCode === 'BASE') return titleCase(baseName);
  return `${titleCase(baseName)} ${variantCode}`;
}
