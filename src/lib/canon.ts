export type CanonicalVariantCode = 'BASE' | 'ES' | 'CL' | 'MR';

const VARIANT_TOKENS: CanonicalVariantCode[] = ['ES', 'CL', 'MR'];
const VARIANT_PATTERN = new RegExp(`\\b(${VARIANT_TOKENS.join('|')})\\b`, 'i');

const EDITION_KEYWORDS = [
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
  'EDITION',
  'EVENT'
];

const EDITION_REGEX = new RegExp(`\\b(${EDITION_KEYWORDS.join('|')})\\b`, 'i');
const LIVERY_REGEX = /\b(LIVERY|PAINT)\b/i;

function sanitizeToken(value: string): string {
  return value
    .replace(/[^a-z0-9]+/gi, '_')
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '')
    .toUpperCase();
}

function tokens(input: string): string[] {
  return input.split(/[^a-z0-9]+/i).filter(Boolean);
}

function titleCase(value: string): string {
  return value
    .toLowerCase()
    .split(/\s+/)
    .filter(Boolean)
    .map((part) => part[0].toUpperCase() + part.slice(1))
    .join(' ');
}

export function buildHullKey(manufacturer?: string, family?: string): string {
  const manufacturerToken = manufacturer ? sanitizeToken(manufacturer) : 'UNKNOWN';
  const familyToken = family ? sanitizeToken(family) : 'HULL';
  return `${manufacturerToken}_${familyToken}`;
}

export function extractVariantCode(source?: string): CanonicalVariantCode {
  if (!source) return 'BASE';
  const match = source.match(VARIANT_PATTERN);
  if (!match) return 'BASE';
  const value = match[1].toUpperCase();
  if (VARIANT_TOKENS.includes(value as CanonicalVariantCode)) {
    return value as CanonicalVariantCode;
  }
  return 'BASE';
}

export interface EditionDetection {
  editionCode?: string;
  livery?: string | null;
}

export function detectEditionOrLivery(name?: string): EditionDetection {
  if (!name) {
    return {};
  }

  const detected: string[] = [];
  for (const token of tokens(name)) {
    const upper = token.toUpperCase();
    if (EDITION_KEYWORDS.includes(upper)) {
      detected.push(upper);
    }
  }

  let editionCode: string | undefined;
  if (detected.length) {
    const priority = ['IAE', 'INVICTUS'];
    detected.sort((a, b) => {
      const score = (value: string) => {
        const idx = priority.indexOf(value);
        return idx === -1 ? priority.length : idx;
      };
      const diff = score(a) - score(b);
      return diff !== 0 ? diff : a.localeCompare(b);
    });
    editionCode = sanitizeToken(detected.join('_'));
  }

  let livery: string | null = null;
  const liveryMatch = name.match(LIVERY_REGEX);
  if (liveryMatch) {
    const tail = name.slice(liveryMatch.index! + liveryMatch[0].length).trim();
    if (tail) {
      const summary = tokens(tail).slice(0, 3).join(' ');
      if (summary) {
        livery = titleCase(summary);
      }
    }
  }

  return { editionCode, livery };
}

export function isEditionOnly(name?: string): boolean {
  if (!name) return false;
  if (extractVariantCode(name) !== 'BASE') return false;
  return EDITION_REGEX.test(name);
}

export function toCanonicalVariantExtId(hullKey: string, variantCode: CanonicalVariantCode): string {
  return `${hullKey}_${sanitizeToken(variantCode)}`;
}

export function cleanFamilyName(input: string, variantCode: CanonicalVariantCode, manufacturer?: string): string {
  const upperVariant = variantCode.toUpperCase();
  const manufacturerToken = manufacturer ? sanitizeToken(manufacturer) : undefined;
  const filtered = tokens(input).filter((token) => {
    const upper = token.toUpperCase();
    if (upper === upperVariant) return false;
    if (VARIANT_TOKENS.includes(upper as CanonicalVariantCode)) return false;
    if (EDITION_KEYWORDS.includes(upper)) return false;
    if (manufacturerToken && upper === manufacturerToken) return false;
    return true;
  });
  return filtered.length ? filtered.map(sanitizeToken).join('_') : sanitizeToken(input);
}

export function canonicalVariantName(baseName: string, variantCode: CanonicalVariantCode): string {
  if (variantCode === 'BASE') return titleCase(baseName);
  return `${titleCase(baseName)} ${variantCode}`;
}
