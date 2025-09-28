// Canonical variant suffixes observed in the data set (plus a few manual staples such as CL/ES/MR).
const VARIANT_TOKENS = [
  '1',
  '1T',
  '2',
  '25',
  '3',
  'A',
  'A1',
  'A2',
  'AA',
  'ALPHA',
  'ANDROMEDA',
  'ANTARES',
  'AQUILA',
  'ARCHIMEDES',
  'ARGOS',
  'ATLS',
  'BETA',
  'BIS2950',
  'BIS2951',
  'BLACK',
  'BLADE',
  'BLUE',
  'C',
  'C1',
  'C2',
  'CARBON',
  'CARGO',
  'CITIZENCON2018',
  'CIVILIAN',
  'CL',
  'COMET',
  'COMPETITION',
  'CROCODILE',
  'DELTA',
  'DS',
  'DUNESTALKER',
  'DUNLEVY',
  'DUR',
  'ECLIPSE',
  'EMERALD',
  'ES',
  'EX',
  'EXEC',
  'EXECUTIVE',
  'EXPEDITION',
  'F7C',
  'F7CM',
  'F7CR',
  'F7CS',
  'F8',
  'F8C',
  'FIREBIRD',
  'FORCE',
  'FORTUNE',
  'FREELANCER',
  'FURY',
  'GAMMA',
  'GEMINI',
  'GEO',
  'GLADIUS',
  'GLAIVE',
  'GRAD01',
  'GRAD02',
  'GRAD03',
  'GUARDIAN',
  'HAMMERHEAD',
  'HARBINGER',
  'HEARTSEEKER',
  'HOPLITE',
  'IKTI',
  'INDUST',
  'INDUSTRIAL',
  'INFERNO',
  'ION',
  'JAVELIN',
  'KUE',
  'LN',
  'LX',
  'M',
  'M2',
  'MAKO',
  'MAX',
  'MEDIC',
  'MEDIVAC',
  'MERLIN',
  'MILITARY',
  'MILT',
  'MIRU',
  'MK1',
  'MK2',
  'MOD',
  'MR',
  'MT',
  'MX',
  'NOX',
  'OMEGA',
  'P',
  'PEREGRINE',
  'PHOENIX',
  'PINK',
  'PIR',
  'PIRATE',
  'PISCES',
  'PLAT',
  'PROSPECTOR',
  'PULSE',
  'QI',
  'RAMBLER',
  'RAVEN',
  'RAZOR',
  'RC',
  'RECLAIMER',
  'RED',
  'REDEEMER',
  'RELIANT',
  'RENEGADE',
  'RETALIATOR',
  'RN',
  'ROVER',
  'RUNNER',
  'SABRE',
  'SCOUT',
  'SCYTHE',
  'SEN',
  'SENTINEL',
  'SHOWDOWN',
  'SHRIKE',
  'SNOWBLIND',
  'STALKER',
  'STARFARER',
  'STEALTH',
  'STEALTHINDUSTRIAL',
  'STEEL',
  'SYULEN',
  'TAC',
  'TALUS',
  'TANA',
  'TAURUS',
  'TITAN',
  'TOURING',
  'TR',
  'TRANSPORT',
  'TRIAGE',
  'UTILITY',
  'VALIANT',
  'VANGUARD',
  'VELOCITY',
  'WARLOCK',
  'WILDFIRE',
  'WOLF',
  'YELLOW'
] as const;

const VARIANT_TOKEN_SET = new Set<string>(VARIANT_TOKENS);

export type CanonicalVariantCode = string;

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
  let bestKnown: string | undefined;
  let fallback: string | undefined;

  for (const token of tokens(source)) {
    const normalized = sanitizeToken(token);
    if (!normalized) continue;
    if (normalized === 'BASE') {
      return 'BASE';
    }
    if (!fallback) {
      fallback = normalized;
    }
    if (VARIANT_TOKEN_SET.has(normalized)) {
      if (!bestKnown || normalized.length > bestKnown.length || (normalized.length === bestKnown.length && normalized < bestKnown)) {
        bestKnown = normalized;
      }
    }
  }

  return bestKnown ?? fallback ?? 'BASE';
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
  const variantToken = sanitizeToken(variantCode);
  const manufacturerTokens = new Set<string>();
  if (manufacturer) {
    const normalized = sanitizeToken(manufacturer);
    manufacturerTokens.add(normalized);
    if (normalized.endsWith('S')) {
      manufacturerTokens.add(normalized.slice(0, -1));
    }
    for (const token of tokens(manufacturer)) {
      manufacturerTokens.add(sanitizeToken(token));
    }
  }

  const filtered = tokens(input).filter((token) => {
    const upper = sanitizeToken(token);
    if (!upper) return false;
    if (upper === variantToken) return false;
    if (upper !== 'BASE' && VARIANT_TOKEN_SET.has(upper)) return false;
    if (EDITION_KEYWORDS.includes(upper)) return false;
    if (manufacturerTokens.has(upper)) return false;
    for (const candidate of manufacturerTokens) {
      if (candidate && (upper.startsWith(candidate) || candidate.startsWith(upper))) {
        return false;
      }
    }
    return true;
  });

  return filtered.length ? filtered.map(sanitizeToken).join('_') : sanitizeToken(input);
}

export function canonicalVariantName(baseName: string, variantCode: CanonicalVariantCode): string {
  if (variantCode === 'BASE') return titleCase(baseName);
  return `${titleCase(baseName)} ${variantCode}`;
}
