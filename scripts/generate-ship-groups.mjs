import { readFile, writeFile } from 'node:fs/promises';
import { join, basename } from 'node:path';
import fg from 'fast-glob';

const VARIANT_TOKENS = [
  '1', '1T', '2', '25', '3', 'A', 'A1', 'A2', 'AA', 'ALPHA', 'ANDROMEDA', 'ANTARES',
  'AQUILA', 'ARCHIMEDES', 'ARGOS', 'ATLS', 'BETA', 'BIS2950', 'BIS2951', 'BLACK', 'BLADE',
  'BLUE', 'C', 'C1', 'C2', 'CARBON', 'CARGO', 'CITIZENCON2018', 'CIVILIAN', 'CL', 'COMET',
  'COMPETITION', 'CROCODILE', 'DELTA', 'DS', 'DUNESTALKER', 'DUNLEVY', 'DUR', 'ECLIPSE',
  'EMERALD', 'ES', 'EX', 'EXEC', 'EXECUTIVE', 'EXPEDITION', 'F7C', 'F7CM', 'F7CR', 'F7CS',
  'F8', 'F8C', 'FIREBIRD', 'FORCE', 'FORTUNE', 'FREELANCER', 'FURY', 'GAMMA', 'GEMINI', 'GEO',
  'GLADIUS', 'GLAIVE', 'GRAD01', 'GRAD02', 'GRAD03', 'GUARDIAN', 'HAMMERHEAD', 'HARBINGER',
  'HEARTSEEKER', 'HOPLITE', 'IKTI', 'INDUST', 'INDUSTRIAL', 'INFERNO', 'ION', 'JAVELIN', 'KUE',
  'LN', 'LX', 'M', 'M2', 'MAKO', 'MAX', 'MEDIC', 'MEDIVAC', 'MERLIN', 'MILITARY', 'MILT',
  'MIRU', 'MK1', 'MK2', 'MOD', 'MR', 'MT', 'MX', 'NOX', 'OMEGA', 'P', 'PEREGRINE', 'PHOENIX',
  'PINK', 'PIR', 'PIRATE', 'PISCES', 'PLAT', 'PROSPECTOR', 'PULSE', 'QI', 'RAMBLER', 'RAVEN',
  'RAZOR', 'RC', 'RECLAIMER', 'RED', 'REDEEMER', 'RELIANT', 'RENEGADE', 'RETALIATOR', 'RN',
  'ROVER', 'RUNNER', 'SABRE', 'SCOUT', 'SCYTHE', 'SEN', 'SENTINEL', 'SHOWDOWN', 'SHRIKE',
  'SNOWBLIND', 'STALKER', 'STARFARER', 'STEALTH', 'STEEL', 'SYULEN', 'TAC', 'TALUS', 'TANA',
  'TAURUS', 'TITAN', 'TOURING', 'TR', 'TRANSPORT', 'TRIAGE', 'UTILITY', 'VALIANT', 'VANGUARD',
  'VELOCITY', 'WARLOCK', 'WILDFIRE', 'WOLF', 'YELLOW'
];

const VARIANT_TOKEN_SET = new Set(VARIANT_TOKENS);

function sanitizeToken(value) {
  return value
    .replace(/[^a-z0-9]+/gi, '_')
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '')
    .toUpperCase();
}

function partitionVariantSuffix(tokens) {
  if (!tokens.length) {
    return { base: [], suffix: [] };
  }
  const base = [...tokens];
  const suffix = [];
  while (base.length > 1) {
    const candidate = base[base.length - 1];
    if (!candidate || !VARIANT_TOKEN_SET.has(candidate)) break;
    suffix.unshift(candidate);
    base.pop();
  }
  if (!base.length && suffix.length) {
    base.push(...suffix);
    suffix.length = 0;
  }
  return { base, suffix };
}

function buildHullKey(manufacturer, family) {
  const manufacturerToken = manufacturer ? sanitizeToken(manufacturer) : 'UNKNOWN';
  const familyToken = family ? sanitizeToken(family) : 'HULL';
  return `${manufacturerToken}_${familyToken}`;
}

function titleCaseTokens(tokens) {
  if (!tokens.length) return 'Hull';
  return tokens
    .map((token) => token.toLowerCase())
    .map((token) => token.split('_').map((part) => part ? part[0].toUpperCase() + part.slice(1) : part).join(' '))
    .join(' ')
    .replace(/_/g, ' ');
}

const DATA_ROOT = 'data/raw';
const CHANNEL = process.argv[2] ?? 'LIVE';
const VERSION = process.argv[3] ?? '4.3.1';
const rawDir = join(DATA_ROOT, CHANNEL, VERSION);

const hulls = new Map();

const files = await fg('ships/*.json', { cwd: rawDir });

for (const relative of files) {
  if (relative.endsWith('-raw.json')) continue;
  const absolute = join(rawDir, relative);
  const data = JSON.parse(await readFile(absolute, 'utf8'));
  const fileBase = basename(relative, '.json');
  const className = typeof data.ClassName === 'string' ? data.ClassName : undefined;
  const manufacturerCode =
    (data.Manufacturer && typeof data.Manufacturer.Code === 'string' && data.Manufacturer.Code) ||
    (data.manufacturer && typeof data.manufacturer.code === 'string' && data.manufacturer.code) ||
    fileBase.split('_')[0] ||
    'UNKNOWN';
  const manufacturer = sanitizeToken(manufacturerCode);
  const parts = fileBase.split('_').slice(1);
  const tokens = parts.map((part) => sanitizeToken(part)).filter(Boolean);
  const { base: familyTokens, suffix } = partitionVariantSuffix(tokens);
  const effectiveFamily = familyTokens.length ? familyTokens : tokens;
  const familyKey = effectiveFamily.length ? effectiveFamily.join('_') : 'HULL';
  const hullKey = buildHullKey(manufacturer, familyKey);
  const hullName = titleCaseTokens(effectiveFamily);
  const variantTokens = suffix.length ? suffix : [];
  const variantCode = variantTokens.length ? variantTokens.join('_') : 'BASE';
  const matchId = className ?? fileBase;

  let entry = hulls.get(hullKey);
  if (!entry) {
    entry = {
      manufacturer,
      name: hullName,
      variants: new Map()
    };
    hulls.set(hullKey, entry);
  }

  if (!entry.variants.has(variantCode)) {
    entry.variants.set(variantCode, matchId);
  }
}

const sortedHulls = [...hulls.entries()].sort(([a], [b]) => a.localeCompare(b));
const payload = {
  _comment: `Generated from ${CHANNEL}/${VERSION} raw ship data on ${new Date().toISOString()}`,
  hulls: Object.fromEntries(
    sortedHulls.map(([key, entry]) => [
      key,
      {
        manufacturer: entry.manufacturer,
        name: entry.name,
        variants: Object.fromEntries([...entry.variants.entries()].sort(([a], [b]) => a.localeCompare(b)))
      }
    ])
  )
};

const outputPath = join('schemas', 'ship-groups.json');
await writeFile(outputPath, JSON.stringify(payload, null, 2) + '\n', 'utf8');
console.log(`Wrote ${outputPath} for ${sortedHulls.length} hulls.`);
