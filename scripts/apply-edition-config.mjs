import { readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

const SHIP_CONFIG_PATH = join('schemas', 'ship-groups.json');
const RAW_SHIPS_PATH = join('data', 'raw', 'LIVE', '4.3.1', 'ships.json');

function sanitizeToken(value) {
  return value
    .replace(/[^a-z0-9]+/gi, '_')
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '')
    .toUpperCase();
}

const editionMatchers = [
  {
    test: (name) => /Wikelo/i.test(name),
    transform: (name) => sanitizeToken(name)
  },
  {
    test: (name) => /PYAM\s+Exec/i.test(name),
    transform: (name) => sanitizeToken(name)
  }
];

async function main() {
  const [configRaw, shipsRaw] = await Promise.all([
    readFile(SHIP_CONFIG_PATH, 'utf8'),
    readFile(RAW_SHIPS_PATH, 'utf8')
  ]);
  const config = JSON.parse(configRaw);
  const ships = new Map(
    JSON.parse(shipsRaw).map((entry) => [entry.ClassName, entry.Name])
  );

  let updated = false;

  for (const hullEntry of Object.values(config.hulls)) {
    const variants = hullEntry.variants;
    for (const [variantKey, rawVariantValue] of Object.entries(variants)) {
      let value = rawVariantValue;
      let match;
      let configurations;

      if (typeof value === 'string') {
        match = value;
        configurations = {};
      } else if (value && typeof value === 'object') {
        match = typeof value.match === 'string' ? value.match : undefined;
        configurations = value.configurations ?? {};
      } else {
        continue;
      }

      if (!match) continue;
      const displayName = ships.get(match);
      if (!displayName) continue;

      for (const matcher of editionMatchers) {
        if (!matcher.test(displayName)) continue;
        const code = matcher.transform(displayName);
        if (!code) continue;
        if (!configurations) configurations = {};
        if (!configurations[code]) {
          configurations[code] = displayName;
          updated = true;
        }
      }

      if (typeof value === 'string') {
        if (Object.keys(configurations).length) {
          variants[variantKey] = {
            match,
            configurations
          };
        }
      } else if (value && typeof value === 'object') {
        if (configurations && Object.keys(configurations).length) {
          value.match = match;
          value.configurations = configurations;
        }
      }
    }
  }

  if (updated) {
    await writeFile(SHIP_CONFIG_PATH, JSON.stringify(config, null, 2) + '\n', 'utf8');
    console.log('Updated edition configurations in ship-groups.json');
  } else {
    console.log('No edition matches found.');
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
