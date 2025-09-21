import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { Ajv, type ValidateFunction } from 'ajv';
import addMetaSchema2020Import from 'ajv/dist/refs/json-schema-2020-12/index.js';
import addFormatsPlugin, { type FormatsPluginOptions } from 'ajv-formats';
import { createRequire } from 'node:module';
import type { NormalizedDataBundle } from './types/index.js';

const require = createRequire(import.meta.url);
const ajv = new Ajv({ allErrors: true, strict: false });
if (typeof addMetaSchema2020Import === 'function') {
  (addMetaSchema2020Import as unknown as (this: Ajv, $data?: boolean) => Ajv).call(ajv);
} else {
  const metaSchemaFn = (addMetaSchema2020Import as {
    default?: (this: Ajv, $data?: boolean) => Ajv;
  }).default;
  if (typeof metaSchemaFn === 'function') {
    metaSchemaFn.call(ajv);
  }
}
const metaSchema202012 = require('ajv/dist/refs/json-schema-2020-12/schema.json');
if (!ajv.getSchema('https://json-schema.org/draft/2020-12/schema')) {
  ajv.addMetaSchema(metaSchema202012);
}
const addFormats = addFormatsPlugin as unknown as (
  ajv: Ajv,
  options?: FormatsPluginOptions
) => Ajv;
addFormats(ajv);

const validatorCache = new Map<string, ValidateFunction>();

async function loadValidator (schemaPath: string): Promise<ValidateFunction> {
  if (validatorCache.has(schemaPath)) {
    return validatorCache.get(schemaPath)!;
  }
  const schema = JSON.parse(await readFile(schemaPath, 'utf8'));
  const validator = ajv.compile(schema);
  validatorCache.set(schemaPath, validator);
  return validator;
}

function ensureArray (data: unknown, label: string): asserts data is unknown[] {
  if (!Array.isArray(data)) {
    throw new Error(`Expected ${label} to be an array.`);
  }
}

export async function validateArray (schemaPath: string, data: unknown, label: string) {
  ensureArray(data, label);
  const validator = await loadValidator(schemaPath);
  for (const [index, entry] of data.entries()) {
    if (!validator(entry)) {
      const message = ajv.errorsText(validator.errors, { dataVar: `${label}[${index}]` });
      throw new Error(message);
    }
  }
}

const defaultSchemaDir = join(process.cwd(), 'schemas');

export async function validateNormalizedBundle (
  bundle: NormalizedDataBundle,
  schemaDir: string = defaultSchemaDir
) {
  await Promise.all([
    validateArray(join(schemaDir, 'manufacturer.json'), bundle.manufacturers, 'manufacturers'),
    validateArray(join(schemaDir, 'ship.json'), bundle.ships, 'ships'),
    validateArray(join(schemaDir, 'ship_variant.json'), bundle.ship_variants, 'ship_variants'),
    validateArray(join(schemaDir, 'item.json'), bundle.items, 'items'),
    validateArray(join(schemaDir, 'hardpoint.json'), bundle.hardpoints, 'hardpoints'),
    validateArray(join(schemaDir, 'item_stats.json'), bundle.item_stats, 'item_stats'),
    validateArray(join(schemaDir, 'ship_stats.json'), bundle.ship_stats, 'ship_stats'),
    validateArray(join(schemaDir, 'installed_item.json'), bundle.installed_items, 'installed_items'),
    validateArray(join(schemaDir, 'locale.json'), bundle.locales, 'locales')
  ]);
}
