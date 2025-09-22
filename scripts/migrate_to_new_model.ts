#!/usr/bin/env tsx
import 'dotenv/config';
import { argv, exit } from 'node:process';
import { join } from 'node:path';
import { log } from '../src/utils/log.js';
import { directus, readByQuery, createOne, updateOne, createMany, updateMany } from '../src/utils/directus.js';
import { loadTransformConfig } from '../src/config/transform.js';
import {
  buildHullKey,
  cleanFamilyName,
  extractVariantCode,
  canonicalVariantName,
  detectEditionOrLivery,
  isEditionOnly,
  toCanonicalVariantExtId
} from '../src/lib/canon.js';
import type {
  NormalizedExternalReference,
  NormalizedHardpointV2,
  ShipVariantStatsV2
} from '../src/types/index.js';

interface CompanyRow {
  id: string;
  code?: string | null;
  name?: string | null;
  content?: string | null;
}

interface NewCompanyRow {
  id: string;
  code?: string | null;
}

interface ShipRow {
  id: string;
  name?: string | null;
  class?: string | null;
  size?: string | null;
  manufacturer?: string | { id?: string } | null;
  manufacturer_id?: string | null;
  manufacturer_code?: string | null;
  manufacturer_name?: string | null;
  description?: string | null;
  wiki_slug?: string | null;
}

interface ShipVariantRow {
  id: string;
  ship?: string | { id?: string } | null;
  ship_id?: string | null;
  variant_code?: string | null;
  name?: string | null;
  external_id?: string | null;
  thumbnail?: string | null;
  patch?: string | null;
}

interface ShipStatRow {
  id: string;
  ship_variant?: string | { id?: string } | null;
  stats?: Record<string, unknown> | null;
}

interface HardpointRow {
  id: string;
  ship_variant?: string | { id?: string } | null;
  code?: string | null;
  category?: string | null;
  position?: string | null;
  size?: number | null;
  gimballed?: boolean | null;
  powered?: boolean | null;
  seats?: number | null;
}

interface ItemRow {
  id: string;
  external_id?: string | null;
  type?: string | null;
  subtype?: string | null;
  name?: string | null;
  manufacturer?: string | { id?: string } | null;
  size?: number | null;
  grade?: number | string | null;
  class?: string | null;
  description?: string | null;
}

interface ItemStatRow {
  id: string;
  item?: string | { id?: string } | null;
  stats?: Record<string, unknown> | null;
  price_auec?: unknown;
  availability?: unknown;
}

interface MigrationOptions {
  apply: boolean;
  hardpointsAsCollection: boolean;
  allowedItemTypes: Set<string>;
}

interface CompanySnapshot {
  id: string;
  code: string;
  name: string;
  description?: string | null;
  externalRefs: NormalizedExternalReference[];
}

interface HullSnapshot {
  shipId: string;
  hullKey: string;
  name: string;
  companyCode: string;
  paints: Set<string>;
  externalRefs: NormalizedExternalReference[];
}

interface VariantSnapshot {
  external_id: string;
  ship_external: string;
  name: string;
  variant_code?: string;
  external_refs: NormalizedExternalReference[];
  thumbnail?: string;
  release_patch?: string;
  stats: ShipVariantStatsV2;
}

interface ItemSnapshot {
  external_id: string;
  name: string;
  company_code?: string;
  type: string;
  subtype?: string;
  size?: number;
  grade?: string;
  class?: string;
  description?: string;
  stats: Record<string, unknown>;
  external_refs: NormalizedExternalReference[];
}

interface HardpointSnapshot {
  external_id: string;
  ship_variant_external: string;
  code: string;
  category: string;
  position?: string;
  size?: number;
  gimballed?: boolean;
  powered?: boolean;
  seats?: number;
}

const OLD_COLLECTIONS = {
  companies: 'companies',
  ships: 'ships',
  variants: 'ship_variants',
  shipStats: 'ship_stats',
  items: 'items',
  itemStats: 'item_stats',
  hardpoints: 'hardpoints'
} as const;

const NEW_COLLECTIONS = {
  companies: process.env.SC_COMPANY_COLLECTION ?? 'sc_companies',
  ships: process.env.SC_SHIP_COLLECTION ?? 'sc_ships',
  shipVariants: process.env.SC_SHIP_VARIANT_COLLECTION ?? 'sc_ship_variants',
  items: process.env.SC_ITEM_COLLECTION ?? 'sc_items',
  hardpoints: process.env.SC_HARDPOINT_COLLECTION ?? 'sc_hardpoints'
} as const;

const REVISION_TARGETS = [
  NEW_COLLECTIONS.companies,
  NEW_COLLECTIONS.ships,
  NEW_COLLECTIONS.shipVariants,
  NEW_COLLECTIONS.items,
  NEW_COLLECTIONS.hardpoints
];

function normalizeCode(value: unknown, fallback?: string): string {
  const raw = typeof value === 'string' ? value : typeof fallback === 'string' ? fallback : '';
  const trimmed = raw.trim();
  if (!trimmed) {
    throw new Error('Missing company code.');
  }
  return trimmed.toUpperCase();
}

function normalizeString(value: unknown): string | undefined {
  if (typeof value === 'string') {
    const trimmed = value.trim();
    return trimmed || undefined;
  }
  if (value === undefined || value === null) return undefined;
  return String(value).trim() || undefined;
}

function normalizeNumber(value: unknown): number | undefined {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) return undefined;
    const parsed = Number(trimmed);
    if (Number.isFinite(parsed)) return parsed;
  }
  return undefined;
}

function cloneJson<T>(value: T): T {
  return value === undefined || value === null ? value : JSON.parse(JSON.stringify(value));
}

function ref(source: string, id: unknown, note?: string | null): NormalizedExternalReference | undefined {
  if (id === undefined || id === null) return undefined;
  const normalised = String(id);
  if (!normalised.trim()) return undefined;
  const entry: NormalizedExternalReference = {
    source,
    id: normalised.trim()
  };
  if (note && note.trim()) {
    entry.note = note.trim();
  }
  return entry;
}

function extractId(value: unknown): string | undefined {
  if (!value) return undefined;
  if (typeof value === 'string' && value.trim()) return value.trim();
  if (typeof value === 'object' && value !== null && 'id' in value) {
    const candidate = (value as { id?: unknown }).id;
    if (typeof candidate === 'string' && candidate.trim()) return candidate.trim();
  }
  return undefined;
}

function snapshotToRecord<T extends Record<string, unknown>>(snapshot: T): Record<string, unknown> {
  return snapshot as Record<string, unknown>;
}

async function fetchAll<T>(
  collection: string,
  fields: string[],
  filter?: Record<string, unknown>
): Promise<T[]> {
  const result: T[] = [];
  const limit = 200;
  let offset = 0;
  while (true) {
    const batch = await readByQuery<T>(collection, {
      fields,
      filter,
      limit,
      offset
    });
    if (!batch.length) break;
    result.push(...batch);
    if (batch.length < limit) break;
    offset += limit;
  }
  return result;
}

function chunk<T>(items: readonly T[], size = 200): T[][] {
  if (size <= 0) throw new Error('Chunk size must be positive.');
  const buckets: T[][] = [];
  for (let i = 0; i < items.length; i += size) {
    buckets.push(items.slice(i, i + size));
  }
  return buckets;
}

function parseCliOptions(): MigrationOptions {
  const args = argv.slice(2);
  let apply = false;
  let hardpointsAsCollection = true;
  let allowedTypesOverride: Set<string> | undefined;

  for (let i = 0; i < args.length; i++) {
    const token = args[i];
    if (token === '--apply') {
      apply = true;
      continue;
    }
    if (token === '--dry-run') {
      apply = false;
      continue;
    }
    if (token.startsWith('--hardpoints=')) {
      const [, value] = token.split('=', 2);
      hardpointsAsCollection = !['false', '0', 'no'].includes((value ?? '').toLowerCase());
      continue;
    }
    if (token === '--hardpoints-as-collection') {
      hardpointsAsCollection = true;
      continue;
    }
    if (token === '--hardpoints-in-stats') {
      hardpointsAsCollection = false;
      continue;
    }
    if (token.startsWith('--allowed-types=')) {
      const [, value] = token.split('=', 2);
      if (value) {
        const set = new Set<string>();
        for (const part of value.split(/[\s,]+/)) {
          const trimmed = part.trim();
          if (trimmed) set.add(trimmed.toUpperCase());
        }
        allowedTypesOverride = set;
      }
      continue;
    }
  }

  const config = loadTransformConfig();
  const allowedItemTypes = allowedTypesOverride ?? config.allowedItemTypes;
  if (allowedItemTypes.size === 0) {
    log.warn('ALLOWED_ITEM_TYPES is empty; migration will process all item types.');
  }

  return {
    apply,
    hardpointsAsCollection,
    allowedItemTypes
  };
}

async function ensureVersioning(collection: string, apply: boolean) {
  try {
    const response = await directus.request<{ data?: { meta?: { versioning?: boolean } } }>({
      method: 'GET',
      path: `/collections/${collection}`
    });
    const enabled = Boolean(response?.data?.meta?.versioning);
    if (enabled) {
      log.debug(`Versioning already enabled for ${collection}`);
      return;
    }
    log.info(`Enabling versioning for ${collection}${apply ? '' : ' (dry-run)'}`);
    if (apply) {
      await directus.request({
        method: 'PATCH',
        path: `/collections/${collection}`,
        body: {
          meta: {
            versioning: true
          }
        }
      });
    }
  } catch (error) {
    log.error(`Failed to ensure versioning for ${collection}`, error);
    if (apply) throw error;
  }
}

function resolveCompanySnapshots(rows: CompanyRow[]): {
  snapshots: CompanySnapshot[];
  idMap: Map<string, CompanySnapshot>;
  canonicalByLower: Map<string, CompanySnapshot>;
  duplicates: Array<{ duplicate: CompanyRow; canonical: CompanySnapshot }>;
} {
  const idMap = new Map<string, CompanySnapshot>();
  const canonicalByLower = new Map<string, CompanySnapshot>();
  const duplicates: Array<{ duplicate: CompanyRow; canonical: CompanySnapshot }> = [];

  for (const row of rows) {
    const codeSource = row.code ?? row.name ?? row.id;
    if (!codeSource) {
      log.warn('Skipping company without code', { id: row.id });
      continue;
    }
    let code: string;
    try {
      code = normalizeCode(codeSource);
    } catch (error) {
      log.warn('Unable to normalize company code', {
        id: row.id,
        error: error instanceof Error ? error.message : String(error)
      });
      continue;
    }
    const lower = code.toLowerCase();
    let canonical = canonicalByLower.get(lower);
    if (!canonical) {
      canonical = {
        id: row.id,
        code,
        name: row.name ?? code,
        description: row.content ?? null,
        externalRefs: [
          ...(ref('directus:companies.id', row.id) ? [ref('directus:companies.id', row.id)!] : []),
          ...(row.code && row.code.trim().toUpperCase() !== code
            ? [ref('directus:companies.code', row.code)!]
            : [])
        ].filter(Boolean) as NormalizedExternalReference[]
      };
      canonicalByLower.set(lower, canonical);
      idMap.set(row.id, canonical);
    } else {
      duplicates.push({ duplicate: row, canonical });
      idMap.set(row.id, canonical);
    }
  }

  const snapshots = Array.from(new Set(canonicalByLower.values()));
  return { snapshots, idMap, canonicalByLower, duplicates };
}

function ensureRef(array: NormalizedExternalReference[], entry: NormalizedExternalReference | undefined) {
  if (!entry) return;
  if (!array.some((existing) => existing.source === entry.source && existing.id === entry.id)) {
    array.push(entry);
  }
}

function coalesce<T>(...values: (T | undefined | null)[]): T | undefined {
  for (const value of values) {
    if (value !== undefined && value !== null) return value;
  }
  return undefined;
}

function mergeShipStats(
  ship: ShipRow,
  variantStats: Record<string, unknown> | undefined
): ShipVariantStatsV2 {
  const stats: ShipVariantStatsV2 = {};
  if (ship.class) {
    stats.classification = ship.class;
  }
  if (ship.size) {
    stats.size = ship.size;
  }
  if (ship.description) {
    stats.description = ship.description;
  }
  if (variantStats) {
    Object.assign(stats, cloneJson(variantStats));
  }
  return stats;
}

async function migrate() {
  const options = parseCliOptions();
  log.info(options.apply ? 'Running migration in APPLY mode' : 'Running migration in DRY-RUN mode');

  const companies = await fetchAll<CompanyRow>(OLD_COLLECTIONS.companies, [
    'id',
    'code',
    'name',
    'content'
  ]);
  const { snapshots: companySnapshots, idMap: companyIdMap, duplicates } = resolveCompanySnapshots(companies);

  if (duplicates.length) {
    log.info('Detected duplicate companies referencing the same code', {
      duplicates: duplicates.slice(0, 5).map((entry) => ({ duplicate: entry.duplicate.id, canonical: entry.canonical.id })),
      total: duplicates.length
    });
  }

  // Sync new companies collection
  const existingNewCompanies = await fetchAll<NewCompanyRow>(NEW_COLLECTIONS.companies, ['id', 'code']);
  const newCompaniesByCode = new Map<string, string>();
  for (const row of existingNewCompanies) {
    if (!row.code) continue;
    newCompaniesByCode.set(row.code.toUpperCase(), row.id);
  }

  const companyCreates: Array<{ code: string; name: string; external_refs: NormalizedExternalReference[]; status: string }> = [];
  for (const snapshot of companySnapshots) {
    if (newCompaniesByCode.has(snapshot.code)) continue;
    companyCreates.push({
      code: snapshot.code,
      name: snapshot.name,
      external_refs: snapshot.externalRefs,
      status: 'published'
    });
  }

  if (companyCreates.length) {
    log.info(`Preparing to insert ${companyCreates.length} companies into ${NEW_COLLECTIONS.companies}`);
    if (options.apply) {
      for (const batch of chunk(companyCreates)) {
        await createMany(NEW_COLLECTIONS.companies, batch);
      }
      const refreshed = await fetchAll<NewCompanyRow>(NEW_COLLECTIONS.companies, ['id', 'code']);
      newCompaniesByCode.clear();
      for (const row of refreshed) {
        if (!row.code) continue;
        newCompaniesByCode.set(row.code.toUpperCase(), row.id);
      }
    }
  }

  for (const snapshot of companySnapshots) {
    const id = newCompaniesByCode.get(snapshot.code);
    if (!id) {
      log.warn('Missing new company entry after create phase', { code: snapshot.code });
    }
  }

  const ships = await fetchAll<ShipRow>(OLD_COLLECTIONS.ships, [
    'id',
    'name',
    'class',
    'size',
    'description',
    'wiki_slug',
    'manufacturer',
    'manufacturer.id',
    'manufacturer.code',
    'manufacturer.name'
  ]);

  const shipVariants = await fetchAll<ShipVariantRow>(OLD_COLLECTIONS.variants, [
    'id',
    'ship',
    'ship.id',
    'ship_id',
    'variant_code',
    'name',
    'external_id',
    'thumbnail',
    'patch'
  ]);

  const shipStats = await fetchAll<ShipStatRow>(OLD_COLLECTIONS.shipStats, [
    'id',
    'ship_variant',
    'ship_variant.id',
    'stats'
  ]);
  const shipStatMap = new Map<string, Record<string, unknown>>();
  for (const row of shipStats) {
    const variantId = extractId(row.ship_variant);
    if (!variantId) continue;
    if (row.stats) {
      shipStatMap.set(variantId, row.stats);
    }
  }

  const hardpoints = await fetchAll<HardpointRow>(OLD_COLLECTIONS.hardpoints, [
    'id',
    'ship_variant',
    'ship_variant.id',
    'code',
    'category',
    'position',
    'size',
    'gimballed',
    'powered',
    'seats'
  ]);

  const items = await fetchAll<ItemRow>(OLD_COLLECTIONS.items, [
    'id',
    'external_id',
    'type',
    'subtype',
    'name',
    'manufacturer',
    'manufacturer.id',
    'size',
    'grade',
    'class',
    'description'
  ]);
  const itemStats = await fetchAll<ItemStatRow>(OLD_COLLECTIONS.itemStats, [
    'id',
    'item',
    'item.id',
    'stats',
    'price_auec',
    'availability'
  ]);
  const itemStatMap = new Map<string, ItemStatRow>();
  for (const stat of itemStats) {
    const itemId = extractId(stat.item);
    if (itemId) itemStatMap.set(itemId, stat);
  }

  const companyCodeToNewId = new Map<string, string>();
  for (const snapshot of companySnapshots) {
    const newId = newCompaniesByCode.get(snapshot.code);
    if (newId) {
      companyCodeToNewId.set(snapshot.code, newId);
    }
  }

  const hullSnapshots = new Map<string, HullSnapshot>();
  const variantSnapshots: VariantSnapshot[] = [];
  const hardpointSnapshots: HardpointSnapshot[] = [];

  for (const ship of ships) {
    const rawCode = ship.manufacturer_code ?? normalizeString((ship.manufacturer as { code?: string } | undefined)?.code);
    const companySnapshot = companyIdMap.get(ship.manufacturer_id ?? extractId(ship.manufacturer) ?? '');
    const canonicalCompanyCode = companySnapshot?.code ?? (rawCode ? normalizeCode(rawCode) : undefined);
    if (!canonicalCompanyCode) {
      log.warn('Skipping ship without manufacturer code', { ship: ship.id });
      continue;
    }

    const name = ship.name ?? ship.wiki_slug ?? ship.id;
    const variantCode = extractVariantCode(name ?? '');
    const family = cleanFamilyName(name ?? canonicalCompanyCode, variantCode, canonicalCompanyCode);
    const hullKey = buildHullKey(canonicalCompanyCode, family);

    let hull = hullSnapshots.get(hullKey);
    if (!hull) {
      hull = {
        shipId: ship.id,
        hullKey,
        name: canonicalVariantName(family.replace(/_/g, ' '), 'BASE'),
        companyCode: canonicalCompanyCode,
        paints: new Set<string>(),
        externalRefs: []
      };
      ensureRef(hull.externalRefs, ref('directus:ships.id', ship.id));
      ensureRef(hull.externalRefs, ref('directus:ships.wiki_slug', ship.wiki_slug));
      hullSnapshots.set(hullKey, hull);
    }
  }

  const variantByShip = new Map<string, ShipVariantRow[]>();
  for (const variant of shipVariants) {
    const shipId = extractId(variant.ship) ?? variant.ship_id;
    if (!shipId) continue;
    const list = variantByShip.get(shipId) ?? [];
    list.push(variant);
    variantByShip.set(shipId, list);
  }

  for (const ship of ships) {
    const rawCode = ship.manufacturer_code ?? normalizeString((ship.manufacturer as { code?: string } | undefined)?.code);
    const companySnapshot = companyIdMap.get(ship.manufacturer_id ?? extractId(ship.manufacturer) ?? '');
    const canonicalCompanyCode = companySnapshot?.code ?? (rawCode ? normalizeCode(rawCode) : undefined);
    if (!canonicalCompanyCode) continue;
    const name = ship.name ?? ship.wiki_slug ?? ship.id;
    const variantCode = extractVariantCode(name ?? '');
    const family = cleanFamilyName(name ?? canonicalCompanyCode, variantCode, canonicalCompanyCode);
    const hullKey = buildHullKey(canonicalCompanyCode, family);
    const hull = hullSnapshots.get(hullKey);
    if (!hull) continue;

    const variants = variantByShip.get(ship.id) ?? [];
    if (!variants.length) {
      const variantId = toCanonicalVariantExtId(hullKey, variantCode);
      const stats = mergeShipStats(ship, undefined);
      const refs: NormalizedExternalReference[] = [];
      ensureRef(refs, ref('directus:ships.id', ship.id));
      variantSnapshots.push({
        external_id: variantId,
        ship_external: hull.hullKey,
        name: name ?? variantId,
        variant_code: variantCode,
        external_refs: refs,
        thumbnail: undefined,
        release_patch: undefined,
        stats
      });
      continue;
    }

    for (const variant of variants) {
      const canonicalVariantId = variant.external_id
        ? variant.external_id
        : toCanonicalVariantExtId(
            hull.hullKey,
            extractVariantCode(`${variant.variant_code ?? ''} ${variant.name ?? ''}`)
          );
      const edition = detectEditionOrLivery(variant.name ?? '');
      if (edition.livery) {
        hull.paints.add(edition.livery);
      }
      const isEdition = isEditionOnly(variant.name ?? '');
      const statsRaw = shipStatMap.get(variant.id);
      const stats = mergeShipStats(ship, statsRaw);
      if (options.hardpointsAsCollection) {
        // Hardpoints handled later
      }
      const refs: NormalizedExternalReference[] = [];
      ensureRef(refs, ref('directus:ship_variants.id', variant.id));
      ensureRef(refs, ref('directus:ship_variants.external_id', variant.external_id));
      ensureRef(refs, ref('directus:ship_variants.patch', variant.patch));
      variantSnapshots.push({
        external_id: canonicalVariantId,
        ship_external: hull.hullKey,
        name: variant.name ?? canonicalVariantName(family.replace(/_/g, ' '), extractVariantCode(variant.name ?? '')),
        variant_code: variant.variant_code ?? undefined,
        external_refs: refs,
        thumbnail: variant.thumbnail ?? undefined,
        release_patch: variant.patch ?? undefined,
        stats
      });
      if (isEdition && edition.editionCode) {
        hull.paints.add(edition.editionCode);
      }
    }
  }

  const variantIdByOld = new Map<string, string>();
  for (const variant of shipVariants) {
    const shipId = extractId(variant.ship) ?? variant.ship_id;
    if (!shipId) continue;
    const rawCode = normalizeString((ships.find((row) => row.id === shipId)?.manufacturer as { code?: string })?.code);
    const companySnapshot = companyIdMap.get(
      ships.find((row) => row.id === shipId)?.manufacturer_id ?? extractId(ships.find((row) => row.id === shipId)?.manufacturer) ?? ''
    );
    const canonicalCompanyCode = companySnapshot?.code ?? (rawCode ? normalizeCode(rawCode) : undefined);
    if (!canonicalCompanyCode) continue;
    const shipName = ships.find((row) => row.id === shipId)?.name ?? ships.find((row) => row.id === shipId)?.wiki_slug ?? shipId;
    const family = cleanFamilyName(shipName ?? canonicalCompanyCode, extractVariantCode(shipName ?? ''), canonicalCompanyCode);
    const hullKey = buildHullKey(canonicalCompanyCode, family);
    const canonicalVariantId = variant.external_id
      ? variant.external_id
      : toCanonicalVariantExtId(
          hullKey,
          extractVariantCode(`${variant.variant_code ?? ''} ${variant.name ?? ''}`)
        );
    variantIdByOld.set(variant.id, canonicalVariantId);
  }

  if (options.hardpointsAsCollection) {
    for (const hp of hardpoints) {
      const variantId = variantIdByOld.get(extractId(hp.ship_variant) ?? '');
      if (!variantId) continue;
      const code = normalizeString(hp.code);
      const category = normalizeString(hp.category);
      if (!code || !category) continue;
      hardpointSnapshots.push({
        external_id: `${variantId}:${code}`,
        ship_variant_external: variantId,
        code,
        category,
        position: normalizeString(hp.position),
        size: hp.size ?? undefined,
        gimballed: hp.gimballed ?? undefined,
        powered: hp.powered ?? undefined,
        seats: hp.seats ?? undefined
      });
    }
  } else {
    const hardpointsByVariant = new Map<string, NormalizedHardpointV2[]>();
    for (const hp of hardpoints) {
      const variantId = variantIdByOld.get(extractId(hp.ship_variant) ?? '');
      if (!variantId) continue;
      const code = normalizeString(hp.code);
      const category = normalizeString(hp.category);
      if (!code || !category) continue;
      const entry: NormalizedHardpointV2 = {
        external_id: `${variantId}:${code}`,
        ship_variant_external: variantId,
        code,
        category,
        position: normalizeString(hp.position),
        size: hp.size ?? undefined,
        gimballed: hp.gimballed ?? undefined,
        powered: hp.powered ?? undefined,
        seats: hp.seats ?? undefined
      };
      const list = hardpointsByVariant.get(variantId) ?? [];
      list.push(entry);
      hardpointsByVariant.set(variantId, list);
    }
    for (const variant of variantSnapshots) {
      const hardpointList = hardpointsByVariant.get(variant.external_id);
      if (hardpointList?.length) {
        variant.stats.hardpoints = hardpointList.map((hp) => ({
          code: hp.code,
          category: hp.category,
          position: hp.position,
          size: hp.size,
          gimballed: hp.gimballed,
          powered: hp.powered,
          seats: hp.seats
        }));
      }
    }
  }

  const itemSnapshots: ItemSnapshot[] = [];
  for (const item of items) {
    const type = normalizeString(item.type);
    if (options.allowedItemTypes.size && type && !options.allowedItemTypes.has(type.toUpperCase())) {
      continue;
    }
    const externalId = item.external_id ?? item.id;
    if (!externalId) continue;
    const statsRow = itemStatMap.get(item.id);
    const stats = cloneJson(statsRow?.stats ?? {});
    if (statsRow?.price_auec !== undefined) {
      (stats as Record<string, unknown>).price_auec = statsRow.price_auec;
    }
    if (statsRow?.availability !== undefined) {
      (stats as Record<string, unknown>).availability = statsRow.availability;
    }
    const manufacturerId = extractId(item.manufacturer);
    const companySnapshot = manufacturerId ? companyIdMap.get(manufacturerId) : undefined;
    const snapshot: ItemSnapshot = {
      external_id: externalId,
      name: item.name ?? externalId,
      company_code: companySnapshot?.code,
      type: type ?? 'UNKNOWN',
      subtype: normalizeString(item.subtype),
      size: item.size ?? undefined,
      grade: normalizeString(item.grade),
      class: normalizeString(item.class),
      description: normalizeString(item.description),
      stats,
      external_refs: [
        ref('directus:items.id', item.id),
        ref('directus:items.external_id', item.external_id)
      ].filter(Boolean) as NormalizedExternalReference[]
    };
    itemSnapshots.push(snapshot);
  }

  const newShipsExisting = await fetchAll<{ id: string; name?: string | null; company?: string | { id?: string } | null; external_refs?: NormalizedExternalReference[] | null }>(
    NEW_COLLECTIONS.ships,
    ['id', 'name', 'company', 'company.id', 'external_refs']
  );
  const newShipMap = new Map<string, string>();
  for (const row of newShipsExisting) {
    const externalRefs = Array.isArray(row.external_refs) ? row.external_refs : [];
    for (const refEntry of externalRefs) {
      if (refEntry.source === 'migration:hullKey' && refEntry.id) {
        newShipMap.set(refEntry.id, row.id);
      }
    }
  }

  const newVariantsExisting = await fetchAll<{ id: string; external_id?: string | null }>(
    NEW_COLLECTIONS.shipVariants,
    ['id', 'external_id']
  );
  const newVariantMap = new Map<string, string>();
  for (const row of newVariantsExisting) {
    if (row.external_id) newVariantMap.set(row.external_id, row.id);
  }

  const newItemsExisting = await fetchAll<{ id: string; external_id?: string | null }>(
    NEW_COLLECTIONS.items,
    ['id', 'external_id']
  );
  const newItemMap = new Map<string, string>();
  for (const row of newItemsExisting) {
    if (row.external_id) newItemMap.set(row.external_id, row.id);
  }

  const hullCreates: Array<Record<string, unknown>> = [];
  for (const hull of hullSnapshots.values()) {
    const companyId = companyCodeToNewId.get(hull.companyCode);
    if (!companyId) {
      log.warn('Skipping hull without mapped company', { hull: hull.hullKey, company: hull.companyCode });
      continue;
    }
    const existingId = newShipMap.get(hull.hullKey);
    const payload = {
      company: companyId,
      name: hull.name,
      external_refs: [
        ...hull.externalRefs,
        { source: 'migration:hullKey', id: hull.hullKey }
      ],
      paints: hull.paints.size ? Array.from(hull.paints) : null,
      status: 'published'
    } satisfies Record<string, unknown>;
    if (existingId) {
      if (options.apply) {
        await updateOne(NEW_COLLECTIONS.ships, existingId, payload);
      }
      continue;
    }
    hullCreates.push(payload);
  }

  if (hullCreates.length) {
    log.info(`Preparing to create ${hullCreates.length} hull records in ${NEW_COLLECTIONS.ships}`);
    if (options.apply) {
      for (const batch of chunk(hullCreates)) {
        await createMany(NEW_COLLECTIONS.ships, batch);
      }
    }
  }

  const refreshedShips = options.apply
    ? await fetchAll<{ id: string; external_refs?: NormalizedExternalReference[] | null }>(
        NEW_COLLECTIONS.ships,
        ['id', 'external_refs']
      )
    : newShipsExisting;
  if (options.apply) {
    newShipMap.clear();
    for (const row of refreshedShips) {
      const refs = Array.isArray(row.external_refs) ? row.external_refs : [];
      for (const refEntry of refs) {
        if (refEntry.source === 'migration:hullKey' && refEntry.id) {
          newShipMap.set(refEntry.id, row.id);
        }
      }
    }
  }

  const variantCreates: Array<Record<string, unknown>> = [];
  for (const variant of variantSnapshots) {
    const shipId = newShipMap.get(variant.ship_external);
    if (!shipId) {
      log.warn('Skipping variant without hull mapping', { variant: variant.external_id, ship: variant.ship_external });
      continue;
    }
    const payload = {
      ship: shipId,
      name: variant.name,
      variant_code: variant.variant_code ?? null,
      external_id: variant.external_id,
      external_refs: variant.external_refs,
      stats: variant.stats,
      thumbnail: variant.thumbnail ?? null,
      release_patch: variant.release_patch ?? null,
      status: 'published'
    } satisfies Record<string, unknown>;
    const existingId = newVariantMap.get(variant.external_id);
    if (existingId) {
      if (options.apply) {
        await updateOne(NEW_COLLECTIONS.shipVariants, existingId, payload);
      }
      continue;
    }
    variantCreates.push(payload);
  }

  if (variantCreates.length) {
    log.info(`Preparing to create ${variantCreates.length} ship variants in ${NEW_COLLECTIONS.shipVariants}`);
    if (options.apply) {
      for (const batch of chunk(variantCreates)) {
        await createMany(NEW_COLLECTIONS.shipVariants, batch);
      }
    }
  }

  if (options.apply) {
    const refreshedVariants = await fetchAll<{ id: string; external_id?: string | null }>(
      NEW_COLLECTIONS.shipVariants,
      ['id', 'external_id']
    );
    newVariantMap.clear();
    for (const row of refreshedVariants) {
      if (row.external_id) newVariantMap.set(row.external_id, row.id);
    }
  }

  const itemCreates: Array<Record<string, unknown>> = [];
  for (const item of itemSnapshots) {
    const payload = {
      external_id: item.external_id,
      name: item.name,
      type: item.type,
      subtype: item.subtype ?? null,
      size: item.size ?? null,
      grade: item.grade ?? null,
      class: item.class ?? null,
      description: item.description ?? null,
      company: item.company_code ? companyCodeToNewId.get(item.company_code) ?? null : null,
      external_refs: item.external_refs,
      stats: item.stats,
      status: 'published'
    } satisfies Record<string, unknown>;
    const existingId = newItemMap.get(item.external_id);
    if (existingId) {
      if (options.apply) {
        await updateOne(NEW_COLLECTIONS.items, existingId, payload);
      }
      continue;
    }
    itemCreates.push(payload);
  }

  if (itemCreates.length) {
    log.info(`Preparing to create ${itemCreates.length} items in ${NEW_COLLECTIONS.items}`);
    if (options.apply) {
      for (const batch of chunk(itemCreates)) {
        await createMany(NEW_COLLECTIONS.items, batch);
      }
    }
  }

  if (options.hardpointsAsCollection) {
    const existingHardpoints = await fetchAll<{ id: string; ship_variant?: string | { id?: string } | null; code?: string | null }>(
      NEW_COLLECTIONS.hardpoints,
      ['id', 'ship_variant', 'ship_variant.id', 'code']
    );
    const hardpointKey = (shipVariantId: string, code: string) => `${shipVariantId}:${code.toLowerCase()}`;
    const existingHardpointMap = new Map<string, string>();
    for (const hp of existingHardpoints) {
      const variantId = extractId(hp.ship_variant);
      const code = hp.code?.toLowerCase();
      if (variantId && code) {
        existingHardpointMap.set(hardpointKey(variantId, code), hp.id);
      }
    }
    const hardpointCreates: Array<Record<string, unknown>> = [];
    for (const hp of hardpointSnapshots) {
      const variantId = newVariantMap.get(hp.ship_variant_external);
      if (!variantId) continue;
      const key = hardpointKey(variantId, hp.code);
      const payload = {
        ship_variant: variantId,
        code: hp.code,
        category: hp.category,
        position: hp.position ?? null,
        size: hp.size ?? null,
        gimballed: hp.gimballed ?? null,
        powered: hp.powered ?? null,
        seats: hp.seats ?? null,
        status: 'published'
      } satisfies Record<string, unknown>;
      const existingId = existingHardpointMap.get(key);
      if (existingId) {
        if (options.apply) {
          await updateOne(NEW_COLLECTIONS.hardpoints, existingId, payload);
        }
        continue;
      }
      hardpointCreates.push(payload);
    }
    if (hardpointCreates.length) {
      log.info(`Preparing to create ${hardpointCreates.length} hardpoints in ${NEW_COLLECTIONS.hardpoints}`);
      if (options.apply) {
        for (const batch of chunk(hardpointCreates)) {
          await createMany(NEW_COLLECTIONS.hardpoints, batch);
        }
      }
    }
  }

  if (options.apply) {
    for (const collection of REVISION_TARGETS) {
      await ensureVersioning(collection, true);
    }
  } else {
    log.info('Dry-run complete. No data persisted.');
  }
}

migrate()
  .then(() => {
    log.info('Migration script finished.');
    exit(0);
  })
  .catch((error) => {
    log.error('Migration script failed', error);
    exit(1);
  });
