import { performance } from 'node:perf_hooks';
import { join } from 'node:path';
import { readJsonOrDefault } from './utils/fs.js';
import {
  createMany,
  createOne,
  createOneWithVersion,
  deleteMany,
  readByQuery,
  updateMany,
  updateOne,
  updateOneWithVersion
} from './utils/directus.js';
import { log } from './utils/log.js';
import { CompanyResolver } from './utils/companyResolver.js';
import { computeDiff } from './diffs.js';
import { loadShipGrouping } from './config/ship-groups.js';
import type {
  Channel,
  NormalizedBundleV2,
  NormalizedCompanyV2,
  NormalizedDataBundle,
  NormalizedExternalReference,
  NormalizedHardpointV2,
  NormalizedInstalledItem,
  NormalizedItemV2,
  NormalizedShipVariantV2,
  NormalizedShipV2
} from './types/index.js';

export interface BuildMetadata {
  build_hash?: string | null;
  released_at?: string | null;
  status?: 'pending' | 'ingested' | 'failed';
}

export interface BuildRecord {
  id: string;
  status: string;
  released_at?: string | null;
  build_hash?: string | null;
}

export interface LoadStatistics {
  companies: number;
  ships: number;
  ship_variants: number;
  items: number;
  hardpoints: number;
}

export interface LoadResult {
  build: BuildRecord;
  stats: LoadStatistics;
}

export interface LoadOptions {
  metadata?: BuildMetadata;
  build?: BuildRecord;
}

const COLLECTIONS = {
  companies: process.env.SC_COMPANY_COLLECTION ?? 'companies',
  ships: process.env.SC_SHIP_COLLECTION ?? 'ships',
  shipVariants: process.env.SC_SHIP_VARIANT_COLLECTION ?? 'ship_variants',
  items: process.env.SC_ITEM_COLLECTION ?? 'items',
  hardpoints: process.env.SC_HARDPOINT_COLLECTION ?? 'hardpoints',
  shipConfigurations: process.env.SC_SHIP_CONFIGURATION_COLLECTION ?? 'ship_configurations',
  shipConfigurationHardpoints:
    process.env.SC_SHIP_CONFIGURATION_HP_COLLECTION ?? 'ship_configuration_hardpoints'
} as const;

const PAGE_LIMIT = 200;
const HARDPOINT_KEY_SEPARATOR = '::';
const DEFAULT_VARIANT_CODE = 'BASE';
const ITEM_TYPE_ALLOWLIST = new Set<string>(['QUANTUMDRIVE', 'SHIELD', 'SHIELDCONTROLLER']);
const HARDPOINT_CATEGORY_ALLOWLIST: string[] = ['Shield', 'ShieldController', 'QuantumDrive'];
const HARDPOINT_CATEGORY_ALLOWLIST_SET = new Set<string>(
  HARDPOINT_CATEGORY_ALLOWLIST.map((category) => category.toUpperCase())
);

export function mapByExternalId<T extends { external_id: string }> (items: T[]): Map<string, T> {
  const map = new Map<string, T>();
  for (const item of items) {
    map.set(item.external_id, item);
  }
  return map;
}

async function fetchAllRows<T> (
  collection: string,
  fields: string[],
  filter?: Record<string, unknown>
): Promise<T[]> {
  const rows: T[] = [];
  let offset = 0;
  while (true) {
    const query: Record<string, unknown> = {
      fields,
      limit: PAGE_LIMIT,
      offset
    };

    if (filter !== undefined) {
      query.filter = filter;
    }

    const batch = await readByQuery<T>(collection, query);
    if (!batch.length) break;
    rows.push(...batch);
    if (batch.length < PAGE_LIMIT) break;
    offset += PAGE_LIMIT;
  }
  return rows;
}

function nullable<T> (value: T | undefined | null): T | null {
  return value === undefined ? null : (value as T | null);
}

function extractId (value: unknown): string | undefined {
  if (!value) return undefined;
  if (typeof value === 'string') {
    const trimmed = value.trim();
    return trimmed ? trimmed : undefined;
  }
  if (typeof value === 'object' && value !== null && 'id' in value) {
    const id = (value as { id?: unknown }).id;
    if (typeof id === 'string') {
      const trimmed = id.trim();
      if (trimmed) return trimmed;
    }
  }
  return undefined;
}

function normalizeString (value: unknown): string | undefined {
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (trimmed) return trimmed;
    return undefined;
  }
  if (value === undefined || value === null) return undefined;
  return String(value).trim() || undefined;
}

function normalizeNumber (value: unknown): number | undefined {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) return undefined;
    const parsed = Number(trimmed);
    if (Number.isFinite(parsed)) return parsed;
  }
  return undefined;
}

function normalizeBooleanFlag (value: unknown): boolean | undefined {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
    if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
  }
  return undefined;
}

function sortRefs (refs: NormalizedExternalReference[]): NormalizedExternalReference[] {
  return [...refs].sort((a, b) => {
    const source = a.source.localeCompare(b.source);
    if (source !== 0) return source;
    return a.id.localeCompare(b.id);
  });
}

function cloneRefs (refs: readonly NormalizedExternalReference[]): NormalizedExternalReference[] {
  return refs.map((ref) => ({
    source: ref.source,
    id: ref.id,
    ...(ref.note ? { note: ref.note } : {})
  }));
}

function normalizeExternalRefsInput (value: unknown): NormalizedExternalReference[] {
  if (!Array.isArray(value)) return [];
  const refs: NormalizedExternalReference[] = [];
  for (const entry of value) {
    if (!entry || typeof entry !== 'object') continue;
    const source = normalizeString((entry as Record<string, unknown>).source);
    const id = normalizeString((entry as Record<string, unknown>).id);
    if (!source || !id) continue;
    const note = normalizeString((entry as Record<string, unknown>).note);
    refs.push(note ? { source, id, note } : { source, id });
  }
  return sortRefs(refs);
}

function buildRefKeys (refs: NormalizedExternalReference[]): string[] {
  return refs.map((ref) => `${ref.source}:${ref.id}`.toUpperCase());
}

function normalizePaintsInput (value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  const set = new Set<string>();
  for (const entry of value) {
    const normalized = normalizeString(entry);
    if (normalized) {
      set.add(normalized);
    }
  }
  return [...set].sort((a, b) => a.localeCompare(b));
}

function snapshotToRecord<T> (value: T): Record<string, unknown> {
  return value as unknown as Record<string, unknown>;
}

function shipCompositeKey (
  manufacturerId: string | null | undefined,
  name: string | undefined
): string | undefined {
  if (!manufacturerId || !name) return undefined;
  const trimmed = name.trim();
  if (!trimmed) return undefined;
  return `${manufacturerId}:${trimmed.toLowerCase()}`;
}

function variantCompositeKey (shipId: string | undefined, variantCode: string | undefined): string | undefined {
  if (!shipId) return undefined;
  const code = (variantCode ?? DEFAULT_VARIANT_CODE).trim().toUpperCase();
  return `${shipId}:${code}`;
}

function itemCompositeKey (type: string | undefined, name: string | undefined): string | undefined {
  if (!type || !name) return undefined;
  return `${type.trim().toUpperCase()}:${name.trim().toLowerCase()}`;
}

function cloneJson<T> (value: T): T {
  if (value === undefined || value === null) return value as T;
  return JSON.parse(JSON.stringify(value)) as T;
}

function sanitizeItemStats (value: Record<string, unknown>): Record<string, unknown> {
  const stats = { ...value };
  if ('paints' in stats) {
    delete stats.paints;
  }
  return stats;
}

function toOptionalString (value: unknown): string | undefined {
  if (typeof value === 'string') {
    const trimmed = value.trim();
    return trimmed ? trimmed : undefined;
  }
  return undefined;
}

function normalizeExternalId (value: string | undefined): string | undefined {
  if (!value) return undefined;
  return value.trim().toUpperCase();
}

function humanizeCode (code: string): string {
  return code
    .toLowerCase()
    .split('_')
    .map((part) => (part ? part[0].toUpperCase() + part.slice(1) : part))
    .join(' ');
}

function chunkArray<T>(input: readonly T[], size: number): T[][] {
  if (size <= 0) throw new Error('chunk size must be > 0');
  const result: T[][] = [];
  for (let i = 0; i < input.length; i += size) {
    result.push(input.slice(i, i + size));
  }
  return result;
}

async function buildItemIdMap (): Promise<Map<string, string>> {
  type ItemIdRow = { id: string; external_id?: string | null };
  const rows = await fetchAllRows<ItemIdRow>(COLLECTIONS.items, ['id', 'external_id']);
  const map = new Map<string, string>();
  for (const row of rows) {
    const ext = normalizeString(row.external_id);
    if (ext) map.set(ext, row.id);
  }
  return map;
}

async function buildHardpointIdMap (): Promise<Map<string, string>> {
  type HardpointIdRow = { id: string; external_id?: string | null };
  const rows = await fetchAllRows<HardpointIdRow>(COLLECTIONS.hardpoints, ['id', 'external_id']);
  const map = new Map<string, string>();
  for (const row of rows) {
    const ext = normalizeExternalId(row.external_id as string | undefined);
    if (ext) map.set(ext, row.id);
  }
  return map;
}


async function loadNormalizedBundleV2 (
  dir: string,
  channel: Channel,
  version: string
): Promise<NormalizedBundleV2> {
  const companies = await readJsonOrDefault<NormalizedCompanyV2[]>(join(dir, 'companies.v2.json'), []);
  const ships = await readJsonOrDefault<NormalizedShipV2[]>(join(dir, 'ships.v2.json'), []);
  const variants = await readJsonOrDefault<NormalizedShipVariantV2[]>(
    join(dir, 'ship_variants.v2.json'),
    []
  );
  const items = await readJsonOrDefault<NormalizedItemV2[]>(join(dir, 'items.v2.json'), []);
  const hardpoints = await readJsonOrDefault<NormalizedHardpointV2[]>(join(dir, 'hardpoints.v2.json'), []);

  return {
    channel,
    version,
    companies,
    ships,
    ship_variants: variants,
    items,
    hardpoints
  } satisfies NormalizedBundleV2;
}

function sanitizeVariantStats (
  variant: NormalizedShipVariantV2
): { stats: Record<string, unknown>; hardpoints: NormalizedHardpointV2[] } {
  const stats = cloneJson<Record<string, unknown>>(variant.stats ?? {});
  const hardpoints: NormalizedHardpointV2[] = [];
  const raw = (stats as Record<string, unknown>).hardpoints;
  if (Array.isArray(raw)) {
    for (const entry of raw) {
      if (!entry || typeof entry !== 'object') continue;
      const code = normalizeString((entry as Record<string, unknown>).code);
      const category = normalizeString((entry as Record<string, unknown>).category);
      if (!code || !category) continue;
      const position = normalizeString((entry as Record<string, unknown>).position);
      const size = normalizeNumber((entry as Record<string, unknown>).size);
      const gimballed = normalizeBooleanFlag((entry as Record<string, unknown>).gimballed);
      const powered = normalizeBooleanFlag((entry as Record<string, unknown>).powered);
      const seats = normalizeNumber((entry as Record<string, unknown>).seats);
      const externalId = normalizeString((entry as Record<string, unknown>).external_id);
      hardpoints.push({
        external_id: externalId ?? `${variant.external_id}:${code}`,
        ship_variant_external: variant.external_id,
        code,
        category,
        position,
        size,
        gimballed,
        powered,
        seats
      });
    }
    delete (stats as Record<string, unknown>).hardpoints;
  }
  return { stats, hardpoints };
}

function splitVariantStats (
  bundle: NormalizedBundleV2
): { statsByVariant: Map<string, Record<string, unknown>>; hardpoints: NormalizedHardpointV2[] } {
  const statsByVariant = new Map<string, Record<string, unknown>>();
  const extracted: NormalizedHardpointV2[] = [];
  for (const variant of bundle.ship_variants) {
    const { stats, hardpoints } = sanitizeVariantStats(variant);
    statsByVariant.set(variant.external_id, stats);
    extracted.push(...hardpoints);
  }
  const explicit = bundle.hardpoints?.length ? bundle.hardpoints : [];
  return {
    statsByVariant,
    hardpoints: explicit.length ? explicit : extracted
  };
}

interface CompanySnapshot {
  code: string;
  name: string | null;
  category: string | null;
  external_refs: NormalizedExternalReference[];
  status: string;
}

interface CompanyState {
  id: string;
  snapshot: CompanySnapshot;
}

interface ExistingCompanyRow {
  id: string;
  code?: string | null;
  name?: string | null;
  category?: unknown;
  external_refs?: unknown;
  status?: string | null;
}

function normalizeCompanyCode (value: unknown): string | undefined {
  const normalized = normalizeString(value);
  return normalized ? normalized.toUpperCase() : undefined;
}

function makeCompanySnapshotFromRow (row: ExistingCompanyRow): CompanySnapshot | undefined {
  const code = normalizeCompanyCode(row.code);
  if (!code) return undefined;
  return {
    code,
    name: normalizeString(row.name) ?? null,
    category: extractId(row.category) ?? null,
    external_refs: normalizeExternalRefsInput(row.external_refs),
    status: normalizeString(row.status) ?? 'draft'
  };
}

function makeCompanySnapshotFromNormalized (
  company: NormalizedCompanyV2,
  defaultCategory: string | null
): CompanySnapshot | undefined {
  const code = normalizeCompanyCode(company.code);
  if (!code) {
    return undefined;
  }
  return {
    code,
    name: normalizeString(company.name) ?? null,
    category: defaultCategory,
    external_refs: sortRefs(cloneRefs(company.external_refs ?? [])),
    status: 'published'
  };
}

function pickDefaultCompanyCategory (existing: Iterable<CompanySnapshot>): string | null {
  const counts = new Map<string, number>();
  for (const snapshot of existing) {
    if (!snapshot.category) continue;
    counts.set(snapshot.category, (counts.get(snapshot.category) ?? 0) + 1);
  }
  if (!counts.size) return null;
  let selected: string | null = null;
  let highest = -1;
  for (const [id, count] of counts.entries()) {
    if (count > highest) {
      selected = id;
      highest = count;
    }
  }
  return selected;
}

function loggableDirectusError (error: unknown): Record<string, unknown> {
  if (!(error instanceof Error)) {
    return { error };
  }
  const output: Record<string, unknown> = {
    name: error.name,
    message: error.message
  };
  const directus = (error as any)?.directus;
  if (directus && typeof directus === 'object') {
    output.directus = directus;
  }
  const errors = (error as any)?.errors;
  if (Array.isArray(errors)) {
    output.errors = errors;
  }
  const response = (error as any)?.response;
  if (response && typeof response === 'object') {
    output.response = {
      status: (response as any).status,
      statusText: (response as any).statusText,
      url: (response as any).url
    };
  }
  return output;
}

async function syncCompanies (companies: NormalizedCompanyV2[]): Promise<Map<string, string>> {
  const existingRows = await fetchAllRows<ExistingCompanyRow>(COLLECTIONS.companies, [
    'id',
    'code',
    'name',
    'category',
    'category.id',
    'external_refs',
    'status'
  ]);

  const byCode = new Map<string, CompanyState>();
  for (const row of existingRows) {
    const snapshot = makeCompanySnapshotFromRow(row);
    if (!snapshot) continue;
    byCode.set(snapshot.code, { id: row.id, snapshot });
  }

  const defaultCategory = pickDefaultCompanyCategory(
    Array.from(byCode.values(), (state) => state.snapshot)
  );

  if (!defaultCategory) {
    log.warn('No default company category detected; skipping creation of missing companies.');
  }

  const idMap = new Map<string, string>();

  for (const entry of companies) {
    const snapshot = makeCompanySnapshotFromNormalized(entry, defaultCategory);
    if (!snapshot) {
      log.warn('Skipping normalized company entry with invalid code');
      continue;
    }

    const existing = byCode.get(snapshot.code);
    if (existing) {
      const diff = computeDiff(
        snapshotToRecord(existing.snapshot),
        snapshotToRecord(snapshot),
        ['name', 'external_refs', 'status']
      );
      if (diff) {
        try {
          await updateOne(COLLECTIONS.companies, existing.id, {
            name: snapshot.name,
            external_refs: snapshot.external_refs,
            status: snapshot.status
          });
        } catch (error) {
          log.error('Failed to update Directus company', {
            code: snapshot.code,
            ...loggableDirectusError(error)
          });
          throw error;
        }
        byCode.set(snapshot.code, { id: existing.id, snapshot });
      }
      idMap.set(snapshot.code, existing.id);
    } else {
      if (!snapshot.category) {
        log.warn('Missing default company category; unable to create company', {
          code: snapshot.code
        });
        continue;
      }
      let created;
      try {
        created = await createOne<{ id: string }>(COLLECTIONS.companies, {
          code: snapshot.code,
          name: snapshot.name,
          category: snapshot.category,
          external_refs: snapshot.external_refs,
          status: snapshot.status
        });
      } catch (error) {
        log.error('Failed to create Directus company', {
          code: snapshot.code,
          ...loggableDirectusError(error)
        });
        throw error;
      }
      idMap.set(snapshot.code, created.id);
      byCode.set(snapshot.code, { id: created.id, snapshot });
      log.info('Created Directus company entry from normalized data', {
        code: snapshot.code,
        id: created.id
      });
    }
  }

  return idMap;
}

interface ShipSnapshot {
  name: string;
  manufacturer: string | null;
  external_refs: NormalizedExternalReference[];
  paints: string[];
}

interface ShipState {
  id: string;
  snapshot: ShipSnapshot;
  compositeKey?: string;
  refKeys: string[];
}

interface ExistingShipRow {
  id: string;
  name?: string | null;
  manufacturer?: string | { id?: string } | null;
  external_refs?: unknown;
  paints?: unknown;
}

function makeShipState (id: string, snapshot: ShipSnapshot): ShipState {
  const refs = buildRefKeys(snapshot.external_refs);
  const compositeKey = shipCompositeKey(snapshot.manufacturer, snapshot.name);
  return { id, snapshot, refKeys: refs, compositeKey };
}

function attachShipState (
  state: ShipState,
  byComposite: Map<string, ShipState>,
  byRef: Map<string, ShipState>
) {
  if (state.compositeKey) {
    byComposite.set(state.compositeKey, state);
  }
  for (const key of state.refKeys) {
    byRef.set(key, state);
  }
}

function detachShipState (
  state: ShipState,
  byComposite: Map<string, ShipState>,
  byRef: Map<string, ShipState>
) {
  if (state.compositeKey) {
    const current = byComposite.get(state.compositeKey);
    if (current?.id === state.id) {
      byComposite.delete(state.compositeKey);
    }
  }
  for (const key of state.refKeys) {
    const current = byRef.get(key);
    if (current?.id === state.id) {
      byRef.delete(key);
    }
  }
}

async function syncShips (
  ships: NormalizedShipV2[],
  resolveCompanyId: (code: string) => Promise<string | undefined>,
  versionName: string,
  promoteVersions: boolean
): Promise<Map<string, string>> {
  const existingRows = await fetchAllRows<ExistingShipRow>(COLLECTIONS.ships, [
    'id',
    'name',
    'manufacturer',
    'manufacturer.id',
    'external_refs',
    'paints'
  ]);

  const byId = new Map<string, ShipState>();
  const byComposite = new Map<string, ShipState>();
  const byRef = new Map<string, ShipState>();
  const byExt = new Map<string, HardpointState>();

  for (const row of existingRows) {
    const name = normalizeString(row.name) ?? '';
    const manufacturerId = extractId(row.manufacturer) ?? null;
    const snapshot: ShipSnapshot = {
      name,
      manufacturer: manufacturerId,
      external_refs: normalizeExternalRefsInput(row.external_refs),
      paints: normalizePaintsInput(row.paints)
    };
    const state = makeShipState(row.id, snapshot);
    byId.set(state.id, state);
    attachShipState(state, byComposite, byRef);
  }

  const shipIdByExternal = new Map<string, string>();
  for (const ship of ships) {
    const manufacturerId = ship.company_code ? await resolveCompanyId(ship.company_code) : undefined;
    const snapshot: ShipSnapshot = {
      name: ship.name,
      manufacturer: manufacturerId ?? null,
      external_refs: sortRefs(cloneRefs(ship.external_refs ?? [])),
      paints: normalizePaintsInput(ship.paints ?? [])
    };
    const composite = shipCompositeKey(snapshot.manufacturer, snapshot.name);
    let state: ShipState | undefined = composite ? byComposite.get(composite) : undefined;
    if (!state && snapshot.external_refs.length) {
      for (const key of buildRefKeys(snapshot.external_refs)) {
        state = byRef.get(key);
        if (state) break;
      }
    }

    if (state) {
      snapshot.paints = [...state.snapshot.paints];
    }

    const payload: Record<string, unknown> = {
      manufacturer: snapshot.manufacturer,
      name: snapshot.name,
      external_refs: snapshot.external_refs,
      paints: snapshot.paints,
      status: 'published'
    };

    if (state) {
      const diff = computeDiff(snapshotToRecord(state.snapshot), snapshotToRecord(snapshot), [
        'name',
        'manufacturer',
        'external_refs',
        'paints'
      ]);
      if (diff) {
        detachShipState(state, byComposite, byRef);
        await updateOneWithVersion(COLLECTIONS.ships, state.id, payload, versionName, promoteVersions);
        const nextState = makeShipState(state.id, snapshot);
        byId.set(state.id, nextState);
        attachShipState(nextState, byComposite, byRef);
        state = nextState;
      }
    } else {
      const created = await createOneWithVersion<{ id: string }>(
        COLLECTIONS.ships,
        payload,
        versionName,
        promoteVersions
      );
      const newState = makeShipState(created.id, snapshot);
      byId.set(newState.id, newState);
      attachShipState(newState, byComposite, byRef);
      state = newState;
    }

    if (state) {
      shipIdByExternal.set(ship.external_id, state.id);
    }
  }

  return shipIdByExternal;
}

interface ShipVariantSnapshot {
  ship: string;
  name: string;
  variant_code?: string;
  external_refs: NormalizedExternalReference[];
  stats: Record<string, unknown>;
  thumbnail?: string | null;
  release_patch?: string | null;
}

interface ShipVariantState {
  id: string;
  snapshot: ShipVariantSnapshot;
  compositeKey?: string;
  refKeys: string[];
}

interface ExistingShipVariantRow {
  id: string;
  ship?: string | { id?: string } | null;
  name?: string | null;
  variant_code?: string | null;
  external_refs?: unknown;
  stats?: unknown;
  thumbnail?: string | null;
  release_patch?: string | null;
}

function makeVariantState (id: string, snapshot: ShipVariantSnapshot): ShipVariantState {
  const compositeKey = variantCompositeKey(snapshot.ship, snapshot.variant_code);
  const refKeys = buildRefKeys(snapshot.external_refs);
  return { id, snapshot, compositeKey, refKeys };
}

function attachVariantState (
  state: ShipVariantState,
  byComposite: Map<string, ShipVariantState>,
  byRef: Map<string, ShipVariantState>
) {
  if (state.compositeKey) byComposite.set(state.compositeKey, state);
  for (const key of state.refKeys) {
    byRef.set(key, state);
  }
}

function detachVariantState (
  state: ShipVariantState,
  byComposite: Map<string, ShipVariantState>,
  byRef: Map<string, ShipVariantState>
) {
  if (state.compositeKey) {
    const current = byComposite.get(state.compositeKey);
    if (current?.id === state.id) {
      byComposite.delete(state.compositeKey);
    }
  }
  for (const key of state.refKeys) {
    const current = byRef.get(key);
    if (current?.id === state.id) {
      byRef.delete(key);
    }
  }
}

async function syncShipVariants (
  variants: NormalizedShipVariantV2[],
  statsByVariant: Map<string, Record<string, unknown>>,
  shipMap: Map<string, string>,
  versionName: string,
  promoteVersions: boolean
): Promise<Map<string, string>> {
  const existingRows = await fetchAllRows<ExistingShipVariantRow>(COLLECTIONS.shipVariants, [
    'id',
    'ship',
    'ship.id',
    'name',
    'variant_code',
    'external_refs',
    'stats',
    'thumbnail',
    'release_patch'
  ]);

  const byId = new Map<string, ShipVariantState>();
  const byComposite = new Map<string, ShipVariantState>();
  const byRef = new Map<string, ShipVariantState>();

  for (const row of existingRows) {
    const shipId = extractId(row.ship) ?? '';
    const variantCode = normalizeString(row.variant_code) ?? undefined;
    const snapshot: ShipVariantSnapshot = {
      ship: shipId,
      name: normalizeString(row.name) ?? '',
      variant_code: variantCode,
      external_refs: normalizeExternalRefsInput(row.external_refs),
      stats: cloneJson<Record<string, unknown>>((row.stats as Record<string, unknown>) ?? {}),
      thumbnail: normalizeString(row.thumbnail) ?? null,
      release_patch: normalizeString(row.release_patch) ?? null
    };
    const state = makeVariantState(row.id, snapshot);
    byId.set(state.id, state);
    attachVariantState(state, byComposite, byRef);
  }

  const variantIdByExternal = new Map<string, string>();

  for (const variant of variants) {
    const shipId = shipMap.get(variant.ship_external);
    if (!shipId) {
      throw new Error(`Missing ship mapping for variant ${variant.external_id}`);
    }
    const stats = cloneJson(statsByVariant.get(variant.external_id) ?? variant.stats ?? {});
    const snapshot: ShipVariantSnapshot = {
      ship: shipId,
      name: variant.name,
      variant_code: variant.variant_code ?? undefined,
      external_refs: sortRefs(cloneRefs(variant.external_refs ?? [])),
      stats,
      thumbnail: variant.thumbnail ?? null,
      release_patch: variant.release_patch ?? null
    };
    const composite = variantCompositeKey(snapshot.ship, snapshot.variant_code);
    let state: ShipVariantState | undefined = composite ? byComposite.get(composite) : undefined;
    if (!state && snapshot.external_refs.length) {
      for (const key of buildRefKeys(snapshot.external_refs)) {
        state = byRef.get(key);
        if (state) break;
      }
    }

    const payload: Record<string, unknown> = {
      ship: snapshot.ship,
      name: snapshot.name,
      variant_code: snapshot.variant_code ?? null,
      external_refs: snapshot.external_refs,
      stats: snapshot.stats,
      thumbnail: snapshot.thumbnail,
      release_patch: snapshot.release_patch,
      status: 'published'
    };

    if (state) {
      const diff = computeDiff(snapshotToRecord(state.snapshot), snapshotToRecord(snapshot), [
        'name',
        'variant_code',
        'external_refs',
        'stats',
        'thumbnail',
        'release_patch'
      ]);
      if (diff) {
        detachVariantState(state, byComposite, byRef);
        await updateOneWithVersion(
          COLLECTIONS.shipVariants,
          state.id,
          payload,
          versionName,
          promoteVersions
        );
        const nextState = makeVariantState(state.id, snapshot);
        byId.set(state.id, nextState);
        attachVariantState(nextState, byComposite, byRef);
        state = nextState;
      }
    } else {
      const created = await createOneWithVersion<{ id: string }>(
        COLLECTIONS.shipVariants,
        payload,
        versionName,
        promoteVersions
      );
      const newState = makeVariantState(created.id, snapshot);
      byId.set(newState.id, newState);
      attachVariantState(newState, byComposite, byRef);
      state = newState;
    }

    if (state) {
      variantIdByExternal.set(variant.external_id, state.id);
    }
  }

  return variantIdByExternal;
}

interface ItemSnapshot {
  name: string;
  type: string;
  subtype?: string | null;
  size?: number | null;
  grade?: string | null;
  class?: string | null;
  manufacturer?: string | null;
  external_refs: NormalizedExternalReference[];
  stats: Record<string, unknown>;
  external_id: string;
}

interface ItemState {
  id: string;
  snapshot: ItemSnapshot;
  compositeKey?: string;
  refKeys: string[];
}

interface ExistingItemRow {
  id: string;
  name?: string | null;
  type?: string | null;
  subtype?: string | null;
  size?: number | string | null;
  grade?: string | null;
  class?: string | null;
  manufacturer?: string | { id?: string } | null;
  external_refs?: unknown;
  stats?: unknown;
  external_id?: string | null;
}

function makeItemState (id: string, snapshot: ItemSnapshot): ItemState {
  const compositeKey = itemCompositeKey(snapshot.type, snapshot.name);
  const refKeys = buildRefKeys(snapshot.external_refs);
  return { id, snapshot, compositeKey, refKeys };
}

function attachItemState (
  state: ItemState,
  byComposite: Map<string, ItemState>,
  byRef: Map<string, ItemState>
) {
  if (state.compositeKey) byComposite.set(state.compositeKey, state);
  for (const key of state.refKeys) {
    byRef.set(key, state);
  }
}

function detachItemState (
  state: ItemState,
  byComposite: Map<string, ItemState>,
  byRef: Map<string, ItemState>
) {
  if (state.compositeKey) {
    const current = byComposite.get(state.compositeKey);
    if (current?.id === state.id) byComposite.delete(state.compositeKey);
  }
  for (const key of state.refKeys) {
    const current = byRef.get(key);
    if (current?.id === state.id) byRef.delete(key);
  }
}

async function syncItems (
  items: NormalizedItemV2[],
  resolveCompanyId: (code: string) => Promise<string | undefined>,
  versionName: string,
  promoteVersions: boolean
): Promise<void> {
  const started = performance.now();
  const allowlistedTypes = Array.from(ITEM_TYPE_ALLOWLIST);
  const itemsToSync = items.filter((item) => {
    const normalizedType = normalizeString(item.type)?.toUpperCase();
    return normalizedType ? ITEM_TYPE_ALLOWLIST.has(normalizedType) : false;
  });
  const skippedItemCount = items.length - itemsToSync.length;

  log.info('Syncing items', {
    total: itemsToSync.length,
    skipped_non_allowlisted: skippedItemCount
  });

  const existingFilter: Record<string, unknown> | undefined =
    allowlistedTypes.length === 1
      ? { type: { _eq: allowlistedTypes[0] } }
      : { _or: allowlistedTypes.map((type) => ({ type: { _eq: type } })) };

  const existingRows = await fetchAllRows<ExistingItemRow>(COLLECTIONS.items, [
    'id',
    'name',
    'type',
    'subtype',
    'size',
    'grade',
    'class',
    'manufacturer',
    'manufacturer.id',
    'external_refs',
    'stats',
    'external_id'
  ], existingFilter);

  const byId = new Map<string, ItemState>();
  const byComposite = new Map<string, ItemState>();
  const byRef = new Map<string, ItemState>();
  let createdCount = 0;
  let updated = 0;
  let unchanged = 0;

  for (const row of existingRows) {
    const type = normalizeString(row.type)?.toUpperCase() ?? '';
    const snapshot: ItemSnapshot = {
      name: normalizeString(row.name) ?? '',
      type,
      subtype: normalizeString(row.subtype) ?? null,
      size: normalizeNumber(row.size) ?? null,
      grade: normalizeString(row.grade) ?? null,
      class: normalizeString(row.class) ?? null,
      manufacturer: extractId(row.manufacturer) ?? null,
      external_refs: normalizeExternalRefsInput(row.external_refs),
      stats: sanitizeItemStats(
        cloneJson<Record<string, unknown>>((row.stats as Record<string, unknown>) ?? {})
      ),
      external_id: normalizeString(row.external_id) ?? ''
    };
    const state = makeItemState(row.id, snapshot);
    byId.set(state.id, state);
    attachItemState(state, byComposite, byRef);
  }

  for (const item of itemsToSync) {
    const normalizedType = normalizeString(item.type)?.toUpperCase();
    if (!normalizedType) continue;
    const manufacturerId = item.company_code ? await resolveCompanyId(item.company_code) : undefined;
    const stats = sanitizeItemStats(cloneJson(item.stats ?? {}));
    if (item.description) {
      (stats as Record<string, unknown>).description = item.description;
    }
    const snapshot: ItemSnapshot = {
      name: item.name,
      type: normalizedType,
      subtype: item.subtype ?? null,
      size: item.size ?? null,
      grade: item.grade ?? null,
      class: item.class ?? null,
      manufacturer: manufacturerId ?? null,
      external_refs: sortRefs(cloneRefs(item.external_refs ?? [])),
      stats,
      external_id: item.external_id
    };
    const composite = itemCompositeKey(snapshot.type, snapshot.name);
    let state: ItemState | undefined = composite ? byComposite.get(composite) : undefined;
    if (!state && snapshot.external_refs.length) {
      for (const key of buildRefKeys(snapshot.external_refs)) {
        state = byRef.get(key);
        if (state) break;
      }
    }

    const payload: Record<string, unknown> = {
      name: snapshot.name,
      type: snapshot.type,
      subtype: snapshot.subtype,
      size: snapshot.size,
      grade: snapshot.grade,
      class: snapshot.class,
      manufacturer: snapshot.manufacturer,
      external_refs: snapshot.external_refs,
      stats: snapshot.stats,
      external_id: snapshot.external_id,
      status: 'published'
    };

    if (state) {
      const diff = computeDiff(snapshotToRecord(state.snapshot), snapshotToRecord(snapshot), [
        'name',
        'type',
        'subtype',
        'size',
        'grade',
        'class',
        'manufacturer',
        'external_refs',
        'stats',
        'external_id'
      ]);
      if (diff) {
        detachItemState(state, byComposite, byRef);
        await updateOneWithVersion(COLLECTIONS.items, state.id, payload, versionName, promoteVersions);
        const nextState = makeItemState(state.id, snapshot);
        byId.set(state.id, nextState);
        attachItemState(nextState, byComposite, byRef);
        updated += 1;
      } else {
        unchanged += 1;
      }
    } else {
      const createdItem = await createOneWithVersion<{ id: string }>(
        COLLECTIONS.items,
        payload,
        versionName,
        promoteVersions
      );
      const newState = makeItemState(createdItem.id, snapshot);
      byId.set(newState.id, newState);
      attachItemState(newState, byComposite, byRef);
      createdCount += 1;
    }
  }

  const durationMs = Math.round(performance.now() - started);
  log.info('Items sync complete', {
    total: itemsToSync.length,
    created: createdCount,
    updated,
    unchanged,
    skipped_non_allowlisted: skippedItemCount,
    duration_ms: durationMs
  });
}

interface HardpointSnapshot {
  ship_variant: string;
  code: string;
  category: string;
  position?: string | null;
  size?: number | null;
  gimballed?: boolean | null;
  powered?: boolean | null;
  path?: string | null;
  meta?: Record<string, unknown> | null;
  external_id: string;
  parent?: string | null;
  item?: string | null;
  item_quantity?: number | null;
  is_leaf?: boolean | null;
}

interface HardpointState {
  id: string;
  snapshot: HardpointSnapshot;
  key: string;
}

interface ExistingHardpointRow {
  id: string;
  ship_variant?: string | { id?: string } | null;
  code?: string | null;
  category?: string | null;
  position?: string | null;
  size?: number | string | null;
  gimballed?: boolean | null;
  powered?: boolean | null;
  meta?: unknown;
  path?: string | null;
  external_id?: string | null; // wichtig für byExt/hardpointIdByExternal
  parent?: string | { id?: string } | null;
  item?: string | { id?: string } | null;
  item_quantity?: number | string | null;
  is_leaf?: boolean | null;
}


interface ExistingShipConfigurationRow {
  id: string;
  ship_variant?: string | { id?: string } | null;
  code?: string | null;
  name?: string | null;
  profile?: string | null;
}

interface ExistingShipConfigurationHardpointRow {
  id: string;
  configuration?: string | { id?: string } | null;
  hardpoint?: string | { id?: string } | null;
  item?: string | { id?: string } | null;
  quantity?: number | string | null;
}

interface ResolvedShipConfiguration {
  configurationId: string;
  configurationCode: string;
  variantExternalId: string;
  shipVariantIds: string[];
  profiles: string[];
  isBase: boolean;
}


function hardpointKey (shipVariantId: string, code: string): string {
  return `${shipVariantId}${HARDPOINT_KEY_SEPARATOR}${code.toLowerCase()}`;
}


async function syncShipConfigurations (
  grouping: ReturnType<typeof loadShipGrouping>,
  variants: NormalizedShipVariantV2[],
  variantIdMap: Map<string, string>
): Promise<ResolvedShipConfiguration[]> {
  const existingRows = await fetchAllRows<ExistingShipConfigurationRow>(COLLECTIONS.shipConfigurations, [
    'id',
    'ship_variant',
    'ship_variant.id',
    'code',
    'name'
  ]);

  const existingByVariant = new Map<string, Map<string, ExistingShipConfigurationRow>>();
  for (const row of existingRows) {
    const variantId = extractId(row.ship_variant);
    if (!variantId) continue;
    const code = toOptionalString(row.code)?.toUpperCase();
    if (!code) continue;
    let map = existingByVariant.get(variantId);
    if (!map) {
      map = new Map();
      existingByVariant.set(variantId, map);
    }
    map.set(code, row);
  }

  const resolved: ResolvedShipConfiguration[] = [];
  const deleteIds: string[] = [];

  for (const variant of variants) {
    const variantExternalId = normalizeExternalId(variant.external_id);
    if (!variantExternalId) continue;
    const directusVariantId = variantIdMap.get(variantExternalId);
    if (!directusVariantId) {
      log.warn('Missing Directus ship_variant for configuration sync', {
        variant: variant.external_id
      });
      continue;
    }

    const assignment =
      grouping.lookupVariant(variant.ship_external, variant.variant_code) ??
      grouping.lookupShipVariantId(variantExternalId) ??
      grouping.lookupShipId(variantExternalId);

    const configs = new Map<string, { name: string; shipVariantIds: string[]; profiles: string[]; isBase: boolean }>();

    const baseName = variant.name || humanizeCode('BASE');
    configs.set('BASE', {
      name: baseName,
      shipVariantIds: [variantExternalId],
      profiles: [],
      isBase: true
    });

    if (assignment) {
      for (const config of assignment.configurations) {
        const code = config.code?.trim().toUpperCase();
        if (!code || code === 'BASE') continue;
        const shipVariantIds = (config.shipVariantIds.length ? config.shipVariantIds : [variantExternalId])
          .map((id) => normalizeExternalId(id))
          .filter((id): id is string => Boolean(id));
        if (!shipVariantIds.length) shipVariantIds.push(variantExternalId);
        const profiles = (config.profiles ?? [])
          .map((profile) => normalizeExternalId(profile))
          .filter((profile): profile is string => Boolean(profile));
        const name = config.name ?? humanizeCode(code);
        configs.set(code, {
          name,
          shipVariantIds,
          profiles,
          isBase: false
        });
      }
    }

    const variantExisting = new Map(existingByVariant.get(directusVariantId) ?? []);

    for (const [code, config] of configs.entries()) {
      const existing = variantExisting.get(code);
      if (existing) {
        const updates: Record<string, unknown> = {};
        const existingName = toOptionalString(existing.name) ?? '';
        if (existingName !== config.name) {
          updates.name = config.name;
        }
        if (Object.keys(updates).length) {
          await updateOne(COLLECTIONS.shipConfigurations, existing.id, updates);
        }
        variantExisting.delete(code);
        resolved.push({
          configurationId: existing.id,
          configurationCode: code,
          variantExternalId,
          shipVariantIds: config.shipVariantIds,
          profiles: config.profiles,
          isBase: config.isBase
        });
      } else {
        const created = await createOne<{ id: string }>(COLLECTIONS.shipConfigurations, {
          ship_variant: directusVariantId,
          code,
          name: config.name
        });
        resolved.push({
          configurationId: created.id,
          configurationCode: code,
          variantExternalId,
          shipVariantIds: config.shipVariantIds,
          profiles: config.profiles,
          isBase: config.isBase
        });
      }
    }

    for (const row of variantExisting.values()) {
      deleteIds.push(row.id);
    }
  }

  if (deleteIds.length) {
    await deleteMany(COLLECTIONS.shipConfigurations, deleteIds);
  }

  return resolved;
}

async function syncShipConfigurationHardpoints (
  configurations: ResolvedShipConfiguration[],
  installedItems: NormalizedInstalledItem[],
  hardpointIdMap: Map<string, string>,
  itemIdMap: Map<string, string>
): Promise<void> {
  if (!configurations.length) {
    log.info('Skipping configuration hardpoints sync', {
      reason: 'no configurations',
      installed_items: installedItems.length
    });
    return;
  }

  const started = performance.now();
  log.info('Syncing configuration hardpoints', {
    configurations: configurations.length,
    installed_items: installedItems.length
  });

  const existingRows = await fetchAllRows<ExistingShipConfigurationHardpointRow>(
    COLLECTIONS.shipConfigurationHardpoints,
    ['id', 'configuration', 'configuration.id', 'hardpoint', 'hardpoint.id', 'item', 'item.id', 'quantity']
  );

  const existingMap = new Map<string, ExistingShipConfigurationHardpointRow>();
  for (const row of existingRows) {
    const configurationId = extractId(row.configuration);
    const hardpointId = extractId(row.hardpoint);
    if (!configurationId || !hardpointId) continue;
    const itemId = extractId(row.item) ?? null;
    const key = `${configurationId}|${hardpointId}|${itemId ?? 'null'}`;
    existingMap.set(key, row);
  }

  const itemsByVariant = new Map<string, NormalizedInstalledItem[]>();
  for (const item of installedItems) {
    const shipId = normalizeExternalId(item.ship_variant_external_id);
    if (!shipId) continue;
    if (!itemsByVariant.has(shipId)) {
      itemsByVariant.set(shipId, []);
    }
    itemsByVariant.get(shipId)!.push(item);
  }

  const profileMap = new Map<string, Set<string>>();
  for (const config of configurations) {
    if (config.isBase) continue;
    const set = profileMap.get(config.variantExternalId) ?? new Set<string>();
    for (const profile of config.profiles) {
      set.add(profile);
    }
    profileMap.set(config.variantExternalId, set);
  }

  const desired = new Map<string, { configuration: string; hardpoint: string; item?: string; quantity: number }>();
  let missingHardpoint = 0;
  let missingItem = 0;
  let profileFiltered = 0;
  let matchedRecords = 0;

  for (const config of configurations) {
    let normalizedVariantIds = config.shipVariantIds
      .map((id) => normalizeExternalId(id))
      .filter((id): id is string => Boolean(id));
    if (!normalizedVariantIds.length) {
      const fallbackId = normalizeExternalId(config.variantExternalId);
      if (fallbackId) {
        normalizedVariantIds = [fallbackId];
      } else {
        continue;
      }
    }

    const competingProfiles = profileMap.get(config.variantExternalId) ?? new Set<string>();

    for (const variantId of normalizedVariantIds) {
      const records = itemsByVariant.get(variantId);
      if (!records || !records.length) continue;

      for (const record of records) {
        const hardpointExternal = normalizeExternalId(record.hardpoint_external_id);
        if (!hardpointExternal) continue;
        const hardpointId = hardpointIdMap.get(hardpointExternal);
        if (!hardpointId) {
          log.warn('Missing hardpoint mapping for configuration loadout', {
            configuration: config.configurationCode,
            hardpoint: record.hardpoint_external_id
          });
          missingHardpoint += 1;
          continue;
        }

        const profile = normalizeExternalId(record.profile);
        if (config.profiles.length) {
          if (!profile || !config.profiles.includes(profile)) {
            profileFiltered += 1;
            continue;
          }
        } else if (profile && competingProfiles.has(profile)) {
          profileFiltered += 1;
          continue;
        }

        const itemId = record.item_external_id ? itemIdMap.get(record.item_external_id) : undefined;
        if (!itemId) {
          missingItem += 1;
          continue;
        }

        const quantity =
          typeof record.quantity === 'number'
            ? record.quantity
            : Number(record.quantity ?? 1) || 1;

        const key = `${config.configurationId}|${hardpointId}|${itemId}`;
        const existing = desired.get(key);
        if (existing) {
          existing.quantity += quantity;
        } else {
          desired.set(key, {
            configuration: config.configurationId,
            hardpoint: hardpointId,
            item: itemId,
            quantity
          });
        }
        matchedRecords += 1;
      }
    }
  }

  const toCreate: Array<Record<string, unknown>> = [];
  const toUpdate: Array<Record<string, unknown>> = [];
  const toDelete: string[] = [];

  for (const [key, payload] of desired.entries()) {
    const existing = existingMap.get(key);
    if (existing) {
      existingMap.delete(key);
      const existingQuantity =
        typeof existing.quantity === 'number'
          ? existing.quantity
          : Number(existing.quantity ?? 1) || 1;
      if (existingQuantity !== payload.quantity) {
        toUpdate.push({ id: existing.id, quantity: payload.quantity });
      }
    } else {
      toCreate.push({
        configuration: payload.configuration,
        hardpoint: payload.hardpoint,
        item: payload.item ?? null,
        quantity: payload.quantity
      });
    }
  }

  for (const row of existingMap.values()) {
    toDelete.push(row.id);
  }

  for (const batch of chunkArray(toDelete, 100)) {
    if (batch.length) {
      await deleteMany(COLLECTIONS.shipConfigurationHardpoints, batch);
    }
  }

  for (const batch of chunkArray(toCreate, 100)) {
    if (batch.length) {
      await createMany(COLLECTIONS.shipConfigurationHardpoints, batch);
    }
  }

  for (const batch of chunkArray(toUpdate, 100)) {
    if (batch.length) {
      await updateMany(COLLECTIONS.shipConfigurationHardpoints, batch);
    }
  }

  const durationMs = Math.round(performance.now() - started);
  log.info('Configuration hardpoints sync complete', {
    linked_records: matchedRecords,
    desired_entries: desired.size,
    created: toCreate.length,
    updated: toUpdate.length,
    deleted: toDelete.length,
    skipped: {
      missing_hardpoint: missingHardpoint,
      missing_item: missingItem,
      profile_filtered: profileFiltered
    },
    duration_ms: durationMs
  });
}

async function syncHardpoints (
  hardpoints: NormalizedHardpointV2[],
  variantMap: Map<string, string>,
  installedByHardpoint: Map<string, { item_external_id: string; quantity: number }>,
  itemIdMap: Map<string, string>,
  versionName: string,
  promoteVersions: boolean
): Promise<void> {
  if (!hardpoints.length) return;

  const started = performance.now();
  const hardpointsToSync = hardpoints.filter((hp) => {
    const normalizedCategory = normalizeString(hp.category)?.toUpperCase();
    return normalizedCategory ? HARDPOINT_CATEGORY_ALLOWLIST_SET.has(normalizedCategory) : false;
  });
  const skippedNonAllowlisted = hardpoints.length - hardpointsToSync.length;

  log.info('Syncing hardpoints', {
    total: hardpointsToSync.length,
    skipped_non_allowlisted: skippedNonAllowlisted
  });

  if (!hardpointsToSync.length) {
    return;
  }

  const existingFilter: Record<string, unknown> | undefined =
    HARDPOINT_CATEGORY_ALLOWLIST.length === 1
      ? { category: { _eq: HARDPOINT_CATEGORY_ALLOWLIST[0] } }
      : {
          _or: HARDPOINT_CATEGORY_ALLOWLIST.map((category) => ({ category: { _eq: category } }))
        };

  // Vorhandene Rows inkl. external_id laden
  const existingRows = await fetchAllRows<ExistingHardpointRow>(COLLECTIONS.hardpoints, [
    'id',
    'ship_variant',
    'ship_variant.id',
    'code',
    'category',
    'position',
    'size',
    'gimballed',
    'powered',
    'meta',
    'path',
    'parent',
    'parent.id',
    'item',
    'item.id',
    'item_quantity',
    'is_leaf',
    'external_id'
  ], existingFilter);


  // Map zur Parent-Auflösung: external_id -> Directus-ID (mit vorhandenen füttern)
  const hardpointIdByExternal = new Map<string, string>();
  for (const row of existingRows) {
    const ext = normalizeString(row.external_id);
    if (ext) hardpointIdByExternal.set(ext, row.id);
  }

  // State-Map nach external_id (nicht mehr nach "code")
  const byExt = new Map<string, HardpointState>();
  for (const row of existingRows) {
    const shipVariantId = extractId(row.ship_variant);
    const code = normalizeString(row.code);
    const ext = normalizeString(row.external_id);
    const category = normalizeString(row.category) ?? '';
    if (!shipVariantId || !code || !ext) continue;

    const meta = row.meta && typeof row.meta === 'object'
      ? cloneJson<Record<string, unknown>>(row.meta as Record<string, unknown>)
      : null;
    const parentId = extractId(row.parent) ?? null;
    const itemId = extractId(row.item) ?? null;
    const itemQuantity = normalizeNumber(row.item_quantity) ?? null;
    const isLeaf = normalizeBooleanFlag(row.is_leaf) ?? false;

    const snapshot: HardpointSnapshot = {
      ship_variant: shipVariantId,
      code,
      category,
      position: normalizeString(row.position) ?? null,
      size: normalizeNumber(row.size) ?? null,
      gimballed: normalizeBooleanFlag(row.gimballed) ?? null,
      powered: normalizeBooleanFlag(row.powered) ?? null,
      path: normalizeString(row.path) ?? null,
      meta,
      external_id: ext,
      parent: parentId,
      item: itemId,
      item_quantity: itemQuantity,
      is_leaf: isLeaf
    };
    byExt.set(ext, { id: row.id, snapshot, key: ext });
  }

  // Eltern vor Kindern verarbeiten: Tiefe über path bestimmen
  const depth = (extId: string): number => {
    const parts = extId.split(':');                 // [variant, path...]
    const path = parts.slice(1).join(':');
    return path ? path.split('/').length : 1;
  };
  hardpointsToSync.sort((a, b) =>
    depth(String((a as any).external_id ?? '')) - depth(String((b as any).external_id ?? ''))
  );

  const seen = new Set<string>();
  let createdCount = 0;
  let updatedCount = 0;
  let unchanged = 0;
  let skippedInvalidId = 0;
  let skippedMissingVariant = 0;
  let skippedMissingCategory = 0;
  let skippedDuplicate = 0;
  let linkedParents = 0;
  let linkedItems = 0;

  for (const hardpoint of hardpointsToSync) {
    // external_id / Pfad & Parent ermitteln
    const extId = normalizeString((hardpoint as any).external_id as string);
    if (!extId) {
      skippedInvalidId += 1;
      continue;
    }

    const parts = extId.split(':');  // ["RSI_ZEUS_CL", "hp_turret/.."]
    const pathPart = parts.slice(1).join(':');
    const pathSegs = pathPart ? pathPart.split('/') : [];

    const codeSeg = pathSegs.length ? pathSegs[pathSegs.length - 1] : hardpoint.code;
    const parentExt = pathSegs.length > 1
      ? `${parts[0]}:${pathSegs.slice(0, -1).join('/')}`
      : null;

    // Item-Zuordnung (legacy installed_items)
    const installed = installedByHardpoint.get(extId);
    const itemExternal = installed?.item_external_id;
    const itemQuantity = installed?.quantity ?? null;

    // Variant auflösen
    const shipVariantId = variantMap.get(hardpoint.ship_variant_external);
    if (!shipVariantId) {
      log.warn('Skipping hardpoint with unknown ship variant', {
        ship_variant: hardpoint.ship_variant_external,
        code: hardpoint.code
      });
      skippedMissingVariant += 1;
      continue;
    }

    const category = normalizeString(hardpoint.category);
    if (!category) {
      skippedMissingCategory += 1;
      continue;
    }

    // pro external_id nur einmal
    const key = extId;
    if (seen.has(key)) {
      skippedDuplicate += 1;
      continue;
    }
    seen.add(key);

    const parentId = parentExt ? hardpointIdByExternal.get(parentExt) ?? null : null;
    const itemId = itemExternal ? itemIdMap.get(itemExternal) ?? null : null;
    if (parentId) linkedParents += 1;
    if (itemId) linkedItems += 1;
    const meta: Record<string, unknown> = {};
    if (hardpoint.seats !== undefined && hardpoint.seats !== null) {
      meta.seats = hardpoint.seats;
    }
    const metaValue = Object.keys(meta).length ? meta : null;
    const pathValue = pathPart || null;
    const isLeaf = itemId ? true : false;

    // Snapshot (nur Felder, die wir diffen wollen)
    const snapshot: HardpointSnapshot = {
      ship_variant: shipVariantId,
      code: codeSeg ?? hardpoint.code,
      category,
      position: normalizeString(hardpoint.position) ?? null,
      size: hardpoint.size ?? null,
      gimballed: hardpoint.gimballed ?? null,
      powered: hardpoint.powered ?? null,
      path: pathValue,
      meta: metaValue,
      external_id: extId,
      parent: parentId,
      item: itemId,
      item_quantity: itemQuantity,
      is_leaf: isLeaf
    };

    // Payload für Directus
    const payload: Record<string, unknown> = {
      ship_variant: snapshot.ship_variant,
      code: snapshot.code,
      category: snapshot.category,
      position: snapshot.position,
      size: snapshot.size,
      gimballed: snapshot.gimballed,
      powered: snapshot.powered,
      path: snapshot.path,
      meta: snapshot.meta,
      status: 'published',
      external_id: extId,
      parent: snapshot.parent,
      item: snapshot.item,
      item_quantity: snapshot.item_quantity,
      is_leaf: snapshot.is_leaf
    };

    const state = byExt.get(key);
    if (state) {
      // Diff inkl. "code", weil du ihn aus dem Pfad abgeleitet änderst
      const diff = computeDiff(snapshotToRecord(state.snapshot), snapshotToRecord(snapshot), [
        'code',
        'category',
        'position',
        'size',
        'gimballed',
        'powered',
        'path',
        'meta',
        'parent',
        'item',
        'item_quantity',
        'is_leaf'
      ]);
      if (diff) {
        await updateOneWithVersion(
          COLLECTIONS.hardpoints,
          state.id,
          payload,
          versionName,
          promoteVersions
        );
        // Map immer aktualisieren, damit nachfolgende Kinder den Parent finden
        hardpointIdByExternal.set(extId, state.id);
        updatedCount += 1;
      } else {
        // auch ohne Update parent map füttern
        hardpointIdByExternal.set(extId, state.id);
        unchanged += 1;
      }
    } else {
      const created = await createOneWithVersion<{ id: string }>(
        COLLECTIONS.hardpoints,
        payload,
        versionName,
        promoteVersions
      );
      // Map für Kinder füllen
      hardpointIdByExternal.set(extId, created.id);
      createdCount += 1;
    }
  }

  const durationMs = Math.round(performance.now() - started);
  log.info('Hardpoints sync complete', {
    total: hardpointsToSync.length,
    created: createdCount,
    updated: updatedCount,
    unchanged,
    linked_items: linkedItems,
    linked_parents: linkedParents,
    skipped: {
      invalid_id: skippedInvalidId,
      missing_variant: skippedMissingVariant,
      missing_category: skippedMissingCategory,
      duplicate: skippedDuplicate,
      non_allowlisted: skippedNonAllowlisted
    },
    duration_ms: durationMs
  });
}


async function loadNormalizedBundleLegacy (dir: string): Promise<NormalizedDataBundle> {
  return {
    manufacturers: await readJsonOrDefault(join(dir, 'manufacturers.json'), []),
    ships: await readJsonOrDefault(join(dir, 'ships.json'), []),
    ship_variants: await readJsonOrDefault(join(dir, 'ship_variants.json'), []),
    items: await readJsonOrDefault(join(dir, 'items.json'), []),
    hardpoints: await readJsonOrDefault(join(dir, 'hardpoints.json'), []),
    item_stats: await readJsonOrDefault(join(dir, 'item_stats.json'), []),
    ship_stats: await readJsonOrDefault(join(dir, 'ship_stats.json'), []),
    installed_items: await readJsonOrDefault(join(dir, 'installed_items.json'), []),
    locales: await readJsonOrDefault(join(dir, 'locales.json'), [])
  };
}

export async function readBuildMetadata (normalizedDir: string): Promise<BuildMetadata> {
  const metadataFile = await readJsonOrDefault<Record<string, unknown>>(
    join(normalizedDir, 'build.json'),
    {}
  );
  return {
    build_hash: (metadataFile.build_hash as string | null | undefined) ?? undefined,
    released_at:
      (metadataFile.released_at as string | null | undefined) ??
      (metadataFile.released_at as string | null | undefined) ??
      undefined,
    status: metadataFile.status as BuildMetadata['status']
  };
}

export async function ensureBuild (
  channel: Channel,
  version: string,
  metadata: BuildMetadata
): Promise<BuildRecord> {
  const existing = await readByQuery<BuildRecord>('builds', {
    filter: { channel: { _eq: channel }, game_version: { _eq: version } },
    limit: 1,
    fields: ['id', 'status', 'build_hash', 'released_at']
  });

  const buildMeta = existing[0];

  if (buildMeta) {
    const patch: Record<string, unknown> = {};
    if (buildMeta.status !== 'pending') {
      patch.status = 'pending';
    }
    if (metadata.build_hash && metadata.build_hash !== buildMeta.build_hash) {
      patch.build_hash = metadata.build_hash;
    }
    if (metadata.released_at && metadata.released_at !== buildMeta.released_at) {
      patch.released_at = metadata.released_at;
    }
    if (Object.keys(patch).length) {
      await updateOne('builds', buildMeta.id, patch);
      return { ...buildMeta, ...patch } as BuildRecord;
    }
    return buildMeta;
  }

  const created = await createOne<BuildRecord>('builds', {
    channel,
    game_version: version,
    status: 'pending',
    build_hash: metadata.build_hash ?? null,
    released_at: metadata.released_at ?? null,
    ingested: null
  });

  return created;
}

export async function loadAll (
  dataRoot: string,
  channel: Channel,
  version: string,
  bundle?: NormalizedDataBundle,
  options: LoadOptions = {}
): Promise<LoadResult> {
  const normalizedDir = join(dataRoot, 'normalized', channel, version);
  const legacyBundle = bundle ?? (await loadNormalizedBundleLegacy(normalizedDir));
  void legacyBundle; // legacy data retained for compatibility but unused in v2 loader.

  const metadata = options.metadata ?? (await readBuildMetadata(normalizedDir));
  const build = options.build ?? (await ensureBuild(channel, version, metadata));
  log.info('Loading v2 data into Directus', { buildId: build.id, channel, version });

  const normalizedV2 = await loadNormalizedBundleV2(normalizedDir, channel, version);
  const shipGrouping = loadShipGrouping();
  const contentVersionName = `V${version}-${channel}`;
  const promoteVersions = channel === 'LIVE';

  // LEGACY installed_items für Item-Zuordnung einlesen
  const legacy = await loadNormalizedBundleLegacy(normalizedDir);
  const installedItems = legacy.installed_items ?? [];

  // Map: hardpoint_external_id -> { item_external_id, quantity }
  const installedByHardpoint = new Map<string, { item_external_id: string; quantity: number }>();
  for (const inst of installedItems) {
    if (inst?.hardpoint_external_id) {
      const existing = installedByHardpoint.get(inst.hardpoint_external_id);
      if (existing) {
        existing.quantity += (inst.quantity ?? 1);
      } else {
        installedByHardpoint.set(inst.hardpoint_external_id, {
          item_external_id: inst.item_external_id,
          quantity: inst.quantity ?? 1
        });
      }
    }
  }

  const { statsByVariant, hardpoints } = splitVariantStats(normalizedV2);

  const companyIdSeed = await syncCompanies(normalizedV2.companies);

  const companyResolver = new CompanyResolver(COLLECTIONS.companies);
  await companyResolver.warmup();

  const missingCompanyKey = '__EMPTY__';
  const companyIdCache = new Map<string, string | null>();
  const resolveCompanyId = async (code: string): Promise<string | undefined> => {
    const normalized = code.trim().toUpperCase();
    const cacheKey = normalized || missingCompanyKey;
    const cached = companyIdCache.get(cacheKey);
    if (cached !== undefined) {
      return cached ?? undefined;
    }

    if (!normalized) {
      log.warn('Encountered entity without company code; skipping manufacturer assignment');
      companyIdCache.set(cacheKey, null);
      return undefined;
    }

    const seeded = companyIdSeed.get(normalized);
    if (seeded) {
      companyIdCache.set(cacheKey, seeded);
      return seeded;
    }

    const id = await companyResolver.lookupId(normalized);
    if (!id) {
      companyIdCache.set(cacheKey, null);
      log.warn('No Directus company entry found for manufacturer code; skipping assignment', {
        code: normalized
      });
      return undefined;
    }

    companyIdCache.set(cacheKey, id);
    return id;
  };

  for (const company of normalizedV2.companies) {
    await resolveCompanyId(company.code ?? '');
  }

  const shipIdMap = await syncShips(normalizedV2.ships, resolveCompanyId, contentVersionName, promoteVersions);

  const variantIdMap = await syncShipVariants(
    normalizedV2.ship_variants,
    statsByVariant,
    shipIdMap,
    contentVersionName,
    promoteVersions
  );

  const resolvedConfigurations = await syncShipConfigurations(
    shipGrouping,
    normalizedV2.ship_variants,
    variantIdMap
  );

  await syncItems(normalizedV2.items, resolveCompanyId, contentVersionName, promoteVersions);

  const itemIdMap = await buildItemIdMap();
  await syncHardpoints(
    hardpoints,
    variantIdMap,           // Map variant external -> Directus ID
    installedByHardpoint,   // Map hardpoint external -> { item_external_id, quantity }
    itemIdMap,              // Map item external -> Directus ID
    contentVersionName,
    promoteVersions
  );

  const hardpointIdMap = await buildHardpointIdMap();
  await syncShipConfigurationHardpoints(
    resolvedConfigurations,
    installedItems,
    hardpointIdMap,
    itemIdMap
  );

  const completed = await updateOne<BuildRecord>('builds', build.id, {
    status: 'ingested',
    ingested: new Date().toISOString()
  });

  const stats: LoadStatistics = {
    companies: normalizedV2.companies.length,
    ships: normalizedV2.ships.length,
    ship_variants: normalizedV2.ship_variants.length,
    items: normalizedV2.items.length,
    hardpoints: hardpoints.length
  };

  log.info('Load complete', {
    buildId: completed.id,
    stats
  });

  return { build: completed, stats };
}
