import { join } from 'node:path';
import { readJsonOrDefault } from './utils/fs.js';
import { createOne, readByQuery, updateOne } from './utils/directus.js';
import { log } from './utils/log.js';
import { CompanyResolver } from './utils/companyResolver.js';
import { DiffWriter, computeDiff } from './diffs.js';
import type {
  Channel,
  NormalizedBundleV2,
  NormalizedCompanyV2,
  NormalizedDataBundle,
  NormalizedExternalReference,
  NormalizedHardpointV2,
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
  diffs: number;
}

export interface LoadResult {
  build: BuildRecord;
  stats: LoadStatistics;
}

export interface LoadOptions {
  metadata?: BuildMetadata;
  build?: BuildRecord;
  skipDiffs?: boolean;
}

const COLLECTIONS = {
  companies: process.env.SC_COMPANY_COLLECTION ?? 'companies',
  ships: process.env.SC_SHIP_COLLECTION ?? 'ships',
  shipVariants: process.env.SC_SHIP_VARIANT_COLLECTION ?? 'ship_variants',
  items: process.env.SC_ITEM_COLLECTION ?? 'items',
  hardpoints: process.env.SC_HARDPOINT_COLLECTION ?? 'hardpoints'
} as const;

const PAGE_LIMIT = 200;
const HARDPOINT_KEY_SEPARATOR = '::';
const DEFAULT_VARIANT_CODE = 'BASE';

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

function shipCompositeKey (manufacturerId: string | undefined, name: string | undefined): string | undefined {
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

interface ShipSnapshot {
  name: string;
  manufacturer: string;
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
  ensureCompanyId: (code: string) => Promise<string>,
  diffWriter: DiffWriter
): Promise<{ map: Map<string, string>; diffs: number }> {
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
    const manufacturerId = extractId(row.manufacturer) ?? '';
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
  let diffs = 0;

  for (const ship of ships) {
    const manufacturerId = await ensureCompanyId(ship.company_code);
    const snapshot: ShipSnapshot = {
      name: ship.name,
      manufacturer: manufacturerId,
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
        await updateOne(COLLECTIONS.ships, state.id, payload);
        const nextState = makeShipState(state.id, snapshot);
        byId.set(state.id, nextState);
        attachShipState(nextState, byComposite, byRef);
        if (diffWriter.addChange({
          entityType: COLLECTIONS.ships,
          entityId: state.id,
          changeType: 'updated',
          diff
        })) {
          diffs++;
        }
        state = nextState;
      }
    } else {
      const created = await createOne<{ id: string }>(COLLECTIONS.ships, payload);
      const newState = makeShipState(created.id, snapshot);
      byId.set(newState.id, newState);
      attachShipState(newState, byComposite, byRef);
      if (diffWriter.addChange({
        entityType: COLLECTIONS.ships,
        entityId: newState.id,
        changeType: 'created',
        diff: computeDiff(undefined, snapshotToRecord(snapshot), ['name', 'manufacturer', 'external_refs', 'paints'])!
      })) {
        diffs++;
      }
      state = newState;
    }

    if (state) {
      shipIdByExternal.set(ship.external_id, state.id);
    }
  }

  return { map: shipIdByExternal, diffs };
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
  diffWriter: DiffWriter
): Promise<{ map: Map<string, string>; diffs: number }> {
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
  let diffs = 0;

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
        await updateOne(COLLECTIONS.shipVariants, state.id, payload);
        const nextState = makeVariantState(state.id, snapshot);
        byId.set(state.id, nextState);
        attachVariantState(nextState, byComposite, byRef);
        if (diffWriter.addChange({
          entityType: COLLECTIONS.shipVariants,
          entityId: state.id,
          changeType: 'updated',
          diff
        })) {
          diffs++;
        }
        state = nextState;
      }
    } else {
      const created = await createOne<{ id: string }>(COLLECTIONS.shipVariants, payload);
      const newState = makeVariantState(created.id, snapshot);
      byId.set(newState.id, newState);
      attachVariantState(newState, byComposite, byRef);
      if (diffWriter.addChange({
        entityType: COLLECTIONS.shipVariants,
        entityId: newState.id,
        changeType: 'created',
        diff: computeDiff(undefined, snapshotToRecord(snapshot), [
          'name',
          'variant_code',
          'external_refs',
          'stats',
          'thumbnail',
          'release_patch'
        ])!
      })) {
        diffs++;
      }
      state = newState;
    }

    if (state) {
      variantIdByExternal.set(variant.external_id, state.id);
    }
  }

  return { map: variantIdByExternal, diffs };
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
  ensureCompanyId: (code: string) => Promise<string>,
  diffWriter: DiffWriter
): Promise<number> {
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
    'stats'
  ]);

  const byId = new Map<string, ItemState>();
  const byComposite = new Map<string, ItemState>();
  const byRef = new Map<string, ItemState>();

  for (const row of existingRows) {
    const type = normalizeString(row.type) ?? '';
    const snapshot: ItemSnapshot = {
      name: normalizeString(row.name) ?? '',
      type,
      subtype: normalizeString(row.subtype) ?? null,
      size: normalizeNumber(row.size) ?? null,
      grade: normalizeString(row.grade) ?? null,
      class: normalizeString(row.class) ?? null,
      manufacturer: extractId(row.manufacturer) ?? null,
      external_refs: normalizeExternalRefsInput(row.external_refs),
      stats: cloneJson<Record<string, unknown>>((row.stats as Record<string, unknown>) ?? {})
    };
    const state = makeItemState(row.id, snapshot);
    byId.set(state.id, state);
    attachItemState(state, byComposite, byRef);
  }

  let diffs = 0;

  for (const item of items) {
    const manufacturerId = item.company_code ? await ensureCompanyId(item.company_code) : null;
    const stats = cloneJson(item.stats ?? {});
    if (item.description) {
      (stats as Record<string, unknown>).description = item.description;
    }
    const snapshot: ItemSnapshot = {
      name: item.name,
      type: item.type,
      subtype: item.subtype ?? null,
      size: item.size ?? null,
      grade: item.grade ?? null,
      class: item.class ?? null,
      manufacturer: manufacturerId,
      external_refs: sortRefs(cloneRefs(item.external_refs ?? [])),
      stats
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
        'stats'
      ]);
      if (diff) {
        detachItemState(state, byComposite, byRef);
        await updateOne(COLLECTIONS.items, state.id, payload);
        const nextState = makeItemState(state.id, snapshot);
        byId.set(state.id, nextState);
        attachItemState(nextState, byComposite, byRef);
        if (diffWriter.addChange({
          entityType: COLLECTIONS.items,
          entityId: state.id,
          changeType: 'updated',
          diff
        })) {
          diffs++;
        }
      }
    } else {
      const created = await createOne<{ id: string }>(COLLECTIONS.items, payload);
      const newState = makeItemState(created.id, snapshot);
      byId.set(newState.id, newState);
      attachItemState(newState, byComposite, byRef);
      if (diffWriter.addChange({
        entityType: COLLECTIONS.items,
        entityId: newState.id,
        changeType: 'created',
        diff: computeDiff(undefined, snapshotToRecord(snapshot), [
          'name',
          'type',
          'subtype',
          'size',
          'grade',
          'class',
          'manufacturer',
          'external_refs',
          'stats'
        ])!
      })) {
        diffs++;
      }
    }
  }

  return diffs;
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


function hardpointKey (shipVariantId: string, code: string): string {
  return `${shipVariantId}${HARDPOINT_KEY_SEPARATOR}${code.toLowerCase()}`;
}

async function syncHardpoints (
  hardpoints: NormalizedHardpointV2[],
  variantMap: Map<string, string>,
  installedByHardpoint: Map<string, { item_external_id: string; quantity: number }>,
  itemIdMap: Map<string, string>,
  diffWriter: DiffWriter
): Promise<number> {
  if (!hardpoints.length) return 0;

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
  ]);


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
  hardpoints.sort((a, b) =>
    depth(String((a as any).external_id ?? '')) - depth(String((b as any).external_id ?? ''))
  );

  const seen = new Set<string>();
  let diffs = 0;

  for (const hardpoint of hardpoints) {
    // external_id / Pfad & Parent ermitteln
    const extId = normalizeString((hardpoint as any).external_id as string);
    if (!extId) continue;

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
      continue;
    }

    const category = normalizeString(hardpoint.category);
    if (!category) continue;

    // pro external_id nur einmal
    const key = extId;
    if (seen.has(key)) continue;
    seen.add(key);

    const parentId = parentExt ? hardpointIdByExternal.get(parentExt) ?? null : null;
    const itemId = itemExternal ? itemIdMap.get(itemExternal) ?? null : null;
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
        await updateOne(COLLECTIONS.hardpoints, state.id, payload);
        // Map immer aktualisieren, damit nachfolgende Kinder den Parent finden
        hardpointIdByExternal.set(extId, state.id);
        if (diffWriter.addChange({
          entityType: COLLECTIONS.hardpoints,
          entityId: state.id,
          changeType: 'updated',
          diff
        })) {
          diffs++;
        }
      } else {
        // auch ohne Update parent map füttern
        hardpointIdByExternal.set(extId, state.id);
      }
    } else {
      const created = await createOne<{ id: string }>(COLLECTIONS.hardpoints, payload);
      // Map für Kinder füllen
      hardpointIdByExternal.set(extId, created.id);
      if (diffWriter.addChange({
        entityType: COLLECTIONS.hardpoints,
        entityId: created.id,
        changeType: 'created',
        diff: computeDiff(undefined, snapshotToRecord(snapshot), [
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
        ])!
      })) {
        diffs++;
      }
    }
  }

  return diffs;
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

  const itemIdMap = await buildItemIdMap();

  const { statsByVariant, hardpoints } = splitVariantStats(normalizedV2);

  const companyResolver = new CompanyResolver(COLLECTIONS.companies);
  await companyResolver.warmup();

  const companyIdCache = new Map<string, string>();
  const ensureCompanyId = async (code: string): Promise<string> => {
    const normalized = code.trim().toUpperCase();
    if (!normalized) {
      throw new Error('Encountered entity without company code.');
    }
    const cached = companyIdCache.get(normalized);
    if (cached) return cached;
    const id = await companyResolver.resolveId(normalized);
    companyIdCache.set(normalized, id);
    return id;
  };

  for (const company of normalizedV2.companies) {
    try {
      await ensureCompanyId(company.code);
    } catch (error) {
      log.warn('Failed to ensure company', {
        code: company.code,
        error: error instanceof Error ? error.message : String(error)
      });
    }
  }

  const diffWriter = new DiffWriter({ skip: options.skipDiffs });
  let diffCount = 0;

  const shipSync = await syncShips(normalizedV2.ships, ensureCompanyId, diffWriter);
  diffCount += shipSync.diffs;

  const variantSync = await syncShipVariants(
    normalizedV2.ship_variants,
    statsByVariant,
    shipSync.map,
    diffWriter
  );
  diffCount += variantSync.diffs;

  diffCount += await syncItems(normalizedV2.items, ensureCompanyId, diffWriter);
  diffCount += await syncHardpoints(
    hardpoints,
    variantSync.map,          // Map variant external -> Directus ID
    installedByHardpoint,     // Map hardpoint external -> { item_external_id, quantity }
    itemIdMap,                // Map item external -> Directus ID
    diffWriter
  );

  await diffWriter.flush(build.id);

  const completed = await updateOne<BuildRecord>('builds', build.id, {
    status: 'ingested',
    ingested: new Date().toISOString()
  });

  const stats: LoadStatistics = {
    companies: normalizedV2.companies.length,
    ships: normalizedV2.ships.length,
    ship_variants: normalizedV2.ship_variants.length,
    items: normalizedV2.items.length,
    hardpoints: hardpoints.length,
    diffs: diffCount
  };

  log.info('Load complete', {
    buildId: completed.id,
    stats
  });

  return { build: completed, stats };
}
