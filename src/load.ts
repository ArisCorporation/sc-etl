import { join } from 'node:path';
import {
  createMany,
  createOne,
  deleteMany,
  readByQuery,
  updateMany,
  updateOne,
  upsertByExternalId
} from './utils/directus.js';
import { readJsonOrDefault } from './utils/fs.js';
import { log } from './utils/log.js';
import type {
  Channel,
  NormalizedDataBundle,
  NormalizedHardpoint,
  NormalizedInstalledItem,
  NormalizedItem,
  NormalizedItemStat,
  NormalizedLocaleEntry,
  NormalizedManufacturer,
  NormalizedShip,
  NormalizedShipStat,
  NormalizedShipVariant
} from './types/index.js';

interface BuildMetadata {
  build_hash?: string | null;
  released?: string | null;
  status?: 'pending' | 'ingested' | 'failed';
}

interface BuildRecord {
  id: string;
  status: string;
  released?: string | null;
  build_hash?: string | null;
}

export interface LoadResult {
  build: BuildRecord;
}

export function mapByExternalId<T extends { external_id: string }>(items: T[]): Map<string, T> {
  const map = new Map<string, T>();
  for (const item of items) {
    map.set(item.external_id, item);
  }
  return map;
}

function chunk<T>(items: readonly T[], size: number): T[][] {
  if (size <= 0) throw new Error('chunk size must be greater than zero');
  const result: T[][] = [];
  for (let i = 0; i < items.length; i += size) {
    result.push(items.slice(i, i + size));
  }
  return result;
}

function requireId(map: Map<string, string>, externalId: string, context: string): string {
  const id = map.get(externalId);
  if (!id) {
    throw new Error(`Missing Directus id for ${context} (external_id=${externalId})`);
  }
  return id;
}

function nullable<T>(value: T | undefined | null): T | null {
  return value === undefined ? null : (value as T | null);
}

async function loadNormalizedBundle(dir: string): Promise<NormalizedDataBundle> {
  return {
    manufacturers: await readJsonOrDefault<NormalizedManufacturer[]>(
      join(dir, 'manufacturers.json'),
      []
    ),
    ships: await readJsonOrDefault<NormalizedShip[]>(join(dir, 'ships.json'), []),
    ship_variants: await readJsonOrDefault<NormalizedShipVariant[]>(
      join(dir, 'ship_variants.json'),
      []
    ),
    items: await readJsonOrDefault<NormalizedItem[]>(join(dir, 'items.json'), []),
    hardpoints: await readJsonOrDefault<NormalizedHardpoint[]>(join(dir, 'hardpoints.json'), []),
    item_stats: await readJsonOrDefault<NormalizedItemStat[]>(join(dir, 'item_stats.json'), []),
    ship_stats: await readJsonOrDefault<NormalizedShipStat[]>(join(dir, 'ship_stats.json'), []),
    installed_items: await readJsonOrDefault<NormalizedInstalledItem[]>(
      join(dir, 'installed_items.json'),
      []
    ),
    locales: await readJsonOrDefault<NormalizedLocaleEntry[]>(join(dir, 'locales.json'), [])
  };
}

export async function ensureBuild(
  channel: Channel,
  version: string,
  metadata: BuildMetadata
): Promise<BuildRecord> {
  const existing = await readByQuery<BuildRecord>('game_builds', {
    filter: { channel: { _eq: channel }, game_version: { _eq: version } },
    limit: 1,
    fields: ['id', 'status', 'build_hash', 'released']
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
    if (metadata.released && metadata.released !== buildMeta.released) {
      patch.released = metadata.released;
    }
    if (Object.keys(patch).length) {
      await updateOne('game_builds', buildMeta.id, patch);
      return { ...buildMeta, ...patch } as BuildRecord;
    }
    return buildMeta;
  }

  const created = await createOne<BuildRecord>('game_builds', {
    channel,
    game_version: version,
    status: 'pending',
    build_hash: metadata.build_hash ?? null,
    released: metadata.released ?? null,
    ingested: null
  });

  return created;
}

async function upsertLocales(entries: NormalizedLocaleEntry[]) {
  if (!entries.length) return;
  for (const batch of chunk(entries, 200)) {
    const filter = {
      _or: batch.map((entry) => ({
        _and: [
          { namespace: { _eq: entry.namespace } },
          { key: { _eq: entry.key } },
          { lang: { _eq: entry.lang } }
        ]
      }))
    };

    const existing = await readByQuery('locales', {
      filter,
      fields: ['id', 'namespace', 'key', 'lang'],
      limit: batch.length
    });

    const existingMap = new Map<string, string>();
    for (const row of existing) {
      const key = `${row.namespace}:${row.key}:${row.lang}`;
      existingMap.set(key, row.id as string);
    }

    const toCreate = [] as NormalizedLocaleEntry[];
    const toUpdate: Array<NormalizedLocaleEntry & { id: string }> = [];

    for (const entry of batch) {
      const key = `${entry.namespace}:${entry.key}:${entry.lang}`;
      const id = existingMap.get(key);
      if (id) {
        toUpdate.push({ ...entry, id });
      } else {
        toCreate.push(entry);
      }
    }

    if (toCreate.length) {
      await createMany(
        'locales',
        toCreate.map((entry) => ({
          namespace: entry.namespace,
          key: entry.key,
          lang: entry.lang,
          value: entry.value
        }))
      );
    }

    if (toUpdate.length) {
      await updateMany(
        'locales',
        toUpdate.map((entry) => ({
          id: entry.id,
          namespace: entry.namespace,
          key: entry.key,
          lang: entry.lang,
          value: entry.value
        }))
      );
    }
  }
}

async function syncByKey(
  collection: 'item_stats' | 'ship_stats',
  buildId: string,
  keyField: 'item' | 'ship_variant',
  records: Array<Record<string, unknown>>
) {
  if (!records.length) return;

  const existing = await readByQuery(collection, {
    filter: { build: { _eq: buildId } },
    fields: ['id', keyField],
    limit: -1
  });

  const existingMap = new Map<string, string>();
  for (const row of existing ?? []) {
    existingMap.set(row[keyField] as string, row.id as string);
  }

  const seen = new Set<string>();
  const toCreate: typeof records = [];
  const toUpdate: Array<typeof records[number] & { id: string }> = [];

  for (const record of records) {
    const keyValue = record[keyField];
    if (typeof keyValue !== 'string') {
      throw new Error(`Expected ${keyField} to be a string when syncing ${collection}`);
    }
    seen.add(keyValue);
    const id = existingMap.get(keyValue);
    if (id) {
      toUpdate.push({ ...record, id });
    } else {
      toCreate.push(record);
    }
  }

  const toDelete = Array.from(existingMap.entries())
    .filter(([key]) => !seen.has(key))
    .map(([, id]) => id);

  if (toCreate.length) {
    await createMany(collection, toCreate);
  }
  if (toUpdate.length) {
    await updateMany(collection, toUpdate);
  }
  if (toDelete.length) {
    await deleteMany(collection, toDelete);
  }
}

function makeInstalledKey(
  shipVariant: string,
  item: string,
  hardpoint: string | null
): string {
  return `${shipVariant}:${item}:${hardpoint ?? ''}`;
}

async function syncHardpoints(
  hardpoints: NormalizedHardpoint[],
  variantIdMap: Map<string, string>
): Promise<Map<string, string>> {
  if (!hardpoints.length) return new Map();

  const directusVariantIds = Array.from(
    new Set(
      hardpoints.map((hp) =>
        requireId(variantIdMap, hp.ship_variant_external_id, 'hardpoints.ship_variant')
      )
    )
  );

  const existing = await readByQuery('hardpoints', {
    filter: { ship_variant: { _in: directusVariantIds } },
    fields: ['id', 'ship_variant', 'code'],
    limit: -1
  });

  const existingByKey = new Map<string, { id: string }>();
  for (const row of existing ?? []) {
    const key = `${row.ship_variant as string}:${(row.code as string).toLowerCase()}`;
    existingByKey.set(key, { id: row.id as string });
  }

  const createdQueue: Array<{ payload: Record<string, unknown>; external: string }> = [];
  const toUpdate: Array<Record<string, unknown>> = [];
  const seen = new Set<string>();
  const externalToId = new Map<string, string>();

  for (const hardpoint of hardpoints) {
    const variantId = requireId(
      variantIdMap,
      hardpoint.ship_variant_external_id,
      'hardpoints.ship_variant'
    );
    const key = `${variantId}:${hardpoint.code.toLowerCase()}`;
    seen.add(key);

    const payload = {
      ship_variant: variantId,
      code: hardpoint.code,
      category: hardpoint.category,
      position: nullable(hardpoint.position),
      size: hardpoint.size ?? null,
      gimballed: hardpoint.gimballed ?? null,
      powered: hardpoint.powered ?? null,
      seats: hardpoint.seats ?? null
    } satisfies Record<string, unknown>;

    const existingRow = existingByKey.get(key);
    if (existingRow) {
      toUpdate.push({ id: existingRow.id, ...payload });
      externalToId.set(hardpoint.external_id, existingRow.id);
    } else {
      createdQueue.push({ payload, external: hardpoint.external_id });
    }
  }

  if (createdQueue.length) {
    const created = await createMany('hardpoints', createdQueue.map((entry) => entry.payload));
    created.forEach((row, idx) => {
      const id = row.id as string;
      externalToId.set(createdQueue[idx].external, id);
    });
  }

  if (toUpdate.length) {
    await updateMany('hardpoints', toUpdate);
  }

  const toDelete = Array.from(existingByKey.entries())
    .filter(([key]) => !seen.has(key))
    .map(([, value]) => value.id);

  if (toDelete.length) {
    await deleteMany('hardpoints', toDelete);
  }

  return externalToId;
}

async function syncInstalledItems(
  buildId: string,
  records: Array<{
    ship_variant: string;
    item: string;
    hardpoint: string | null;
    quantity: number;
    build: string;
  }>
) {
  if (!records.length) return;

  const existing = await readByQuery('installed_items', {
    filter: { build: { _eq: buildId } },
    fields: ['id', 'ship_variant', 'item', 'hardpoint'],
    limit: -1
  });

  const existingMap = new Map<string, string>();
  for (const row of existing ?? []) {
    existingMap.set(
      makeInstalledKey(
        row.ship_variant as string,
        row.item as string,
        (row.hardpoint as string | null) ?? null
      ),
      row.id as string
    );
  }

  const seen = new Set<string>();
  const toCreate: typeof records = [];
  const toUpdate: Array<typeof records[number] & { id: string }> = [];

  for (const record of records) {
    const key = makeInstalledKey(record.ship_variant, record.item, record.hardpoint);
    seen.add(key);
    const id = existingMap.get(key);
    if (id) {
      toUpdate.push({ ...record, id });
    } else {
      toCreate.push(record);
    }
  }

  const toDelete = Array.from(existingMap.entries())
    .filter(([key]) => !seen.has(key))
    .map(([, id]) => id);

  if (toCreate.length) {
    await createMany('installed_items', toCreate);
  }
  if (toUpdate.length) {
    await updateMany('installed_items', toUpdate);
  }
  if (toDelete.length) {
    await deleteMany('installed_items', toDelete);
  }
}

export async function loadAll(
  dataRoot: string,
  channel: Channel,
  version: string,
  bundle?: NormalizedDataBundle
): Promise<LoadResult> {
  const normalizedDir = join(dataRoot, 'normalized', channel, version);
  const data = bundle ?? (await loadNormalizedBundle(normalizedDir));
  const metadataFile = await readJsonOrDefault<Record<string, unknown>>(
    join(normalizedDir, 'build.json'),
    {}
  );
  const metadata: BuildMetadata = {
    build_hash: (metadataFile.build_hash as string | null | undefined) ?? undefined,
    released:
      (metadataFile.released as string | null | undefined) ??
      (metadataFile.released_at as string | null | undefined) ??
      undefined,
    status: metadataFile.status as BuildMetadata['status']
  };

  const build = await ensureBuild(channel, version, metadata);
  log.info('Loading data into Directus', { buildId: build.id, channel, version });

  const manufacturerPayload = data.manufacturers.map((manufacturer) => ({
    external_id: manufacturer.external_id,
    code: manufacturer.code,
    name: manufacturer.name,
    content: nullable(manufacturer.description),
    status: 'published'
  }));
  const manufacturerIdMap = await upsertByExternalId('companies', manufacturerPayload);

  const shipPayload = data.ships.map((ship) => ({
    wiki_slug: ship.external_id,
    name: ship.name,
    class: ship.class,
    size: nullable(ship.size),
    description: nullable(ship.description),
    manufacturer: requireId(
      manufacturerIdMap,
      ship.manufacturer_external_id,
      'ships.manufacturer'
    ),
    status: 'published'
  }));
  const shipIdMap = await upsertByExternalId('ships', shipPayload, { key: 'wiki_slug' });

  const itemPayload = data.items.map((item) => ({
    external_id: item.external_id,
    type: item.type,
    subtype: nullable(item.subtype),
    name: item.name,
    manufacturer: item.manufacturer_external_id
      ? requireId(manufacturerIdMap, item.manufacturer_external_id, 'items.manufacturer')
      : null,
    size: item.size ?? null,
    grade: nullable(item.grade),
    class: nullable(item.class),
    description: nullable(item.description)
  }));
  const itemIdMap = await upsertByExternalId('items', itemPayload);

  const variantPayload = data.ship_variants.map((variant) => ({
    external_id: variant.external_id,
    ship: requireId(shipIdMap, variant.ship_external_id, 'ship_variants.ship'),
    variant_code: nullable(variant.variant_code),
    name: nullable(variant.name),
    thumbnail: nullable(variant.thumbnail),
    description: nullable(variant.description),
    patch: build.id,
    status: 'published'
  }));
  const variantIdMap = await upsertByExternalId('ship_variants', variantPayload);

  const hardpointIdMap = await syncHardpoints(data.hardpoints, variantIdMap);

  if (data.locales.length) {
    await upsertLocales(data.locales);
  }

  if (data.item_stats.length) {
    const payload = data.item_stats.map((stat) => ({
      build: build.id,
      item: requireId(itemIdMap, stat.item_external_id, 'item_stats.item'),
      stats: stat.stats,
      price_auec: stat.price_auec ?? null,
      availability: nullable(stat.availability)
    }));
    await syncByKey('item_stats', build.id, 'item', payload);
  }

  if (data.ship_stats.length) {
    const payload = data.ship_stats.map((stat) => ({
      build: build.id,
      ship_variant: requireId(
        variantIdMap,
        stat.ship_variant_external_id,
        'ship_stats.ship_variant'
      ),
      stats: stat.stats
    }));
    await syncByKey('ship_stats', build.id, 'ship_variant', payload);
  }

  if (data.installed_items.length) {
    // ASSUMPTION: Directus schema (see directus-schema.json) currently misses an `item` field on
    // installed_items; we skip loadouts until the column (UUID M2O -> items) exists.
    log.warn(
      'Skipping installed_items load: Directus schema lacks an `item` field on the installed_items collection.'
    );
  }

  const completed = await updateOne<BuildRecord>('game_builds', build.id, {
    status: 'ingested',
    ingested: new Date().toISOString()
  });

  log.info('Load complete', { buildId: completed.id });

  return { build: completed };
}
