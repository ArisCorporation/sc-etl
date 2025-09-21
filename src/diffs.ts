import { isDeepStrictEqual } from 'node:util';
import { createMany, deleteMany, readByQuery } from './utils/directus.js';
import { log } from './utils/log.js';
import type {
  Channel,
  NormalizedDataBundle,
  NormalizedInstalledItem,
  NormalizedItemStat,
  NormalizedShipStat
} from './types/index.js';

interface DiffRow extends Record<string, unknown> {
  from_build: string | null;
  to_build: string;
  entity_type: 'item_stats' | 'ship_stats' | 'installed_items';
  entity_external_id: string;
  change_type: 'added' | 'removed' | 'modified';
  change_json: Record<string, unknown>;
}

function chunk<T>(items: readonly T[], size: number): T[][] {
  if (size <= 0) throw new Error('chunk size must be greater than zero');
  const result: T[][] = [];
  for (let i = 0; i < items.length; i += size) {
    result.push(items.slice(i, i + size));
  }
  return result;
}

function buildItemStatsMap(stats: NormalizedItemStat[]) {
  return new Map(stats.map((entry) => [entry.item_external_id, entry]));
}

function buildShipStatsMap(stats: NormalizedShipStat[]) {
  return new Map(stats.map((entry) => [entry.ship_variant_external_id, entry]));
}

function installedKey(entry: NormalizedInstalledItem) {
  return `${entry.ship_variant_external_id}:${entry.item_external_id}:${entry.hardpoint_external_id ?? ''}`;
}

function buildInstalledMap(entries: NormalizedInstalledItem[]) {
  return new Map(entries.map((entry) => [installedKey(entry), entry]));
}

function diffPayload(
  before: Record<string, unknown> | undefined,
  after: Record<string, unknown> | undefined,
  fields: string[]
): Record<string, unknown> | null {
  if (!before) {
    return { after };
  }
  if (!after) {
    return { before };
  }
  const changes: Record<string, unknown> = {};
  for (const field of fields) {
    if (!isDeepStrictEqual(before[field], after[field])) {
      changes[field] = { before: before[field] ?? null, after: after[field] ?? null };
    }
  }
  return Object.keys(changes).length ? changes : null;
}

async function fetchPreviousBuild(channel: Channel, currentBuildId: string) {
  const result = await readByQuery<{ id: string }>('game_builds', {
    filter: {
      channel: { _eq: channel },
      status: { _eq: 'ingested' },
      id: { _neq: currentBuildId }
    },
    sort: ['-released', '-ingested'],
    fields: ['id'],
    limit: 1
  });
  return result[0];
}

async function fetchPreviousItemStats(previousBuildId: string) {
  const result = await readByQuery('item_stats', {
    filter: { build: { _eq: previousBuildId } },
    fields: ['item.external_id', 'stats', 'price_auec', 'availability'],
    limit: -1
  });
  const map = new Map<string, NormalizedItemStat>();
  for (const row of result ?? []) {
    const item = (row.item as { external_id?: string } | undefined)?.external_id;
    if (typeof item === 'string') {
      map.set(item, {
        item_external_id: item,
        stats: row.stats as Record<string, unknown>,
        price_auec: row.price_auec as number | undefined,
        availability: row.availability as string | undefined
      });
    }
  }
  return map;
}

async function fetchPreviousShipStats(previousBuildId: string) {
  const result = await readByQuery('ship_stats', {
    filter: { build: { _eq: previousBuildId } },
    fields: ['ship_variant.external_id', 'stats'],
    limit: -1
  });
  const map = new Map<string, NormalizedShipStat>();
  for (const row of result ?? []) {
    const externalId = (row.ship_variant as { external_id?: string } | undefined)?.external_id;
    if (typeof externalId === 'string') {
      map.set(externalId, {
        ship_variant_external_id: externalId,
        stats: row.stats as Record<string, unknown>
      });
    }
  }
  return map;
}

async function fetchPreviousInstalled(previousBuildId: string) {
  const result = await readByQuery('installed_items', {
    filter: { build: { _eq: previousBuildId } },
    fields: ['ship_variant.external_id', 'item.external_id', 'hardpoint.external_id', 'quantity'],
    limit: -1
  });
  const entries: NormalizedInstalledItem[] = [];
  for (const row of result ?? []) {
    const shipVariant = (row.ship_variant as { external_id?: string } | undefined)?.external_id;
    const item = (row.item as { external_id?: string } | undefined)?.external_id;
    if (!shipVariant || !item) continue;
    const hardpoint = (row.hardpoint as { external_id?: string } | undefined)?.external_id;
    entries.push({
      ship_variant_external_id: shipVariant,
      item_external_id: item,
      quantity: (row.quantity as number | undefined) ?? 1,
      hardpoint_external_id: hardpoint ?? undefined
    });
  }
  return buildInstalledMap(entries);
}

function diffItemStats(
  previous: Map<string, NormalizedItemStat>,
  next: Map<string, NormalizedItemStat>,
  fromBuild: string,
  toBuild: string
): DiffRow[] {
  const diffs: DiffRow[] = [];
  const keys = new Set([...previous.keys(), ...next.keys()]);
  for (const key of keys) {
    const before = previous.get(key);
    const after = next.get(key);
    const payload = diffPayload(before as any, after as any, ['stats', 'price_auec', 'availability']);
    if (!payload) continue;
    const changeType: DiffRow['change_type'] = before && after ? 'modified' : before ? 'removed' : 'added';
    diffs.push({
      from_build: before ? fromBuild : null,
      to_build: toBuild,
      entity_type: 'item_stats',
      entity_external_id: key,
      change_type: changeType,
      change_json: payload
    });
  }
  return diffs;
}

function diffShipStats(
  previous: Map<string, NormalizedShipStat>,
  next: Map<string, NormalizedShipStat>,
  fromBuild: string,
  toBuild: string
): DiffRow[] {
  const diffs: DiffRow[] = [];
  const keys = new Set([...previous.keys(), ...next.keys()]);
  for (const key of keys) {
    const before = previous.get(key);
    const after = next.get(key);
    const payload = diffPayload(before as any, after as any, ['stats']);
    if (!payload) continue;
    const changeType: DiffRow['change_type'] = before && after ? 'modified' : before ? 'removed' : 'added';
    diffs.push({
      from_build: before ? fromBuild : null,
      to_build: toBuild,
      entity_type: 'ship_stats',
      entity_external_id: key,
      change_type: changeType,
      change_json: payload
    });
  }
  return diffs;
}

function diffInstalledItems(
  previous: Map<string, NormalizedInstalledItem>,
  next: Map<string, NormalizedInstalledItem>,
  fromBuild: string,
  toBuild: string
): DiffRow[] {
  const diffs: DiffRow[] = [];
  const keys = new Set([...previous.keys(), ...next.keys()]);
  for (const key of keys) {
    const before = previous.get(key);
    const after = next.get(key);
    const payload = diffPayload(before as any, after as any, ['quantity']);
    if (!payload) continue;
    const changeType: DiffRow['change_type'] = before && after ? 'modified' : before ? 'removed' : 'added';
    diffs.push({
      from_build: before ? fromBuild : null,
      to_build: toBuild,
      entity_type: 'installed_items',
      entity_external_id: key,
      change_type: changeType,
      change_json: payload
    });
  }
  return diffs;
}

export interface WriteDiffsOptions {
  channel: Channel;
  currentBuildId: string;
  bundle: NormalizedDataBundle;
}

export async function writeDiffs({ channel, currentBuildId, bundle }: WriteDiffsOptions) {
  const previousBuild = await fetchPreviousBuild(channel, currentBuildId);
  if (!previousBuild) {
    log.info('No previous build found for diff generation', { channel });
    return;
  }

  const previousItemStats = await fetchPreviousItemStats(previousBuild.id);
  const previousShipStats = await fetchPreviousShipStats(previousBuild.id);
  const previousInstalled = await fetchPreviousInstalled(previousBuild.id);

  const nextItemStats = buildItemStatsMap(bundle.item_stats);
  const nextShipStats = buildShipStatsMap(bundle.ship_stats);
  const nextInstalled = buildInstalledMap(bundle.installed_items);

  const diffRows: DiffRow[] = [
    ...diffItemStats(previousItemStats, nextItemStats, previousBuild.id, currentBuildId),
    ...diffShipStats(previousShipStats, nextShipStats, previousBuild.id, currentBuildId),
    ...diffInstalledItems(previousInstalled, nextInstalled, previousBuild.id, currentBuildId)
  ];

  const existing = await readByQuery('diffs', {
    filter: { from_build: { _eq: previousBuild.id }, to_build: { _eq: currentBuildId } },
    fields: ['id'],
    limit: -1
  });
  const existingIds = (existing ?? []).map((row) => row.id as string);
  if (existingIds.length) {
    await deleteMany('diffs', existingIds);
  }

  if (!diffRows.length) {
    log.info('No diffs detected', { previous: previousBuild.id, current: currentBuildId });
    return;
  }

  for (const batch of chunk(diffRows, 200)) {
    await createMany('diffs', batch);
  }

  log.info('Diffs written', { differences: diffRows.length, from: previousBuild.id, to: currentBuildId });
}
