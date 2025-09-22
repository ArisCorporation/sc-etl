#!/usr/bin/env tsx
import 'dotenv/config';
import { log } from '../src/utils/log.js';
import { readByQuery, updateMany, deleteMany } from '../src/utils/directus.js';

interface ManufacturerRecord {
  id: string;
  code?: string | null;
  external_id?: string | null;
  name?: string | null;
  content?: string | null;
}

interface ReferenceRecord {
  id: string;
  manufacturer?: string | { id?: string } | null;
}

// ASSUMPTION: `ships.manufacturer` and `items.manufacturer` store Directus relational ids pointing to `companies`.

function normalizeCode(raw: unknown): string | undefined {
  if (raw === undefined || raw === null) return undefined;
  const text = String(raw).trim();
  if (!text) return undefined;
  return text.toUpperCase();
}

function normaliseId(value: unknown): string | undefined {
  if (!value) return undefined;
  if (typeof value === 'string' && value.trim()) return value;
  if (typeof value === 'object' && 'id' in (value as Record<string, unknown>)) {
    const id = (value as { id?: unknown }).id;
    if (typeof id === 'string') return id;
  }
  return undefined;
}

function parseArgs() {
  const args = process.argv.slice(2);
  const hasApply = args.includes('--apply');
  const hasDryRun = args.includes('--dry-run');
  if (hasApply && hasDryRun) {
    throw new Error('Specify either --apply or --dry-run, not both.');
  }
  return {
    dryRun: !hasApply,
    args
  };
}

async function fetchAll<T>(collection: string, baseQuery: Record<string, unknown>): Promise<T[]> {
  const limit = 200;
  let offset = 0;
  const rows: T[] = [];
  while (true) {
    const batch = await readByQuery<T>(collection, { ...baseQuery, limit, offset });
    if (!batch.length) break;
    rows.push(...batch);
    if (batch.length < limit) break;
    offset += limit;
  }
  return rows;
}

function chunk<T>(items: readonly T[], size = 100): T[][] {
  const buckets: T[][] = [];
  for (let i = 0; i < items.length; i += size) {
    buckets.push(items.slice(i, i + size));
  }
  return buckets;
}

async function main() {
  const { dryRun } = parseArgs();
  log.info('Manufacturer migration started', { mode: dryRun ? 'dry-run' : 'apply' });

  const manufacturers = await fetchAll<ManufacturerRecord>('companies', {
    fields: ['id', 'code', 'external_id', 'name', 'content']
  });

  if (!manufacturers.length) {
    log.warn('No manufacturers found – nothing to migrate.');
    return;
  }

  const groups = new Map<string, ManufacturerRecord[]>();
  for (const manufacturer of manufacturers) {
    const normalized =
      normalizeCode(manufacturer.code) ??
      normalizeCode(manufacturer.external_id) ??
      normalizeCode(manufacturer.name) ??
      normalizeCode(manufacturer.id);
    if (!normalized) {
      log.warn('Manufacturer missing code and fallback; skipping', { id: manufacturer.id });
      continue;
    }
    const bucket = groups.get(normalized) ?? [];
    bucket.push(manufacturer);
    groups.set(normalized, bucket);
  }

  const canonicalUpdates = new Map<string, { id: string; code?: string; external_id?: string }>();
  const duplicateMap = new Map<string, string>();
  const canonicalById = new Map<string, { code: string; record: ManufacturerRecord }>();

  for (const [code, records] of groups) {
    if (!records.length) continue;
    const sorted = [...records].sort((a, b) => {
      const aScore =
        (normalizeCode(a.code) === code ? 0 : 2) + (normalizeCode(a.external_id) === code ? 0 : 1);
      const bScore =
        (normalizeCode(b.code) === code ? 0 : 2) + (normalizeCode(b.external_id) === code ? 0 : 1);
      if (aScore !== bScore) return aScore - bScore;
      return a.id.localeCompare(b.id);
    });
    const [canonical, ...duplicates] = sorted;
    canonicalById.set(canonical.id, { code, record: canonical });

    const existing = canonicalUpdates.get(canonical.id) ?? { id: canonical.id };
    if (normalizeCode(canonical.code) !== code) {
      existing.code = code;
    }
    if (normalizeCode(canonical.external_id) !== code) {
      existing.external_id = code;
    }
    if (existing.code || existing.external_id) {
      canonicalUpdates.set(canonical.id, existing);
    }

    for (const duplicate of duplicates) {
      duplicateMap.set(duplicate.id, canonical.id);
    }
  }

  if (!duplicateMap.size && canonicalUpdates.size === 0) {
    log.info('All manufacturers already normalized.');
    return;
  }

  const duplicates = Array.from(duplicateMap.keys());

  const shipRefs = duplicates.length
    ? await fetchAll<ReferenceRecord>('ships', {
        fields: ['id', 'manufacturer', 'manufacturer.id'],
        filter: { manufacturer: { _in: duplicates } }
      })
    : [];
  const itemRefs = duplicates.length
    ? await fetchAll<ReferenceRecord>('items', {
        fields: ['id', 'manufacturer', 'manufacturer.id'],
        filter: { manufacturer: { _in: duplicates } }
      })
    : [];

  const shipUpdates: Array<{ id: string; manufacturer: string }> = [];
  for (const ship of shipRefs) {
    const current = normaliseId(ship.manufacturer);
    if (!current) continue;
    const target = duplicateMap.get(current);
    if (target && target !== current) {
      shipUpdates.push({ id: ship.id, manufacturer: target });
    }
  }

  const itemUpdates: Array<{ id: string; manufacturer: string | null }> = [];
  for (const item of itemRefs) {
    const current = normaliseId(item.manufacturer);
    if (!current) continue;
    const target = duplicateMap.get(current);
    if (target && target !== current) {
      itemUpdates.push({ id: item.id, manufacturer: target });
    }
  }

  log.info('Proposed manufacturer updates', {
    canonicalUpdates: canonicalUpdates.size,
    duplicates: duplicates.length,
    shipUpdates: shipUpdates.length,
    itemUpdates: itemUpdates.length
  });

  if (dryRun) {
    log.info('Dry-run complete. No changes were written. Use --apply to persist updates.');
    return;
  }

  if (canonicalUpdates.size) {
    for (const batch of chunk(Array.from(canonicalUpdates.values()))) {
      await updateMany('companies', batch);
    }
  }

  if (shipUpdates.length) {
    for (const batch of chunk(shipUpdates)) {
      await updateMany('ships', batch);
    }
  }

  if (itemUpdates.length) {
    for (const batch of chunk(itemUpdates)) {
      await updateMany('items', batch);
    }
  }

  if (duplicates.length) {
    for (const batch of chunk(duplicates)) {
      await deleteMany('companies', batch);
    }
  }

  log.info('Manufacturer migration finished', {
    normalized: canonicalUpdates.size,
    removedDuplicates: duplicates.length
  });
}

main().catch(async (error) => {
  log.error('Manufacturer migration failed', error);
  process.exitCode = 1;
});
