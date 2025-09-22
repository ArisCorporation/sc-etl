#!/usr/bin/env tsx
import 'dotenv/config';
import { log } from '../src/utils/log.js';
import { readByQuery, updateMany, deleteMany } from '../src/utils/directus.js';
import {
  buildHullKey,
  canonicalVariantName,
  cleanFamilyName,
  detectEditionOrLivery,
  extractVariantCode,
  isEditionOnly,
  toCanonicalVariantExtId
} from '../src/lib/canon.js';

interface ShipRecord {
  id: string;
  external_id: string;
  name?: string;
  manufacturer?: { id?: string; code?: string; external_id?: string } | string | null;
}

interface ShipVariantRecord {
  id: string;
  external_id: string;
  ship: string | { id: string } | null;
  name?: string | null;
}

interface InstalledItemRecord {
  id: string;
  ship_variant?: string | { id: string } | null;
  hardpoint?: string | { id: string } | null;
  item?: string | { id: string } | null;
  profile?: string | null;
  livery?: string | null;
}

interface ShipStatRecord {
  id: string;
  ship_variant?: string | { id: string } | null;
}

interface HardpointRecord {
  id: string;
  ship_variant?: string | { id: string } | null;
}

function parseArg(flag: string, args: string[]): string | undefined {
  const index = args.indexOf(flag);
  if (index === -1) return undefined;
  return args[index + 1];
}

function normaliseId(value: unknown): string | undefined {
  if (!value) return undefined;
  if (typeof value === 'string') return value;
  if (typeof value === 'object' && value !== null && 'id' in value && typeof (value as any).id === 'string') {
    return (value as any).id;
  }
  return undefined;
}

async function fetchAll<T>(collection: string, baseQuery: Record<string, unknown>): Promise<T[]> {
  const limit = 500;
  let offset = 0;
  const result: T[] = [];
  while (true) {
    const page = await readByQuery<T>(collection, { ...baseQuery, limit, offset });
    if (!page.length) break;
    result.push(...page);
    if (page.length < limit) break;
    offset += limit;
  }
  return result;
}

function chunk<T>(items: readonly T[], size = 100): T[][] {
  const output: T[][] = [];
  for (let i = 0; i < items.length; i += size) {
    output.push(items.slice(i, i + size));
  }
  return output;
}

function manufacturerCodeFromShip(ship: ShipRecord): string {
  if (ship.manufacturer && typeof ship.manufacturer === 'object') {
    if (ship.manufacturer.code) return ship.manufacturer.code;
    if (ship.manufacturer.external_id) return ship.manufacturer.external_id;
  }
  if (typeof ship.manufacturer === 'string') return ship.manufacturer;
  if (ship.external_id.includes('_')) {
    return ship.external_id.split('_')[0];
  }
  return 'UNKNOWN';
}

function canonicalHullForShip(ship: ShipRecord): { hullKey: string; baseName: string } {
  const manufacturer = manufacturerCodeFromShip(ship);
  const variantCode = extractVariantCode(ship.name ?? ship.external_id ?? '');
  const familyName = cleanFamilyName(ship.external_id ?? ship.name ?? 'SHIP', variantCode, manufacturer);
  const hullKey = buildHullKey(manufacturer, familyName);
  const baseName = familyName.replace(/_/g, ' ');
  return { hullKey, baseName };
}

async function main() {
  const args = process.argv.slice(2);
  const hasApply = args.includes('--apply');
  const hasDryRun = args.includes('--dry-run');
  if (hasApply && hasDryRun) {
    throw new Error('Specify either --apply or --dry-run, not both.');
  }
  const dryRun = !hasApply;
  const channel = parseArg('--channel', args) ?? 'LIVE';

  log.info('Variant deduplication started', {
    channel,
    mode: dryRun ? 'dry-run' : 'apply'
  });

  const ships = await fetchAll<ShipRecord>('ships', {
    fields: ['id', 'external_id', 'name', 'manufacturer', 'manufacturer.code', 'manufacturer.external_id']
  });

  if (!ships.length) {
    log.warn('No ships found – aborting.');
    return;
  }

  const hullKeyByShipId = new Map<string, { hullKey: string; baseName: string }>();
  const shipUpdates: Array<{ id: string; external_id: string }> = [];

  for (const ship of ships) {
    if (!ship.id || !ship.external_id) continue;
    const canonical = canonicalHullForShip(ship);
    hullKeyByShipId.set(ship.id, canonical);
    if (ship.external_id !== canonical.hullKey) {
      shipUpdates.push({ id: ship.id, external_id: canonical.hullKey });
    }
  }

  const variants = await fetchAll<ShipVariantRecord>('ship_variants', {
    fields: ['id', 'external_id', 'ship', 'name']
  });

  const canonicalMap = new Map<
    string,
    {
      canonicalId: string;
      hullKey: string;
      baseName: string;
      candidates: Array<{
        id: string;
        external_id: string;
        name?: string | null;
        editionCode?: string;
        livery?: string;
        editionOnly: boolean;
      }>;
    }
  >();

  for (const variant of variants) {
    const variantId = variant.id;
    const variantExt = variant.external_id;
    const shipRef = normaliseId(variant.ship);
    if (!variantId || !shipRef) continue;
    const hullRef = hullKeyByShipId.get(shipRef);
    if (!hullRef) continue;
    const nameSource = variant.name ?? variantExt ?? '';
    const variantCode = extractVariantCode(nameSource || variantExt);
    const canonicalId = toCanonicalVariantExtId(hullRef.hullKey, variantCode);
    const edition = detectEditionOrLivery(nameSource);
    const editionOnly = isEditionOnly(nameSource);

    let entry = canonicalMap.get(canonicalId);
    if (!entry) {
      entry = {
        canonicalId,
        hullKey: hullRef.hullKey,
        baseName: hullRef.baseName,
        candidates: []
      };
      canonicalMap.set(canonicalId, entry);
    }

    entry.candidates.push({
      id: variantId,
      external_id: variantExt,
      name: variant.name,
      editionCode: edition.editionCode,
      livery: edition.livery,
      editionOnly
    });
  }

  const variantMigrationMap = new Map<string, string>();
  const editionMetadata = new Map<string, { profile?: string; livery?: string }>();
  const keeperUpdates: Array<{ id: string; external_id: string; variant_code: string; name?: string | null }> = [];
  const variantsToDelete: string[] = [];
  const mergeLog: Array<{ canonical: string; merged: string[] }> = [];

  for (const entry of canonicalMap.values()) {
    const { canonicalId, candidates, baseName } = entry;
    if (!candidates.length) continue;

    candidates.sort((a, b) => {
      const aExact = a.external_id === canonicalId ? 0 : a.editionOnly ? 2 : 1;
      const bExact = b.external_id === canonicalId ? 0 : b.editionOnly ? 2 : 1;
      if (aExact !== bExact) return aExact - bExact;
      return a.external_id.localeCompare(b.external_id);
    });

    const keeper = candidates[0];
    const duplicates = candidates.slice(1);

    variantMigrationMap.set(keeper.id, canonicalId);
    if (keeper.external_id !== canonicalId) {
      const variantCode = canonicalId.split('_').pop() ?? 'BASE';
      const displayName = canonicalVariantName(baseName, variantCode);
      keeperUpdates.push({
        id: keeper.id,
        external_id: canonicalId,
        variant_code: variantCode,
        name: displayName
      });
    }

    for (const duplicate of duplicates) {
      variantMigrationMap.set(duplicate.id, canonicalId);
      variantsToDelete.push(duplicate.id);
      if (duplicate.editionCode || duplicate.livery) {
        editionMetadata.set(duplicate.id, {
          profile: duplicate.editionCode,
          livery: duplicate.livery ?? null
        });
      }
    }

    mergeLog.push({
      canonical: canonicalId,
      merged: duplicates.map((dup) => dup.external_id)
    });
  }

  const shipStats = await fetchAll<ShipStatRecord>('ship_stats', {
    fields: ['id', 'ship_variant']
  });
  const installedItems = await fetchAll<InstalledItemRecord>('installed_items', {
    fields: ['id', 'ship_variant', 'hardpoint', 'item', 'profile', 'livery']
  });
  const hardpoints = await fetchAll<HardpointRecord>('hardpoints', {
    fields: ['id', 'ship_variant']
  });

  const shipStatUpdates: Array<{ id: string; ship_variant: string }> = [];
  for (const stat of shipStats) {
    const variantRef = normaliseId(stat.ship_variant);
    if (!variantRef) continue;
    const canonical = variantMigrationMap.get(variantRef);
    if (canonical && canonical !== variantRef) {
      shipStatUpdates.push({ id: stat.id, ship_variant: canonical });
    }
  }

  const hardpointUpdates: Array<{ id: string; ship_variant: string }> = [];
  for (const hardpoint of hardpoints) {
    const variantRef = normaliseId(hardpoint.ship_variant);
    if (!variantRef) continue;
    const canonical = variantMigrationMap.get(variantRef);
    if (canonical && canonical !== variantRef) {
      hardpointUpdates.push({ id: hardpoint.id, ship_variant: canonical });
    }
  }

  const installedUpdates: Array<{ id: string; ship_variant: string; profile?: string | null; livery?: string | null }> = [];
  for (const installed of installedItems) {
    const variantRef = normaliseId(installed.ship_variant);
    if (!variantRef) continue;
    const canonical = variantMigrationMap.get(variantRef);
    if (!canonical || canonical === variantRef) continue;
    const edition = editionMetadata.get(variantRef);
    installedUpdates.push({
      id: installed.id,
      ship_variant: canonical,
      profile: edition?.profile ?? installed.profile ?? null,
      livery: edition?.livery ?? installed.livery ?? null
    });
  }

  if (shipUpdates.length) {
    log.info('Ship hull keys to update', shipUpdates.length);
  }
  if (keeperUpdates.length) {
    log.info('Variant records to upsert', keeperUpdates.length);
  }
  if (shipStatUpdates.length) {
    log.info('Ship stats to migrate', shipStatUpdates.length);
  }
  if (hardpointUpdates.length) {
    log.info('Hardpoints to migrate', hardpointUpdates.length);
  }
  if (installedUpdates.length) {
    log.info('Installed items to migrate', installedUpdates.length);
  }
  if (variantsToDelete.length) {
    log.info('Duplicate variants to remove', variantsToDelete.length);
  }

  for (const merge of mergeLog) {
    if (merge.merged.length) {
      log.info('Merged variants', merge);
    }
  }

  if (dryRun) {
    log.info('Dry-run complete. No changes were written. Use --apply to persist updates.');
    return;
  }

  for (const batch of chunk(shipUpdates)) {
    await updateMany('ships', batch);
  }
  for (const batch of chunk(keeperUpdates)) {
    await updateMany('ship_variants', batch);
  }
  for (const batch of chunk(shipStatUpdates)) {
    await updateMany('ship_stats', batch);
  }
  for (const batch of chunk(hardpointUpdates)) {
    await updateMany('hardpoints', batch);
  }
  for (const batch of chunk(installedUpdates)) {
    await updateMany('installed_items', batch);
  }
  if (variantsToDelete.length) {
    for (const batch of chunk(variantsToDelete)) {
      await deleteMany('ship_variants', batch);
    }
  }

  log.info('Variant deduplication finished');
}

main().catch((error) => {
  log.error('Migration failed', error);
  process.exitCode = 1;
});
