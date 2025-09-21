import { join, relative } from 'node:path';
import fg from 'fast-glob';
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
import { pathExists, readJson, readJsonOrDefault, writeJson } from './utils/fs.js';
import { log } from './utils/log.js';
import {
  buildHullKey,
  canonicalVariantName,
  cleanFamilyName,
  detectEditionOrLivery,
  extractVariantCode,
  isEditionOnly,
  toCanonicalVariantExtId
} from './lib/canon.js';

const ITEM_CLASS_MAX_LENGTH = 10;
const ITEM_CLASS_PREFIXES = new Set(['char', 'item', 'ship', 'vehicle']);

const SHIP_CLASSIFICATION_PREFIXES = new Set(['ship', 'vehicle']);
const SHIP_ITEM_TYPE_ALLOW = new Set([
  'AIMODULE',
  'AMMOBOX',
  'ARMOR',
  'ATTACHEDPART',
  'BOMB',
  'BOMBLAUNCHER',
  'CAPACITORASSIGNMENTCONTROLLER',
  'CARGO',
  'CARGOGRID',
  'COMMSCONTROLLER',
  'CONTAINER',
  'CONTROLPANEL',
  'COOLER',
  'COOLERCONTROLLER',
  'DOCKINGANIMATOR',
  'DOCKINGCOLLAR',
  'EMP',
  'ENERGYCONTROLLER',
  'EXTERNALFUELTANK',
  'FLIGHTCONTROLLER',
  'FUELCONTROLLER',
  'FUELINTAKE',
  'FUELTANK',
  'GRAVITYGENERATOR',
  'JUMPDRIVE',
  'LANDINGSYSTEM',
  'LIFESUPPORTGENERATOR',
  'LIFESUPPORTVENT',
  'MAINTHRUSTER',
  'MANNEUVERTHRUSTER',
  'MININGCONTROLLER',
  'MININGMODIFIER',
  'MISSILE',
  'MISSILECONTROLLER',
  'MISSILELAUNCHER',
  'MODULE',
  'PAINTS',
  'POWERPLANT',
  'QUANTUMDRIVE',
  'QUANTUMFUELTANK',
  'QUANTUMINTERDICTIONGENERATOR',
  'RADAR',
  'RELAY',
  'REMOTECONNECTION',
  'SALVAGECONTROLLER',
  'SALVAGEFIELDEMITTER',
  'SALVAGEFIELDSUPPORTER',
  'SALVAGEFILLERSTATION',
  'SALVAGEHEAD',
  'SALVAGEINTERNALSTORAGE',
  'SALVAGEMODIFIER',
  'SCANNER',
  'SELFDESTRUCT',
  'SENSOR',
  'SHIELD',
  'SHIELDCONTROLLER',
  'SPACEMINE',
  'STATUSSCREEN',
  'TARGETSELECTOR',
  'TOOLARM',
  'TOWINGBEAM',
  'TRACTORBEAM',
  'TRANSPONDER',
  'TURRET',
  'TURRETBASE',
  'UTILITYTURRET',
  'WEAPONATTACHMENT',
  'WEAPONCONTROLLER',
  'WEAPONDEFENSIVE',
  'WEAPONGUN',
  'WEAPONMINING',
  'WHEELEDCONTROLLER'
]);

// ASSUMPTION: Raw structures follow the stable identifiers exposed under the `id` field.
interface RawManufacturer {
  id?: string | number;
  code?: string;
  Code?: string;
  name?: string;
  Name?: string;
  description?: string;
  Description?: string;
  data_source?: string;
  reference?: string;
  Reference?: string;
}

// ASSUMPTION: Ships reference manufacturers via `manufacturer_id` or nested `manufacturer.id`.
interface RawShip {
  id?: string | number;
  UUID?: string;
  ClassName?: string;
  name?: string;
  Name?: string;
  class?: string;
  Class?: string;
  Role?: string;
  Career?: string;
  size?: string | number;
  Size?: string | number;
  description?: string;
  Description?: string;
  manufacturer_id?: string | number;
  manufacturer?: { id?: string | number; code?: string; Code?: string };
  Manufacturer?: { Code?: string; Name?: string };
  FlightCharacteristics?: Record<string, unknown>;
  Propulsion?: Record<string, unknown>;
  QuantumTravel?: Record<string, unknown>;
  Insurance?: Record<string, unknown>;
  Cargo?: number | string;
  CargoGrids?: unknown;
  Health?: number | string;
  Crew?: number | string;
  Mass?: number | string;
  DamageBeforeDestruction?: Record<string, unknown>;
  DamageBeforeDetach?: Record<string, unknown>;
  ShieldFaceType?: string;
  Width?: number | string;
  Length?: number | string;
  Height?: number | string;
}

// ASSUMPTION: Variants reference parent ship via `ship_id`.
interface RawVariant {
  id: string | number;
  ship_id: string | number;
  variant_code?: string;
  name?: string;
  thumbnail?: string;
  description?: string;
}

// ASSUMPTION: Hardpoints reference variants via `ship_variant_id`.
interface RawHardpoint {
  id?: string | number;
  ship_variant_id: string | number;
  code: string;
  category: string;
  position?: string;
  size?: string | number;
  gimballed?: boolean;
  powered?: boolean;
  seats?: number;
}

// ASSUMPTION: Items may include manufacturer linkage and optional numeric attributes.
interface RawItem {
  id?: string | number;
  reference?: string;
  className?: string;
  itemName?: string;
  type?: string;
  Type?: string;
  subtype?: string;
  subType?: string;
  name?: string;
  Name?: string;
  manufacturer_id?: string | number;
  manufacturer?: string;
  size?: string | number;
  grade?: string;
  class?: string;
  description?: string;
  stdItem?: {
    UUID?: string;
    Name?: string;
    Description?: string;
    Type?: string;
    Size?: string | number;
    Grade?: string | number;
    Manufacturer?: { Code?: string; Name?: string };
  };
}

// ASSUMPTION: Stats payload is opaque JSON coming from the game data export.
interface RawItemStat {
  item_id: string | number;
  stats: Record<string, unknown>;
  price_auec?: number;
  availability?: string;
}

interface RawShipStat {
  ship_variant_id: string | number;
  stats: Record<string, unknown>;
}

interface RawInstalledItem {
  ship_variant_id: string | number;
  item_id: string | number;
  quantity?: number;
  hardpoint_id?: string | number;
}

interface RawShipLoadoutEntry {
  portName?: string;
  className?: string;
  classReference?: string;
  entries?: RawShipLoadoutEntry[];
  Item?: {
    __ref?: string;
    className?: string;
    classReference?: string;
    Components?: {
      SAttachableComponentParams?: {
        AttachDef?: {
          Type?: string;
          SubType?: string;
          Size?: string | number;
          Tags?: string | string[];
        };
      };
    };
  };
}

interface RawShipSupplement {
  Loadout?: RawShipLoadoutEntry[];
  ScVehicle?: RawShip;
}

interface ShipRecord {
  externalId: string;
  ship: RawShip;
  loadout: RawShipLoadoutEntry[];
}

interface VariantLoadoutRecord {
  variantId: string;
  record: ShipRecord;
  profile?: string;
  livery?: string;
  isEditionOnly: boolean;
}

interface CanonicalVariantGroup {
  variantId: string;
  hullKey: string;
  variantCode: string;
  baseName: string;
  names: Set<string>;
  descriptions: Set<string>;
  records: VariantLoadoutRecord[];
}

function asExternalId(value: string | number | undefined): string {
  if (value === undefined || value === null) {
    throw new Error('Missing external identifier in raw payload.');
  }
  return String(value);
}

function optionalString(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim() !== '' ? value : undefined;
}

function optionalNumber(value: unknown): number | undefined {
  if (value === undefined || value === null) return undefined;
  const num = Number(value);
  return Number.isFinite(num) ? num : undefined;
}

function coalesce<T>(...values: (T | null | undefined)[]): T | undefined {
  for (const value of values) {
    if (value !== undefined && value !== null) return value;
  }
  return undefined;
}

function sortByExternalId<T extends { external_id: string }>(items: T[]): T[] {
  return [...items].sort((a, b) => a.external_id.localeCompare(b.external_id));
}

function sortInstalledItems(items: NormalizedInstalledItem[]): NormalizedInstalledItem[] {
  return [...items].sort((a, b) =>
    `${a.ship_variant_external_id}:${a.item_external_id}:${a.hardpoint_external_id ?? ''}:${a.profile ?? ''}:${a.livery ?? ''}`.localeCompare(
      `${b.ship_variant_external_id}:${b.item_external_id}:${b.hardpoint_external_id ?? ''}:${b.profile ?? ''}:${b.livery ?? ''}`
    )
  );
}

function sortLocales(entries: NormalizedLocaleEntry[]): NormalizedLocaleEntry[] {
  return [...entries].sort((a, b) =>
    `${a.namespace}:${a.key}:${a.lang}`.localeCompare(`${b.namespace}:${b.key}:${b.lang}`)
  );
}

function makeLocaleEntries(
  filePath: string,
  lang: string,
  payload: Record<string, unknown>
): NormalizedLocaleEntry[] {
  const entries: NormalizedLocaleEntry[] = [];

  const stack: Array<{ prefix: string; value: unknown }> = Object.entries(payload).map(
    ([key, value]) => ({ prefix: key, value })
  );

  while (stack.length) {
    const { prefix, value } = stack.pop()!;

    if (typeof value === 'string') {
      const [namespace, ...rest] = prefix.split('.');
      const key = rest.length ? rest.join('.') : namespace;
      const ns = rest.length ? namespace : 'default';
      entries.push({ namespace: ns, key, lang, value });
      continue;
    }

    if (value && typeof value === 'object') {
      for (const [childKey, childValue] of Object.entries(value)) {
        stack.push({ prefix: `${prefix}.${childKey}`, value: childValue });
      }
    }
  }

  if (!entries.length) {
    log.warn('Locales file contained no string entries', { file: filePath });
  }

  return entries;
}

function getItemClassSegments(value: string): string[] {
  return value
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .replace(/([A-Z]+)([A-Z][a-z])/g, '$1_$2')
    .split(/[_\.]+/)
    .filter(Boolean);
}

function dropKnownClassPrefix(segments: string[]): string[] {
  if (!segments.length) return segments;
  const [head, ...rest] = segments;
  if (ITEM_CLASS_PREFIXES.has(head.toLowerCase())) {
    return rest;
  }
  return segments;
}

function getItemClassification(item: RawItem): string | undefined {
  const raw = (item as any).classification ?? (item as any).Classification;
  if (Array.isArray(raw)) {
    for (const entry of raw) {
      const value = optionalString(entry);
      if (value) return value;
    }
    return undefined;
  }
  const std = optionalString((item.stdItem as any)?.Classification);
  return optionalString(raw) ?? std;
}

function normalizeTypeToken(value: string | undefined): string | undefined {
  if (!value) return undefined;
  return value.replace(/[^a-z0-9]/gi, '').toUpperCase();
}

function getItemBaseType(item: RawItem): string | undefined {
  const direct = optionalString(item.type);
  if (direct) return direct;
  const std = optionalString(item.stdItem?.Type);
  if (std) return std.split('.')[0];
  return undefined;
}

function isShipRelevantItem(item: RawItem): boolean {
  const classification = getItemClassification(item);
  if (classification) {
    const prefix = classification.split('.')[0]?.toLowerCase();
    if (prefix && SHIP_CLASSIFICATION_PREFIXES.has(prefix)) {
      return true;
    }
  }

  const baseType = normalizeTypeToken(getItemBaseType(item));
  if (!baseType) return false;

  if (SHIP_ITEM_TYPE_ALLOW.has(baseType)) return true;

  if (baseType.startsWith('WEAPON') && baseType !== 'WEAPONPERSONAL') return true;
  if (baseType.endsWith('TURRET')) return true;
  if (baseType.endsWith('THRUSTER')) return true;
  if (baseType.endsWith('CONTROLLER') && baseType !== 'DOORCONTROLLER' && baseType !== 'LIGHTCONTROLLER') return true;

  return false;
}

function pickItemClassCandidate(segments: string[]): string | undefined {
  if (!segments.length) return undefined;

  let best: { value: string; score: number } | undefined;

  const consider = (slice: string[], joiner: string, endsAtLast: boolean) => {
    if (!slice.length) return;
    if (!slice.some((part) => /[a-zA-Z]/.test(part))) return;
    const candidate = slice.join(joiner);
    if (!candidate || candidate.length > ITEM_CLASS_MAX_LENGTH) return;
    const score = (endsAtLast ? ITEM_CLASS_MAX_LENGTH : 0) + candidate.length;
    if (!best || score > best.score) {
      best = { value: candidate, score };
    }
  };

  for (let start = 0; start < segments.length; start++) {
    for (let end = start + 1; end <= segments.length; end++) {
      const slice = segments.slice(start, end);
      const endsAtLast = end === segments.length;
      consider(slice, ' ', endsAtLast);
      if (slice.length > 1) {
        consider(slice, '', endsAtLast);
      }
    }
  }

  return best?.value;
}

function normalizeItemClass(raw: string | undefined): string | undefined {
  if (!raw) return undefined;
  const trimmed = raw.trim();
  if (!trimmed) return undefined;
  if (trimmed.length <= ITEM_CLASS_MAX_LENGTH) return trimmed;

  const segments = getItemClassSegments(trimmed);
  const withoutPrefix = dropKnownClassPrefix(segments);
  const effectiveSegments = withoutPrefix.length ? withoutPrefix : segments;
  const candidate = pickItemClassCandidate(effectiveSegments);
  if (candidate) {
    return candidate;
  }
  return trimmed.slice(0, ITEM_CLASS_MAX_LENGTH);
}

async function readShipRecords(rawDir: string): Promise<ShipRecord[]> {
  const aggregatedShips = await readJsonOrDefault<RawShip[]>(join(rawDir, 'ships.json'), []);
  const map = new Map<string, ShipRecord>();

  for (const ship of aggregatedShips) {
    const idSource = coalesce(ship.id, ship.UUID, ship.ClassName, ship.Name, (ship as any)?.name);
    if (!idSource) continue;
    const externalId = asExternalId(idSource);
    map.set(externalId, { externalId, ship, loadout: [] });
  }

  const shipDir = join(rawDir, 'ships');
  if (await pathExists(shipDir)) {
    const files = await fg('ships/*.json', { cwd: rawDir });
    for (const file of files) {
      if (file.endsWith('-raw.json')) continue;
      const absolute = join(rawDir, file);
      let base: RawShip;
      try {
        base = await readJson<RawShip>(absolute);
      } catch (error) {
        log.warn('Failed to read ship file', { file, error });
        continue;
      }

      let loadout: RawShipLoadoutEntry[] = [];
      const rawPath = file.replace(/\.json$/i, '-raw.json');
      const rawAbsolute = join(rawDir, rawPath);
      if (await pathExists(rawAbsolute)) {
        try {
          const supplement = await readJson<RawShipSupplement>(rawAbsolute);
          if (supplement.ScVehicle) {
            base = { ...(supplement.ScVehicle as RawShip), ...base };
          }
          if (Array.isArray(supplement.Loadout)) {
            loadout = supplement.Loadout;
          }
        } catch (error) {
          log.warn('Failed to read ship raw file', { file: rawPath, error });
        }
      }

      const idSource = coalesce(
        (base as any)?.id,
        base.UUID,
        base.ClassName,
        base.Name,
        (base as any)?.name,
        file.replace(/^ships\//, '').replace(/\.json$/i, '')
      );
      if (!idSource) {
        log.warn('Ship file missing identifier', { file });
        continue;
      }

      const externalId = asExternalId(idSource);
      const existing = map.get(externalId);
      if (existing) {
        existing.ship = { ...existing.ship, ...base };
        if (loadout.length) existing.loadout = loadout;
      } else {
        map.set(externalId, { externalId, ship: base, loadout });
      }
    }
  }

  return [...map.values()];
}

function deriveVariantCode(className: string | undefined, fallbackName?: string): string | undefined {
  const normalized = optionalString(className);
  if (!normalized) return optionalString(fallbackName);
  const segments = normalized.split('_').filter(Boolean);
  if (segments.length <= 1) return normalized;
  const withoutManufacturer = segments.slice(1);
  const candidate = withoutManufacturer.join(' ');
  return candidate || normalized;
}

function getPortName(entry: RawShipLoadoutEntry, index: number): string {
  return (
    optionalString(entry.portName) ??
    optionalString(entry.className) ??
    optionalString(entry.classReference) ??
    `slot_${index}`
  );
}

function extractAttachCategory(entry: RawShipLoadoutEntry): string {
  const attachDef = entry.Item?.Components?.SAttachableComponentParams?.AttachDef;
  return (
    optionalString(attachDef?.Type) ??
    optionalString(attachDef?.SubType) ??
    optionalString(entry.className) ??
    'Unknown'
  );
}

function extractAttachSize(entry: RawShipLoadoutEntry): number | undefined {
  const attachDef = entry.Item?.Components?.SAttachableComponentParams?.AttachDef;
  return optionalNumber(attachDef?.Size);
}

function extractItemReference(entry: RawShipLoadoutEntry): string | undefined {
  const candidates = [entry.Item?.__ref, entry.Item?.classReference, entry.classReference];
  for (const candidate of candidates) {
    if (typeof candidate !== 'string') continue;
    const value = candidate.trim();
    if (!value) continue;
    if (/^0{8}-0{4}-0{4}-0{4}-0{12}$/i.test(value)) continue;
    return value;
  }
  return undefined;
}

function buildLoadoutData(
  records: VariantLoadoutRecord[],
  itemIds: Set<string>,
  ignoredItemIds: Set<string>
) {
  if (!records.length) {
    return {
      hardpoints: [],
      installedItems: [],
      missingItems: [] as { shipVariant: string; item: string }[]
    };
  }

  const hardpointMap = new Map<string, NormalizedHardpoint>();
  const installMap = new Map<
    string,
    {
      shipVariant: string;
      itemId: string;
      hardpointExternalId?: string;
      quantity: number;
      profile?: string;
      livery?: string;
    }
  >();
  const missingInstall = new Map<string, { shipVariant: string; item: string }>();

  for (const recordEntry of records) {
    const { variantId, record, profile, livery } = recordEntry;
    if (!record.loadout.length) continue;

    let index = 0;
    const stack = record.loadout.map((entry) => ({ entry, path: [] as string[], profile, livery }));

    while (stack.length) {
      const { entry, path, profile: currentProfile, livery: currentLivery } = stack.pop()!;
      const portName = getPortName(entry, index++);
      const nextPath = [...path, portName];
      const hardpointExternalId = `${variantId}:${nextPath.join('/')}`;

      if (!hardpointMap.has(hardpointExternalId)) {
        hardpointMap.set(hardpointExternalId, {
          external_id: hardpointExternalId,
          ship_variant_external_id: variantId,
          code: portName,
          category: extractAttachCategory(entry),
          position: undefined,
          size: extractAttachSize(entry),
          gimballed: undefined,
          powered: undefined,
          seats: undefined
        });
      }

      const itemRef = extractItemReference(entry);
      if (itemRef) {
        const itemId = asExternalId(itemRef);
        if (itemIds.has(itemId)) {
          const installKey = `${variantId}|${itemId}|${hardpointExternalId}|${currentProfile ?? ''}|${currentLivery ?? ''}`;
          const existing = installMap.get(installKey);
          if (existing) {
            existing.quantity += 1;
          } else {
            installMap.set(installKey, {
              shipVariant: variantId,
              itemId,
              hardpointExternalId,
              quantity: 1,
              profile: currentProfile,
              livery: currentLivery
            });
          }
        } else if (!ignoredItemIds.has(itemId)) {
          const missKey = `${variantId}|${itemId}`;
          if (!missingInstall.has(missKey)) {
            missingInstall.set(missKey, { shipVariant: variantId, item: itemId });
          }
        }
      }

      if (Array.isArray(entry.entries) && entry.entries.length) {
        for (const child of entry.entries) {
          stack.push({ entry: child, path: nextPath, profile: currentProfile, livery: currentLivery });
        }
      }
    }
  }

  const hardpoints = sortByExternalId([...hardpointMap.values()]);
  const installedItems = sortInstalledItems(
    [...installMap.values()].map((data) => ({
      ship_variant_external_id: data.shipVariant,
      item_external_id: data.itemId,
      quantity: data.quantity,
      hardpoint_external_id: data.hardpointExternalId,
      profile: data.profile,
      livery: data.livery
    }))
  );

  return { hardpoints, installedItems, missingItems: [...missingInstall.values()] };
}

function buildShipStatsFallback(groups: CanonicalVariantGroup[]): NormalizedShipStat[] {
  const stats: NormalizedShipStat[] = [];
  for (const group of groups) {
    const baseRecord = group.records.find((entry) => !entry.isEditionOnly) ?? group.records[0];
    if (!baseRecord) continue;
    const ship = baseRecord.record.ship;
    const payload: Record<string, unknown> = {};
    if (ship.FlightCharacteristics) payload.flight = ship.FlightCharacteristics;
    if (ship.Propulsion) payload.propulsion = ship.Propulsion;
    if (ship.QuantumTravel) payload.quantum = ship.QuantumTravel;
    if (ship.Insurance) payload.insurance = ship.Insurance;
    if (ship.Cargo !== undefined) payload.cargo = ship.Cargo;
    if (ship.CargoGrids) payload.cargo_grids = ship.CargoGrids;
    if (ship.Health !== undefined) payload.health = ship.Health;
    if (ship.Crew !== undefined) payload.crew = ship.Crew;
    if (ship.Mass !== undefined) payload.mass = ship.Mass;
    if (ship.DamageBeforeDestruction) payload.damage_before_destruction = ship.DamageBeforeDestruction;
    if (ship.DamageBeforeDetach) payload.damage_before_detach = ship.DamageBeforeDetach;
    if (ship.ShieldFaceType) payload.shield_face_type = ship.ShieldFaceType;
    const dimensions: Record<string, unknown> = {};
    if (ship.Width !== undefined) dimensions.width = ship.Width;
    if (ship.Length !== undefined) dimensions.length = ship.Length;
    if (ship.Height !== undefined) dimensions.height = ship.Height;
    if (Object.keys(dimensions).length) payload.dimensions = dimensions;
    if (!Object.keys(payload).length) continue;
    stats.push({ ship_variant_external_id: group.variantId, stats: payload });
  }
  return stats;
}

function buildItemStatsFallback(rawItems: RawItem[], itemIds: Set<string>): NormalizedItemStat[] {
  const stats: NormalizedItemStat[] = [];
  for (const item of rawItems) {
    let externalId: string;
    try {
      externalId = asExternalId(
        coalesce(
          item.id,
          item.reference,
          item.className,
          item.itemName,
          item.stdItem?.UUID,
          item.stdItem?.Name
        )
      );
    } catch {
      continue;
    }
    if (!itemIds.has(externalId)) continue;

    const payload: Record<string, unknown> = {};
    if (item.stdItem) payload.stdItem = item.stdItem;
    const classification = (item as any)?.classification ?? (item as any)?.Classification;
    if (classification !== undefined) payload.classification = classification;
    const tags = (item as any)?.tags ?? (item as any)?.Tags;
    if (tags !== undefined) payload.tags = tags;
    if (!Object.keys(payload).length) continue;
    stats.push({ item_external_id: externalId, stats: payload });
  }
  return stats;
}

export async function transform(
  dataRoot: string,
  channel: Channel,
  version: string
): Promise<NormalizedDataBundle> {
  const rawDir = join(dataRoot, 'raw', channel, version);
  const normalizedDir = join(dataRoot, 'normalized', channel, version);

  const rawManufacturers = await readJson<RawManufacturer[]>(join(rawDir, 'manufacturers.json'));
  const manufacturerMap = new Map<string, NormalizedManufacturer>();
  for (const manufacturer of rawManufacturers) {
    const manufacturerCode = optionalString(manufacturer.code) ?? optionalString((manufacturer as any).Code);
    const manufacturerReference = optionalString(manufacturer.reference) ?? optionalString((manufacturer as any).Reference);
    const manufacturerName = optionalString(manufacturer.name) ?? optionalString((manufacturer as any).Name);
    const manufacturerIdCandidate = coalesce(
      manufacturerCode,
      manufacturer.id,
      (manufacturer as any).ID,
      manufacturerReference,
      manufacturerName
    );

    const externalId = asExternalId(manufacturerIdCandidate);

    manufacturerMap.set(externalId, {
      external_id: externalId,
      code: manufacturerCode ?? externalId,
      name: manufacturerName ?? manufacturerCode ?? externalId,
      description:
        optionalString(manufacturer.description) ?? optionalString((manufacturer as any).Description),
      data_source: optionalString(manufacturer.data_source)
    });
  }

  const ensureManufacturer = (
    externalId: string,
    source: 'ship' | 'item',
    context: string,
    details: { code?: string; name?: string; description?: string } = {}
  ) => {
    if (manufacturerMap.has(externalId)) return;
    const code = optionalString(details.code) ?? externalId;
    const name = optionalString(details.name) ?? code;
    const description = optionalString(details.description);
    manufacturerMap.set(externalId, {
      external_id: externalId,
      code,
      name,
      description,
      data_source: 'derived'
    });
    log.warn('Referenced manufacturer missing from raw data; synthesizing entry', {
      source,
      context,
      manufacturer: externalId,
      name
    });
  };

  const shipRecords = await readShipRecords(rawDir);
  const hullMap = new Map<string, NormalizedShip>();
  const shipIdToHullKey = new Map<string, string>();
  const variantGroups = new Map<string, CanonicalVariantGroup>();
  const rawToCanonicalVariant = new Map<string, string>();

  for (const record of shipRecords) {
    const ship = record.ship;
    const rawShipId = record.externalId;

    const manufacturerIdSource = coalesce(
      ship.manufacturer_id,
      ship.manufacturer?.id,
      optionalString(ship.manufacturer?.code) ?? optionalString(ship.manufacturer?.Code),
      optionalString((ship as any).Manufacturer?.Code)
    );
    if (!manufacturerIdSource) {
      log.warn('Skipping ship without manufacturer reference', { ship: rawShipId });
      continue;
    }
    const manufacturerExternalId = asExternalId(manufacturerIdSource);
    ensureManufacturer(manufacturerExternalId, 'ship', rawShipId, {
      code:
        optionalString(ship.manufacturer?.code) ??
        optionalString(ship.manufacturer?.Code) ??
        optionalString((ship as any).Manufacturer?.Code),
      name:
        optionalString((ship as any).Manufacturer?.Name) ??
        optionalString((ship as any).manufacturer?.Name)
    });

    const displayName =
      optionalString(ship.name) ?? optionalString(ship.Name) ?? optionalString(ship.ClassName) ?? rawShipId;
    const classificationSource = optionalString(ship.ClassName) ?? displayName;
    const variantCode = extractVariantCode(`${displayName} ${classificationSource}`);
    const familyName = cleanFamilyName(classificationSource, variantCode, manufacturerExternalId);
    const hullKey = buildHullKey(manufacturerExternalId, familyName);

    shipIdToHullKey.set(rawShipId, hullKey);

    const canonicalVariantId = toCanonicalVariantExtId(hullKey, variantCode);
    rawToCanonicalVariant.set(rawShipId, canonicalVariantId);

    const editionInfo = detectEditionOrLivery(displayName);
    const editionOnly = isEditionOnly(displayName);
    // Example: "Zeus Mk II CL Warbond IAE 2954" -> canonical variant RSI_ZEUS_MKII_CL with profile "IAE2954_WARBOND".

    const hullDisplayName = canonicalVariantName(familyName.replace(/_/g, ' '), 'BASE');
    const descriptionCandidate = optionalString(ship.description) ?? optionalString(ship.Description);
    const sizeCandidate =
      optionalString(ship.size) ??
      optionalString(ship.Size) ??
      optionalNumber(ship.size)?.toString() ??
      optionalNumber(ship.Size)?.toString();
    const classCandidate =
      optionalString(ship.class) ??
      optionalString(ship.Class) ??
      optionalString(ship.Role) ??
      optionalString(ship.Career) ??
      'Unknown';

    const existingHull = hullMap.get(hullKey);
    if (!existingHull) {
      hullMap.set(hullKey, {
        external_id: hullKey,
        name: hullDisplayName,
        class: classCandidate,
        size: sizeCandidate ?? undefined,
        manufacturer_external_id: manufacturerExternalId,
        description: descriptionCandidate
      });
    } else {
      if (!existingHull.description && descriptionCandidate) {
        existingHull.description = descriptionCandidate;
      }
      if (!existingHull.size && sizeCandidate) {
        existingHull.size = sizeCandidate;
      }
      if (existingHull.class === 'Unknown' && classCandidate !== 'Unknown') {
        existingHull.class = classCandidate;
      }
    }

    let group = variantGroups.get(canonicalVariantId);
    if (!group) {
      group = {
        variantId: canonicalVariantId,
        hullKey,
        variantCode,
        baseName: familyName.replace(/_/g, ' '),
        names: new Set<string>(),
        descriptions: new Set<string>(),
        records: []
      };
      group.names.add(canonicalVariantName(group.baseName, variantCode));
      variantGroups.set(canonicalVariantId, group);
    }

    const loadoutRecord: VariantLoadoutRecord = {
      variantId: canonicalVariantId,
      record,
      profile: editionInfo.editionCode,
      livery: editionInfo.livery,
      isEditionOnly: editionOnly
    };
    group.records.push(loadoutRecord);

    if (!editionOnly && displayName) {
      group.names.add(displayName);
    }
    if (editionInfo.editionCode) {
      group.names.add(canonicalVariantName(group.baseName, variantCode));
    }
    if (descriptionCandidate) {
      group.descriptions.add(descriptionCandidate);
    }
  }

  const ships: NormalizedShip[] = sortByExternalId([...hullMap.values()]);

  const rawVariants = await readJsonOrDefault<RawVariant[]>(join(rawDir, 'ship_variants.json'), []);
  for (const variant of rawVariants) {
    const rawVariantId = asExternalId(variant.id);
    const parentShipId = asExternalId(variant.ship_id);
    const hullKey = shipIdToHullKey.get(parentShipId);
    if (!hullKey) continue;
    const variantCode = extractVariantCode(
      `${optionalString(variant.variant_code) ?? ''} ${optionalString(variant.name) ?? ''}`
    );
    const canonicalVariantId = toCanonicalVariantExtId(hullKey, variantCode);
    rawToCanonicalVariant.set(rawVariantId, canonicalVariantId);
    const group = variantGroups.get(canonicalVariantId);
    if (group) {
      if (variant.name) {
        group.names.add(variant.name);
      }
      if (variant.description) {
        group.descriptions.add(variant.description);
      }
    }
  }

  const shipVariants: NormalizedShipVariant[] = sortByExternalId(
    [...variantGroups.values()].map((group) => {
      const names = group.names.size
        ? Array.from(group.names)
        : [canonicalVariantName(group.baseName, group.variantCode)];
      const descriptions = group.descriptions.size ? Array.from(group.descriptions) : [];
      return {
        external_id: group.variantId,
        ship_external_id: group.hullKey,
        variant_code: group.variantCode,
        name: names[0],
        thumbnail: undefined,
        description: descriptions[0]
      } satisfies NormalizedShipVariant;
    })
  );

  const variantIds = new Set(shipVariants.map((variant) => variant.external_id));
  for (const variant of shipVariants) {
    rawToCanonicalVariant.set(variant.external_id, variant.external_id);
  }

  const variantEntries: VariantLoadoutRecord[] = [...variantGroups.values()].flatMap((group) => group.records);

  const rawItems = await readJson<RawItem[]>(join(rawDir, 'items.json'));
  const itemRelevance = new Map<string, boolean>();
  const normalizedItems: NormalizedItem[] = [];

  for (const item of rawItems) {
    const externalId = asExternalId(
      coalesce(
        item.id,
        item.reference,
        item.className,
        item.itemName,
        item.stdItem?.UUID,
        item.stdItem?.Name
      )
    );

    const relevant = isShipRelevantItem(item);
    itemRelevance.set(externalId, relevant);
    if (!relevant) continue;

    const baseType = optionalString(item.type) ??
      (item.stdItem?.Type ? item.stdItem.Type.split('.')[0] : undefined) ??
      'Unknown';

    const subtype =
      optionalString(item.subtype) ??
      optionalString((item as any).subType) ??
      (item.stdItem?.Type && item.stdItem.Type.includes('.')
        ? item.stdItem.Type.split('.').slice(1).join('.')
        : undefined);

    const resolvedName =
      optionalString(item.name) ??
      optionalString((item as any).Name) ??
      optionalString(item.stdItem?.Name) ??
      optionalString(item.itemName) ??
      `Item ${externalId}`;

    const manufacturerIdCandidate = coalesce<string | number>(
      item.manufacturer_id,
      optionalString(item.manufacturer),
      optionalString(item.stdItem?.Manufacturer?.Code)
    );
    const manufacturerExternalId = manufacturerIdCandidate
      ? asExternalId(manufacturerIdCandidate)
      : undefined;
    if (manufacturerExternalId) {
      ensureManufacturer(manufacturerExternalId, 'item', externalId, {
        code:
          optionalString(item.stdItem?.Manufacturer?.Code) ??
          optionalString(item.manufacturer),
        name: optionalString(item.stdItem?.Manufacturer?.Name)
      });
    }

    const rawClass =
      optionalString(item.class) ??
      (item.stdItem?.Type ? item.stdItem.Type.split('.')[0] : undefined);

    normalizedItems.push({
      external_id: externalId,
      type: baseType,
      subtype,
      name: resolvedName,
      manufacturer_external_id: manufacturerExternalId,
      size: optionalNumber(item.size ?? item.stdItem?.Size),
      grade: optionalString(
        item.grade ??
          (typeof item.stdItem?.Grade === 'number'
            ? item.stdItem.Grade.toString()
            : item.stdItem?.Grade)
      ),
      class: normalizeItemClass(rawClass),
      description: optionalString(item.description) ?? optionalString(item.stdItem?.Description)
    } satisfies NormalizedItem);
  }

  const items: NormalizedItem[] = sortByExternalId(normalizedItems);

  const itemIds = new Set(items.map((item) => item.external_id));
  const nonShipItemIds = new Set(
    [...itemRelevance.entries()]
      .filter(([, relevant]) => !relevant)
      .map(([externalId]) => externalId)
  );

  const rawHardpoints = await readJsonOrDefault<RawHardpoint[]>(join(rawDir, 'hardpoints.json'), []);
  const installedItemsRaw = await readJsonOrDefault<RawInstalledItem[]>(
    join(rawDir, 'installed_items.json'),
    []
  );
  const needLoadoutFallback = !rawHardpoints.length || !installedItemsRaw.length;
  const recordsWithLoadout = needLoadoutFallback
    ? variantEntries.filter((entry) => entry.record.loadout.length)
    : [];
  const loadoutFallback = needLoadoutFallback && recordsWithLoadout.length
    ? buildLoadoutData(recordsWithLoadout, itemIds, nonShipItemIds)
    : undefined;

  if (loadoutFallback?.missingItems?.length) {
    for (const miss of loadoutFallback.missingItems) {
      log.warn('Installed item references unknown item', {
        ship_variant: miss.shipVariant,
        item: miss.item
      });
    }
  }

  let hardpoints: NormalizedHardpoint[];
  if (rawHardpoints.length) {
    const hardpointAccumulator: NormalizedHardpoint[] = [];
    for (const hardpoint of rawHardpoints) {
      const variantExternalId = asExternalId(hardpoint.ship_variant_id);
      const canonicalVariantId = rawToCanonicalVariant.get(variantExternalId);
      if (!canonicalVariantId || !variantIds.has(canonicalVariantId)) {
        log.warn('Hardpoint references unknown variant', {
          hardpoint: hardpoint.id ?? hardpoint.code,
          variant: hardpoint.ship_variant_id
        });
        continue;
      }
      const generatedId = `${canonicalVariantId}:${hardpoint.code}`;
      hardpointAccumulator.push({
        external_id: asExternalId(hardpoint.id ?? generatedId),
        ship_variant_external_id: canonicalVariantId,
        code: hardpoint.code,
        category: hardpoint.category,
        position: optionalString(hardpoint.position),
        size: optionalNumber(hardpoint.size),
        gimballed: hardpoint.gimballed,
        powered: hardpoint.powered,
        seats: optionalNumber(hardpoint.seats)
      });
    }
    hardpoints = sortByExternalId(hardpointAccumulator);
  } else {
    hardpoints = loadoutFallback?.hardpoints ?? [];
  }

  const itemStatsRaw = await readJsonOrDefault<RawItemStat[]>(join(rawDir, 'item_stats.json'), []);
  const itemStats: NormalizedItemStat[] = itemStatsRaw.length
    ? itemStatsRaw
        .filter((stat) => {
          const external = asExternalId(stat.item_id);
          const isKnown = itemIds.has(external);
          if (!isKnown) {
            log.warn('Item stats entry references unknown item', { item: stat.item_id });
          }
          return isKnown;
        })
        .map((stat) => ({
          item_external_id: asExternalId(stat.item_id),
          stats: stat.stats ?? {},
          price_auec: optionalNumber(stat.price_auec),
          availability: optionalString(stat.availability)
        }))
    : buildItemStatsFallback(rawItems, itemIds);

  const shipStatsRaw = await readJsonOrDefault<RawShipStat[]>(join(rawDir, 'ship_stats.json'), []);
  const shipStats: NormalizedShipStat[] = shipStatsRaw.length
    ? shipStatsRaw
        .map((stat) => {
          const rawVariantId = asExternalId(stat.ship_variant_id);
          const canonicalVariantId = rawToCanonicalVariant.get(rawVariantId);
          if (!canonicalVariantId || !variantIds.has(canonicalVariantId)) {
            log.warn('Ship stats entry references unknown variant', { variant: stat.ship_variant_id });
            return undefined;
          }
          return {
            ship_variant_external_id: canonicalVariantId,
            stats: stat.stats ?? {}
          } satisfies NormalizedShipStat;
        })
        .filter((entry): entry is NormalizedShipStat => Boolean(entry))
    : buildShipStatsFallback([...variantGroups.values()]);

  const hardpointIds = new Set(hardpoints.map((hp) => hp.external_id));
  let installedItems: NormalizedInstalledItem[];
  if (installedItemsRaw.length) {
    const installedAccumulator: NormalizedInstalledItem[] = [];
    for (const installed of installedItemsRaw) {
      const rawVariantId = asExternalId(installed.ship_variant_id);
      const canonicalVariantId = rawToCanonicalVariant.get(rawVariantId);
      const itemId = asExternalId(installed.item_id);
      const itemKnown = itemIds.has(itemId);
      if (!canonicalVariantId || !variantIds.has(canonicalVariantId) || !itemKnown) {
        log.warn('Skipping installed item with missing references', {
          variant: installed.ship_variant_id,
          item: installed.item_id
        });
        continue;
      }
      const hardpointExternalId = installed.hardpoint_id
        ? asExternalId(installed.hardpoint_id)
        : undefined;
      if (hardpointExternalId && !hardpointIds.has(hardpointExternalId)) {
        log.warn('Installed item references unknown hardpoint', {
          hardpoint: hardpointExternalId
        });
      }
      installedAccumulator.push({
        ship_variant_external_id: canonicalVariantId,
        item_external_id: itemId,
        quantity: optionalNumber(installed.quantity) ?? 1,
        hardpoint_external_id: hardpointExternalId
      });
    }
    installedItems = sortInstalledItems(installedAccumulator);
  } else {
    installedItems = loadoutFallback?.installedItems ?? [];
  }

  const localeFiles = await fg('locales/*.json', { cwd: rawDir });
  const localeEntries: NormalizedLocaleEntry[] = [];
  for (const file of localeFiles) {
    const lang = file.replace(/^locales\//, '').replace(/\.json$/i, '');
    const absolute = join(rawDir, file);
    const payload = await readJson<Record<string, unknown>>(absolute);
    const entries = makeLocaleEntries(relative(rawDir, absolute), lang, payload);
    localeEntries.push(...entries);
  }

  const manufacturers = sortByExternalId([...manufacturerMap.values()]);

  const bundle: NormalizedDataBundle = {
    manufacturers,
    ships,
    ship_variants: shipVariants,
    items,
    hardpoints,
    item_stats: itemStats,
    ship_stats: shipStats,
    installed_items: installedItems,
    locales: sortLocales(localeEntries)
  };

  await writeJson(join(normalizedDir, 'manufacturers.json'), bundle.manufacturers);
  await writeJson(join(normalizedDir, 'ships.json'), bundle.ships);
  await writeJson(join(normalizedDir, 'ship_variants.json'), bundle.ship_variants);
  await writeJson(join(normalizedDir, 'items.json'), bundle.items);
  await writeJson(join(normalizedDir, 'hardpoints.json'), bundle.hardpoints);
  await writeJson(join(normalizedDir, 'item_stats.json'), bundle.item_stats);
  await writeJson(join(normalizedDir, 'ship_stats.json'), bundle.ship_stats);
  await writeJson(join(normalizedDir, 'installed_items.json'), bundle.installed_items);
  await writeJson(join(normalizedDir, 'locales.json'), bundle.locales);

  log.info('Transformation complete', {
    manufacturers: bundle.manufacturers.length,
    ships: bundle.ships.length,
    variants: bundle.ship_variants.length,
    items: bundle.items.length
  });

  return bundle;
}
