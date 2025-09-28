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
  NormalizedShipVariant,
  NormalizedBundleV2,
  NormalizedCompanyV2,
  NormalizedExternalReference,
  NormalizedHardpointV2,
  NormalizedItemV2,
  NormalizedShipV2,
  NormalizedShipVariantV2,
  ShipVariantStatsV2
} from './types/index.js';
import { loadTransformConfig } from './config/transform.js';
import { pathExists, readJson, readJsonOrDefault, writeJson } from './utils/fs.js';
import { log } from './utils/log.js';
import {
  type CanonicalVariantCode,
  buildHullKey,
  canonicalVariantName,
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
    Manufacturer?: { Code?: string; Name?: string; Description?: string };
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

function extractVehicleDefinitionFromSupplement(supplement: RawShipSupplement | undefined): string | undefined {
  if (!supplement) return undefined;
  const candidate = optionalString(
    (supplement as any)?.Raw?.Entity?.Components?.VehicleComponentParams?.vehicleDefinition
  );
  if (candidate) return candidate;
  const alt = optionalString((supplement as any)?.VehicleComponentParams?.vehicleDefinition);
  return alt ?? undefined;
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
  variantCode: CanonicalVariantCode;
  baseName: string;
  baseTokens: string[];
  variantTokens: string[];
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

function sanitizeIdentifierToken(value: string): string {
  return value
    .replace(/[^a-z0-9]+/gi, '_')
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '')
    .toUpperCase();
}

function tokenizeIdentifier(input: string | undefined): string[] {
  if (!input) return [];
  return input
    .split(/[^a-z0-9]+/i)
    .map((part) => sanitizeIdentifierToken(part))
    .filter(Boolean);
}

function arraysEqual(a: string[], b: string[]): boolean {
  if (a.length !== b.length) return false;
  return a.every((value, index) => value === b[index]);
}

function extractVehicleClassName(ship: RawShip): string | undefined {
  const candidate = optionalString((ship as any).vehicleDefinition ?? (ship as any).VehicleDefinition);
  if (!candidate) return undefined;
  const match = candidate.match(/([A-Za-z0-9_:-]+)\.(?:xml|json)$/i);
  return match ? match[1] : undefined;
}

function collectManufacturerTokens(ship: RawShip, manufacturerCode: string | undefined): Set<string> {
  const tokens = new Set<string>();
  const add = (value?: string) => {
    if (!value) return;
    for (const token of tokenizeIdentifier(value)) {
      tokens.add(token);
    }
  };

  if (manufacturerCode) {
    const normalized = sanitizeIdentifierToken(manufacturerCode);
    tokens.add(normalized);
    if (normalized.endsWith('S')) {
      tokens.add(normalized.slice(0, -1));
    }
  }

  add(optionalString(ship.manufacturer?.code));
  add(optionalString(ship.manufacturer?.Code));
  add(optionalString((ship as any).Manufacturer?.Code));
  add(optionalString(ship.manufacturer?.name));
  add(optionalString(ship.manufacturer?.Name));
  add(optionalString((ship as any).Manufacturer?.Name));

  return tokens;
}

function filterManufacturerTokens(tokens: string[], manufacturerTokens: Set<string>): string[] {
  if (!tokens.length || !manufacturerTokens.size) return tokens;
  return tokens.filter((token) => {
    if (manufacturerTokens.has(token)) return false;
    for (const candidate of manufacturerTokens) {
      if (candidate && (token.startsWith(candidate) || candidate.startsWith(token))) {
        return false;
      }
    }
    return true;
  });
}

function deriveFamilyAndVariant(
  ship: RawShip,
  manufacturerTokens: Set<string>
): { familyTokens: string[]; variantTokens: string[] } {
  const className = optionalString(ship.ClassName);
  const baseClassName = extractVehicleClassName(ship) ?? className;

  const classTokens = filterManufacturerTokens(tokenizeIdentifier(className), manufacturerTokens);
  const baseTokens = filterManufacturerTokens(tokenizeIdentifier(baseClassName), manufacturerTokens);

  const familyTokens = baseTokens.length ? baseTokens : classTokens.length ? classTokens : ['HULL'];

  let variantTokens: string[] = [];
  if (!classTokens.length || arraysEqual(classTokens, familyTokens)) {
    variantTokens = [];
  } else if (
    classTokens.length > familyTokens.length &&
    arraysEqual(classTokens.slice(0, familyTokens.length), familyTokens)
  ) {
    variantTokens = classTokens.slice(familyTokens.length);
  } else {
    const difference = classTokens.filter((token) => !familyTokens.includes(token));
    variantTokens = difference.length ? difference : classTokens;
  }

  return { familyTokens, variantTokens };
}

function stripManufacturerPrefix(name: string, manufacturerTokens: Set<string>): string {
  if (!name) return name;
  const parts = name.split(/\s+/).filter(Boolean);
  while (parts.length) {
    const token = sanitizeIdentifierToken(parts[0]);
    if (manufacturerTokens.has(token)) {
      parts.shift();
      continue;
    }
    let matched = false;
    for (const candidate of manufacturerTokens) {
      if (candidate && token && (token.startsWith(candidate) || candidate.startsWith(token))) {
        parts.shift();
        matched = true;
        break;
      }
    }
    if (!matched) break;
  }
  return parts.join(' ');
}

function deriveVariantTokensFromCandidates(
  candidateTokens: string[],
  baseTokens: string[],
  manufacturerTokens: Set<string>
): string[] {
  if (!candidateTokens.length) return [];
  const filtered = filterManufacturerTokens(candidateTokens, manufacturerTokens);
  if (!filtered.length) return [];
  if (filtered.length > baseTokens.length && arraysEqual(filtered.slice(0, baseTokens.length), baseTokens)) {
    return filtered.slice(baseTokens.length);
  }
  const diff = filtered.filter((token) => !baseTokens.includes(token));
  if (diff.length) return diff;
  // If everything matches the base tokens, treat as BASE variant.
  if (arraysEqual(filtered, baseTokens)) return [];
  return filtered;
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

type ExternalReferenceMap = Map<string, NormalizedExternalReference>;

function addExternalRefs(
  collection: Map<string, ExternalReferenceMap>,
  key: string,
  refs: NormalizedExternalReference[]
) {
  if (!refs.length) return;
  let bucket = collection.get(key);
  if (!bucket) {
    bucket = new Map();
    collection.set(key, bucket);
  }
  for (const ref of refs) {
    if (!ref.source || !ref.id) continue;
    const composite = `${ref.source}:${ref.id}`;
    if (!bucket.has(composite)) {
      bucket.set(composite, ref);
    }
  }
}

function refSetToArray(bucket: ExternalReferenceMap | undefined): NormalizedExternalReference[] {
  if (!bucket) return [];
  return [...bucket.values()];
}

function sortExternalRefs(refs: NormalizedExternalReference[]): NormalizedExternalReference[] {
  return [...refs].sort((a, b) => {
    const sourceCmp = a.source.localeCompare(b.source);
    if (sourceCmp !== 0) return sourceCmp;
    return a.id.localeCompare(b.id);
  });
}

function sanitizeManufacturerToken(value: string): string {
  return value.replace(/[^a-z0-9]+/gi, '').toUpperCase();
}

function normalizeManufacturerCode(raw: unknown): string | undefined {
  if (raw === undefined || raw === null) return undefined;
  const text = String(raw).trim();
  if (!text) return undefined;
  return sanitizeManufacturerToken(text);
}

function fallbackManufacturerCode(name?: string, fallback?: string): string {
  const candidate = name ?? fallback ?? 'UNKNOWN';
  // ASSUMPTION: Fallback manufacturer codes strip non-alphanumeric characters.
  return sanitizeManufacturerToken(candidate);
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

function resolveItemTypeToken(item: RawItem): string | undefined {
  const baseType = getItemBaseType(item);
  if (!baseType) return undefined;
  return normalizeTypeToken(baseType);
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

function isAllowedItem(item: RawItem, allowedTypes: Set<string>): boolean {
  if (!allowedTypes.size) {
    return isShipRelevantItem(item);
  }
  const token = resolveItemTypeToken(item);
  if (!token) return false;
  return allowedTypes.has(token);
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
          const vehicleDefinition = extractVehicleDefinitionFromSupplement(supplement);
          if (vehicleDefinition) {
            (base as any).vehicleDefinition = vehicleDefinition;
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

function mergeShipFlightStats(
  target: ShipVariantStatsV2,
  flight: Record<string, unknown> | undefined
) {
  if (!flight) return;
  const perf = target.performance ?? (target.performance = {});
  if (flight.ScmSpeed !== undefined) {
    const scm = optionalNumber((flight as any).ScmSpeed);
    if (scm !== undefined) perf.scm_speed = scm;
  }
  if ((flight as any).BoostSpeedForward !== undefined) {
    const ab = optionalNumber((flight as any).BoostSpeedForward);
    if (ab !== undefined) perf.afterburner_speed = ab;
  }
  const accel = (flight as any).Acceleration as Record<string, unknown> | undefined;
  if (accel) {
    const accels: Record<string, number> = {};
    for (const [key, value] of Object.entries(accel)) {
      const numeric = optionalNumber(value);
      if (numeric !== undefined) {
        accels[key.toLowerCase()] = numeric;
      }
    }
    if (Object.keys(accels).length) {
      perf.accelerations = { ...(perf.accelerations ?? {}), ...accels };
    }
  }
  const pitch = optionalNumber((flight as any).Pitch);
  if (pitch !== undefined) perf.pitch_rate = pitch;
  const yaw = optionalNumber((flight as any).Yaw);
  if (yaw !== undefined) perf.yaw_rate = yaw;
  const roll = optionalNumber((flight as any).Roll);
  if (roll !== undefined) perf.roll_rate = roll;
}

function mergeShipPropulsionStats(
  target: ShipVariantStatsV2,
  propulsion: Record<string, unknown> | undefined,
  quantum: Record<string, unknown> | undefined
) {
  if (!propulsion && !quantum) return;
  const profile = target.propulsion ?? (target.propulsion = {});
  if (propulsion) {
    const hydro = optionalNumber(propulsion.FuelCapacity ?? (propulsion as any).fuel_capacity);
    if (hydro !== undefined) profile.hydrogen_capacity = hydro;
    const thrust = (propulsion as any).ThrustCapacity as Record<string, unknown> | undefined;
    if (thrust) {
      const main = optionalNumber(thrust.Main);
      if (main !== undefined) profile.main_thrusters = Math.round(main);
      const maneuvering = optionalNumber(thrust.Maneuvering);
      if (maneuvering !== undefined) profile.maneuver_thrusters = Math.round(maneuvering);
    }
    const fuelUsage = (propulsion as any).FuelUsage as Record<string, unknown> | undefined;
    if (fuelUsage) {
      const vt = optionalNumber(fuelUsage.Vtol);
      if (vt !== undefined) {
        profile.fuel_intakes = Math.max(profile.fuel_intakes ?? 0, Math.round(vt));
      }
    }
    const intakeRate = optionalNumber(propulsion.FuelIntakeRate);
    if (intakeRate !== undefined) {
      profile.fuel_intakes = intakeRate;
    }
    const powerOutput = optionalNumber((propulsion as any).PowerOutput);
    if (powerOutput !== undefined) {
      profile.power_output = powerOutput;
    }
  }
  if (quantum) {
    const quantumCapacity = optionalNumber(quantum.FuelCapacity ?? (quantum as any).fuel_capacity);
    if (quantumCapacity !== undefined) {
      profile.quantum_capacity = quantumCapacity;
    }
  }
}

function mergeShipDefenceStats(
  target: ShipVariantStatsV2,
  ship: RawShip,
  raw: Record<string, unknown> | undefined
) {
  const defence = target.defence ?? (target.defence = {});
  const shieldSlots = optionalNumber((raw as any)?.shield_slots);
  if (shieldSlots !== undefined) {
    defence.shield_slots = shieldSlots;
  }
  if (ship.ShieldFaceType && defence.shield_slots === undefined) {
    // TODO: translate ShieldFaceType into slot count once mapping is defined.
  }
}

function buildShipVariantStatsV2(
  variantId: string,
  group: CanonicalVariantGroup | undefined,
  rawStats: Record<string, unknown> | undefined
): ShipVariantStatsV2 {
  const stats: ShipVariantStatsV2 = {};

  const baseRecord = group?.records.find((entry) => !entry.isEditionOnly) ?? group?.records[0];
  const ship = baseRecord?.record.ship;

  if (ship) {
    const length = optionalNumber(ship.Length ?? (ship as any).length);
    if (length !== undefined) stats.length = length;
    const width = optionalNumber(ship.Width ?? (ship as any).width);
    if (width !== undefined) stats.width = width;
    const height = optionalNumber(ship.Height ?? (ship as any).height);
    if (height !== undefined) stats.height = height;
    const mass = optionalNumber(ship.Mass ?? (ship as any).mass);
    if (mass !== undefined) stats.mass = mass;
    const cargo = optionalNumber(ship.Cargo ?? (ship as any).cargo);
    if (cargo !== undefined) stats.cargo_capacity = cargo;

    const crewValue = optionalNumber(ship.Crew ?? (ship as any).crew);
    if (crewValue !== undefined) {
      stats.crew = { ...(stats.crew ?? {}), minimum: crewValue };
    }

    mergeShipFlightStats(stats, ship.FlightCharacteristics as Record<string, unknown> | undefined);
    mergeShipPropulsionStats(
      stats,
      ship.Propulsion as Record<string, unknown> | undefined,
      ship.QuantumTravel as Record<string, unknown> | undefined
    );
    mergeShipDefenceStats(stats, ship, rawStats);

    if (ship.Insurance) {
      stats.insurance = { ...(stats.insurance ?? {}), ...ship.Insurance };
    }
  }

  if (rawStats) {
    if (rawStats.dimensions && typeof rawStats.dimensions === 'object') {
      const dims = rawStats.dimensions as Record<string, unknown>;
      const length = optionalNumber(dims.length ?? dims.Length);
      if (length !== undefined) stats.length = length;
      const width = optionalNumber(dims.width ?? dims.Width);
      if (width !== undefined) stats.width = width;
      const height = optionalNumber(dims.height ?? dims.Height);
      if (height !== undefined) stats.height = height;
    }
    const mass = optionalNumber(rawStats.mass);
    if (mass !== undefined) stats.mass = mass;
    const cargo = optionalNumber(rawStats.cargo);
    if (cargo !== undefined) stats.cargo_capacity = cargo;
    const crew = optionalNumber(rawStats.crew);
    if (crew !== undefined) {
      stats.crew = { ...(stats.crew ?? {}), minimum: crew };
    }
    mergeShipFlightStats(stats, rawStats.flight as Record<string, unknown> | undefined);
    mergeShipPropulsionStats(
      stats,
      rawStats.propulsion as Record<string, unknown> | undefined,
      rawStats.quantum as Record<string, unknown> | undefined
    );
    if (rawStats.insurance && typeof rawStats.insurance === 'object') {
      stats.insurance = { ...(stats.insurance ?? {}), ...(rawStats.insurance as Record<string, unknown>) };
    }

    stats.raw = rawStats;
  }

  if (!Object.keys(stats).length && rawStats) {
    stats.raw = rawStats;
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
  const transformConfig = loadTransformConfig();

  const rawManufacturers = await readJson<RawManufacturer[]>(join(rawDir, 'manufacturers.json'));
  const manufacturerMap = new Map<string, NormalizedManufacturer>();
  const companyRefs = new Map<string, ExternalReferenceMap>();
  const shipExternalRefs = new Map<string, ExternalReferenceMap>();
  const shipPaints = new Map<string, Set<string>>();
  const variantExternalRefs = new Map<string, ExternalReferenceMap>();
  const variantThumbnails = new Map<string, string>();
  const variantReleasePatch = new Map<string, string>();
  const itemExternalRefs = new Map<string, ExternalReferenceMap>();

  const registerManufacturer = (
    codeInput: string | undefined,
    source: 'raw' | 'ship' | 'item',
    context: string,
    details: { name?: string; description?: string; data_source?: string; externalRefs?: NormalizedExternalReference[] } = {}
  ): string => {
    let normalised = codeInput ? normalizeManufacturerCode(codeInput) : undefined;
    if (!normalised && details.name) {
      normalised = normalizeManufacturerCode(details.name);
    }
    if (!normalised) {
      normalised = fallbackManufacturerCode(details.name, context);
      if (source !== 'raw') {
        log.warn('Derived manufacturer code from fallback', {
          source,
          context,
          manufacturer: normalised
        });
      }
    }

    const name = optionalString(details.name) ?? normalised;
    const description = optionalString(details.description);
    const dataSource = optionalString(details.data_source) ?? (source === 'raw' ? undefined : 'derived');

    const existing = manufacturerMap.get(normalised);
    if (existing) {
      if (!existing.name && name) existing.name = name;
      if (!existing.description && description) existing.description = description;
      if (!existing.data_source && dataSource) existing.data_source = dataSource;
    } else {
      manufacturerMap.set(normalised, {
        external_id: normalised,
        code: normalised,
        name,
        description,
        data_source: dataSource
      });
    }

    const externalRefs = details.externalRefs ?? [];
    if (!externalRefs.length && context) {
      externalRefs.push({ source: `manufacturer:${source}`, id: context });
    }
    addExternalRefs(companyRefs, normalised, externalRefs);

    return normalised;
  };

  for (const manufacturer of rawManufacturers) {
    const codeCandidate = optionalString(manufacturer.code) ?? optionalString((manufacturer as any).Code);
    const name = optionalString(manufacturer.name) ?? optionalString((manufacturer as any).Name);
    const description =
      optionalString(manufacturer.description) ?? optionalString((manufacturer as any).Description);
    const reference = optionalString(manufacturer.reference) ?? optionalString((manufacturer as any).Reference);
    const context =
      codeCandidate ??
      reference ??
      optionalString(manufacturer.id)?.toString() ??
      optionalString((manufacturer as any).ID)?.toString() ??
      name ??
      'UNKNOWN_MANUFACTURER';
    const externalRefs: NormalizedExternalReference[] = [];
    if (manufacturer.id !== undefined) {
      externalRefs.push({ source: 'raw:manufacturers.id', id: String(manufacturer.id) });
    }
    if (reference) {
      externalRefs.push({ source: 'raw:manufacturers.reference', id: reference });
    }
    registerManufacturer(codeCandidate ?? reference ?? context, 'raw', context, {
      name,
      description,
      data_source: optionalString(manufacturer.data_source),
      externalRefs
    });
  }

  const shipRecords = await readShipRecords(rawDir);
  const hullMap = new Map<string, NormalizedShip>();
  const shipIdToHullKey = new Map<string, string>();
  const variantGroups = new Map<string, CanonicalVariantGroup>();
  const rawToCanonicalVariant = new Map<string, string>();
  const hullMetadata = new Map<string, { baseTokens: string[]; manufacturerTokens: Set<string> }>();

  for (const record of shipRecords) {
    const ship = record.ship;
    const rawShipId = record.externalId;

    const manufacturerCodeInput = coalesce<string | number | undefined>(
      optionalString(ship.manufacturer?.code),
      optionalString(ship.manufacturer?.Code),
      optionalString((ship as any).Manufacturer?.Code),
      ship.manufacturer_id,
      ship.manufacturer?.id
    );
    const manufacturerName =
      optionalString((ship as any).Manufacturer?.Name) ??
      optionalString((ship as any).manufacturer?.Name);
    const manufacturerDescription = optionalString((ship as any).Manufacturer?.Description);
    const manufacturerExternalRefs: NormalizedExternalReference[] = [];
    if (ship.manufacturer_id !== undefined) {
      manufacturerExternalRefs.push({ source: 'raw:ships.manufacturer_id', id: String(ship.manufacturer_id) });
    }
    if (ship.manufacturer?.id !== undefined) {
      manufacturerExternalRefs.push({ source: 'raw:ships.manufacturer.id', id: String(ship.manufacturer.id) });
    }
    if (ship.manufacturer?.code) {
      manufacturerExternalRefs.push({ source: 'raw:ships.manufacturer.code', id: ship.manufacturer.code });
    }
    if ((ship as any).Manufacturer?.Code) {
      manufacturerExternalRefs.push({ source: 'raw:ships.Manufacturer.Code', id: String((ship as any).Manufacturer?.Code) });
    }
    const manufacturerCode = registerManufacturer(
      manufacturerCodeInput !== undefined ? String(manufacturerCodeInput) : undefined,
      'ship',
      rawShipId,
      {
        name: manufacturerName,
        description: manufacturerDescription,
        externalRefs: manufacturerExternalRefs
      }
    );

    const displayName =
      optionalString(ship.name) ?? optionalString(ship.Name) ?? optionalString(ship.ClassName) ?? rawShipId;

    const manufacturerTokens = collectManufacturerTokens(ship, manufacturerCode);
    const { familyTokens, variantTokens } = deriveFamilyAndVariant(ship, manufacturerTokens);

    const familyName = familyTokens.length ? familyTokens.join('_') : 'HULL';
    const variantCode: CanonicalVariantCode = variantTokens.length ? variantTokens.join('_') : 'BASE';
    const hullKey = buildHullKey(manufacturerCode, familyName);

    shipIdToHullKey.set(rawShipId, hullKey);
    hullMetadata.set(hullKey, {
      baseTokens: [...familyTokens],
      manufacturerTokens: new Set(manufacturerTokens)
    });

    const shipRefs: NormalizedExternalReference[] = [
      { source: 'raw:ships.primary', id: rawShipId }
    ];
    if (ship.UUID && ship.UUID !== rawShipId) {
      shipRefs.push({ source: 'raw:ships.UUID', id: String(ship.UUID) });
    }
    if (ship.ClassName) {
      shipRefs.push({ source: 'raw:ships.ClassName', id: String(ship.ClassName) });
    }
    if (ship.Name && ship.Name !== ship.ClassName) {
      shipRefs.push({ source: 'raw:ships.Name', id: String(ship.Name) });
    }
    addExternalRefs(shipExternalRefs, hullKey, shipRefs);

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
        manufacturer_code: manufacturerCode,
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
        baseTokens: [...familyTokens],
        variantTokens: [...variantTokens],
        names: new Set<string>(),
        descriptions: new Set<string>(),
        records: []
      };
      variantGroups.set(canonicalVariantId, group);
    } else {
      if (!group.baseTokens.length && familyTokens.length) {
        group.baseTokens = [...familyTokens];
      }
      if (!group.variantTokens.length && variantTokens.length) {
        group.variantTokens = [...variantTokens];
      }
    }

    const loadoutRecord: VariantLoadoutRecord = {
      variantId: canonicalVariantId,
      record,
      profile: editionInfo.editionCode,
      livery: editionInfo.livery ?? undefined,
      isEditionOnly: editionOnly
    };
    group.records.push(loadoutRecord);

    addExternalRefs(variantExternalRefs, canonicalVariantId, [
      { source: 'raw:ships.id', id: record.externalId }
    ]);

    if (editionInfo.livery) {
      let paints = shipPaints.get(hullKey);
      if (!paints) {
        paints = new Set();
        shipPaints.set(hullKey, paints);
      }
      paints.add(editionInfo.livery);
    }

    if (!editionOnly && displayName) {
      const trimmedName = stripManufacturerPrefix(displayName, manufacturerTokens);
      if (trimmedName) {
        group.names.add(trimmedName);
      }
      group.names.add(displayName);
    }
    if (editionInfo.editionCode) {
      group.names.add(canonicalVariantName(group.baseName, variantCode));
    }
    if (descriptionCandidate) {
      group.descriptions.add(descriptionCandidate);
    }

    group.names.add(canonicalVariantName(group.baseName, variantCode));
  }

  const ships: NormalizedShip[] = sortByExternalId([...hullMap.values()]);

  const rawVariants = await readJsonOrDefault<RawVariant[]>(join(rawDir, 'ship_variants.json'), []);
  for (const variant of rawVariants) {
    const rawVariantId = asExternalId(variant.id);
    const parentShipId = asExternalId(variant.ship_id);
    const hullKey = shipIdToHullKey.get(parentShipId);
    if (!hullKey) continue;
    const hullMeta = hullMetadata.get(hullKey);
    const manufacturerTokens = hullMeta?.manufacturerTokens ?? new Set<string>();
    const baseTokens = hullMeta?.baseTokens ?? [];
    const candidateTokens = [
      ...tokenizeIdentifier(optionalString(variant.variant_code)),
      ...tokenizeIdentifier(optionalString(variant.name))
    ];
    const derivedTokens = deriveVariantTokensFromCandidates(candidateTokens, baseTokens, manufacturerTokens);
    const variantCode = derivedTokens.length ? derivedTokens.join('_') : 'BASE';
    const canonicalVariantId = toCanonicalVariantExtId(hullKey, variantCode);
    rawToCanonicalVariant.set(rawVariantId, canonicalVariantId);
    const group = variantGroups.get(canonicalVariantId);
    if (group) {
      if (!group.baseTokens.length && baseTokens.length) {
        group.baseTokens = [...baseTokens];
      }
      if (!group.variantTokens.length && derivedTokens.length) {
        group.variantTokens = [...derivedTokens];
      }
      group.variantCode = variantCode;
      group.names.add(canonicalVariantName(group.baseName, variantCode));
      if (variant.name) {
        group.names.add(variant.name);
      }
      if (variant.description) {
        group.descriptions.add(variant.description);
      }
    } else {
      const hull = hullMap.get(hullKey);
      const baseName = hull ? hull.name : hullKey.replace(/_/g, ' ');
      const newGroup: CanonicalVariantGroup = {
        variantId: canonicalVariantId,
        hullKey,
        variantCode,
        baseName,
        baseTokens: [...baseTokens],
        variantTokens: [...derivedTokens],
        names: new Set<string>(),
        descriptions: new Set<string>(),
        records: []
      };
      newGroup.names.add(canonicalVariantName(baseName, variantCode));
      if (variant.name) newGroup.names.add(variant.name);
      if (variant.description) newGroup.descriptions.add(variant.description);
      variantGroups.set(canonicalVariantId, newGroup);
    }

    const variantRefs: NormalizedExternalReference[] = [
      { source: 'raw:ship_variants.id', id: rawVariantId },
      { source: 'raw:ship_variants.ship_id', id: String(variant.ship_id) }
    ];
    if (variant.variant_code) {
      variantRefs.push({ source: 'raw:ship_variants.code', id: variant.variant_code });
    }
    addExternalRefs(variantExternalRefs, canonicalVariantId, variantRefs);

    if (variant.thumbnail) {
      variantThumbnails.set(canonicalVariantId, variant.thumbnail);
    }
    if ((variant as any).release_patch) {
      variantReleasePatch.set(canonicalVariantId, String((variant as any).release_patch));
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
  const itemsV2Draft: NormalizedItemV2[] = [];
  const itemsV2Index = new Map<string, NormalizedItemV2>();

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

    const itemRefs: NormalizedExternalReference[] = [{ source: 'raw:items.primary', id: externalId }];
    if (item.reference) {
      itemRefs.push({ source: 'raw:items.reference', id: String(item.reference) });
    }
    if (item.className) {
      itemRefs.push({ source: 'raw:items.className', id: String(item.className) });
    }
    if (item.itemName) {
      itemRefs.push({ source: 'raw:items.itemName', id: String(item.itemName) });
    }
    if (item.stdItem?.UUID) {
      itemRefs.push({ source: 'raw:items.stdItem.UUID', id: String(item.stdItem.UUID) });
    }
    if (item.stdItem?.Name) {
      itemRefs.push({ source: 'raw:items.stdItem.Name', id: String(item.stdItem.Name) });
    }
    addExternalRefs(itemExternalRefs, externalId, itemRefs);

    const relevant = isAllowedItem(item, transformConfig.allowedItemTypes);
    itemRelevance.set(externalId, relevant);
    if (!relevant) continue;

    const baseType =
      optionalString(item.type) ?? (item.stdItem?.Type ? item.stdItem.Type.split('.')[0] : undefined) ?? 'Unknown';

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

    const manufacturerName = optionalString(item.stdItem?.Manufacturer?.Name);
    const manufacturerCodeInput = coalesce<string | number | undefined>(
      optionalString(item.stdItem?.Manufacturer?.Code),
      optionalString(item.manufacturer),
      item.manufacturer_id,
      manufacturerName
    );
    const manufacturerDescription = optionalString(item.stdItem?.Manufacturer?.Description);
    const itemManufacturerRefs: NormalizedExternalReference[] = [];
    if (item.manufacturer_id !== undefined) {
      itemManufacturerRefs.push({ source: 'raw:items.manufacturer_id', id: String(item.manufacturer_id) });
    }
    if (item.manufacturer) {
      itemManufacturerRefs.push({ source: 'raw:items.manufacturer', id: String(item.manufacturer) });
    }
    if (item.stdItem?.Manufacturer?.Code) {
      itemManufacturerRefs.push({ source: 'raw:items.stdItem.Manufacturer.Code', id: String(item.stdItem.Manufacturer.Code) });
    }
    const manufacturerCode = manufacturerCodeInput !== undefined
      ? registerManufacturer(String(manufacturerCodeInput), 'item', externalId, {
          name: manufacturerName,
          description: manufacturerDescription,
          externalRefs: itemManufacturerRefs
        })
      : undefined;

    const rawClass =
      optionalString(item.class) ??
      (item.stdItem?.Type ? item.stdItem.Type.split('.')[0] : undefined);

    const sizeValue = optionalNumber(item.size ?? item.stdItem?.Size);
    const gradeValue = optionalString(
      item.grade ?? (typeof item.stdItem?.Grade === 'number' ? item.stdItem.Grade.toString() : item.stdItem?.Grade)
    );
    const normalizedClass = normalizeItemClass(rawClass);
    const description = optionalString(item.description) ?? optionalString(item.stdItem?.Description);

    normalizedItems.push({
      external_id: externalId,
      type: baseType,
      subtype,
      name: resolvedName,
      manufacturer_code: manufacturerCode,
      size: sizeValue,
      grade: gradeValue,
      class: normalizedClass,
      description
    } satisfies NormalizedItem);

    const statsSeed: Record<string, unknown> = {};
    if (item.stdItem) statsSeed.std_item = item.stdItem;
    const classification = (item as any)?.classification ?? (item as any)?.Classification;
    if (classification !== undefined) statsSeed.classification = classification;
    const tags = (item as any)?.tags ?? (item as any)?.Tags;
    if (tags !== undefined) statsSeed.tags = tags;

    const nextItem: NormalizedItemV2 = {
      external_id: externalId,
      name: resolvedName,
      company_code: manufacturerCode,
      type: normalizeTypeToken(baseType) ?? baseType.toUpperCase(),
      subtype,
      size: sizeValue,
      grade: gradeValue,
      class: normalizedClass,
      description,
      external_refs: [],
      stats: statsSeed,
      date_created: undefined // TODO: wire up item creation timestamp when source exposes it.
    };

    itemsV2Draft.push(nextItem);
    itemsV2Index.set(externalId, nextItem);
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
  const itemStatsMap = new Map<string, NormalizedItemStat>();
  for (const entry of itemStats) {
    itemStatsMap.set(entry.item_external_id, entry);
  }
  for (const [itemId, statEntry] of itemStatsMap.entries()) {
    const draft = itemsV2Index.get(itemId);
    if (!draft) continue;
    draft.stats = { ...draft.stats, ...statEntry.stats };
    if (statEntry.price_auec !== undefined) {
      draft.stats.price_auec = statEntry.price_auec;
    }
    if (statEntry.availability !== undefined) {
      draft.stats.availability = statEntry.availability;
    }
  }

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
  const shipStatsMap = new Map<string, Record<string, unknown>>();
  for (const entry of shipStats) {
    shipStatsMap.set(entry.ship_variant_external_id, entry.stats);
  }

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

  for (const [itemId, refs] of itemExternalRefs.entries()) {
    const draft = itemsV2Index.get(itemId);
    if (draft) {
      draft.external_refs = refSetToArray(refs);
    }
  }

  const companiesV2: NormalizedCompanyV2[] = manufacturers.map((manufacturer) => ({
    code: manufacturer.code,
    name: manufacturer.name,
    external_refs: sortExternalRefs(refSetToArray(companyRefs.get(manufacturer.code)))
  }));

  const shipsV2: NormalizedShipV2[] = ships.map((ship) => {
    const paints = shipPaints.get(ship.external_id);
    return {
      external_id: ship.external_id,
      name: ship.name,
      company_code: ship.manufacturer_code,
      external_refs: sortExternalRefs(refSetToArray(shipExternalRefs.get(ship.external_id))),
      paints: paints && paints.size ? [...paints].sort() : undefined
    } satisfies NormalizedShipV2;
  });

  const hardpointsV2Collection = new Map<string, NormalizedHardpointV2[]>();
  const hardpointsV2: NormalizedHardpointV2[] = hardpoints.map((hp) => {
    const converted: NormalizedHardpointV2 = {
      external_id: hp.external_id,
      ship_variant_external: hp.ship_variant_external_id,
      code: hp.code,
      category: hp.category,
      position: hp.position,
      size: hp.size,
      gimballed: hp.gimballed,
      powered: hp.powered,
      seats: hp.seats
    };
    if (!hardpointsV2Collection.has(converted.ship_variant_external)) {
      hardpointsV2Collection.set(converted.ship_variant_external, []);
    }
    hardpointsV2Collection.get(converted.ship_variant_external)!.push(converted);
    return converted;
  });

  for (const bucket of hardpointsV2Collection.values()) {
    bucket.sort((a, b) => a.external_id.localeCompare(b.external_id));
  }

  const shipVariantsV2: NormalizedShipVariantV2[] = sortByExternalId(
    shipVariants.map((variant) => {
      const group = variantGroups.get(variant.external_id);
      const refs = sortExternalRefs(refSetToArray(variantExternalRefs.get(variant.external_id)));
      if (!refs.length) {
        refs.push({ source: 'normalized:ship_variant', id: variant.external_id });
      }
      const statsPayload = buildShipVariantStatsV2(
        variant.external_id,
        group,
        shipStatsMap.get(variant.external_id)
      );
      if (!transformConfig.hardpointsAsCollection) {
        const hardpointBundle = hardpointsV2Collection.get(variant.external_id);
        if (hardpointBundle && hardpointBundle.length) {
          statsPayload.hardpoints = hardpointBundle;
        }
      }
      const variantCode = group?.variantCode ?? extractVariantCode(variant.name ?? variant.external_id);
      const fallbackBaseName = group?.baseName ?? variant.ship_external_id.replace(/_/g, ' ');
      const fallbackName = canonicalVariantName(fallbackBaseName, variantCode);
      return {
        external_id: variant.external_id,
        ship_external: variant.ship_external_id,
        name: variant.name ?? fallbackName,
        variant_code: variant.variant_code,
        external_refs: refs,
        thumbnail: variantThumbnails.get(variant.external_id),
        release_patch: variantReleasePatch.get(variant.external_id),
        stats: statsPayload,
        date_created: undefined // TODO: derive ship variant creation timestamp from SC Data Dumper exports.
      } satisfies NormalizedShipVariantV2;
    })
  );

  const itemsV2: NormalizedItemV2[] = sortByExternalId(itemsV2Draft).map((item) => ({
    ...item,
    external_refs: sortExternalRefs(item.external_refs)
  }));

  const bundleV2: NormalizedBundleV2 = {
    channel,
    version,
    companies: companiesV2,
    ships: shipsV2,
    ship_variants: shipVariantsV2,
    items: itemsV2,
    hardpoints: transformConfig.hardpointsAsCollection ? hardpointsV2 : undefined
  };

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

  await writeJson(join(normalizedDir, 'companies.v2.json'), bundleV2.companies);
  await writeJson(join(normalizedDir, 'ships.v2.json'), bundleV2.ships);
  await writeJson(join(normalizedDir, 'ship_variants.v2.json'), bundleV2.ship_variants);
  await writeJson(join(normalizedDir, 'items.v2.json'), bundleV2.items);
  if (bundleV2.hardpoints) {
    await writeJson(join(normalizedDir, 'hardpoints.v2.json'), bundleV2.hardpoints);
  } else {
    await writeJson(join(normalizedDir, 'hardpoints.v2.json'), []);
  }

  log.info('Transformation complete', {
    manufacturers: bundle.manufacturers.length,
    ships: bundle.ships.length,
    variants: bundle.ship_variants.length,
    items: bundle.items.length,
    companies_v2: bundleV2.companies.length,
    ships_v2: bundleV2.ships.length,
    variants_v2: bundleV2.ship_variants.length,
    items_v2: bundleV2.items.length,
    hardpoints_mode: transformConfig.hardpointsAsCollection ? 'collection' : 'embedded'
  });

  return bundle;
}
