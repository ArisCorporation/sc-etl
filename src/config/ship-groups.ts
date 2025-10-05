import rawShipGroups from '../../schemas/ship-groups.json' with { type: 'json' };
import { log } from '../utils/log.js';
import type { CanonicalVariantCode } from '../lib/canon.js';

interface RawHullConfig {
  manufacturer?: unknown;
  name?: unknown;
  variants?: Record<string, unknown>;
}

interface VariantConfiguration {
  code: string;
  match: string;
}

interface NormalizedVariantConfig {
  matchIds: string[];
  shipVariantIds: string[];
  variantCodes: CanonicalVariantCode[];
  names: string[];
  editions: string[];
  configurations: VariantConfiguration[];
}

interface HullDefinition {
  hullKey: string;
  manufacturer: string;
  name: string;
}

export interface VariantAssignment extends HullDefinition {
  variantCode: CanonicalVariantCode;
  names: string[];
  editions: string[];
  configurations: VariantConfiguration[];
}

function toString(value: unknown): string | undefined {
  if (typeof value === 'string') {
    const trimmed = value.trim();
    return trimmed ? trimmed : undefined;
  }
  if (typeof value === 'number' || typeof value === 'boolean') {
    return String(value);
  }
  return undefined;
}

function collectStrings(value: unknown): string[] {
  if (value === undefined || value === null) return [];
  const source = Array.isArray(value) ? value : [value];
  const result: string[] = [];
  for (const entry of source) {
    const str = toString(entry);
    if (!str) continue;
    result.push(str);
  }
  return result;
}

function dedupeStrings(values: string[]): string[] {
  const seen = new Set<string>();
  const result: string[] = [];
  for (const value of values) {
    const trimmed = value.trim();
    if (!trimmed) continue;
    const key = trimmed.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    result.push(trimmed);
  }
  return result;
}

function sanitizeToken(value: string | undefined): string | undefined {
  if (!value) return undefined;
  const normalized = value
    .replace(/[^a-z0-9]+/gi, '_')
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '')
    .toUpperCase();
  return normalized || undefined;
}

function sanitizeHullKey(value: string | undefined): string {
  return sanitizeToken(value) ?? 'HULL';
}

function sanitizeVariantCode(value: string | undefined): CanonicalVariantCode {
  return sanitizeToken(value) ?? 'BASE';
}

function normalizeVariant(input: unknown): NormalizedVariantConfig {
  if (typeof input === 'string' || Array.isArray(input)) {
    const matchIds = dedupeStrings(collectStrings(input));
    return {
      matchIds,
      shipVariantIds: [],
      variantCodes: [],
      names: [],
      editions: [],
      configurations: []
    };
  }

  if (!input || typeof input !== 'object') {
    return { matchIds: [], shipVariantIds: [], variantCodes: [], names: [], editions: [], configurations: [] };
  }

  const record = input as Record<string, unknown>;
  const matchIds = dedupeStrings([
    ...collectStrings(record.match ?? (record as any).match_id),
    ...collectStrings(record.id),
    ...collectStrings(record.ids),
    ...collectStrings(record.matches)
  ]);

  const shipVariantIds = dedupeStrings(
    collectStrings(record.ship_variant_ids ?? record.shipVariantIds)
  );
  const variantCodes = dedupeStrings(
    collectStrings(
      record.ship_variant_codes ??
        record.shipVariantCodes ??
        record.codes ??
        record.variant_codes ??
        record.aliases
    )
  )
    .map((code) => sanitizeVariantCode(code))
    .filter((code, index, array) => array.indexOf(code) === index);
  const names = dedupeStrings(collectStrings(record.names ?? record.display_names));
  const editions = dedupeStrings(collectStrings(record.editions ?? record.profiles));

  const configurations: VariantConfiguration[] = [];
  const rawConfigurations = record.configurations;
  if (rawConfigurations && typeof rawConfigurations === 'object' && !Array.isArray(rawConfigurations)) {
    for (const [rawCode, rawMatch] of Object.entries(rawConfigurations as Record<string, unknown>)) {
      const match = toString(rawMatch);
      if (!match) continue;
      const code = sanitizeToken(rawCode) ?? sanitizeToken(match);
      if (!code) continue;
      configurations.push({ code, match });
    }
  }

  if (!matchIds.length && configurations.length) {
    const fallbackMatch = configurations[0]?.match;
    if (fallbackMatch) {
      matchIds.push(fallbackMatch);
    }
  }

  return { matchIds, shipVariantIds, variantCodes, names, editions, configurations };
}

function normalizeLookupKey(value: string | undefined): string | undefined {
  if (!value) return undefined;
  const trimmed = value.trim();
  return trimmed ? trimmed.toLowerCase() : undefined;
}

function assignmentsEqual(a: VariantAssignment, b: VariantAssignment): boolean {
  return a.hullKey === b.hullKey && a.variantCode === b.variantCode;
}

export class ShipGrouping {
  private readonly hulls = new Map<string, HullDefinition>();
  private readonly canonicalVariants = new Map<string, VariantAssignment>();
  private readonly perHullVariants = new Map<string, Map<string, VariantAssignment>>();
  private readonly shipIdAssignments = new Map<string, VariantAssignment>();
  private readonly shipVariantIdAssignments = new Map<string, VariantAssignment>();

  constructor(rawConfig: unknown = rawShipGroups) {
    const root = rawConfig && typeof rawConfig === 'object' ? (rawConfig as Record<string, unknown>) : {};
    const rawHulls = root.hulls;

    if (rawHulls && typeof rawHulls === 'object') {
      for (const [rawHullKey, rawHullValue] of Object.entries(rawHulls as Record<string, RawHullConfig>)) {
        const hullKey = sanitizeHullKey(rawHullKey);
        const manufacturer = toString(rawHullValue?.manufacturer) ?? hullKey.split('_')[0] ?? 'UNKNOWN';
        const name = toString(rawHullValue?.name) ?? hullKey;
        const hullDef: HullDefinition = { hullKey, manufacturer, name };
        this.hulls.set(hullKey, hullDef);

        const variantLookup = new Map<string, VariantAssignment>();
        this.perHullVariants.set(hullKey, variantLookup);

        const rawVariants = rawHullValue?.variants;
        if (!rawVariants || typeof rawVariants !== 'object') continue;

        for (const [rawVariantKey, variantValue] of Object.entries(rawVariants)) {
          const variantCode = sanitizeVariantCode(rawVariantKey);
          const normalized = normalizeVariant(variantValue);
          const assignment: VariantAssignment = {
            ...hullDef,
            variantCode,
            names: normalized.names,
            editions: normalized.editions,
            configurations: [...normalized.configurations]
          };

          this.registerVariant(hullKey, assignment, normalized);
        }
      }
    }
  }

  private registerVariant(
    hullKey: string,
    assignment: VariantAssignment,
    config: NormalizedVariantConfig
  ): void {
    const canonicalKey = `${hullKey}:${assignment.variantCode}`;
    const existingCanonical = this.canonicalVariants.get(canonicalKey);
    if (existingCanonical && !assignmentsEqual(existingCanonical, assignment)) {
      log.warn('Ship grouping config collision on canonical variant', {
        hull: hullKey,
        variant: assignment.variantCode
      });
      return;
    }
    this.canonicalVariants.set(canonicalKey, assignment);

    const variantLookup = this.perHullVariants.get(hullKey);
    if (!variantLookup) return;

    const variantCodes = new Set<CanonicalVariantCode>([assignment.variantCode]);
    for (const alias of config.variantCodes) {
      variantCodes.add(alias);
    }

    for (const code of variantCodes) {
      const existing = variantLookup.get(code);
      if (existing && !assignmentsEqual(existing, assignment)) {
        log.warn('Ship grouping variant code collision', {
          hull: hullKey,
          variant: assignment.variantCode,
          code
        });
        continue;
      }
      variantLookup.set(code, assignment);
    }

    const matchIds = config.matchIds.length ? config.matchIds : [assignment.variantCode];
    for (const id of matchIds) {
      this.registerShipId(id, assignment);
    }

    for (const id of config.shipVariantIds) {
      this.registerShipVariantId(id, assignment);
    }
  }

  private registerShipId(id: string, assignment: VariantAssignment): void {
    const key = normalizeLookupKey(id);
    if (!key) return;
    const existing = this.shipIdAssignments.get(key);
    if (existing && !assignmentsEqual(existing, assignment)) {
      log.warn('Ship grouping ship identifier collision', {
        id,
        hull: assignment.hullKey,
        variant: assignment.variantCode
      });
      return;
    }
    this.shipIdAssignments.set(key, assignment);
  }

  private registerShipVariantId(id: string, assignment: VariantAssignment): void {
    const key = normalizeLookupKey(id);
    if (!key) return;
    const existing = this.shipVariantIdAssignments.get(key);
    if (existing && !assignmentsEqual(existing, assignment)) {
      log.warn('Ship grouping ship_variant identifier collision', {
        id,
        hull: assignment.hullKey,
        variant: assignment.variantCode
      });
      return;
    }
    this.shipVariantIdAssignments.set(key, assignment);
  }

  getHull(hullKey: string): HullDefinition | undefined {
    return this.hulls.get(sanitizeHullKey(hullKey));
  }

  lookupShipId(...candidates: (string | undefined)[]): VariantAssignment | undefined {
    for (const candidate of candidates) {
      const key = normalizeLookupKey(candidate);
      if (!key) continue;
      const assignment = this.shipIdAssignments.get(key);
      if (assignment) {
        return assignment;
      }
    }
    return undefined;
  }

  lookupShipVariantId(id: string | undefined): VariantAssignment | undefined {
    const key = normalizeLookupKey(id);
    if (!key) return undefined;
    return this.shipVariantIdAssignments.get(key);
  }

  lookupVariant(hullKey: string, ...candidateCodes: (string | undefined)[]): VariantAssignment | undefined {
    const lookup = this.perHullVariants.get(sanitizeHullKey(hullKey));
    if (!lookup) return undefined;

    for (const candidate of candidateCodes) {
      const normalized = candidate ? sanitizeVariantCode(candidate) : undefined;
      if (!normalized) continue;
      const assignment = lookup.get(normalized);
      if (assignment) return assignment;
    }

    return undefined;
  }

  entries(): VariantAssignment[] {
    return [...this.canonicalVariants.values()];
  }

  resolveConfiguration(assignment: VariantAssignment, ...candidates: (string | undefined)[]): string | undefined {
    if (!assignment.configurations.length) return undefined;
    const normalizedCandidates = candidates
      .map((candidate) => (candidate ? candidate.toLowerCase() : undefined))
      .filter((candidate): candidate is string => Boolean(candidate));
    if (!normalizedCandidates.length) return undefined;

    for (const configuration of assignment.configurations) {
      const normalizedTarget = configuration.match.trim().toLowerCase();
      if (!normalizedTarget) continue;
      for (const candidate of normalizedCandidates) {
        if (candidate.includes(normalizedTarget)) {
          return configuration.code;
        }
      }
    }

    return undefined;
  }
}

export function loadShipGrouping(): ShipGrouping {
  return new ShipGrouping();
}
