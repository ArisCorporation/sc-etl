import { log } from '../utils/log.js';

export interface TransformConfig {
  allowedItemTypes: Set<string>;
  hardpointsAsCollection: boolean;
}

function normalizeToken(value: string): string | undefined {
  const trimmed = value.trim();
  if (!trimmed) return undefined;
  return trimmed.toUpperCase();
}

function parseAllowedItemTypes(raw: string | undefined): Set<string> {
  if (!raw) return new Set();

  const candidates: string[] = [];
  const trimmed = raw.trim();

  if (trimmed.startsWith('[')) {
    try {
      const parsed = JSON.parse(trimmed);
      if (Array.isArray(parsed)) {
        for (const entry of parsed) {
          if (typeof entry === 'string') {
            candidates.push(entry);
          } else if (entry !== null && entry !== undefined) {
            log.warn('Ignoring non-string ALLOWED_ITEM_TYPES entry from JSON payload', {
              entry
            });
          }
        }
      }
    } catch (error) {
      log.warn('Failed to parse ALLOWED_ITEM_TYPES as JSON array, falling back to CSV parsing', {
        error: error instanceof Error ? error.message : String(error)
      });
    }
  }

  if (!candidates.length) {
    candidates.push(
      ...trimmed
        .split(/[,\s]+/)
        .map((token) => token.trim())
        .filter(Boolean)
    );
  }

  const normalized = new Set<string>();
  for (const token of candidates) {
    const upper = normalizeToken(token);
    if (upper) normalized.add(upper);
  }
  return normalized;
}

function parseBoolean(raw: string | undefined, defaultValue: boolean): boolean {
  if (!raw) return defaultValue;
  const normalized = raw.trim().toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
  log.warn('Unable to parse boolean env flag, falling back to default', {
    value: raw
  });
  return defaultValue;
}

export function loadTransformConfig(overrides?: Partial<TransformConfig>): TransformConfig {
  const allowedItemTypes = overrides?.allowedItemTypes ?? parseAllowedItemTypes(process.env.ALLOWED_ITEM_TYPES);
  const hardpointsAsCollection =
    overrides?.hardpointsAsCollection ??
    parseBoolean(process.env.HARDPOINTS_AS_COLLECTION, true);

  return {
    allowedItemTypes,
    hardpointsAsCollection
  };
}
