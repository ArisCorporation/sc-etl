import { isDeepStrictEqual } from 'node:util';
import { createMany } from './utils/directus.js';

export type DiffChangeType = 'created' | 'updated' | 'deleted';

export interface DiffEntry {
  entityType: string;
  entityId: string;
  changeType: DiffChangeType;
  diff: Record<string, unknown>;
}

export interface DiffWriterOptions {
  collection?: string;
  skip?: boolean;
  chunkSize?: number;
}

const DEFAULT_COLLECTION = process.env.SC_DIFF_COLLECTION ?? 'diffs';
const DEFAULT_CHUNK_SIZE = 100;

function chunk<T>(items: readonly T[], size: number): T[][] {
  if (size <= 0) throw new Error('chunk size must be greater than zero');
  const result: T[][] = [];
  for (let i = 0; i < items.length; i += size) {
    result.push(items.slice(i, i + size));
  }
  return result;
}

function pickFields(source: Record<string, unknown> | undefined, fields: readonly string[]) {
  if (!source) return {} as Record<string, unknown>;
  const result: Record<string, unknown> = {};
  for (const field of fields) {
    if (Object.prototype.hasOwnProperty.call(source, field)) {
      result[field] = source[field];
    }
  }
  return result;
}

export function computeDiff(
  before: Record<string, unknown> | undefined,
  after: Record<string, unknown> | undefined,
  fields: readonly string[]
): Record<string, unknown> | null {
  if (!before && after) {
    return { after: pickFields(after, fields) };
  }
  if (before && !after) {
    return { before: pickFields(before, fields) };
  }
  if (!before || !after) {
    return null;
  }

  const changes: Record<string, { before: unknown; after: unknown }> = {};
  for (const field of fields) {
    const prev = before[field];
    const next = after[field];
    if (!isDeepStrictEqual(prev, next)) {
      changes[field] = { before: prev ?? null, after: next ?? null };
    }
  }

  return Object.keys(changes).length ? changes : null;
}

export class DiffWriter {
  private readonly collection: string;
  private readonly chunkSize: number;
  private readonly skip: boolean;
  private readonly entries: DiffEntry[] = [];

  constructor(options: DiffWriterOptions = {}) {
    this.collection = options.collection ?? DEFAULT_COLLECTION;
    this.skip = Boolean(options.skip);
    this.chunkSize = Math.max(1, options.chunkSize ?? DEFAULT_CHUNK_SIZE);
  }

  addChange(entry: DiffEntry): boolean {
    if (this.skip) return false;
    if (!entry.diff || !Object.keys(entry.diff).length) return false;
    this.entries.push(entry);
    return true;
  }

  async flush(buildId: string): Promise<void> {
    if (this.skip || !this.entries.length) return;
    const nowIso = new Date().toISOString();
    for (const batch of chunk(this.entries, this.chunkSize)) {
      await createMany(this.collection, batch.map((entry) => ({
        build: buildId,
        entity_type: entry.entityType,
        entity_id: entry.entityId,
        change_type: entry.changeType,
        diff: entry.diff,
        date_created: nowIso
      })));
    }
    this.entries.length = 0;
  }
}
