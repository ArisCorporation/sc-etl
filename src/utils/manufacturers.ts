import { createOne, readByQuery, updateOne } from './directus.js';
import { log } from './log.js';

interface ManufacturerRow {
  id: string;
  code?: string | null;
  external_id?: string | null;
  name?: string | null;
  content?: string | null;
}

interface ManufacturerDetails {
  name?: string;
  description?: string;
}

function normaliseCode(raw: string): string {
  const trimmed = raw.trim();
  if (!trimmed) {
    throw new Error('Manufacturer code cannot be empty.');
  }
  return trimmed.toUpperCase();
}

// ASSUMPTION: Directus stores manufacturers inside the `companies` collection.
const COLLECTION = 'companies';

async function fetchAllManufacturers(): Promise<ManufacturerRow[]> {
  const limit = 200;
  let offset = 0;
  const rows: ManufacturerRow[] = [];
  while (true) {
    const batch = await readByQuery<ManufacturerRow>(COLLECTION, {
      fields: ['id', 'code', 'external_id', 'name', 'content'],
      limit,
      offset
    });
    if (!batch.length) break;
    rows.push(...batch);
    if (batch.length < limit) break;
    offset += limit;
  }
  return rows;
}

export class ManufacturerResolver {
  private cache = new Map<string, ManufacturerRow>();
  private warmed = false;

  async warmup(): Promise<void> {
    if (this.warmed) return;
    const rows = await fetchAllManufacturers();
    for (const row of rows) {
      if (!row.code && !row.external_id) continue;
      try {
        const normalised = normaliseCode((row.code ?? row.external_id ?? '').toString());
        this.cache.set(normalised, { ...row, code: normalised, external_id: normalised });
      } catch (error) {
        log.warn('Skipping manufacturer with invalid code', {
          id: row.id,
          error: (error as Error).message
        });
      }
    }
    this.warmed = true;
  }

  private async ensureWarm(): Promise<void> {
    if (!this.warmed) {
      await this.warmup();
    }
  }

  async resolveId(code: string, details?: ManufacturerDetails): Promise<string> {
    await this.ensureWarm();
    const normalised = normaliseCode(code);
    const existing = this.cache.get(normalised);
    if (existing) {
      if (details) {
        const patch: Record<string, unknown> = {};
        if (details.name && details.name !== existing.name) {
          patch.name = details.name;
        }
        if (details.description !== undefined && details.description !== existing.content) {
          patch.content = details.description;
        }
        if (Object.keys(patch).length) {
          await updateOne(COLLECTION, existing.id, patch);
          this.cache.set(normalised, { ...existing, ...patch });
        }
      }
      return existing.id;
    }

    const payload = {
      external_id: normalised,
      code: normalised,
      name: details?.name ?? normalised,
      content: details?.description ?? null,
      status: 'published'
    } satisfies Record<string, unknown>;

    const created = await createOne<ManufacturerRow>(COLLECTION, payload);
    const id = created.id;
    this.cache.set(normalised, {
      id,
      code: normalised,
      external_id: normalised,
      name: created.name ?? (payload.name as string | undefined),
      content: created.content ?? (payload.content as string | null)
    });
    log.info('Created missing manufacturer', { code: normalised, id });
    return id;
  }
}
