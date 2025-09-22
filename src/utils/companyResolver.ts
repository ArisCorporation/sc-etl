import { readByQuery, createOne } from './directus.js';
import { log } from './log.js';

interface CompanyRow {
  id: string;
  code?: string | null;
}

const DEFAULT_COLLECTION = process.env.SC_COMPANY_COLLECTION ?? 'sc_companies';

function normalizeCode(input: string): string {
  const trimmed = input.trim();
  if (!trimmed) {
    throw new Error('Company code cannot be empty.');
  }
  return trimmed.toUpperCase();
}

async function fetchCompanies(collection: string): Promise<CompanyRow[]> {
  const limit = 200;
  let offset = 0;
  const rows: CompanyRow[] = [];

  while (true) {
    const batch = await readByQuery<CompanyRow>(collection, {
      fields: ['id', 'code'],
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

export class CompanyResolver {
  private readonly collection: string;
  private warmed = false;
  private cache = new Map<string, string>();

  constructor(collection: string = DEFAULT_COLLECTION) {
    this.collection = collection;
  }

  async warmup(): Promise<void> {
    if (this.warmed) return;
    const rows = await fetchCompanies(this.collection);
    for (const row of rows) {
      if (!row.code) continue;
      try {
        const normalized = normalizeCode(row.code);
        this.cache.set(normalized, row.id);
      } catch (error) {
        log.warn('Skipping company with invalid code', {
          collection: this.collection,
          id: row.id,
          error: error instanceof Error ? error.message : String(error)
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

  async resolveId(code: string): Promise<string> {
    await this.ensureWarm();
    const normalized = normalizeCode(code);
    const cached = this.cache.get(normalized);
    if (cached) return cached;

    const payload = {
      code: normalized,
      name: normalized,
      status: 'published'
    } satisfies Record<string, unknown>;

    const created = await createOne<CompanyRow>(this.collection, payload);
    const id = created.id;
    this.cache.set(normalized, id);
    log.info('Created company placeholder', { collection: this.collection, code: normalized, id });
    return id;
  }
}

