import { readByQuery } from './directus.js';
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
    const existing = await this.lookupId(code);
    if (!existing) {
      throw new Error(`Company with code ${code} not found in ${this.collection}`);
    }
    return existing;
  }

  async lookupId(code: string): Promise<string | undefined> {
    await this.ensureWarm();
    try {
      const normalized = normalizeCode(code);
      return this.cache.get(normalized);
    } catch (error) {
      log.warn('Failed to normalise company code during lookup', {
        collection: this.collection,
        code,
        error: error instanceof Error ? error.message : String(error)
      });
      return undefined;
    }
  }
}
