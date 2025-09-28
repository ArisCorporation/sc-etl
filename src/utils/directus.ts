import {
  createDirectus,
  createItem,
  createItems,
  deleteItems,
  readItems,
  rest,
  staticToken,
  updateItem,
  updateItemsBatch
} from '@directus/sdk';
import { log } from './log.js';

const directusUrl = process.env.DIRECTUS_URL;
const directusToken = process.env.DIRECTUS_TOKEN;

if (!directusUrl || !directusToken) {
  throw new Error('DIRECTUS_URL and DIRECTUS_TOKEN must be configured in environment variables.');
}

export const directus = createDirectus(directusUrl)
  .with(staticToken(directusToken))
  .with(rest());

type AnyRecord = Record<string, unknown>;

interface DirectusContext {
  action: string;
  collection: string;
  payload?: unknown;
}

async function unwrapDirectusResult<T>(
  result: unknown,
  context: DirectusContext
): Promise<T> {
  if (typeof Response !== 'undefined' && result instanceof Response) {
    let body: unknown;
    try {
      if (!result.bodyUsed) {
        const text = await result.text();
        if (text) {
          try {
            body = JSON.parse(text);
          } catch {
            body = text;
          }
        }
      }
    } catch (error) {
      body = { parseError: (error as Error)?.message };
    }

    const headers = Object.fromEntries(result.headers.entries());
    const error = new Error(
      `Directus ${context.action} for ${context.collection} failed with status ${result.status} ${result.statusText}`
    );
    (error as any).directus = {
      collection: context.collection,
      action: context.action,
      status: result.status,
      statusText: result.statusText,
      url: result.url,
      headers,
      body
    };
    if (context.payload !== undefined) {
      (error as any).requestPayload = context.payload;
    }
    throw error;
  }
  return result as T;
}

export async function readByQuery<T = AnyRecord>(
  collection: string,
  query: Record<string, unknown>
): Promise<T[]> {
  const result = (await directus.request(
    readItems(collection as any, query as any)
  )) as unknown;

  if (typeof Response !== 'undefined' && result instanceof Response) {
    throw new Error(
      `Directus query for collection ${collection} failed with status ${result.status} ${result.statusText}`
    );
  }

  if (Array.isArray(result)) {
    return result as T[];
  }

  if (result === null || result === undefined) {
    return [];
  }

  if (typeof result === 'object') {
    if ('data' in (result as Record<string, unknown>)) {
      const { data } = result as { data?: unknown };

      if (Array.isArray(data)) {
        return data as T[];
      }

      if (data === null || data === undefined) {
        return [];
      }

      if (data && typeof data === 'object') {
        return [data as T];
      }

      throw new Error(
        `Directus query for collection ${collection} returned unexpected data shape.`
      );
    }

    if (result instanceof Map) {
      throw new Error(
        `Directus query for collection ${collection} returned a Map; this is not supported.`
      );
    }

    return [result as T];
  }

  throw new Error(
    `Directus query for collection ${collection} returned unexpected response type: ${typeof result}.`
  );
}

export async function createMany<T = AnyRecord>(
  collection: string,
  items: AnyRecord[]
): Promise<T[]> {
  if (!items.length) return [];
  const result = await directus.request(createItems(collection as any, items as any));
  return unwrapDirectusResult<T[]>(result, {
    action: 'createMany',
    collection,
    payload: items
  });
}

export async function createOne<T = AnyRecord>(collection: string, item: AnyRecord): Promise<T> {
  const result = await directus.request(createItem(collection as any, item as any));
  return unwrapDirectusResult<T>(result, {
    action: 'createOne',
    collection,
    payload: item
  });
}

export async function updateMany<T = AnyRecord>(
  collection: string,
  items: AnyRecord[]
): Promise<T[]> {
  if (!items.length) return [];
  const result = await directus.request(updateItemsBatch(collection as any, items as any));
  return unwrapDirectusResult<T[]>(result, {
    action: 'updateMany',
    collection,
    payload: items
  });
}

export async function updateOne<T = AnyRecord>(
  collection: string,
  key: string | number,
  item: AnyRecord
): Promise<T> {
  const result = await directus.request(updateItem(collection as any, key as any, item as any));
  return unwrapDirectusResult<T>(result, {
    action: 'updateOne',
    collection,
    payload: { key, item }
  });
}

export async function deleteMany(collection: string, keys: (string | number)[]): Promise<void> {
  if (!keys.length) return;
  const result = await directus.request(deleteItems(collection as any, keys as any));
  await unwrapDirectusResult(result, {
    action: 'deleteMany',
    collection,
    payload: keys
  });
}

interface UpsertOptions {
  key?: string;
  chunkSize?: number;
}

function chunk<T>(arr: readonly T[], size: number): T[][] {
  if (size <= 0) throw new Error('chunk size must be > 0');
  const result: T[][] = [];
  for (let i = 0; i < arr.length; i += size) {
    result.push(arr.slice(i, i + size));
  }
  return result;
}

export async function upsertByExternalId<T extends AnyRecord>(
  collection: string,
  rows: T[],
  options: UpsertOptions = {}
): Promise<Map<string, string>> {
  const key = options.key ?? 'external_id';
  const chunkSize = Math.min(options.chunkSize ?? 100, 100);
  const idMap = new Map<string, string>();

  const uniqueRows = new Map<string, T>();
  const duplicateKeys = new Set<string>();

  for (const row of rows) {
    const value = row[key];
    if (typeof value !== 'string') {
      throw new Error(`Expected string for ${key} in collection ${collection}`);
    }
    if (uniqueRows.has(value)) {
      duplicateKeys.add(value);
    }
    uniqueRows.set(value, row);
  }

  if (duplicateKeys.size) {
    const sample = Array.from(duplicateKeys).slice(0, 5);
    log.warn(
      `Duplicate ${key} values encountered for collection ${collection}; keeping the last occurrence for each.`,
      { duplicates: sample, totalDuplicates: duplicateKeys.size }
    );
  }

  const uniqueEntries = Array.from(uniqueRows.entries());

  for (const batchEntries of chunk(uniqueEntries, chunkSize)) {
    if (batchEntries.length === 0) continue;

    const keys = batchEntries.map(([k]) => k);
    const batch = batchEntries.map(([, row]) => row);

    if (batch.length === 0) continue;

    const existing = await readByQuery(collection, {
      filter: { [key]: { _in: keys } },
      fields: ['id', key],
      limit: keys.length
    });

    const existingMap = new Map(
      existing.map((row: AnyRecord) => [row[key] as string, row.id as string])
    );

    const toCreate = batch.filter((row) => !existingMap.has(row[key] as string));
    const toUpdate = batch
      .filter((row) => existingMap.has(row[key] as string))
      .map((row) => ({ id: existingMap.get(row[key] as string)!, ...row }));

    if (toCreate.length) {
      await createMany(collection, toCreate as AnyRecord[]);
    }
    if (toUpdate.length) {
      await updateMany(collection, toUpdate as AnyRecord[]);
    }

    const refreshed = await readByQuery(collection, {
      filter: { [key]: { _in: keys } },
      fields: ['id', key],
      limit: keys.length
    });

    for (const row of refreshed) {
      const k = row[key];
      if (typeof k === 'string') {
        idMap.set(k, row.id as string);
      }
    }
  }

  return idMap;
}
