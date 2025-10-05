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

const directusApiBase = directusUrl.replace(/\/$/, '');
const collectionVersionSupport = new Map<string, boolean>();

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

interface VersionOptions {
  name: string;
  status?: string;
  key?: string;
  promote?: boolean;
}

async function createItemVersion (
  collection: string,
  key: string | number,
  item: AnyRecord,
  options: VersionOptions
): Promise<void> {
  const support = collectionVersionSupport.get(collection);
  if (support === false) {
    return;
  }
  const versionName = options.name;
  const versionKey = options.key ?? versionName;
  const itemId = String(key);
  const payload: Record<string, unknown> = {
    collection,
    item: itemId,
    key: versionKey,
    name: versionName
  };
  // Optional: only include status if the API supports it; omit otherwise.

  async function findExistingVersionId(): Promise<string | undefined> {
    const params = new URLSearchParams({
      'filter[collection][_eq]': collection,
      'filter[item][_eq]': itemId,
      'filter[key][_eq]': versionKey,
      limit: '1'
    });
    const existingResponse = await fetch(`${directusApiBase}/versions?${params.toString()}`, {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${directusToken}`
      }
    });

    if (!existingResponse.ok) {
      const text = await existingResponse.text().catch(() => undefined);
      log.warn('Failed to fetch existing Directus version', {
        collection,
        item: itemId,
        version: versionKey,
        status: existingResponse.status,
        body: text
      });
      return undefined;
    }

    try {
      const body: any = await existingResponse.json();
      const data = body?.data;
      if (!data) return undefined;
      if (Array.isArray(data)) {
        const [first] = data;
        const rawId = (first as AnyRecord)?.id;
        return typeof rawId === 'string' ? rawId : undefined;
      }
      if (typeof data === 'object') {
        const rawId = (data as AnyRecord)?.id;
        return typeof rawId === 'string' ? rawId : undefined;
      }
    } catch (error) {
      log.warn('Failed to parse Directus version lookup response', {
        collection,
        item: itemId,
        version: versionKey,
        error: error instanceof Error ? error.message : String(error)
      });
    }
    return undefined;
  }

  const createResponse = await fetch(`${directusApiBase}/versions`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${directusToken}`
    },
    body: JSON.stringify(payload)
  });

  let versionId: string | undefined;

  if (!createResponse.ok) {
    const text = await createResponse.text();
    if (createResponse.status === 422 && /already exists/i.test(text)) {
      versionId = await findExistingVersionId();
      if (!versionId) {
        log.warn('Directus version already exists but could not be retrieved', {
          collection,
          item: itemId,
          version: versionKey
        });
        collectionVersionSupport.set(collection, true);
        return;
      }

      const updatePayload: Record<string, unknown> = {
        key: versionKey,
        name: versionName
      };
      if (options.status !== undefined) {
        updatePayload.status = options.status;
      }

      const updateResponse = await fetch(`${directusApiBase}/versions/${versionId}`, {
        method: 'PATCH',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${directusToken}`
        },
        body: JSON.stringify(updatePayload)
      });

      if (!updateResponse.ok) {
        const updateText = await updateResponse.text().catch(() => undefined);
        log.warn('Failed to update Directus version metadata; continuing with save', {
          collection,
          item: itemId,
          version: versionKey,
          status: updateResponse.status,
          body: updateText
        });
      }
    } else if (createResponse.status === 404 && text.includes('/versions')) {
      collectionVersionSupport.set(collection, false);
      log.warn('Directus content versioning endpoint not available; skipping version creation', {
        collection,
        status: createResponse.status
      });
      return;
    } else {
      const error = new Error(
        `Failed to create Directus content version for ${collection} ${itemId}: ${createResponse.status} ${createResponse.statusText}`
      );
      (error as any).directus = {
        status: createResponse.status,
        statusText: createResponse.statusText,
        body: text
      };
      throw error;
    }
  } else {
    try {
      const body: any = await createResponse.json();
      const data = body?.data;
      if (data) {
        if (typeof data === 'object' && !Array.isArray(data)) {
          const rawId = (data as AnyRecord)?.id;
          if (typeof rawId === 'string') versionId = rawId;
        } else if (Array.isArray(data) && data.length) {
          const rawId = (data[0] as AnyRecord)?.id;
          if (typeof rawId === 'string') versionId = rawId;
        }
      }
    } catch (error) {
      log.warn('Failed to parse Directus content version response', {
        collection,
        error: error instanceof Error ? error.message : String(error)
      });
    }
  }

  if (!versionId) {
    log.warn('Directus version response missing id; skipping save step', {
      collection,
      item: itemId
    });
    collectionVersionSupport.set(collection, true);
    return;
  }

  const saveResponse = await fetch(`${directusApiBase}/versions/${versionId}/save`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${directusToken}`
    },
    body: JSON.stringify({ data: item })
  });

  if (!saveResponse.ok) {
    const text = await saveResponse.text();
    if (saveResponse.status === 404 && text.includes('/versions')) {
      collectionVersionSupport.set(collection, false);
      log.warn('Directus content version save endpoint not available; skipping version creation', {
        collection,
        status: saveResponse.status
      });
      return;
    }
    const error = new Error(
      `Failed to save Directus content version snapshot for ${collection} ${itemId}: ${saveResponse.status} ${saveResponse.statusText}`
    );
    (error as any).directus = {
      status: saveResponse.status,
      statusText: saveResponse.statusText,
      body: text
    };
    throw error;
  }

  const shouldPromote = options.promote !== false;

  let mainHash: string | undefined;
  try {
    const body: any = await saveResponse.json();
    const extractHash = (value: unknown): string | undefined => {
      if (!value || typeof value !== 'object') return undefined;
      const record = value as AnyRecord;
      const meta = typeof record.meta === 'object' && record.meta !== null ? (record.meta as AnyRecord) : undefined;
      const candidates = [
        record['hash'],
        record['mainHash'],
        record['main_hash'],
        meta?.['hash'],
        meta?.['mainHash'],
        meta?.['main_hash']
      ];
      for (const candidate of candidates) {
        if (typeof candidate === 'string' && candidate) {
          return candidate;
        }
      }
      return undefined;
    };

    const directHash = extractHash(body);
    if (directHash) {
      mainHash = directHash;
    } else {
      const data = body?.data;
      if (Array.isArray(data)) {
        for (const entry of data) {
          const fromEntry = extractHash(entry);
          if (fromEntry) {
            mainHash = fromEntry;
            break;
          }
        }
      } else {
        const fromData = extractHash(data);
        if (fromData) {
          mainHash = fromData;
        }
      }
    }
  } catch (error) {
    log.warn('Failed to parse Directus content version save response for promotion metadata', {
      collection,
      item: itemId,
      version: versionKey,
      error: error instanceof Error ? error.message : String(error)
    });
  }

  async function fetchVersionHash (): Promise<string | undefined> {
    const response = await fetch(`${directusApiBase}/versions/${versionId}/compare`, {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${directusToken}`
      }
    });

    if (!response.ok) {
      const text = await response.text().catch(() => undefined);
      log.warn('Failed to retrieve Directus main version hash for promotion', {
        collection,
        item: itemId,
        version: versionKey,
        status: response.status,
        body: text
      });
      return undefined;
    }

    try {
      const body: any = await response.json();
      const data = body?.data ?? body;
      if (Array.isArray(data)) {
        for (const entry of data) {
          if (entry && typeof entry === 'object') {
            const candidate = (entry as AnyRecord)['mainHash'] ?? (entry as AnyRecord)['main_hash'];
            if (typeof candidate === 'string' && candidate) {
              return candidate;
            }
          }
        }
      } else if (data && typeof data === 'object') {
        const record = data as AnyRecord;
        const candidate = record['mainHash'] ?? record['main_hash'];
        if (typeof candidate === 'string' && candidate) {
          return candidate;
        }
      }
    } catch (error) {
      log.warn('Failed to parse Directus version detail response for promotion metadata', {
        collection,
        item: itemId,
        version: versionKey,
        error: error instanceof Error ? error.message : String(error)
      });
    }

    return undefined;
  }

  if (shouldPromote && !mainHash) {
    mainHash = await fetchVersionHash();
  }

  if (!shouldPromote) {
    collectionVersionSupport.set(collection, true);
    return;
  }

  if (!mainHash) {
    log.warn('Directus version save response missing mainHash; skipping promotion', {
      collection,
      item: itemId,
      version: versionKey
    });
    collectionVersionSupport.set(collection, true);
    return;
  }

  const promoteResponse = await fetch(`${directusApiBase}/versions/${versionId}/promote`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${directusToken}`
    },
    body: JSON.stringify({ mainHash })
  });

  if (!promoteResponse.ok) {
    const text = await promoteResponse.text().catch(() => undefined);
    if (promoteResponse.status === 404 && text && text.includes('/versions')) {
      log.warn('Directus content version promote endpoint not available; leaving snapshot without promotion', {
        collection,
        status: promoteResponse.status
      });
    } else {
      log.warn('Failed to promote Directus content version to main', {
        collection,
        item: itemId,
        version: versionKey,
        status: promoteResponse.status,
        body: text
      });
    }
  }

  collectionVersionSupport.set(collection, true);
}

export async function createOneWithVersion<T = AnyRecord>(
  collection: string,
  item: AnyRecord,
  versionName: string,
  promote: boolean = true
): Promise<T> {
  const created = await createOne<T>(collection, item);
  const rawId = (created as AnyRecord)?.id;
  const id = typeof rawId === 'string' || typeof rawId === 'number' ? rawId : undefined;
  if (id === undefined) {
    throw new Error(`Directus createOne for ${collection} did not return an id.`);
  }
  await createItemVersion(collection, id, item, { name: versionName, promote });
  return created;
}

export async function updateOneWithVersion<T = AnyRecord>(
  collection: string,
  key: string | number,
  item: AnyRecord,
  versionName: string,
  promote: boolean = true
): Promise<T> {
  const updated = await updateOne<T>(collection, key, item);
  const rawId = (updated as AnyRecord)?.id;
  const id = typeof rawId === 'string' || typeof rawId === 'number' ? rawId : key;
  await createItemVersion(collection, id, item, { name: versionName, promote });
  return updated;
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
