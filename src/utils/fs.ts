import { access, mkdir, readFile, writeFile } from 'node:fs/promises';
import { constants } from 'node:fs';
import { dirname } from 'node:path';

export async function ensureDir(path: string) {
  await mkdir(path, { recursive: true });
}

export async function pathExists(path: string): Promise<boolean> {
  try {
    await access(path, constants.F_OK);
    return true;
  } catch {
    return false;
  }
}

export async function readJson<T>(file: string): Promise<T> {
  const buf = await readFile(file, 'utf8');
  return JSON.parse(buf) as T;
}

export async function readJsonOrDefault<T>(file: string, fallback: T): Promise<T> {
  if (!(await pathExists(file))) return fallback;
  return readJson<T>(file);
}

export async function writeJson(file: string, data: unknown) {
  await ensureDir(dirname(file));
  await writeFile(file, JSON.stringify(data, null, 2), 'utf8');
}
