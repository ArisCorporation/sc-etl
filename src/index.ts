#!/usr/bin/env node
import 'dotenv/config';
import { join } from 'node:path';
import { extract } from './extract.js';
import { transform } from './transform.js';
import { validateNormalizedBundle } from './validate.js';
import { loadAll, ensureBuild, readBuildMetadata, type BuildRecord } from './load.js';
import { log } from './utils/log.js';
import type { Channel } from './types/index.js';
import { pathExists } from './utils/fs.js';
import { IngestionRun } from './utils/ingestion.js';

process.on('uncaughtException', (error) => {
  log.error('Uncaught exception', error);
  process.exit(1);
});

type CliArgs = Record<string, string | boolean | string[]>;

function parseCliArgs(tokens: string[]): CliArgs {
  const result: CliArgs = {};

  for (let i = 0; i < tokens.length; i++) {
    let token = tokens[i];
    if (token === '--') continue;
    if (!token.startsWith('--')) continue;

    token = token.slice(2);
    if (!token) continue;

    let value: string | boolean = true;
    let key = token;

    if (token.includes('=')) {
      const [k, v] = token.split(/=(.*)/s, 2);
      key = k;
      value = v ?? true;
    } else {
      const next = tokens[i + 1];
      if (next && !next.startsWith('--')) {
        value = next;
        i++;
      }
    }

    const existing = result[key];
    if (existing === undefined) {
      result[key] = value;
    } else if (Array.isArray(existing)) {
      existing.push(String(value));
    } else {
      result[key] = [String(existing), String(value)];
    }
  }

  return result;
}

function getStringArg(args: CliArgs, key: string): string | undefined {
  const value = args[key];
  if (value === undefined) return undefined;
  if (Array.isArray(value)) return value[value.length - 1];
  if (typeof value === 'boolean') return value ? 'true' : 'false';
  return value;
}

function splitArgTokens(input: string): string[] {
  const tokens: string[] = [];
  const regex = /"([^"]*)"|'([^']*)'|\S+/g;
  let match: RegExpExecArray | null;
  while ((match = regex.exec(input)) !== null) {
    tokens.push(match[1] ?? match[2] ?? match[0]);
  }
  return tokens;
}

function getStringArrayArg(args: CliArgs, key: string): string[] | undefined {
  const value = args[key];
  if (value === undefined) return undefined;
  if (Array.isArray(value)) {
    return value.flatMap((entry) => splitArgTokens(String(entry)));
  }
  return splitArgTokens(String(value));
}

function resolveChannel(value: unknown): Channel {
  if (value === 'LIVE' || value === 'PTU' || value === 'EPTU') return value;
  return 'LIVE';
}

function normalizeBoolean(value: string): boolean | undefined {
  const normalized = value.trim().toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
  return undefined;
}

function resolveBooleanFlag(
  cliValue: unknown,
  envValue: string | undefined,
  fallback: boolean
): boolean {
  if (Array.isArray(cliValue)) {
    const last = cliValue[cliValue.length - 1];
    cliValue = last;
  }
  if (typeof cliValue === 'boolean') return cliValue;
  if (typeof cliValue === 'string') {
    const parsed = normalizeBoolean(cliValue);
    if (parsed !== undefined) return parsed;
  }
  if (typeof envValue === 'string') {
    const parsed = normalizeBoolean(envValue);
    if (parsed !== undefined) return parsed;
  }
  return fallback;
}

function resolveBoolean(value: unknown) {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'string') {
    const parsed = normalizeBoolean(value);
    if (parsed !== undefined) return parsed;
  }
  return false;
}

async function main() {
  const rawArgs = parseCliArgs(process.argv.slice(2));

  const channel = resolveChannel(getStringArg(rawArgs, 'channel') ?? process.env.CHANNEL);
  const version = (getStringArg(rawArgs, 'version') ?? process.env.GAME_VERSION ?? '0.0.0').toString();
  const dataRoot = (getStringArg(rawArgs, 'data-root') ?? process.env.DATA_ROOT ?? './data').toString();
  const skipDiffs = resolveBooleanFlag(rawArgs['skip-diffs'], process.env.SKIP_DIFFS, false);

  const cliP4k = getStringArg(rawArgs, 'p4k');
  const envP4k = process.env.P4K_PATH ?? process.env.DATA_P4K;

  let p4kPath = cliP4k ?? envP4k;
  if (!p4kPath) {
    const candidateDefaults = [join('data', 'p4k', 'Data.p4k'), 'Data.p4k'];
    for (const candidate of candidateDefaults) {
      if (await pathExists(candidate)) {
        p4kPath = candidate;
        break;
      }
    }
  }

  const cliForceUnp4k = resolveBooleanFlag(rawArgs['force-unp4k'], undefined, false);
  const envForceUnp4k =
    typeof process.env.FORCE_UNP4K === 'string' &&
    ['1', 'true', 'yes'].includes(process.env.FORCE_UNP4K.toLowerCase());
  const forceUnp4kProvided = rawArgs['force-unp4k'] !== undefined || process.env.FORCE_UNP4K !== undefined;
  let forceUnp4k = cliForceUnp4k || envForceUnp4k;

  const cliUnp4kArgs = getStringArrayArg(rawArgs, 'unp4k-arg') ?? [];
  const envUnp4kArgs = process.env.UNP4K_ARGS
    ? process.env.UNP4K_ARGS.split(/\s+/).filter(Boolean)
    : [];
  const unp4kArgs = [...cliUnp4kArgs, ...envUnp4kArgs];

  const defaultUnp4kBin = join('bins', 'unp4k', 'unp4k.exe');
  const unp4kBin =
    (getStringArg(rawArgs, 'unp4k-bin')) ||
    process.env.UNP4K_BIN ||
    defaultUnp4kBin;

  const unforgeBin =
    getStringArg(rawArgs, 'unforge-bin') ||
    process.env.UNFORGE_BIN ||
    join('bins', 'unp4k', 'unforge.exe');

  const unforgeArgs =
    getStringArrayArg(rawArgs, 'unforge-arg') ??
    (process.env.UNFORGE_ARGS ? process.env.UNFORGE_ARGS.split(/\s+/).filter(Boolean) : []);

  const scdBin =
    getStringArg(rawArgs, 'scd-bin') ||
    process.env.SC_DATA_DUMPER_BIN ||
    undefined;
  let scdArgs = (
    getStringArrayArg(rawArgs, 'scd-arg') ??
    (process.env.SC_DATA_DUMPER_ARGS ? process.env.SC_DATA_DUMPER_ARGS.split(/\s+/).filter(Boolean) : [])
  );
  const scdOutput =
    getStringArg(rawArgs, 'scd-output') ||
    process.env.SC_DATA_DUMPER_OUTPUT ||
    undefined;

  let unp4kEnabled = resolveBooleanFlag(rawArgs['unp4k-enabled'], process.env.UNP4K_ENABLED, Boolean(p4kPath));
  if (unp4kEnabled && !p4kPath) {
    throw new Error('UNP4K is enabled but no P4K_PATH/--p4k provided.');
  }

  let unforgeEnabled = resolveBooleanFlag(
    rawArgs['unforge-enabled'],
    process.env.UNFORGE_ENABLED,
    unp4kEnabled
  );

  let scdEnabled = resolveBooleanFlag(
    rawArgs['scd-enabled'],
    process.env.SC_DATA_DUMPER_ENABLED,
    Boolean(scdBin)
  ) && Boolean(scdBin);

  if (scdEnabled && !unforgeEnabled) {
    log.info('Enabling unforge because scDataDumper is active.');
    unforgeEnabled = true;
  }

  if (unforgeEnabled && !unp4kEnabled) {
    if (p4kPath) {
      log.info('Enabling unp4k because unforge is active.');
      unp4kEnabled = true;
    } else {
      throw new Error(
        'unforge/scDataDumper require Data.p4k – configure P4K_PATH or place Data.p4k under data/p4k/.'
      );
    }
  }

  if (!forceUnp4kProvided && (unforgeEnabled || scdEnabled)) {
    forceUnp4k = true;
  }

  if (scdEnabled && scdBin && scdArgs.length === 0) {
    const normalizedBin = scdBin.split(/[/\\]/).pop()?.toLowerCase() ?? '';
    if (normalizedBin === 'docker' || normalizedBin === 'podman') {
      scdArgs = [
        'compose',
        '-f',
        join(process.cwd(), 'bins', 'scdatadumper', 'compose.yaml'),
        'exec',
        'scdatadumper',
        'php',
        'cli.php',
        'load:data',
        '--scUnpackedFormat',
        '{{input}}',
        '{{output}}'
      ];
    } else if (normalizedBin === 'docker-compose' || normalizedBin === 'podman-compose') {
      scdArgs = [
        '-f',
        join(process.cwd(), 'bins', 'scdatadumper', 'compose.yaml'),
        'exec',
        'scdatadumper',
        'php',
        'cli.php',
        'load:data',
        '--scUnpackedFormat',
        '{{input}}',
        '{{output}}'
      ];
    } else if (normalizedBin === 'php' || normalizedBin.endsWith('.php')) {
      scdArgs = ['cli.php', 'load:data', '--scUnpackedFormat', '{{input}}', '{{output}}'];
    } else {
      scdArgs = ['--input', '{{input}}', '--output', '{{output}}'];
    }
  }

  if (scdEnabled && scdBin && scdArgs.length) {
    const normalizedBin = scdBin.split(/[/\\]/).pop()?.toLowerCase() ?? '';
    const composePath = join(process.cwd(), 'bins', 'scdatadumper', 'compose.yaml');
    const hasFileFlag = scdArgs.some((arg) => arg === '-f' || arg === '--file');

    if (normalizedBin === 'docker' || normalizedBin === 'podman') {
      if (scdArgs[0] !== 'compose') {
        scdArgs = ['compose', ...scdArgs];
      }
      if (!hasFileFlag) {
        const insertIndex = scdArgs[0] === 'compose' ? 1 : 0;
        scdArgs.splice(insertIndex, 0, '-f', composePath);
      }
    } else if (normalizedBin === 'docker-compose' || normalizedBin === 'podman-compose') {
      if (!hasFileFlag) {
        scdArgs = ['-f', composePath, ...scdArgs];
      }
    }
  }

  const wineBin =
    process.platform === 'win32'
      ? undefined
      : (getStringArg(rawArgs, 'wine-bin') ||
        process.env.WINE_BIN ||
        'wine');

  log.info('ETL started', { channel, version, dataRoot });

  const ingestionRun = new IngestionRun();
  let buildRecord: BuildRecord | undefined;
  let loadResult: Awaited<ReturnType<typeof loadAll>> | undefined;
  let ingestionSucceeded = false;

  try {
    const extractResult = await extract({
      channel,
      version,
      dataRoot,
      p4kPath,
      forceUnp4k,
      enableUnp4k: unp4kEnabled,
      unp4k:
        unp4kEnabled && p4kPath
          ? {
              bin: unp4kBin,
              args: unp4kArgs
            }
          : undefined,
      enableUnforge: unforgeEnabled,
      unforge:
        unforgeEnabled
          ? {
              bin: unforgeBin,
              args: unforgeArgs
            }
          : undefined,
      enableScDataDumper: scdEnabled,
      scDataDumper:
        scdEnabled && scdBin
          ? {
              bin: scdBin,
              args: scdArgs,
              outputDir: scdOutput
            }
          : undefined,
      wineBin
    });
    log.info('Extracted files', { count: extractResult.discoveredFiles.length });

    const bundle = await transform(dataRoot, channel, version);
    await validateNormalizedBundle(bundle, join(process.cwd(), 'schemas'));

    const normalizedDir = join(dataRoot, 'normalized', channel, version);
    const metadata = await readBuildMetadata(normalizedDir);
    buildRecord = await ensureBuild(channel, version, metadata);
    await ingestionRun.start(buildRecord.id, { channel, version });

    loadResult = await loadAll(dataRoot, channel, version, bundle, {
      build: buildRecord,
      metadata,
      skipDiffs
    });

    await ingestionRun.updateStats({ ...loadResult.stats } as Record<string, unknown>);

    await ingestionRun.finishSuccess();
    ingestionSucceeded = true;

    if (skipDiffs) {
      log.info('Diff generation skipped by request');
    }

    log.info('ETL finished', { buildId: loadResult.build.id });
  } catch (error) {
    if (!ingestionSucceeded) {
      await ingestionRun.finishFail(error);
    }
    throw error;
  }
}

main().catch((error) => {
  log.error('ETL failed', error);
  process.exitCode = 1;
});
