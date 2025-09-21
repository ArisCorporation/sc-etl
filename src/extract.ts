import { join, isAbsolute } from 'node:path';
import { spawn } from 'node:child_process';
import { rm, cp, chmod, readdir } from 'node:fs/promises';
import fg from 'fast-glob';
import { ensureDir, pathExists } from './utils/fs.js';
import { log } from './utils/log.js';
import type { Channel } from './types/index.js';

export interface ExtractOptions {
  channel: Channel;
  version: string;
  dataRoot: string;
  p4kPath?: string;
  unp4k?: {
    bin: string;
    args?: string[];
  };
  enableUnp4k?: boolean;
  forceUnp4k?: boolean;
  unforge?: {
    bin: string;
    args?: string[];
  };
  enableUnforge?: boolean;
  scDataDumper?: {
    bin: string;
    args?: string[];
    outputDir?: string;
  };
  enableScDataDumper?: boolean;
  wineBin?: string;
}

export interface ExtractResult {
  rawDir: string;
  normalizedDir: string;
  discoveredFiles: string[];
}

const REQUIRED_FILES = ['manufacturers.json', 'ships.json', 'items.json'];
const OPTIONAL_FILES = [
  'ship_variants.json',
  'hardpoints.json',
  'item_stats.json',
  'ship_stats.json',
  'installed_items.json'
];

export async function extract(opts: ExtractOptions): Promise<ExtractResult> {
  const rawDir = join(opts.dataRoot, 'raw', opts.channel, opts.version);
  const normalizedDir = join(opts.dataRoot, 'normalized', opts.channel, opts.version);

  const rawDirHasData = await directoryHasPayload(rawDir);

  const shouldRunUnp4k = Boolean(
    opts.enableUnp4k &&
      opts.p4kPath &&
      opts.unp4k &&
      (opts.forceUnp4k || !rawDirHasData)
  );

  if (shouldRunUnp4k) {
    await rm(rawDir, { recursive: true, force: true });
    await ensureDir(rawDir);

    const argSets = opts.unp4k!.args && opts.unp4k!.args.length
      ? [opts.unp4k!.args]
      : [
          ['{{p4k}}', '*.xml'],
          ['{{p4k}}', '*.ini']
        ];

    for (const args of argSets) {
      await runUnp4k({
        bin: opts.unp4k!.bin,
        args,
        p4kPath: opts.p4kPath!,
        outputDir: rawDir,
        wineBin: opts.wineBin
      });
    }
  }

  const shouldRunUnforge = Boolean(opts.enableUnforge && opts.unforge);
  if (shouldRunUnforge) {
    await runUnforge({
      bin: opts.unforge!.bin,
      args: opts.unforge!.args ?? [],
      targetDir: rawDir,
      wineBin: opts.wineBin
    });
  }

  const shouldRunScDataDumper = Boolean(opts.enableScDataDumper && opts.scDataDumper);
  if (shouldRunScDataDumper) {
    await runScDataDumper({
      bin: opts.scDataDumper!.bin,
      args: opts.scDataDumper!.args ?? [],
      inputDir: rawDir,
      outputDir: opts.scDataDumper!.outputDir ?? rawDir,
      wineBin: opts.wineBin
    });
  }

  if (!(await pathExists(rawDir))) {
    if (opts.p4kPath) {
      throw new Error(
        `Raw data directory missing after extraction steps. Expected files under ${rawDir}.`
      );
    }
    throw new Error(`Raw data directory missing: ${rawDir}`);
  }

  const missing: string[] = [];
  for (const file of REQUIRED_FILES) {
    if (!(await pathExists(join(rawDir, file)))) missing.push(file);
  }
  if (missing.length) {
    throw new Error(`Missing required raw data files: ${missing.join(', ')}`);
  }

  const optionalMissing: string[] = [];
  for (const file of OPTIONAL_FILES) {
    if (!(await pathExists(join(rawDir, file)))) optionalMissing.push(file);
  }
  if (optionalMissing.length) {
    log.warn('Optional raw files missing (will be skipped):', optionalMissing);
  }

  const discoveredFiles = await fg(['**/*.json'], { cwd: rawDir });

  await ensureDir(normalizedDir);
  log.info('Extraction check complete', { rawDir, normalizedDir, files: discoveredFiles.length });

  return { rawDir, normalizedDir, discoveredFiles };
}

interface Unp4kRun {
  bin: string;
  args: string[];
  p4kPath: string;
  outputDir: string;
  wineBin?: string;
}

async function runUnp4k(config: Unp4kRun) {
  const templateArgs = config.args.length ? config.args : ['{{p4k}}', '*.xml'];
  const absoluteP4k = isAbsolute(config.p4kPath)
    ? config.p4kPath
    : join(process.cwd(), config.p4kPath);

  let hasP4k = false;

  const resolvedArgs = templateArgs.map((arg) => {
    if (arg === '{{p4k}}') {
      hasP4k = true;
      return absoluteP4k;
    }
    if (arg === '{{output}}') {
      return config.outputDir;
    }

    const replaced = arg
      .replace('{{p4k}}', absoluteP4k)
      .replace('{{output}}', config.outputDir);

    if (replaced.includes(absoluteP4k)) hasP4k = true;
    return replaced;
  });

  if (!hasP4k) {
    resolvedArgs.unshift(absoluteP4k);
  }
  await ensureDir(config.outputDir);

  log.info('Running unp4k', {
    bin: config.bin,
    args: resolvedArgs,
    p4k: absoluteP4k,
    output: config.outputDir
  });

  await executeExternal(config.bin, resolvedArgs, {
    wineBin: config.wineBin,
    cwd: config.outputDir
  });
}

interface UnforgeRun {
  bin: string;
  args: string[];
  targetDir: string;
  wineBin?: string;
}

async function runUnforge(config: UnforgeRun) {
  // ASSUMPTION: unforge wird im extrahierten Verzeichnis ausgeführt und akzeptiert einen
  // Pfad als Argument. Standardmäßig nutzen wir "{{input}}" als Platzhalter.
  const templateArgs = config.args.length ? config.args : ['{{input}}'];
  const absoluteTargetDir = isAbsolute(config.targetDir)
    ? config.targetDir
    : join(process.cwd(), config.targetDir);
  let hasInput = false;

  const resolvedArgs = templateArgs.map((arg) => {
    if (arg === '{{input}}') {
      hasInput = true;
      return absoluteTargetDir;
    }

    const replaced = arg.replace('{{input}}', absoluteTargetDir);
    if (replaced.includes(absoluteTargetDir)) hasInput = true;
    return replaced;
  });

  if (!hasInput) {
    throw new Error('unforge arguments must include the {{input}} placeholder.');
  }

  log.info('Running unforge', {
    bin: config.bin,
    args: resolvedArgs,
    target: absoluteTargetDir
  });

  await executeExternal(config.bin, resolvedArgs, {
    wineBin: config.wineBin,
    cwd: absoluteTargetDir
  });
}

interface ScDataDumperRun {
  bin: string;
  args: string[];
  inputDir: string;
  outputDir: string;
  wineBin?: string;
}

async function runScDataDumper(config: ScDataDumperRun) {
  if (!config.args.length) {
    log.warn('scdatadumper configured without arguments – skipping converter step.');
    return;
  }

  const templateArgs = config.args;
  const placeholderTypes: Array<'input' | 'output' | null> = [];
  let hasInput = false;
  let hasOutput = false;
  const absoluteInputDir = isAbsolute(config.inputDir)
    ? config.inputDir
    : join(process.cwd(), config.inputDir);
  const absoluteOutputDir = isAbsolute(config.outputDir)
    ? config.outputDir
    : join(process.cwd(), config.outputDir);

  const resolvedArgs = templateArgs.map((arg) => {
    if (arg === '{{input}}') {
      hasInput = true;
      placeholderTypes.push('input');
      return absoluteInputDir;
    }
    if (arg === '{{output}}') {
      hasOutput = true;
      placeholderTypes.push('output');
      return absoluteOutputDir;
    }

    let replaced = arg;
    let type: 'input' | 'output' | null = null;

    if (arg.includes('{{input}}')) {
      replaced = replaced.replace(/{{input}}/g, absoluteInputDir);
      hasInput = true;
      type = 'input';
    }
    if (arg.includes('{{output}}')) {
      replaced = replaced.replace(/{{output}}/g, absoluteOutputDir);
      hasOutput = true;
      type = 'output';
    }

    if (type === null) {
      if (replaced.includes(absoluteOutputDir)) {
        hasOutput = true;
        type = 'output';
      } else if (replaced.includes(absoluteInputDir)) {
        hasInput = true;
        type = 'input';
      }
    }

    placeholderTypes.push(type);
    return replaced;
  });

  if (!hasInput || !hasOutput) {
    throw new Error('scdatadumper arguments must include {{input}} and {{output}} placeholders.');
  }

  const normalizedBin = config.bin.split(/[/\\]/).pop()?.toLowerCase() ?? '';
  const composePath = join(process.cwd(), 'bins', 'scdatadumper', 'compose.yaml');
  const isComposeWrapper = normalizedBin === 'docker-compose' || normalizedBin === 'podman-compose';
  const isComposeCli = normalizedBin === 'docker' || normalizedBin === 'podman';
  const argsIncludeExec = resolvedArgs.includes('exec');

  const hostBaseDir = join(process.cwd(), 'bins', 'scdatadumper');
  const hostImportDir = join(hostBaseDir, 'import');
  const hostExportDir = join(hostBaseDir, 'export');
  const cliEntrypoint = join(hostBaseDir, 'cli.php');

  await rm(hostImportDir, { recursive: true, force: true });
  await cp(absoluteInputDir, hostImportDir, { recursive: true, force: true });
  await chmod(hostImportDir, 0o777);

  await rm(hostExportDir, { recursive: true, force: true });
  await ensureDir(hostExportDir);
  await chmod(hostExportDir, 0o777);

  const containerInputDir = '/var/www/html/import';
  const containerOutputDir = '/var/www/html/export';

  const finalArgs = resolvedArgs.map((arg, idx) => {
    if (isComposeWrapper || isComposeCli) {
      const type = placeholderTypes[idx];
      if (type === 'input') {
        return containerInputDir;
      }
      if (type === 'output') {
        return containerOutputDir;
      }
      return arg
        .replace(absoluteInputDir, containerInputDir)
        .replace(absoluteOutputDir, containerOutputDir)
        .replace(config.inputDir, containerInputDir)
        .replace(config.outputDir, containerOutputDir);
    }
    return arg;
  });

  log.info('Running scdatadumper', {
    bin: config.bin,
    args: finalArgs,
    input: config.inputDir,
    output: config.outputDir
  });

  if (argsIncludeExec && (isComposeWrapper || isComposeCli)) {
    const upArgs = isComposeWrapper
      ? ['-f', composePath, 'up', '-d']
      : ['compose', '-f', composePath, 'up', '-d'];
    await executeExternal(config.bin, upArgs, { wineBin: config.wineBin });
  }
  const needsGenerateCache = true;

  if (isComposeWrapper || isComposeCli) {
    const execPrefix = isComposeWrapper
      ? ['-f', composePath, 'exec', 'scdatadumper']
      : ['compose', '-f', composePath, 'exec', 'scdatadumper'];

    if (needsGenerateCache) {
      const generateArgs = [...execPrefix, 'php', 'cli.php', 'generate:cache', containerInputDir];
      await executeExternal(config.bin, generateArgs, { wineBin: config.wineBin });
    }

    await executeExternal(config.bin, finalArgs, { wineBin: config.wineBin });
  } else {
    if (normalizedBin === 'php' || normalizedBin.endsWith('.php')) {
      const resolvePhpArgs = (args: string[]) =>
        args.map((arg) => {
          if (arg === 'cli.php') return cliEntrypoint;
          if (arg === config.inputDir) return absoluteInputDir;
          if (arg === config.outputDir) return absoluteOutputDir;
          if (arg === absoluteInputDir) return absoluteInputDir;
          if (arg === absoluteOutputDir) return absoluteOutputDir;
          return arg;
        });

      if (needsGenerateCache) {
        await executeExternal(
          config.bin,
          resolvePhpArgs(['cli.php', 'generate:cache', absoluteInputDir]),
          { wineBin: config.wineBin, cwd: hostBaseDir }
        );
      }
      await executeExternal(config.bin, resolvePhpArgs(finalArgs), {
        wineBin: config.wineBin,
        cwd: hostBaseDir
      });
    } else {
      await executeExternal(config.bin, finalArgs, { wineBin: config.wineBin });
    }
  }

  if (await pathExists(hostExportDir)) {
    await ensureDir(absoluteOutputDir);
    await cp(hostExportDir, absoluteOutputDir, { recursive: true, force: true });
  }
}

async function executeExternal(
  bin: string,
  args: string[],
  options: { wineBin?: string; cwd?: string } = {}
) {
  const isPathLike = bin.includes('/') || bin.includes('\\');
  const absoluteBin = isPathLike ? (isAbsolute(bin) ? bin : join(process.cwd(), bin)) : bin;
  const needsWine =
    process.platform !== 'win32' && absoluteBin.toLowerCase().endsWith('.exe');
  const spawnBin = needsWine ? options.wineBin ?? 'wine' : absoluteBin;
  const spawnArgs = needsWine ? [absoluteBin, ...args] : args;

  await new Promise<void>((resolve, reject) => {
    const child = spawn(spawnBin, spawnArgs, {
      stdio: 'inherit',
      cwd: options.cwd ?? process.cwd()
    });
    child.on('error', reject);
    child.on('exit', (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`${spawnBin} exited with code ${code}`));
      }
    });
  });
}

async function directoryHasPayload(path: string): Promise<boolean> {
  if (!(await pathExists(path))) return false;
  const entries = await readdir(path);
  return entries.some((entry) => entry !== '.gitkeep');
}
