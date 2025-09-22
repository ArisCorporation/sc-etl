import { createOne, updateOne } from './directus.js';
import { log } from './log.js';

interface IngestionRunRecord {
  id: string;
  state: string;
  stats_json?: Record<string, unknown> | null;
  log?: string | null;
}

// ASSUMPTION: Directus `ingestion_runs` collection exposes build/state/log/stats_json timestamps.
const COLLECTION = 'ingestion_runs';

function nowIso(): string {
  return new Date().toISOString();
}

function serializeError(error: unknown): string {
  if (error instanceof Error) {
    return error.stack ?? `${error.name}: ${error.message}`;
  }
  return typeof error === 'string' ? error : JSON.stringify(error, null, 2);
}

export class IngestionRun {
  private runId?: string;
  private stats: Record<string, unknown> = {};
  private started = false;

  async start(buildId: string, extra?: Record<string, unknown>): Promise<void> {
    if (this.started) {
      return;
    }

    const payload = {
      build: buildId,
      state: 'running',
      stats_json: {},
      log: null,
      started_at: nowIso(),
      ...extra
    } satisfies Record<string, unknown>;

    const created = await createOne<IngestionRunRecord>(COLLECTION, payload);
    this.runId = created.id;
    this.started = true;
    this.stats = {};
    log.info('Ingestion run started', { ingestion_run_id: this.runId, buildId });
  }

  async updateStats(partial: Record<string, unknown>): Promise<void> {
    if (!this.started || !this.runId) return;
    const changed = Object.assign(this.stats, partial);
    await updateOne(COLLECTION, this.runId, {
      stats_json: changed,
      updated_at: nowIso()
    });
  }

  async finishSuccess(): Promise<void> {
    if (!this.started || !this.runId) return;
    await updateOne(COLLECTION, this.runId, {
      state: 'success',
      stats_json: this.stats,
      finished_at: nowIso()
    });
    log.info('Ingestion run finished successfully', { ingestion_run_id: this.runId });
  }

  async finishFail(error: unknown): Promise<void> {
    if (!this.started || !this.runId) return;
    await updateOne(COLLECTION, this.runId, {
      state: 'failed',
      stats_json: this.stats,
      log: serializeError(error),
      finished_at: nowIso()
    });
    log.error('Ingestion run failed', { ingestion_run_id: this.runId, error });
  }
}
