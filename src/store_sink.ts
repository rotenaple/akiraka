import { EventStore } from './event_store';
import type { EnrichedAkariEvent } from './types';

/**
 * Bumped when enrichment changes in a way that makes old rows wrong.
 *
 * Stored per event so a reprocess can select only the rows written by a
 * superseded build, instead of rewriting a store that is mostly current.
 */
export const ENRICH_VERSION = '1';

export interface StoreSinkOptions {
    /** Rows to accumulate before a commit. */
    batchSize?: number;
    /** Flush a partial batch once this long has passed with no new row. */
    idleFlushMs?: number;
    /** Passed to EventStore; true for a store this process must not write. */
    readOnly?: boolean;
}

const DEFAULT_BATCH = 2000;
const DEFAULT_IDLE_FLUSH_MS = 500;

/**
 * A writable-stream-shaped sink that puts enriched lines into the event store.
 *
 * The enricher funnels every event - live traffic, file replay and gap-fill
 * recoveries alike - through one `outputStream.write(line)`, so sitting on that
 * stream gets a recovered event into the store without touching the three call
 * paths. Writes are batched by size and elapsed time together: live traffic is
 * about one event a second and wants each to land now, while a replay pushes
 * millions through in one tick and wants them gathered first. Either threshold
 * alone fails one of the two.
 */
export class StoreSink {
    private readonly store: EventStore;
    private readonly batchSize: number;
    private readonly idleFlushMs: number;
    private pending: Array<EnrichedAkariEvent & { enrich_v: string }> = [];
    private lastFlush = Date.now();
    private closed = false;

    /** Rows committed across this sink's life. */
    written = 0;
    /** Rows a store rejected even on their own. See `flush`. */
    dropped = 0;
    /** Store writes that failed before the record could be read. */
    unparsed = 0;
    /** Non-event lines: Akari's connection marker and anything like it. */
    synthetic = 0;

    constructor(file: string, options: StoreSinkOptions = {}) {
        this.store = new EventStore(file, { readOnly: options.readOnly ?? false });
        this.batchSize = Math.max(1, options.batchSize ?? DEFAULT_BATCH);
        this.idleFlushMs = Math.max(0, options.idleFlushMs ?? DEFAULT_IDLE_FLUSH_MS);
    }

    /** Stream-compatible write. Accepts a line with or without its newline. */
    write(chunk: unknown, ..._rest: unknown[]): boolean {
        const text = typeof chunk === 'string'
            ? chunk
            : Buffer.isBuffer(chunk) ? chunk.toString('utf8') : String(chunk ?? '');
        if (!text.trim()) return true;

        for (const line of text.split('\n')) {
            if (!line.trim()) continue;
            let record: EnrichedAkariEvent;
            try {
                record = JSON.parse(line) as EnrichedAkariEvent;
            } catch {
                // A line that will not parse is not an event. Counting it is the
                // point: silently discarding input is how a store loses data
                // without anybody noticing.
                this.unparsed++;
                continue;
            }
            // Akari writes a `{"event":-1,...,"category":"conninit"}` marker when
            // the stream connects. It is not an event and the store refuses
            // non-positive ids, so batching it would abort an otherwise good
            // batch, force a row-by-row retry, and log what reads like data loss -
            // once per reconnect. It is counted apart from `dropped` for exactly
            // that reason: nothing was lost.
            if (!Number.isInteger(record.event) || record.event <= 0) {
                this.synthetic++;
                continue;
            }
            this.pending.push({ ...record, enrich_v: ENRICH_VERSION });
        }

        if (this.pending.length >= this.batchSize || Date.now() - this.lastFlush >= this.idleFlushMs) {
            this.flush();
        }
        return true;
    }

    /**
     * Commit what is buffered.
     *
     * A batch that fails is retried one row at a time. That way a single
     * malformed record costs one event rather than the whole batch, and the
     * count of what was lost is reported instead of inferred from a row count.
     */
    flush(): void {
        if (this.pending.length === 0) return;
        const batch = this.pending;
        this.pending = [];
        this.lastFlush = Date.now();

        try {
            this.written += this.store.upsertBatch(batch);
            return;
        } catch (err) {
            const message = err instanceof Error ? err.message : String(err);
            console.error(`[store] batch of ${batch.length} failed (${message}); retrying row by row`);
        }

        for (const record of batch) {
            try {
                if (this.store.upsert(record)) this.written++;
            } catch (err) {
                this.dropped++;
                const message = err instanceof Error ? err.message : String(err);
                console.error(`[store] dropped event ${record?.event}: ${message}`);
            }
        }
    }

    /**
     * Commit, then release the file.
     *
     * Safe to call twice. The exit path and the SIGTERM handler can both reach
     * this, and closing an already-closed handle would turn a clean shutdown
     * into an unhandled error.
     */
    close(): void {
        if (this.closed) return;
        this.closed = true;
        try {
            this.flush();
        } finally {
            this.store.close();
        }
    }

    /** Counts for the shutdown log line. */
    summary(): string {
        const parts = [
            `${this.written.toLocaleString()} written`,
            `${this.dropped} dropped`,
            `${this.unparsed} unparsed`,
        ];
        if (this.synthetic > 0) parts.push(`${this.synthetic} synthetic`);
        return parts.join(', ');
    }

    /**
     * Record a range the store knows it lacks.
     *
     * The sink owns the store, so gap recording is delegated rather than opening
     * a second connection to the same file.
     */
    recordGap(start: number, end: number): void {
        this.store.recordGap(start, end);
    }
}

/**
 * Forward to both the primary output and a store.
 *
 * Used when both `--out` and `--store` are given, so one enriched line reaches
 * both. Either side may be absent: with no primary the line goes only to the
 * store, which is the default.
 */
export function teeToStore(
    primary: NodeJS.WritableStream | null,
    sink: StoreSink
): NodeJS.WritableStream {
    return {
        write(chunk: unknown, ...rest: unknown[]): boolean {
            sink.write(chunk);
            if (!primary) return true;
            return (primary.write as (...a: unknown[]) => boolean)(chunk, ...rest);
        },
    } as unknown as NodeJS.WritableStream;
}
