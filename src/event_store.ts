import * as fs from 'fs';
import * as path from 'path';
import { StringDecoder } from 'string_decoder';
import { DatabaseSync } from 'node:sqlite';
import type { EnrichedAkariEvent } from './types.js';

/**
 * The event store: SQLite with `event` as the primary key.
 *
 * One decision, replacing an append-only file: a repeated event cannot become a
 * second row, every category is kept, and `ORDER BY event` is the key order.
 *
 * Two consequences shaped the schema:
 *
 * - Identity and position are the same column. There is no ingest-order cursor to
 *   keep in step with a row count, so a cursor stays valid across a rebuild.
 * - Every category is stored, unconditionally. Filters belong on the read side.
 */

/** Bumped whenever SCHEMA changes; checked on open. */
export const SCHEMA_VERSION = 1;

const SCHEMA = `
CREATE TABLE IF NOT EXISTS events (
    event        INTEGER PRIMARY KEY,
    time         INTEGER,
    category     TEXT NOT NULL,
    actor        TEXT,
    receptor     TEXT,
    origin       TEXT,
    destination  TEXT,
    law_issue_id INTEGER,
    law_option   INTEGER,
    enrich_v     TEXT
);
CREATE INDEX IF NOT EXISTS events_category_event ON events (category, event);
-- Deliberately no index on time. Nothing queries it yet, and at 14 bytes an event
-- it would add around 900 MB to a full store. Adding one later costs the same
-- single rebuild, so there is nothing to save by carrying it now.

-- The bulky field lives apart so that reading events never touches it.
-- WITHOUT ROWID suits a pure key-to-value map: it stores no separate rowid, which
-- on a table this size is worth hundreds of megabytes.
CREATE TABLE IF NOT EXISTS event_data (
    event INTEGER PRIMARY KEY,
    data  TEXT NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Ids the store knows it is missing, as inclusive ranges. Written when a jump in
-- the event id is seen or when Akari reports one on reconnect, and removed as the
-- ids arrive. Kept apart from the events table so completeness is a read of a
-- small table rather than a scan of every row.
CREATE TABLE IF NOT EXISTS gaps (
    start INTEGER PRIMARY KEY,
    end   INTEGER NOT NULL
);
`;

/**
 * Only the derived fields move when an id repeats. Source fields are written once
 * and left alone: they come from Akari and should not change for a given id, so
 * letting a re-parse overwrite them would trade good source data for a guess.
 *
 * COALESCE is the important part. A re-parse that fails to match must not
 * regress a resolved law_issue_id back to null, while a genuine correction -
 * number over number - still lands.
 */
const UPSERT_EVENT = `
INSERT INTO events (event, time, category, actor, receptor, origin, destination,
                    law_issue_id, law_option, enrich_v)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(event) DO UPDATE SET
    law_issue_id = COALESCE(excluded.law_issue_id, events.law_issue_id),
    law_option   = COALESCE(excluded.law_option,   events.law_option),
    enrich_v     = excluded.enrich_v
`;

/** data is source data: first writer wins, same as the other source fields. */
const UPSERT_DATA = `
INSERT INTO event_data (event, data) VALUES (?, ?)
ON CONFLICT(event) DO NOTHING
`;

const SELECT_COLUMNS =
    'event, time, category, actor, receptor, origin, destination, law_issue_id, law_option, enrich_v';

/** An event as stored, with data separate. */
export interface StoredEvent {
    event: number;
    time: number | null;
    category: string;
    actor: string | null;
    receptor: string | null;
    origin: string | null;
    destination: string | null;
    law_issue_id: number | null;
    law_option: number | null;
    enrich_v: string | null;
}

/** One event plus its prose, as both are needed on the way in and on the way out. */
export interface StoredEventWithData extends StoredEvent {
    data: string[] | null;
}

export interface StoreOptions {
    /** Open for querying only. Ingest and upsert refuse to run. */
    readOnly?: boolean;
}

export interface IngestResult {
    /** Complete lines consumed from the source. */
    linesRead: number;
    /** Lines that could not be stored: no id, a synthetic id, or unparseable. */
    skipped: number;
    /** Distinct events inserted during this pass. */
    inserted: number;
    /** Bytes of the source consumed, which is the resume point. */
    bytesRead: number;
    /** Highest event id stored, for a consumer's first cursor. */
    maxEvent: number;
}

export interface QueryOptions {
    /** Exclusive: only events with a greater id are returned. */
    afterEvent?: number;
    categories?: string[];
    /** Restrict to these nations appearing as actor, receptor or origin. */
    actors?: string[];
    limit?: number;
}

export interface StoreStats {
    rows: number;
    minEvent: number;
    maxEvent: number;
    categories: Array<{ category: string; rows: number }>;
    enriched: number;
}

export class EventStore {
    private db: DatabaseSync;
    private upsertEvent: ReturnType<DatabaseSync['prepare']>;
    private upsertData: ReturnType<DatabaseSync['prepare']>;
    private readOnly: boolean;
    private inTransaction = false;
    private statsCache: { at: number; value: StoreStats } | null = null;
    private categoriesCache: { at: number; value: Array<{ category: string; rows: number }> } | null = null;
    /** Highest id recorded missing, or -1. Cached so the per-row check is an int compare. */
    private gapHiCache = -1;

    constructor(readonly file: string, options: StoreOptions = {}) {
        this.readOnly = options.readOnly === true;

        if (!this.readOnly) {
            const dir = path.dirname(file);
            if (dir) fs.mkdirSync(dir, { recursive: true });
        }

        this.db = new DatabaseSync(file, { readOnly: this.readOnly });
        if (!this.readOnly) {
            // Only switch journal mode if it is not already WAL. Assigning the
            // pragma takes locks, and another connection holding the same file -
            // the query service normally is - turns that into SQLITE_IOERR rather
            // than SQLITE_BUSY. Reading the mode is a plain query.
            if (this.journalMode() !== 'wal') {
                this.db.exec('PRAGMA journal_mode = WAL');
            }
            this.db.exec('PRAGMA synchronous = NORMAL');
            this.db.exec('PRAGMA foreign_keys = ON');
            this.checkSchema();
        } else {
            this.db.exec('PRAGMA query_only = ON');
        }

        this.upsertEvent = this.db.prepare(UPSERT_EVENT);
        this.upsertData = this.db.prepare(UPSERT_DATA);
        if (!this.readOnly) this.refreshGapHi();
    }

    close(): void {
        this.db.close();
    }

    /** True when the file on disk was written by a newer schema than this code. */
    get schemaTooNew(): boolean {
        const row = this.db.prepare('PRAGMA user_version').get() as { user_version: number };
        return row.user_version > SCHEMA_VERSION;
    }

    private checkSchema(): void {
        const row = this.db.prepare('PRAGMA user_version').get() as { user_version: number };
        if (row.user_version > SCHEMA_VERSION) {
            // Closing before throwing: a half-open handle would keep the file
            // locked, which is a confusing way to learn that the store is newer.
            this.db.close();
            throw new Error(
                `${this.file} is schema version ${row.user_version}, but this build understands ` +
                `${SCHEMA_VERSION}. Refusing to write to a store this code cannot interpret.`
            );
        }
        this.db.exec(SCHEMA);
        if (row.user_version < SCHEMA_VERSION) {
            this.db.exec(`PRAGMA user_version = ${SCHEMA_VERSION}`);
        }
    }

    meta(key: string): string | undefined {
        const row = this.db.prepare('SELECT value FROM meta WHERE key = ?').get(key) as
            | { value: string }
            | undefined;
        return row?.value;
    }

    setMeta(key: string, value: string): void {
        this.assertWritable();
        this.db.prepare('INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)').run(key, value);
    }

    private assertWritable(): void {
        if (this.readOnly) throw new Error(`${this.file} is open read-only`);
    }

    private begin(): void {
        if (!this.inTransaction) {
            this.db.exec('BEGIN');
            this.inTransaction = true;
        }
    }

    private commit(): void {
        if (this.inTransaction) {
            this.db.exec('COMMIT');
            this.inTransaction = false;
            // Both write paths commit, so dropping the cache here covers them both.
            this.statsCache = null;
        }
    }

    private rollback(): void {
        if (this.inTransaction) {
            try { this.db.exec('ROLLBACK'); } catch { /* already unwound */ }
            this.inTransaction = false;
        }
    }

    /**
     * The journal mode currently in force, read without changing anything.
     *
     * Reading the pragma and assigning it are not the same operation, and only one
     * is safe while another process has the file open.
     */
    journalMode(): string {
        const row = this.db.prepare('PRAGMA journal_mode').get() as
            | { journal_mode: string }
            | undefined;
        return row?.journal_mode ?? '';
    }

    /** Highest event id stored, or 0 when empty. */
    maxEvent(): number {
        const row = this.db.prepare('SELECT COALESCE(MAX(event), 0) AS m FROM events').get() as { m: number };
        return row.m;
    }

    count(): number {
        const row = this.db.prepare('SELECT COUNT(*) AS n FROM events').get() as { n: number };
        return row.n;
    }

    /**
     * Runs of ids the store holds nothing for.
     *
     * Computed from the ids present rather than from recorded bookkeeping, so it
     * cannot disagree with the contents. A gap and a lost event look identical
     * here, and that is the honest answer: whether a missing id ever existed is
     * not something this store can know.
     */
    gaps(): { gaps: number; missingIds: number; largestGap: number; largestGapAt: number } {
        const pairs = `
            SELECT event, LEAD(event) OVER (ORDER BY event) AS next FROM events
        `;
        const totals = this.db
            .prepare(
                `SELECT COUNT(*) AS gaps,
                        COALESCE(SUM(next - event - 1), 0) AS missing,
                        COALESCE(MAX(next - event - 1), 0) AS largest
                   FROM (${pairs}) WHERE next IS NOT NULL AND next > event + 1`
            )
            .get() as { gaps: number; missing: number; largest: number };
        const worst = this.db
            .prepare(
                `SELECT event FROM (${pairs})
                  WHERE next IS NOT NULL
                  ORDER BY (next - event - 1) DESC LIMIT 1`
            )
            .get() as { event: number } | undefined;
        return {
            gaps: totals.gaps,
            missingIds: totals.missing,
            largestGap: totals.largest,
            largestGapAt: worst?.event ?? 0,
        };
    }

    /**
     * Record that [start, end] is missing.
     *
     * Merged with any range it touches, so the table stays a set of maximal runs
     * and a report is a read of it. Idempotent, and cheap: it is called when the
     * id stream jumps, which is rare, not per event.
     */
    recordGap(start: number, end: number): void {
        this.assertWritable();
        if (!Number.isInteger(start) || !Number.isInteger(end) || start <= 0 || end < start) return;

        const touching = this.db
            .prepare('SELECT start, end FROM gaps WHERE end >= ? AND start <= ?')
            .all(start - 1, end + 1) as Array<{ start: number; end: number }>;

        let lo = start;
        let hi = end;
        for (const row of touching) {
            if (row.start < lo) lo = row.start;
            if (row.end > hi) hi = row.end;
        }

        this.db.prepare('DELETE FROM gaps WHERE end >= ? AND start <= ?').run(start - 1, end + 1);
        this.db.prepare('INSERT OR REPLACE INTO gaps (start, end) VALUES (?, ?)').run(lo, hi);
        if (hi > this.gapHiCache) this.gapHiCache = hi;
    }

    /**
     * Forget ids in [start, end], which are now held.
     *
     * An overlapping range is removed and whatever lies outside the cleared span
     * is put back, so clearing the middle of a run splits it. Written without a
     * transaction of its own, because the single-id path runs inside the upsert's
     * transaction and a commit here would close that early.
     */
    clearRange(start: number, end: number): void {
        this.assertWritable();
        if (end < start) return;

        const overlapping = this.db
            .prepare('SELECT start, end FROM gaps WHERE end >= ? AND start <= ?')
            .all(start, end) as Array<{ start: number; end: number }>;
        if (overlapping.length === 0) return;

        const del = this.db.prepare('DELETE FROM gaps WHERE end >= ? AND start <= ?');
        const ins = this.db.prepare('INSERT OR REPLACE INTO gaps (start, end) VALUES (?, ?)');
        del.run(start, end);
        for (const row of overlapping) {
            if (row.start < start) ins.run(row.start, start - 1);
            if (row.end > end) ins.run(end + 1, row.end);
        }
        this.refreshGapHi();
    }

    /** The recorded missing ranges, in id order. */
    listGaps(limit = 0): Array<{ start: number; end: number }> {
        const sql = 'SELECT start, end FROM gaps ORDER BY start' + (limit > 0 ? ' LIMIT ?' : '');
        try {
            const stmt = this.db.prepare(sql);
            const rows = (limit > 0 ? stmt.all(limit) : stmt.all()) as Array<{
                start: number;
                end: number;
            }>;
            // Rebuilt as plain objects: rows from node:sqlite carry a null
            // prototype, which is a needless surprise for a caller comparing them.
            return rows.map(row => ({ start: Number(row.start), end: Number(row.end) }));
        } catch {
            // No table: a store written before this existed, opened read-only.
            // It is created on the next writable open.
            return [];
        }
    }

    /** Completeness from the gap table, which is O(gaps) rather than O(rows). */
    gapTotals(): { gaps: number; missingIds: number; largestGap: number; largestGapAt: number } {
        try {
            const totals = this.db
                .prepare(
                    `SELECT COUNT(*) AS gaps,
                            COALESCE(SUM(end - start + 1), 0) AS missing,
                            COALESCE(MAX(end - start + 1), 0) AS largest
                       FROM gaps`
                )
                .get() as { gaps: number; missing: number; largest: number };
            const worst = this.db
                .prepare('SELECT start FROM gaps ORDER BY (end - start) DESC LIMIT 1')
                .get() as { start: number } | undefined;
            return {
                gaps: totals.gaps,
                missingIds: totals.missing,
                largestGap: totals.largest,
                largestGapAt: worst?.start ?? 0,
            };
        } catch {
            return { gaps: 0, missingIds: 0, largestGap: 0, largestGapAt: 0 };
        }
    }

    /**
     * Rebuild the gap table from the events table.
     *
     * Only needed to seed a store that predates the table, or to confirm it after
     * a restore. That is a full ordered scan - minutes on a large store - which is
     * the very cost the table exists to keep off the read path, so it is a
     * deliberate, explicit call and not something a query does.
     */
    rebuildGaps(): { gaps: number; missingIds: number } {
        this.assertWritable();
        const rows = this.db
            .prepare(
                `SELECT event + 1 AS start, next - 1 AS end FROM (
                     SELECT event, LEAD(event) OVER (ORDER BY event) AS next FROM events
                 ) WHERE next IS NOT NULL AND next > event + 1`
            )
            .all() as Array<{ start: number; end: number }>;

        this.begin();
        try {
            this.db.prepare('DELETE FROM gaps').run();
            const ins = this.db.prepare('INSERT INTO gaps (start, end) VALUES (?, ?)');
            for (const row of rows) ins.run(row.start, row.end);
            this.commit();
        } catch (err) {
            this.rollback();
            throw err;
        }
        this.refreshGapHi();
        return { gaps: rows.length, missingIds: rows.reduce((a, r) => a + (r.end - r.start + 1), 0) };
    }

    /** Shrink the recorded gaps around one id that has just been stored. */
    private clearId(id: number): void {
        const row = this.db
            .prepare('SELECT start, end FROM gaps WHERE start <= ? AND end >= ?')
            .get(id, id) as { start: number; end: number } | undefined;
        if (!row) return;

        this.db.prepare('DELETE FROM gaps WHERE start = ?').run(row.start);
        const ins = this.db.prepare('INSERT OR REPLACE INTO gaps (start, end) VALUES (?, ?)');
        if (row.start < id) ins.run(row.start, id - 1);
        if (row.end > id) ins.run(id + 1, row.end);
        if (row.end >= this.gapHiCache) this.refreshGapHi();
    }

    private refreshGapHi(): void {
        try {
            const row = this.db.prepare('SELECT MAX(end) AS hi FROM gaps').get() as
                | { hi: number | null }
                | undefined;
            this.gapHiCache = row?.hi ?? -1;
        } catch {
            this.gapHiCache = -1;
        }
    }

    get(id: number): StoredEventWithData | null {
        const row = this.db
            .prepare(
                `SELECT ${SELECT_COLUMNS}, (SELECT data FROM event_data WHERE event = events.event) AS data
                   FROM events WHERE event = ?`
            )
            .get(id) as (StoredEvent & { data: string | null }) | undefined;
        if (!row) return null;
        return { ...row, data: row.data === null ? null : safeParseArray(row.data) };
    }

    /**
     * Store one parsed record.
     *
     * Returns whether the event id was new, so a caller can tell a first sighting
     * from a repeat without a second lookup.
     */
    upsert(record: EnrichedAkariEvent & { enrich_v?: string | null }): boolean {
        this.assertWritable();
        let inserted: boolean;
        this.begin();
        try {
            inserted = this.writeOne(record);
            this.commit();
        } catch (err) {
            // Roll back so a failed write never leaves a partial event behind.
            this.rollback();
            throw err;
        }
        return inserted;
    }

    /**
     * Store many parsed records in one transaction, returning how many were new.
     *
     * `upsert` commits per row, which suits a tail following live traffic and not a
     * file replay: a sync per event turns a minutes-long import into an hours-long
     * one. Batch mode and gap filling replay files, so they use this.
     *
     * A bad record aborts the whole batch, so the caller retries from the same
     * offset - which is safe, because the store keys on the event id.
     */
    upsertBatch(records: Array<EnrichedAkariEvent & { enrich_v?: string | null }>): number {
        this.assertWritable();
        if (records.length === 0) return 0;
        let inserted = 0;
        this.begin();
        try {
            for (const record of records) {
                if (this.writeOne(record)) inserted++;
            }
            this.commit();
        } catch (err) {
            this.rollback();
            throw err;
        }
        return inserted;
    }

    /**
     * One row, inside whatever transaction the caller has open.
     *
     * Kept separate from `upsert` because `begin` is re-entrancy guarded and
     * `commit` is not: a loop built on `upsert` would have the first row's commit
     * close the batch.
     */
    private writeOne(record: EnrichedAkariEvent & { enrich_v?: string | null }): boolean {
        if (!Number.isInteger(record.event) || record.event <= 0) {
            throw new Error(`refusing to store event id ${record.event}`);
        }

        const existed = this.db
            .prepare('SELECT 1 AS present FROM events WHERE event = ?')
            .get(record.event) as { present: number } | undefined;

        this.upsertEvent.run(
            record.event,
            finiteOrNull(record.time),
            typeof record.category === 'string' ? record.category : 'unknown',
            strOrNull(record.actor),
            strOrNull(record.receptor),
            strOrNull(record.origin),
            strOrNull(record.destination),
            finiteOrNull(record.law_issue_id),
            finiteOrNull(record.law_option),
            record.enrich_v ?? null
        );
        if (Array.isArray(record.data)) {
            this.upsertData.run(record.event, JSON.stringify(record.data));
        }
        // A recovered event lands here. Drop its id from the gap set if it was
        // recorded missing, so completeness recovers as fills arrive. The cache
        // keeps this an integer compare for the common case of no gap at all.
        if (record.event <= this.gapHiCache) this.clearId(record.event);
        return existed === undefined;
    }

    /**
     * Read a source JSONL into the store, resuming from a byte offset.
     *
     * The offset is the whole of the resume state, because the source is
     * append-only. A source that shrank is treated as replaced rather than left
     * holding rows the file no longer contains.
     */
    async ingest(source: string, options: { commitEveryBytes?: number } = {}): Promise<IngestResult> {
        this.assertWritable();
        const result: IngestResult = {
            linesRead: 0, skipped: 0, inserted: 0, bytesRead: 0, maxEvent: 0,
        };
        if (!fs.existsSync(source)) return result;

        // Keyed on the resolved path, so that naming the same file two ways - an
        // absolute path from one caller, a relative one from another - resumes
        // rather than silently reading the whole file again.
        const offsetKey = `offset:${path.resolve(source)}`;

        let offset = Number(this.meta(offsetKey) ?? 0);
        if (!Number.isFinite(offset) || offset < 0) offset = 0;
        const size = fs.statSync(source).size;
        if (size < offset) {
            // Replaced, not appended to. Clearing is destructive, so it is the
            // caller's decision; here we simply refuse to splice two files.
            throw new Error(
                `${source} is ${size} bytes but ${offset} were already consumed; it was replaced or ` +
                'truncated. Point at the new file, or clear the store deliberately.'
            );
        }
        if (size === offset) {
            result.bytesRead = offset;
            result.maxEvent = this.maxEvent();
            return result;
        }

        const startOffset = offset;
        const stream = fs.createReadStream(source, { start: offset, highWaterMark: 4 * 1024 * 1024 });
        // A chunk boundary can fall inside a multi-byte character and law text is
        // not ASCII, so decoding chunk by chunk would corrupt it.
        const decoder = new StringDecoder('utf8');
        let pending = '';
        let bytesRead = 0;
        let sinceCommit = 0;

        try {
            for await (const chunk of stream) {
                bytesRead += (chunk as Buffer).length;
                pending += decoder.write(chunk as Buffer);

                // Hold back an incomplete trailing line: it may be a record still
                // being written, and consuming it now would leave the saved offset
                // inside the record.
                const lastNewline = pending.lastIndexOf('\n');
                if (lastNewline < 0) continue;
                const complete = pending.slice(0, lastNewline);
                pending = pending.slice(lastNewline + 1);

                // One transaction per chunk, not per row. Committing each row
                // separately is correct but costs a WAL sync per event, which on a
                // 65-million-row import is the difference between an hour and a
                // few minutes. A failure rolls the chunk back, and the offset is
                // only saved after a chunk completes, so nothing is lost.
                this.begin();
                try {
                    for (const line of complete.split('\n')) {
                        if (!line.startsWith('{')) continue;
                        result.linesRead++;

                        let parsed: EnrichedAkariEvent;
                        try {
                            parsed = JSON.parse(line) as EnrichedAkariEvent;
                        } catch {
                            result.skipped++;
                            continue;
                        }
                        // Synthetic markers such as the conndrop pseudo-event carry
                        // a non-positive id. They are not events, and a negative key
                        // would sit below every real cursor.
                        if (!Number.isInteger(parsed.event) || parsed.event <= 0) {
                            result.skipped++;
                            continue;
                        }

                        this.upsertEvent.run(
                            parsed.event,
                            finiteOrNull(parsed.time),
                            typeof parsed.category === 'string' ? parsed.category : 'unknown',
                            strOrNull(parsed.actor),
                            strOrNull(parsed.receptor),
                            strOrNull(parsed.origin),
                            strOrNull(parsed.destination),
                            finiteOrNull(parsed.law_issue_id),
                            finiteOrNull(parsed.law_option),
                            null
                        );
                        if (Array.isArray(parsed.data)) {
                            this.upsertData.run(parsed.event, JSON.stringify(parsed.data));
                        }
                        result.inserted++;
                    }
                    this.commit();
                } catch (err) {
                    this.rollback();
                    throw err;
                }

                // Consumed means everything except the fragment still held back.
                // Advancing by bytes read instead would step over that fragment,
                // and the record it becomes would never be seen.
                const consumed = startOffset + bytesRead - Buffer.byteLength(pending, 'utf8');
                result.bytesRead = consumed;
                sinceCommit += (chunk as Buffer).length;
                if (sinceCommit >= (options.commitEveryBytes ?? 64 * 1024 * 1024)) {
                    this.setMeta(offsetKey, String(consumed));
                    sinceCommit = 0;
                    console.error(
                        `[store] ${result.linesRead.toLocaleString()} lines, ` +
                        `${this.count().toLocaleString()} events, ${(consumed / 1e9).toFixed(2)} GB read`
                    );
                }
            }
            // Flush a multi-byte character the decoder was still holding.
            pending += decoder.end();
        } catch (err) {
            this.rollback();
            throw err;
        } finally {
            stream.destroy();
        }

        // The trailing fragment is deliberately not consumed. If the file simply
        // ends without a newline the last record waits for the next pass, which
        // costs one repeated read and nothing else, because the id is the key.
        this.setMeta(offsetKey, String(result.bytesRead));
        result.maxEvent = this.maxEvent();
        return result;
    }

    /**
     * Events after `afterEvent`, ordered by id.
     *
     * Ordering by the primary key rather than by an ingest counter is what lets a
     * consumer keep a cursor across a rebuild of this store.
     */
    query(options: QueryOptions = {}): StoredEvent[] {
        const clauses: string[] = [];
        const params: Array<number | string> = [];

        clauses.push('event > ?');
        params.push(Math.max(0, Math.trunc(options.afterEvent ?? 0)));

        if (options.categories?.length) {
            clauses.push(`category IN (${options.categories.map(() => '?').join(',')})`);
            params.push(...options.categories);
        }
        for (const nation of options.actors ?? []) {
            clauses.push('(actor = ? OR receptor = ? OR origin = ?)');
            params.push(nation, nation, nation);
        }

        let sql = `SELECT ${SELECT_COLUMNS} FROM events WHERE ${clauses.join(' AND ')} ORDER BY event`;
        if (options.limit !== undefined) {
            sql += ' LIMIT ?';
            params.push(Math.max(0, Math.trunc(options.limit)));
        }
        return this.db.prepare(sql).all(...params) as unknown as StoredEvent[];
    }

    /**
     * Write events back out as Akari-format JSONL.
     *
     * Absent optional fields are omitted rather than written as empty strings,
     * because Akari omits them: a consumer distinguishing "no actor" from "the
     * empty string" would otherwise see a difference not in the source data.
     */
    exportJsonl(
        write: (line: string) => void,
        options: QueryOptions & { withData?: boolean } = {}
    ): number {
        const wantData = options.withData === true;
        const columns = wantData
            ? `${SELECT_COLUMNS}, (SELECT d.data FROM event_data d WHERE d.event = events.event) AS data`
            : SELECT_COLUMNS;

        const clauses = ['event > ?'];
        const params: Array<number | string> = [Math.max(0, Math.trunc(options.afterEvent ?? 0))];
        if (options.categories?.length) {
            clauses.push(`category IN (${options.categories.map(() => '?').join(',')})`);
            params.push(...options.categories);
        }
        for (const nation of options.actors ?? []) {
            clauses.push('(actor = ? OR receptor = ? OR origin = ?)');
            params.push(nation, nation, nation);
        }

        let sql = `SELECT ${columns} FROM events WHERE ${clauses.join(' AND ')} ORDER BY event`;
        if (options.limit !== undefined) {
            sql += ' LIMIT ?';
            params.push(Math.max(0, Math.trunc(options.limit)));
        }

        const rows = this.db.prepare(sql).all(...params) as unknown as Array<StoredEvent & { data?: string | null }>;
        for (const row of rows) {
            const out: Record<string, unknown> = {
                event: row.event,
                time: row.time ?? 0,
                category: row.category,
            };
            if (row.actor !== null) out.actor = row.actor;
            if (row.receptor !== null) out.receptor = row.receptor;
            if (row.origin !== null) out.origin = row.origin;
            if (row.destination !== null) out.destination = row.destination;
            if (wantData && row.data !== null && row.data !== undefined) {
                out.data = safeParseArray(row.data);
            }
            if (row.law_issue_id !== null) out.law_issue_id = row.law_issue_id;
            if (row.law_option !== null) out.law_option = row.law_option;

            write(`${toJsonlLine(out)}\n`);
        }
        return rows.length;
    }

    /**
     * Row counts per category, plus the id range held.
     *
     * A category list is what a caller checks against its own expectations: a
     * missing category is how silent data loss from a bad ingest shows up.
     *
     * Exact, and therefore slow: COUNT(*) over tens of millions of rows is a full
     * scan. Use statsCached for anything on a request path.
     */
    stats(): StoreStats {
        const range = this.db
            .prepare('SELECT COALESCE(MIN(event), 0) AS lo, COALESCE(MAX(event), 0) AS hi, COUNT(*) AS n FROM events')
            .get() as { lo: number; hi: number; n: number };
        const cats = this.db
            .prepare('SELECT category, COUNT(*) AS n FROM events GROUP BY category ORDER BY n DESC')
            .all() as Array<{ category: string; n: number }>;
        const enriched = this.db
            .prepare('SELECT COUNT(*) AS n FROM events WHERE law_issue_id IS NOT NULL')
            .get() as { n: number };
        return {
            rows: range.n,
            minEvent: range.lo,
            maxEvent: range.hi,
            categories: cats.map(c => ({ category: c.category, rows: c.n })),
            enriched: enriched.n,
        };
    }

    /**
     * stats(), reused if it was computed recently.
     *
     * The counts only move as events arrive, so recomputing per request spends a
     * full scan to answer a question whose answer changes once an event is written.
     */
    statsCached(maxAgeMs: number): StoreStats {
        const now = Date.now();
        if (this.statsCache && now - this.statsCache.at < maxAgeMs) return this.statsCache.value;
        const value = this.stats();
        this.statsCache = { at: now, value };
        return value;
    }

    /**
     * The highest event id, which is an index seek rather than a scan.
     *
     * Safe to call on a request path: it reads the last entry of the primary key.
     */
    maxEventCached(): number {
        return this.maxEvent();
    }

    /**
     * Whether the store holds anything, without counting rows.
     *
     * Ids start above zero, so a non-empty store has a positive maximum and this
     * answers the question with an index seek. Asking COUNT(*) instead costs a
     * full scan of the whole store.
     */
    hasEvents(): boolean {
        return this.maxEvent() > 0;
    }

    /**
     * The category list, reusing a recent count if there is one.
     *
     * Split from stats() so a caller that only needs the category names - a config
     * check, for instance - does not pay for the row count that goes with them.
     */
    categoriesCached(maxAgeMs: number): Array<{ category: string; rows: number }> {
        const now = Date.now();
        if (this.statsCache && now - this.statsCache.at < maxAgeMs) return this.statsCache.value.categories;
        const cats = this.db
            .prepare('SELECT category, COUNT(*) AS n FROM events GROUP BY category ORDER BY n DESC')
            .all() as Array<{ category: string; n: number }>;
        const value = cats.map(c => ({ category: c.category, rows: c.n }));
        this.categoriesCache = { at: now, value };
        return value;
    }
}

/**
 * Serialise one line of Akari-format JSONL.
 *
 * U+2028, U+2029 and U+0085 are legal inside a JSON string, and JSON.stringify
 * emits them raw, but enough readers treat them as line terminators that a
 * faithful value comes back as two truncated lines. Akari writes them escaped, so
 * the export does too - otherwise the round trip is lossy in a way that only shows
 * up on the rare event carrying one.
 */
function toJsonlLine(value: unknown): string {
    let json = JSON.stringify(value);
    // Built from character codes rather than written as literal characters: these
    // three are line terminators, so embedding them raw in source is the same
    // hazard this function exists to prevent.
    for (const code of [0x2028, 0x2029, 0x0085]) {
        json = json.split(String.fromCharCode(code)).join('\\u' + code.toString(16).padStart(4, '0'));
    }
    return json;
}
/**
 * Coerce to a number, or null when there is no number.
 *
 * The explicit checks matter: Number(null) and Number('') are both 0, and 0 is a
 * real issue id - the law cache starts at "0|1|voting is voluntary". Coercing
 * without them files "no match" as "issue 0", and the two cannot be told apart.
 */
function finiteOrNull(value: unknown): number | null {
    if (value === null || value === undefined || value === '') return null;
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
}

/** An empty string is how some sources spell "absent"; treat it as absent. */
function strOrNull(value: unknown): string | null {
    return typeof value === 'string' && value.length > 0 ? value : null;
}

function safeParseArray(value: string): string[] {
    try {
        const parsed = JSON.parse(value);
        return Array.isArray(parsed) ? parsed : [];
    } catch {
        return [];
    }
}
