import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, rmSync, existsSync, readFileSync } from 'fs';
import * as os from 'os';
import * as path from 'path';
import { fileURLToPath } from 'url';
import { DatabaseSync } from 'node:sqlite';
import { ingestCsv, parseArgs } from '../dist/ingest_csv.js';

/**
 * CSV -> store, in one step.
 *
 * The store path is the point of these: a reader with a CSV of events (from
 * another operator, for gaps older than the API retains) should not have to write
 * a JSONL first and then import it. Both destinations are now options on one
 * command, the same way the enricher already works.
 */

const CACHE = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'data', 'issues_cache.txt');

/** CSV-quote a field, doubling inner quotes. */
function csvField(value) {
    return '"' + value.replace(/"/g, '""') + '"';
}

/** A Postgres array literal from a JS string, as the source export would carry it. */
function pgLiteral(...elements) {
    return '{' + elements.map(e => JSON.stringify(e)).join(',') + '}';
}

// Known outcome: the one test-enrich.js asserts on, so the expected ids are not
// something this test invented.
const LAW_TEXT = 'following new legislation in @@test_nation@@, animal liberationists are regularly arrested';
const LAW_ISSUE = 7;
const LAW_OPTION = 1;

function writeCsv(dir) {
    const csv = [
        `3,1000,test_nation,,,,law,${csvField(pgLiteral(LAW_TEXT))}`,
        `4,1001,mover_nation,,from_region,to_region,move,{}`,
        `5,1002,test_nation,,,,law,${csvField(pgLiteral('some unrecognised decision text'))}`,
    ].join('\n') + '\n';
    const file = path.join(dir, 'events.csv');
    writeFileSync(file, csv);
    return file;
}

function options(dir, overrides = {}) {
    return {
        inPath: writeCsv(dir),
        outPath: null,
        storePath: null,
        rawOutPath: null,
        dedupPath: null,
        cachePath: CACHE,
        refreshCache: false,
        limit: Infinity,
        ...overrides,
    };
}

function workspace() {
    return mkdtempSync(path.join(os.tmpdir(), 'ingest-csv-'));
}

async function cleanup(dir) {
    for (let attempt = 0; attempt < 20; attempt++) {
        try {
            rmSync(dir, { recursive: true, force: true });
            return;
        } catch {
            await new Promise(r => setTimeout(r, 50));
        }
    }
}

test('a CSV goes straight into the store, enriched', async () => {
    const dir = workspace();
    try {
        const db = path.join(dir, 'events.db');
        const stats = await ingestCsv(options(dir, { storePath: db }));

        assert.equal(stats.read, 3);
        assert.equal(stats.stored, 3, 'every record reached the store');

        const store = new DatabaseSync(db, { readOnly: true });
        assert.equal(store.prepare('SELECT COUNT(*) c FROM events').get().c, 3);

        // Enrichment happened before the write, not after: a stored law event has
        // its ids, and one whose outcome is not in the cache is stored with null
        // rather than dropped.
        const law = store.prepare('SELECT law_issue_id, law_option FROM events WHERE event = 3').get();
        assert.equal(law.law_issue_id, LAW_ISSUE);
        assert.equal(law.law_option, LAW_OPTION);

        const unknown = store.prepare('SELECT law_issue_id FROM events WHERE event = 5').get();
        assert.equal(unknown.law_issue_id, null);

        assert.equal(store.prepare("SELECT category FROM events WHERE event = 4").get().category, 'move');
        store.close();
    } finally {
        await cleanup(dir);
    }
});

test('with only a store, no JSONL is written', async () => {
    const dir = workspace();
    try {
        const db = path.join(dir, 'events.db');
        await ingestCsv(options(dir, { storePath: db }));
        // A default JSONL path here would write a file nobody asked for.
        assert.equal(existsSync(path.join(dir, 'events.enriched.jsonl')), false);
    } finally {
        await cleanup(dir);
    }
});

test('with only JSONL, it still writes JSONL', async () => {
    const dir = workspace();
    try {
        const jsonl = path.join(dir, 'out.jsonl');
        const stats = await ingestCsv(options(dir, { outPath: jsonl }));
        assert.equal(stats.stored, 0, 'no store was asked for');
        assert.equal(existsSync(jsonl), true);
        assert.equal(readFileSync(jsonl, 'utf8').trim().split('\n').length, 3);
    } finally {
        await cleanup(dir);
    }
});

test('both destinations can be written at once', async () => {
    const dir = workspace();
    try {
        const db = path.join(dir, 'events.db');
        const jsonl = path.join(dir, 'out.jsonl');
        const stats = await ingestCsv(options(dir, { storePath: db, outPath: jsonl }));
        assert.equal(stats.stored, 3);
        assert.equal(existsSync(jsonl), true);
        const store = new DatabaseSync(db, { readOnly: true });
        assert.equal(store.prepare('SELECT COUNT(*) c FROM events').get().c, 3);
        store.close();
    } finally {
        await cleanup(dir);
    }
});

test('re-ingesting the same CSV does not duplicate rows', async () => {
    const dir = workspace();
    try {
        const db = path.join(dir, 'events.db');
        await ingestCsv(options(dir, { storePath: db }));
        // A second run over the same file: `event` is the key, so this is a no-op
        // rather than a second copy.
        const again = await ingestCsv(options(dir, { storePath: db }));
        assert.equal(again.stored, 0);
        const store = new DatabaseSync(db, { readOnly: true });
        assert.equal(store.prepare('SELECT COUNT(*) c FROM events').get().c, 3);
        store.close();
    } finally {
        await cleanup(dir);
    }
});

test('--store without --out leaves no default output path', () => {
    const dir = workspace();
    try {
        const csv = writeCsv(dir);
        const withStore = parseArgs(['--in', csv, '--store', path.join(dir, 'e.db')]);
        assert.equal(withStore.storePath, path.join(dir, 'e.db'));
        assert.equal(withStore.outPath, null);

        // Neither given: keep the old default of JSONL beside the input.
        const neither = parseArgs(['--in', csv]);
        assert.equal(neither.outPath, path.join(dir, 'events.enriched.jsonl'));
    } finally {
        rmSync(dir, { recursive: true, force: true });
    }
});
