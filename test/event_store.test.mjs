import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, appendFileSync, readFileSync, rmSync, truncateSync } from 'fs';
import * as os from 'os';
import * as path from 'path';
import { EventStore, SCHEMA_VERSION } from '../dist/event_store.js';

function workspace() {
    const dir = mkdtempSync(path.join(os.tmpdir(), 'event-store-'));
    return { dir, source: path.join(dir, 'events.enriched.jsonl'), db: path.join(dir, 'events.db') };
}

/** Run a body with an open store, always closing it. */
async function withStore(db, options, body) {
    const store = new EventStore(db, options);
    try {
        return await body(store);
    } finally {
        store.close();
    }
}

const lawLine = JSON.stringify({
    event: 100,
    time: 1767696589,
    category: 'law',
    actor: 'frabens',
    origin: 'the_pacific',
    data: ['grocery stores can be audibly identified by the giggling emerging from them'],
    law_issue_id: 1010,
    law_option: 1,
});

const minimalLine = JSON.stringify({ event: 101, time: 1767696590, category: 'ncte', data: ['x'] });

// ---- the primary key ----

test('a repeated event id is stored once', async () => {
    const { dir, source, db } = workspace();
    try {
        writeFileSync(source, [lawLine, lawLine, lawLine].join('\n') + '\n');
        await withStore(db, {}, async store => {
            await store.ingest(source);
            assert.equal(store.count(), 1);
        });
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('source fields are written once and never overwritten by a later record', async () => {
    const { dir, db } = workspace();
    try {
        await withStore(db, {}, async store => {
            store.upsert(JSON.parse(lawLine));
            // A re-parse that disagrees about the source fields must not win.
            store.upsert({
                event: 100, time: 999, category: 'rmbpost',
                actor: 'someone-else', origin: 'elsewhere', data: [],
            });
            const row = store.get(100);
            assert.equal(row.time, 1767696589);
            assert.equal(row.category, 'law');
            assert.equal(row.actor, 'frabens');
        });
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('a failed re-parse cannot regress a resolved law id back to null', async () => {
    const { dir, db } = workspace();
    try {
        await withStore(db, {}, async store => {
            store.upsert(JSON.parse(lawLine));
            store.upsert({ event: 100, time: 1767696589, category: 'law', data: ['unmatched text'] });
            const row = store.get(100);
            assert.equal(row.law_issue_id, 1010, 'resolution must survive a null re-parse');
            assert.equal(row.law_option, 1);
        });
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('a genuine correction still lands over an existing number', async () => {
    const { dir, db } = workspace();
    try {
        await withStore(db, {}, async store => {
            store.upsert(JSON.parse(lawLine));
            store.upsert({ event: 100, time: 1767696589, category: 'law', law_issue_id: 1042, law_option: 2 });
            const row = store.get(100);
            assert.equal(row.law_issue_id, 1042);
            assert.equal(row.law_option, 2);
        });
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('every category is stored; nothing is filtered at write time', async () => {
    const { dir, source, db } = workspace();
    try {
        const lines = [
            JSON.stringify({ event: 1, time: 1, category: 'law', data: [] }),
            JSON.stringify({ event: 2, time: 2, category: 'rupdate', data: [] }),
            JSON.stringify({ event: 3, time: 3, category: 'chcensus', data: [] }),
            JSON.stringify({ event: 4, time: 4, category: 'ncte', data: [] }),
        ];
        writeFileSync(source, lines.join('\n') + '\n');
        await withStore(db, {}, async store => {
            await store.ingest(source);
            assert.equal(store.count(), 4);
            const cats = store.stats().categories.map(c => c.category).sort();
            assert.deepEqual(cats, ['chcensus', 'law', 'ncte', 'rupdate']);
        });
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('an explicit null is not coerced to the number zero', async () => {
    // Issue id 0 is real - the cache opens with "0|1|voting is voluntary" - so
    // filing "no match" as 0 would be indistinguishable from it afterwards, and
    // the query that finds unresolved rows would come back empty.
    const { dir, db } = workspace();
    try {
        await withStore(db, {}, async store => {
            store.upsert({ event: 1, time: 1, category: 'law', law_issue_id: null, law_option: null, data: [] });
            store.upsert({ event: 2, time: 2, category: 'law', law_issue_id: 0, law_option: 1, data: [] });

            const unmatched = store.get(1);
            assert.equal(unmatched.law_issue_id, null, 'null must stay null');
            assert.equal(unmatched.law_option, null);

            const realZero = store.get(2);
            assert.equal(realZero.law_issue_id, 0, 'a genuine issue 0 must survive');
            assert.equal(realZero.law_option, 1);

            assert.equal(store.stats().enriched, 1, 'only the resolved row counts as enriched');
        });
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('a null is also preserved through an ingest', async () => {
    const { dir, source, db } = workspace();
    try {
        writeFileSync(source, JSON.stringify({
            event: 5, time: 5, category: 'law', data: ['text'],
            law_issue_id: null, law_option: null,
        }) + '\n');
        await withStore(db, {}, async store => {
            await store.ingest(source);
            const row = store.get(5);
            assert.equal(row.law_issue_id, null);
            assert.equal(row.law_option, null);
            assert.equal(store.stats().enriched, 0);
        });
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

// ---- ingest mechanics ----

test('ingest skips synthetic and unparseable lines and counts them', async () => {
    const { dir, source, db } = workspace();
    try {
        writeFileSync(source, [
            JSON.stringify({ event: -1, time: 1, category: 'conndrop', data: ['x'] }),
            JSON.stringify({ event: 0, time: 1, category: 'conndrop', data: ['x'] }),
            '{not json',
            JSON.stringify({ event: 5, time: 5, category: 'law', data: [] }),
        ].join('\n') + '\n');
        await withStore(db, {}, async store => {
            const r = await store.ingest(source);
            assert.equal(r.linesRead, 4);
            assert.equal(r.skipped, 3, 'two synthetic ids and one unparseable line');
            assert.equal(store.count(), 1);
        });
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('ingest resumes from its byte offset rather than re-reading', async () => {
    const { dir, source, db } = workspace();
    try {
        writeFileSync(source, JSON.stringify({ event: 1, time: 1, category: 'law', data: [] }) + '\n');
        await withStore(db, {}, async store => {
            const first = await store.ingest(source);
            assert.equal(first.inserted, 1);

            appendFileSync(source, JSON.stringify({ event: 2, time: 2, category: 'law', data: [] }) + '\n');
            const second = await store.ingest(source);
            assert.equal(second.linesRead, 1, 'only the appended line is read');
            assert.equal(second.inserted, 1);
            assert.equal(store.count(), 2);
        });
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('a trailing partial line is left for the next pass', async () => {
    const { dir, source, db } = workspace();
    try {
        const complete = JSON.stringify({ event: 1, time: 1, category: 'law', data: [] }) + '\n';
        // A second record that is cut off mid-way, as if still being written.
        writeFileSync(source, complete + '{"event":2,"time":2,"categ');
        await withStore(db, {}, async store => {
            const r = await store.ingest(source);
            assert.equal(r.linesRead, 1);
            assert.equal(store.count(), 1);

            // Completing the line lets the next pass pick it up.
            writeFileSync(source, complete + JSON.stringify({ event: 2, time: 2, category: 'law', data: [] }) + '\n');
            const again = await store.ingest(source);
            assert.equal(again.inserted, 1);
            assert.equal(store.count(), 2);
        });
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('a source that shrank is refused rather than spliced', async () => {
    const { dir, source, db } = workspace();
    try {
        writeFileSync(source, [
            JSON.stringify({ event: 1, time: 1, category: 'law', data: [] }),
            JSON.stringify({ event: 2, time: 2, category: 'law', data: [] }),
            JSON.stringify({ event: 3, time: 3, category: 'law', data: [] }),
        ].join('\n') + '\n');
        await withStore(db, {}, async store => {
            await store.ingest(source);
            const before = store.count();
            truncateSync(source, 10);
            await assert.rejects(() => store.ingest(source), /replaced or truncated/);
            assert.equal(store.count(), before, 'a refused ingest must not delete anything');
        });
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

// ---- reading ----

test('afterEvent is exclusive and ordered by the key', async () => {
    const { dir, db } = workspace();
    try {
        await withStore(db, {}, async store => {
            for (const id of [10, 20, 30, 40]) {
                store.upsert({ event: id, time: id, category: 'law', data: [] });
            }
            assert.deepEqual(store.query({ afterEvent: 0 }).map(r => r.event), [10, 20, 30, 40]);
            assert.deepEqual(store.query({ afterEvent: 20 }).map(r => r.event), [30, 40]);
            assert.deepEqual(store.query({ afterEvent: 40 }), [], 'a cursor at the head yields nothing');
            assert.deepEqual(store.query({ afterEvent: 0, limit: 2 }).map(r => r.event), [10, 20]);
        });
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('a cursor survives rows being inserted out of order', async () => {
    // This is the property an ingest-order cursor cannot offer: a backfilled older
    // event must not appear "newer" than the cursor, nor be skipped by it.
    const { dir, db } = workspace();
    try {
        await withStore(db, {}, async store => {
            for (const id of [100, 200, 300]) {
                store.upsert({ event: id, time: id, category: 'law', data: [] });
            }
            const cursor = store.maxEvent();
            assert.equal(cursor, 300);

            // Backfill delivers an older event after the cursor has moved past it.
            store.upsert({ event: 150, time: 150, category: 'law', data: [] });
            assert.equal(store.maxEvent(), 300, 'an older event must not rewind the head');

            // It is still delivered when asked for explicitly.
            assert.ok(store.query({ afterEvent: 100 }).some(r => r.event === 150));
        });
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('query filters by category', async () => {
    const { dir, db } = workspace();
    try {
        await withStore(db, {}, async store => {
            store.upsert({ event: 1, time: 1, category: 'law', data: [] });
            store.upsert({ event: 2, time: 2, category: 'ncte', data: [] });
            store.upsert({ event: 3, time: 3, category: 'nrefound', data: [] });
            const rows = store.query({ categories: ['ncte', 'nrefound'] });
            assert.deepEqual(rows.map(r => r.event), [2, 3]);
        });
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

// ---- export parity ----

test('export reproduces the original akari line', async () => {
    const { dir, source, db } = workspace();
    try {
        writeFileSync(source, lawLine + '\n');
        await withStore(db, {}, async store => {
            await store.ingest(source);
            const out = [];
            store.exportJsonl(line => out.push(line), { withData: true });
            assert.equal(out.length, 1);
            assert.deepEqual(
                JSON.parse(out[0]),
                JSON.parse(lawLine),
                'a round trip must be indistinguishable from the source'
            );
        });
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('export omits absent optional fields instead of writing empty strings', async () => {
    const { dir, source, db } = workspace();
    try {
        writeFileSync(source, minimalLine + '\n');
        await withStore(db, {}, async store => {
            await store.ingest(source);
            const out = [];
            store.exportJsonl(line => out.push(line), { withData: true });
            const parsed = JSON.parse(out[0]);
            assert.equal('actor' in parsed, false, 'absent must stay absent');
            assert.equal('receptor' in parsed, false);
            assert.equal('law_issue_id' in parsed, false);
            assert.deepEqual(parsed, JSON.parse(minimalLine));
        });
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('export can leave the bulky field behind', async () => {
    const { dir, source, db } = workspace();
    try {
        writeFileSync(source, lawLine + '\n');
        await withStore(db, {}, async store => {
            await store.ingest(source);
            const without = [];
            store.exportJsonl(line => without.push(line));
            assert.equal('data' in JSON.parse(without[0]), false);

            const withData = [];
            store.exportJsonl(line => withData.push(line), { withData: true });
            assert.ok(Array.isArray(JSON.parse(withData[0]).data));
        });
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('export honours the cursor and category filter', async () => {
    const { dir, db } = workspace();
    try {
        await withStore(db, {}, async store => {
            store.upsert({ event: 1, time: 1, category: 'law', data: [] });
            store.upsert({ event: 2, time: 2, category: 'ncte', data: [] });
            store.upsert({ event: 3, time: 3, category: 'law', data: [] });

            const out = [];
            store.exportJsonl(line => out.push(line), { categories: ['law'], afterEvent: 1 });
            assert.deepEqual(out.map(l => JSON.parse(l).event), [3]);
        });
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('export escapes the characters that read as line terminators', async () => {
    // U+0085, U+2028 and U+2029 are legal in a JSON string and JSON.stringify
    // emits them raw, but enough readers split lines on them that a faithful
    // value comes back as two truncated ones. Akari escapes them; so must this.
    const { dir, db } = workspace();
    try {
        const tricky = ['Served' + String.fromCharCode(0x85), 'line' + String.fromCharCode(0x2028), 'sep' + String.fromCharCode(0x2029)];
        await withStore(db, {}, async store => {
            store.upsert({ event: 1, time: 1, category: 'chfield', actor: 'x', data: tricky });
            const out = [];
            store.exportJsonl(line => out.push(line), { withData: true });

            assert.equal(out.length, 1, 'the escaped value must stay on one line');
            for (const code of ['\\u0085', '\\u2028', '\\u2029']) {
                assert.ok(out[0].includes(code), `expected ${code} to be escaped`);
            }
            assert.equal(out[0].includes(String.fromCharCode(0x85)), false, 'no raw terminator');
            // And the value itself survives the round trip.
            assert.deepEqual(JSON.parse(out[0]).data, tricky);
        });
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('a full round trip over many events is lossless', async () => {
    const { dir, source, db } = workspace();
    try {
        const lines = [];
        for (let i = 1; i <= 500; i++) {
            lines.push(JSON.stringify({
                event: 1000 + i,
                time: 1700000000 + i,
                category: i % 3 === 0 ? 'law' : i % 3 === 1 ? 'ncte' : 'rupdate',
                ...(i % 2 === 0 ? { actor: `nation_${i}` } : {}),
                ...(i % 5 === 0 ? { receptor: `other_${i}` } : {}),
                data: [`text ${i}`],
            }));
        }
        writeFileSync(source, lines.join('\n') + '\n');

        await withStore(db, {}, async store => {
            await store.ingest(source);
            assert.equal(store.count(), 500);

            const out = [];
            store.exportJsonl(line => out.push(line), { withData: true });
            assert.equal(out.length, 500);
            for (let i = 0; i < 500; i++) {
                assert.deepEqual(JSON.parse(out[i]), JSON.parse(lines[i]), `row ${i} differs`);
            }
        });
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

// ---- guards ----

test('a read-only store refuses to be written to', async () => {
    const { dir, db } = workspace();
    try {
        await withStore(db, {}, store => store.upsert({ event: 1, time: 1, category: 'law', data: [] }));
        await withStore(db, { readOnly: true }, store => {
            assert.equal(store.count(), 1, 'reads still work');
            assert.throws(() => store.upsert({ event: 2, time: 2, category: 'law', data: [] }), /read-only/);
            assert.throws(() => store.setMeta('k', 'v'), /read-only/);
        });
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('a store from a newer schema is refused', async () => {
    const { dir, db } = workspace();
    try {
        await withStore(db, {}, () => {});
        // Stand in for a store written by a future build.
        const { DatabaseSync } = await import('node:sqlite');
        const raw = new DatabaseSync(db);
        raw.exec(`PRAGMA user_version = ${SCHEMA_VERSION + 1}`);
        raw.close();

        assert.throws(() => new EventStore(db), /schema version/);

        // Reading is still allowed, and says so, so a caller can report the
        // mismatch rather than silently serving stale-shaped rows.
        const reader = new EventStore(db, { readOnly: true });
        assert.equal(reader.schemaTooNew, true);
        reader.close();
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('the bulk field is stored apart from the event columns', async () => {
    const { dir, source, db } = workspace();
    try {
        writeFileSync(source, lawLine + '\n');
        await withStore(db, {}, async store => {
            await store.ingest(source);
            const row = store.get(100);
            assert.deepEqual(row.data, [JSON.parse(lawLine).data[0]]);

            // Reading without asking for data must not drag it along.
            const [light] = store.query({});
            assert.equal('data' in light, false);
        });
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('a store opens read-write while another connection holds it open', async () => {
    // The enricher and the query service run against the same file at the same
    // time, always. Opening the writer used to fail while the reader had the file
    // open, because the constructor assigned PRAGMA journal_mode = WAL and taking
    // that pragma needs locks - which over a bind mount surfaces as SQLITE_IOERR
    // rather than SQLITE_BUSY, so it read as "disk I/O error" with nothing in the
    // message pointing at the other process.
    //
    // The bind mount is what turns a lock conflict into an I/O error, so this
    // cannot reproduce the original failure on a local filesystem. What it pins is
    // the behaviour: a writer coexists with a reader and leaves the mode alone.
    const { dir, db } = workspace();
    try {
        const first = new EventStore(db);
        first.upsert({ event: 1, time: 10, category: 'law', data: ['a'] });
        assert.equal(first.journalMode(), 'wal', 'the store is WAL once written to');

        // A reader holding the file, as the query service does.
        const reader = new EventStore(db, { readOnly: true });
        try {
            assert.equal(reader.maxEvent(), 1);
        } finally { reader.close(); }

        // And a second writer, which is the case that used to throw.
        const second = new EventStore(db);
        try {
            second.upsert({ event: 2, time: 20, category: 'law', data: ['b'] });
            assert.equal(second.maxEvent(), 2);
        } finally { second.close(); }

        first.close();
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('the bulk field is first-write-wins, so a repeat cannot blank it', async () => {
    // Load-bearing for recovery: re-importing a range the store already holds must
    // not overwrite the stored payload with whatever the source carries this time.
    const { dir, db } = workspace();
    try {
        const store = new EventStore(db);
        store.upsert({ event: 1, time: 1, category: 'law', data: ['the real payload'] });
        store.upsert({ event: 1, time: 999, category: 'not-law', data: [] });
        const row = store.get(1);
        assert.deepEqual(row.data, ['the real payload'], 'data is not blanked by a repeat');
        assert.equal(row.category, 'law', 'nor is a source field');
        assert.equal(row.time, 1);
        store.close();
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('the store survives being reopened', async () => {
    const { dir, source, db } = workspace();
    try {
        writeFileSync(source, lawLine + '\n');
        await withStore(db, {}, store => store.ingest(source));
        await withStore(db, {}, store => {
            assert.equal(store.count(), 1);
            assert.equal(store.maxEvent(), 100);
            // The resume offset survived too, so nothing is re-read.
            const r = readFileSync(db);
            assert.ok(r.length > 0);
        });
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

// ---- the gap table ----

test('recorded gaps merge and report as maximal runs', async () => {
    const { dir, db } = workspace();
    try {
        await withStore(db, {}, store => {
            store.recordGap(100, 110);
            store.recordGap(200, 205);
            assert.deepEqual(store.listGaps(), [{ start: 100, end: 110 }, { start: 200, end: 205 }]);

            // Adjacent and overlapping ranges collapse, so the table stays a set of
            // runs rather than a log of every detection.
            store.recordGap(111, 120);
            store.recordGap(115, 130);
            assert.deepEqual(store.listGaps(), [{ start: 100, end: 130 }, { start: 200, end: 205 }]);

            const totals = store.gapTotals();
            assert.equal(totals.gaps, 2);
            assert.equal(totals.missingIds, 31 + 6);
            assert.equal(totals.largestGap, 31);
            assert.equal(totals.largestGapAt, 100);
        });
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('storing an id clears it from the gap, splitting the run', async () => {
    const { dir, db } = workspace();
    try {
        await withStore(db, {}, store => {
            store.recordGap(100, 110);
            store.upsert({ event: 105, time: 1, category: 'law', data: [] });
            assert.deepEqual(store.listGaps(), [{ start: 100, end: 104 }, { start: 106, end: 110 }]);

            // Clearing an end shrinks the run rather than leaving an empty one.
            store.upsert({ event: 100, time: 1, category: 'law', data: [] });
            assert.deepEqual(store.listGaps(), [{ start: 101, end: 104 }, { start: 106, end: 110 }]);
        });
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('rebuildGaps reads the gaps out of the events table', async () => {
    const { dir, db } = workspace();
    try {
        await withStore(db, {}, store => {
            for (const id of [1, 2, 5, 6, 10]) {
                store.upsert({ event: id, time: id, category: 'law', data: [] });
            }
            const rebuilt = store.rebuildGaps();
            assert.equal(rebuilt.gaps, 2);
            assert.equal(rebuilt.missingIds, 2 + 3);
            assert.deepEqual(store.listGaps(), [{ start: 3, end: 4 }, { start: 7, end: 9 }]);
            assert.equal(store.gapTotals().largestGap, 3);
        });
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('an invalid or synthetic range is not recorded', async () => {
    const { dir, db } = workspace();
    try {
        await withStore(db, {}, store => {
            store.recordGap(0, 5);
            store.recordGap(10, 9);
            store.recordGap(-3, 5);
            assert.deepEqual(store.listGaps(), []);
            assert.equal(store.gapTotals().gaps, 0);
        });
    } finally { rmSync(dir, { recursive: true, force: true }); }
});
