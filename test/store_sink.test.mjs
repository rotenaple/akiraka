// The sink is what makes the store the collector's live source of truth, so
// these cover the two things that can quietly lose events: a buffer that never
// commits, and a batch where one bad row takes the rest down with it.
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { EventStore } from '../dist/event_store.js';
import { StoreSink, teeToStore, ENRICH_VERSION } from '../dist/store_sink.js';

function tmpStore(name) {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'akiraka-sink-'));
    return { dir, file: path.join(dir, `${name}.db`) };
}

const ev = (id, over = {}) => JSON.stringify({
    event: id,
    time: 1700000000 + id,
    category: 'law',
    actor: 'someone',
    data: ['a', 'b'],
    ...over,
});

test('a single write is committed without an explicit flush', () => {
    // The idle-flush branch is the whole reason a live event does not sit in
    // memory until something else happens to arrive.
    const { dir, file } = tmpStore('single');
    const sink = new StoreSink(file, { idleFlushMs: 0 });

    sink.write(ev(1) + '\n');

    const store = new EventStore(file, { readOnly: true });
    assert.equal(store.maxEvent(), 1);
    store.close();

    sink.close();
    fs.rmSync(dir, { recursive: true, force: true });
});

test('many lines in one write land as one batch', () => {
    const { dir, file } = tmpStore('multi');
    const sink = new StoreSink(file, { batchSize: 10, idleFlushMs: 60_000 });

    // One write carrying a partial batch, which must NOT be on disk yet.
    sink.write([1, 2, 3].map(i => ev(i)).join('\n') + '\n');

    let store = new EventStore(file, { readOnly: true });
    assert.equal(store.maxEvent(), 0, 'a partial batch must stay buffered');
    store.close();

    // Crossing the threshold commits everything gathered so far.
    sink.write([4, 5, 6, 7, 8, 9, 10].map(i => ev(i)).join('\n') + '\n');
    store = new EventStore(file, { readOnly: true });
    assert.equal(store.maxEvent(), 10);
    store.close();

    sink.close();
    fs.rmSync(dir, { recursive: true, force: true });
});

test('a trailing partial line is held back rather than stored as broken JSON', () => {
    const { dir, file } = tmpStore('partial');
    const sink = new StoreSink(file, { batchSize: 100, idleFlushMs: 0 });

    // What a poll sees mid-append: complete lines plus the start of the next.
    sink.write(ev(1) + '\n' + ev(2) + '\n' + '{"event":3,"tim');

    const store = new EventStore(file, { readOnly: true });
    assert.equal(store.maxEvent(), 2, 'the fragment is neither stored nor counted as unparsed');
    store.close();

    sink.close();
    assert.equal(sink.dropped, 0);
    fs.rmSync(dir, { recursive: true, force: true });
});

test('a non-positive id is filtered before the store is asked', () => {
    // Previously this row reached the store, which refused it, aborting the batch
    // and forcing a row-by-row retry. Filtering it here means the sink cannot
    // provoke a store rejection through its input at all - the only ids it now
    // sends are ones the store will accept.
    const { dir, file } = tmpStore('pre-filtered');
    const sink = new StoreSink(file, { batchSize: 100, idleFlushMs: 0 });

    sink.write([ev(1), ev(0), ev(2)].join('\n') + '\n');
    sink.close();

    assert.equal(sink.dropped, 0, 'the store was never asked, so nothing was rejected');
    assert.equal(sink.synthetic, 1);

    const store = new EventStore(file, { readOnly: true });
    assert.equal(store.maxEvent(), 2, 'rows either side of it are stored');
    store.close();
    fs.rmSync(dir, { recursive: true, force: true });
});

test('an unparseable line is counted, not silently dropped', () => {
    const { dir, file } = tmpStore('garbage');
    const sink = new StoreSink(file, { batchSize: 100, idleFlushMs: 0 });

    sink.write('not json at all\n' + ev(7) + '\n');
    sink.close();

    assert.equal(sink.unparsed, 1);
    const store = new EventStore(file, { readOnly: true });
    assert.equal(store.maxEvent(), 7, 'the good line still lands');
    store.close();
    fs.rmSync(dir, { recursive: true, force: true });
});

test('enrich_v is recorded so a superseded build can be found later', () => {
    const { dir, file } = tmpStore('enrichv');
    const sink = new StoreSink(file, { idleFlushMs: 0 });
    sink.write(ev(1) + '\n');
    sink.close();

    const store = new EventStore(file, { readOnly: true });
    assert.equal(store.get(1).enrich_v, ENRICH_VERSION);
    store.close();
    fs.rmSync(dir, { recursive: true, force: true });
});

test('data is stored, because the collector keeps it and the replica drops it', () => {
    const { dir, file } = tmpStore('data');
    const sink = new StoreSink(file, { idleFlushMs: 0 });
    sink.write(ev(1, { data: ['poll', 'the', 'bill'] }) + '\n');
    sink.close();

    const store = new EventStore(file, { readOnly: true });
    assert.deepEqual(store.get(1).data, ['poll', 'the', 'bill']);
    store.close();
    fs.rmSync(dir, { recursive: true, force: true });
});

test('a replayed event does not become a second row', () => {
    const { dir, file } = tmpStore('replay');
    const sink = new StoreSink(file, { idleFlushMs: 0 });
    sink.write(ev(1) + '\n');
    sink.close();
    const afterFirst = sink.written;

    // Restart behaviour re-reads from offset 0 and re-appends. The primary key is
    // what absorbs it, and `written` distinguishes new from repeat for the caller.
    const again = new StoreSink(file, { idleFlushMs: 0 });
    again.write(ev(1) + '\n');
    again.write(ev(2) + '\n');
    again.close();

    assert.equal(afterFirst, 1);
    assert.equal(again.written, 1, 'only the new id is counted as inserted');
    assert.equal(again.dropped, 0);

    const store = new EventStore(file, { readOnly: true });
    assert.equal(store.maxEvent(), 2);
    store.close();
    fs.rmSync(dir, { recursive: true, force: true });
});

test('teed writes reach the file and the store', () => {
    const { dir, file } = tmpStore('tee');
    const jsonl = path.join(dir, 'out.jsonl');
    const primary = fs.createWriteStream(jsonl, { flags: 'a' });
    const sink = new StoreSink(file, { idleFlushMs: 0 });
    const tee = teeToStore(primary, sink);

    tee.write(ev(1) + '\n');
    tee.write(ev(2) + '\n');
    sink.close();

    return new Promise(resolve => primary.end(resolve)).then(() => {
        const lines = fs.readFileSync(jsonl, 'utf8').split('\n').filter(l => l.trim());
        assert.equal(lines.length, 2, 'the archive still gets everything');
        const store = new EventStore(file, { readOnly: true });
        assert.equal(store.maxEvent(), 2, 'and so does the store');
        store.close();
        fs.rmSync(dir, { recursive: true, force: true });
    });
});

test('teed to a null primary still fills the store', () => {
    // The post-freeze shape: the JSONL sink is simply absent. Nothing should
    // reach stdout, and nothing should be lost.
    const { dir, file } = tmpStore('tee-null');
    const sink = new StoreSink(file, { idleFlushMs: 0 });
    const tee = teeToStore(null, sink);

    tee.write(ev(1) + '\n');
    tee.write(ev(2) + '\n');
    sink.close();

    const store = new EventStore(file, { readOnly: true });
    assert.equal(store.maxEvent(), 2);
    store.close();
    fs.rmSync(dir, { recursive: true, force: true });
});

test("Akiri's conninit marker is counted as synthetic, not as a dropped event", () => {
    // Akari writes this on every stream connect. Batching it would abort a good
    // batch, force a row-by-row retry, and log what reads like data loss.
    const { dir, file } = tmpStore('synthetic');
    const sink = new StoreSink(file, { batchSize: 100, idleFlushMs: 0 });

    sink.write(JSON.stringify({ event: -1, time: 1791192969, category: 'conninit' }) + '\n');
    sink.write(ev(1) + '\n');
    sink.close();

    assert.equal(sink.synthetic, 1);
    assert.equal(sink.dropped, 0, 'nothing was lost, so nothing is reported as dropped');
    assert.equal(sink.unparsed, 0);
    assert.equal(sink.written, 1);

    const store = new EventStore(file, { readOnly: true });
    assert.equal(store.maxEvent(), 1, 'the real event still landed');
    store.close();
    fs.rmSync(dir, { recursive: true, force: true });
});

test('the synthetic marker does not cost its neighbours a retry', () => {
    const { dir, file } = tmpStore('synthetic-batch');
    const sink = new StoreSink(file, { batchSize: 100, idleFlushMs: 0 });

    sink.write([
        ev(1),
        JSON.stringify({ event: 0, time: 1, category: 'conninit' }),
        ev(2),
    ].join('\n') + '\n');
    sink.close();

    assert.equal(sink.synthetic, 1);
    assert.equal(sink.dropped, 0);
    const store = new EventStore(file, { readOnly: true });
    assert.equal(store.maxEvent(), 2);
    store.close();
    fs.rmSync(dir, { recursive: true, force: true });
});

test('close is idempotent, because exit and SIGTERM both reach it', () => {
    const { dir, file } = tmpStore('double-close');
    const sink = new StoreSink(file, { idleFlushMs: 0 });
    sink.write(ev(1) + '\n');
    sink.close();
    // The exit path and the signal handler can both run; a second close must not
    // turn a clean shutdown into an unhandled error on a closed handle.
    assert.doesNotThrow(() => sink.close());
    fs.rmSync(dir, { recursive: true, force: true });
});

test('upsertBatch commits once and reports how many ids were new', () => {
    const { dir, file } = tmpStore('batch');
    const store = new EventStore(file);

    const first = store.upsertBatch([
        { event: 1, time: 1, category: 'law', data: [] },
        { event: 2, time: 2, category: 'law', data: [] },
    ]);
    const second = store.upsertBatch([
        { event: 2, time: 2, category: 'law', data: [] },
        { event: 3, time: 3, category: 'law', data: [] },
    ]);

    assert.equal(first, 2);
    assert.equal(second, 1, 'the repeat is upserted but not counted as new');
    assert.equal(store.maxEvent(), 3);
    store.close();
    fs.rmSync(dir, { recursive: true, force: true });
});

test('a batch that throws leaves nothing behind', () => {
    const { dir, file } = tmpStore('batch-atomic');
    const store = new EventStore(file);

    assert.throws(() => store.upsertBatch([
        { event: 1, time: 1, category: 'law', data: [] },
        { event: -5, time: 2, category: 'law', data: [] },
    ]), /refusing to store event id/);

    // The good row preceding the bad one must be gone: a batch that half-applied
    // leaves the caller unable to say what it wrote.
    assert.equal(store.maxEvent(), 0);

    store.close();
    fs.rmSync(dir, { recursive: true, force: true });
});
