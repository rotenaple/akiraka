import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, rmSync } from 'fs';
import * as os from 'os';
import * as path from 'path';
import { EventStore } from '../dist/event_store.js';
import { createQueryServer, parseRequest, loadEndpointConfig, unservableEndpoints } from '../dist/query_api.js';

const CTE = ['ncte', 'cte', 'nrefound'];

const ev = (event, category, extra = {}) =>
    ({ event, time: 1700000000 + event, actor: `nation_${event}`, category, data: [`t${event}`], ...extra });

const CONFIG = {
    endpoints: {
        cessations: {
            categories: CTE,
            fields: ['event', 'time', 'actor', 'receptor', 'origin', 'category'],
        },
        law: { categories: ['law'], fields: ['event', 'time', 'actor', 'law_issue_id', 'law_option'] },
        everything: {},
        tiny: { categories: CTE, fields: ['event'], maxRows: 2 },
    },
};

/** A store over the given records, a server on an ephemeral port, and a fetch helper. */
async function withServer(records, options = {}, config = CONFIG) {
    const dir = mkdtempSync(path.join(os.tmpdir(), 'query-api-'));
    const db = path.join(dir, 'events.db');

    const store = new EventStore(db);
    try {
        for (const record of records) store.upsert(record);
    } finally {
        store.close();
    }

    const reader = new EventStore(db, { readOnly: true });
    const server = createQueryServer(reader, config, options.serverOptions);
    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
    const base = `http://127.0.0.1:${server.address().port}`;

    const get = async (p, headers = {}) => {
        const res = await fetch(`${base}${p}`, { headers });
        const text = await res.text();
        return {
            status: res.status,
            headers: res.headers,
            text,
            json: () => JSON.parse(text),
            rows: () => text.split('\n').filter(Boolean).map(l => JSON.parse(l)),
        };
    };

    return {
        store: reader, base, get,
        close: async () => {
            await new Promise(r => server.close(r));
            reader.close();
            rmSync(dir, { recursive: true, force: true });
        },
    };
}

test('an endpoint serves only its categories, with only its fields', async () => {
    const s = await withServer([
        ev(1, 'ncte', { origin: 'r1', law_issue_id: 9 }),
        ev(2, 'law', { law_issue_id: 77, law_option: 3 }),
        ev(3, 'nrefound'),
    ]);
    try {
        const res = await s.get('/events/cessations');
        assert.equal(res.status, 200);
        const rows = res.rows();
        assert.deepEqual(rows.map(r => r.event), [1, 3], 'law must not appear');
        for (const row of rows) {
            assert.deepEqual(
                Object.keys(row).sort(),
                ['actor', 'category', 'event', 'origin', 'time'].filter(k => k in row).sort(),
                'projection must exclude law_issue_id and carry nothing else',
            );
        }
    } finally { await s.close(); }
});

test('the event id is the cursor for the next page', async () => {
    const s = await withServer([ev(1, 'ncte'), ev(2, 'ncte'), ev(3, 'ncte')]);
    try {
        const rows = (await s.get('/events/cessations')).rows();
        assert.deepEqual(rows.map(r => r.event), [1, 2, 3]);

        const second = await s.get(`/events/cessations?after_event=${rows.at(-1).event}`);
        assert.deepEqual(second.rows(), [], 'after_event is exclusive, so the last row is not repeated');
    } finally { await s.close(); }
});

test('paging covers every row exactly once', async () => {
    const s = await withServer(Array.from({ length: 7 }, (_, i) => ev(i + 1, 'ncte')));
    try {
        const seen = [];
        let cursor = 0;
        for (let guard = 0; guard < 10; guard++) {
            const rows = (await s.get(`/events/everything?after_event=${cursor}&limit=3`)).rows();
            if (rows.length === 0) break;
            seen.push(...rows.map(r => r.event));
            cursor = rows.at(-1).event;
        }
        assert.deepEqual(seen, [1, 2, 3, 4, 5, 6, 7]);
    } finally { await s.close(); }
});

test('an endpoint maxRows caps the page and says so rather than truncating quietly', async () => {
    const s = await withServer(Array.from({ length: 10 }, (_, i) => ev(i + 1, 'ncte')));
    try {
        const res = await s.get('/events/tiny?limit=5');
        assert.equal(res.status, 400);
        assert.equal(res.json().max_rows, 2);
        assert.match(res.json().error, /exceeds/);
    } finally { await s.close(); }
});

test('limit at or below the cap is honoured', async () => {
    const s = await withServer(Array.from({ length: 10 }, (_, i) => ev(i + 1, 'ncte')));
    try {
        assert.equal((await s.get('/events/tiny')).rows().length, 2);
        assert.equal((await s.get('/events/tiny?limit=1')).rows().length, 1);
    } finally { await s.close(); }
});

test('an unknown endpoint lists the known ones', async () => {
    const s = await withServer([ev(1, 'ncte')]);
    try {
        const res = await s.get('/events/nope');
        assert.equal(res.status, 404);
        assert.match(res.json().error, /nope/);
        assert.ok(res.json().known.includes('cessations'));
    } finally { await s.close(); }
});

test('a per-endpoint max_rows above the server default is honoured', async () => {
    // Two bugs met here. The config file is snake_case and the discovery response
    // reports max_rows, but the reader wanted camelCase maxRows - so every
    // max_rows in the file was decorative. And the cap was computed as
    // Math.min(definition.maxRows ?? default, default), making the server default
    // a ceiling, so even a correctly-spelled value was clamped back to 10000.
    //
    // Both were found by asking for a large page and being refused. Worth a test
    // because the failure is invisible: a smaller page than asked for, served
    // without complaint, which is just a slow client nobody suspects.
    const s = await withServer(Array.from({ length: 30 }, (_, i) => ev(i + 1, 'ncte')), {},
        {
            endpoints: {
                big: { categories: CTE, maxRows: 25 },
                small: { categories: CTE, maxRows: 3 },
            },
        });
    try {
        assert.equal((await s.get('/events/big')).rows().length, 25, 'the cap is the endpoint\'s own');
        assert.equal((await s.get('/events/big?limit=10')).rows().length, 10, 'and a smaller ask is honoured');
        assert.equal((await s.get('/events/small')).rows().length, 3);

        // Over the cap is still refused rather than clamped, or a client would
        // believe it had the page it asked for.
        const over = await s.get('/events/small?limit=10');
        assert.equal(over.status, 400);
        assert.match(over.json().error, /exceeds/);
    } finally { await s.close(); }
});

test('max_rows in the config file is read, not maxRows', async () => {
    const dir = mkdtempSync(path.join(os.tmpdir(), 'query-config-'));
    const file = path.join(dir, 'endpoints.json');
    try {
        writeFileSync(file, JSON.stringify({
            endpoints: { snake: { categories: CTE, max_rows: 7 } },
        }));
        const loaded = loadEndpointConfig(file);
        assert.equal(loaded.endpoints.snake.maxRows, 7, 'snake_case is what the file and the API use');
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('a nonsense page cap is rejected at load rather than at request time', () => {
    // Better at load: an endpoint whose cap is a string would otherwise serve
    // 10000-row pages forever and nobody would find out until a client asked for
    // more and got a 400 mentioning a number it never configured.
    const dir = mkdtempSync(path.join(os.tmpdir(), 'query-config-'));
    const file = path.join(dir, 'endpoints.json');
    try {
        for (const bad of [{ max_rows: 0 }, { max_rows: -5 }, { max_rows: 1.5 }, { max_rows: 'ten' }]) {
            writeFileSync(file, JSON.stringify({ endpoints: { x: { ...bad } } }));
            assert.throws(() => loadEndpointConfig(file), /max_rows must be a positive integer/,
                `should reject ${JSON.stringify(bad)}`);
        }
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('the pre-store cursor is refused, not silently ignored', async () => {
    // A client written against the ingest-order API sends ?since=<seq> and reads
    // _seq and x-akiraka-max-seq. None of those exist here, and ignoring them is
    // worse than failing: after_event falls back to 0, so the client receives the
    // oldest rows in the store, computes a cursor of NaN, and asks for the same
    // first page forever - 200 and well-formed rows the whole time. One clear
    // error beats a client that looks like it is working.
    const s = await withServer(Array.from({ length: 5 }, (_, i) => ev(i + 1, 'ncte')));
    try {
        const res = await s.get('/events/cessations?since=0&limit=2');
        assert.equal(res.status, 400);
        assert.match(res.json().error, /after_event/);
        assert.match(res.json().error, /not since/);

        // The modern spelling still works, including alongside the old one being
        // present and wrong - the presence of since is what is rejected.
        assert.equal((await s.get('/events/cessations?after_event=0&limit=2')).status, 200);
    } finally { await s.close(); }
});

test('a bad after_event or limit is rejected with a usable message', async () => {
    const s = await withServer([ev(1, 'ncte')]);
    try {
        for (const q of ['?after_event=abc', '?after_event=-1', '?after_event=1.5', '?limit=0', '?limit=abc']) {
            const res = await s.get(`/events/cessations${q}`);
            assert.equal(res.status, 400, `${q} should be rejected`);
            assert.ok(res.json().error.length > 0);
        }
    } finally { await s.close(); }
});

test('health and endpoint discovery are served', async () => {
    const s = await withServer([ev(1, 'ncte'), ev(2, 'ncte')]);
    try {
        const health = await s.get('/health');
        assert.equal(health.status, 200);
        assert.equal(health.json().ok, true);
        assert.equal(health.json().maxEvent, 2);
        assert.equal(health.json().hasEvents, true);
        // Neither the row count nor the category list: both need a scan of the
        // store, and the compose health check allows three seconds.
        assert.equal('rows' in health.json(), false);
        assert.equal('categories' in health.json(), false);

        const list = await s.get('/endpoints');
        assert.equal(list.status, 200);
        assert.equal(list.json().endpoints.cessations.path, '/events/cessations');
        assert.equal(list.json().endpoints.tiny.max_rows, 2);
        assert.equal(list.json().endpoints.everything.categories, null);
    } finally { await s.close(); }
});

test('non-GET and unknown paths are refused', async () => {
    const s = await withServer([ev(1, 'ncte')]);
    try {
        assert.equal((await fetch(`${s.base}/events/cessations`, { method: 'POST' })).status, 405);
        assert.equal((await s.get('/')).status, 404);
        assert.equal((await s.get('/events')).status, 404);
    } finally { await s.close(); }
});

test('response headers describe the page', async () => {
    const s = await withServer([ev(1, 'ncte'), ev(2, 'ncte')]);
    try {
        const res = await s.get('/events/cessations?after_event=0&limit=5');
        assert.equal(res.headers.get('content-type'), 'application/x-ndjson; charset=utf-8');
        assert.equal(res.headers.get('x-akiraka-endpoint'), 'cessations');
        assert.equal(res.headers.get('x-akiraka-count'), '2');
        assert.equal(res.headers.get('x-akiraka-max-event'), '2');
        assert.equal(res.headers.get('x-akiraka-last-event'), '2', 'the client should not have to parse the body to find the cursor');
    } finally { await s.close(); }
});

test('an empty page still reports where the cursor should go', async () => {
    const s = await withServer([ev(1, 'ncte'), ev(2, 'ncte')]);
    try {
        const res = await s.get('/events/cessations?after_event=99');
        assert.equal(res.status, 200);
        assert.deepEqual(res.rows(), []);
        assert.equal(res.headers.get('x-akiraka-last-event'), '99', 'an empty page must not appear to rewind the cursor');
    } finally { await s.close(); }
});

test('an endpoint with no categories serves the whole store', async () => {
    const s = await withServer([ev(1, 'ncte'), ev(2, 'law'), ev(3, 'rupdate')]);
    try {
        assert.deepEqual((await s.get('/events/everything')).rows().map(r => r.event), [1, 2, 3]);
    } finally { await s.close(); }
});

test('a null field is left out rather than written as null', async () => {
    // Akari omits absent fields, and SQLite hands back every column as a key with
    // null in it, so projecting naively would add `"receptor": null` to rows that
    // never had a receptor.
    const s = await withServer([ev(1, 'ncte'), ev(2, 'nrefound')]);
    try {
        const rows = (await s.get('/events/cessations')).rows();
        assert.equal('receptor' in rows[0], false, 'no receptor was stored, so none should appear');
        assert.equal('origin' in rows[0], false);
        assert.ok('event' in rows[0] && 'time' in rows[0], 'the key and the timestamp are always present');
    } finally { await s.close(); }
});

test('every category is stored whatever the endpoints ask for', async () => {
    // The store is not configured with a category list, so it cannot drop anything.
    // An earlier index took that list at ingest, and the shipped default silently
    // discarded 99% of events before this was caught.
    const s = await withServer([ev(1, 'ncte'), ev(2, 'law'), ev(3, 'rupdate')]);
    try {
        const cats = s.store.stats().categories.map(c => c.category).sort();
        assert.deepEqual(cats, ['law', 'ncte', 'rupdate']);
        assert.equal(s.store.count(), 3);
    } finally { await s.close(); }
});

test('an endpoint asking for a category the store lacks is reported', async () => {
    // A category the store never had is a config problem worth surfacing, not a
    // page that stays empty forever looking like "no events of that kind yet".
    const s = await withServer([ev(1, 'ncte')]);
    try {
        const problems = unservableEndpoints(s.store, CONFIG);
        assert.ok(problems.some(p => /law: not in the store/.test(p)), `law should be reported: ${problems}`);
        assert.ok(problems.every(p => /not in the store/.test(p)), 'every problem should name the cause');
    } finally { await s.close(); }
});

test('an empty store is reported as empty, not as broken configuration', async () => {
    const dir = mkdtempSync(path.join(os.tmpdir(), 'query-empty-'));
    const db = path.join(dir, 'events.db');
    try {
        const empty = new EventStore(db);
        empty.close();
        const reader = new EventStore(db, { readOnly: true });
        try {
            const problems = unservableEndpoints(reader, CONFIG);
            assert.equal(problems.length, 1);
            assert.match(problems[0], /empty/);
        } finally { reader.close(); }
    } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('a bearer token is required when one is configured', async () => {
    const s = await withServer([ev(1, 'ncte')], { serverOptions: { token: 'sekrit' } });
    try {
        assert.equal((await s.get('/events/cessations')).status, 401, 'no token');
        assert.equal((await s.get('/events/cessations', { authorization: 'Bearer wrong' })).status, 401);
        assert.equal((await s.get('/events/cessations', { authorization: 'sekrit' })).status, 401, 'not a Bearer header');
        assert.equal((await s.get('/events/cessations', { authorization: 'Bearer sekrit' })).status, 200);
        assert.equal((await s.get('/events/cessations', { authorization: 'Bearer sekritx' })).status, 401);

        // Health stays open, or a monitor would need the secret just to probe.
        assert.equal((await s.get('/health')).status, 200);
    } finally { await s.close(); }
});

test('no authentication is required when no token is configured', async () => {
    const s = await withServer([ev(1, 'ncte')]);
    try {
        assert.equal((await s.get('/events/cessations')).status, 200);
    } finally { await s.close(); }
});

test('parseRequest leaves the page size to the endpoint in both directions', () => {
    // Absent rather than the server maximum: the endpoint owns its page size, and
    // defaulting here made a bare GET exceed a small endpoint's cap and 400.
    const bare = parseRequest('/events/cessations');
    assert.equal(bare.endpoint, 'cessations');
    assert.equal(bare.afterEvent, 0);
    assert.equal(bare.limit, undefined, 'no page size requested means the endpoint decides');

    const withCursor = parseRequest('/events/cessations?after_event=42');
    assert.equal(withCursor.afterEvent, 42);
    assert.equal(withCursor.limit, undefined);

    // Asked-for size is passed through unreduced. This used to be clamped to a cap
    // passed in from the caller, which meant every request was capped at the
    // server default before the endpoint was resolved - so a larger per-endpoint
    // cap never applied and the over-cap refusal downstream could never fire.
    assert.deepEqual(
        parseRequest('/events/cessations?limit=500'),
        { endpoint: 'cessations', afterEvent: 0, limit: 500 }
    );
    assert.deepEqual(
        parseRequest('/events/catchup?limit=100000'),
        { endpoint: 'catchup', afterEvent: 0, limit: 100000 }
    );

    assert.equal(parseRequest('/events/cessations?after_event=x').error !== undefined, true);
    assert.equal(parseRequest('/events/?after_event=1').error !== undefined, true);
});

test('loadEndpointConfig rejects malformed definitions', () => {
    const dir = mkdtempSync(path.join(os.tmpdir(), 'query-config-'));
    try {
        const write = (body) => {
            const f = path.join(dir, 'endpoints.json');
            writeFileSync(f, JSON.stringify(body));
            return f;
        };
        assert.throws(() => loadEndpointConfig(write({})), /endpoints/);
        assert.throws(() => loadEndpointConfig(write({ endpoints: { 'bad name': {} } })), /must be letters/);
        assert.throws(() => loadEndpointConfig(write({ endpoints: { a: { categories: 'x' } } })), /must be an array/);
        assert.throws(() => loadEndpointConfig(write({ endpoints: { a: { fields: 'x' } } })), /must be an array/);
        assert.doesNotThrow(() => loadEndpointConfig(write(CONFIG)));
    } finally { rmSync(dir, { recursive: true, force: true }); }
});
/**
 * A query that throws must answer that one request, not take the process with it.
 *
 * This was not hypothetical. On the live collector, `after_event=375094339`
 * returned nothing at all - the socket closed mid-response and the service died -
 * while 0, 300000000 and 375000000 served fine. `restart: unless-stopped` brought
 * it back and it answered /health, so it looked healthy while every reader paging
 * through that part of the store got nothing.
 *
 * The throw is SQLITE_CORRUPT from reading a page in the store's write frontier.
 * Simulated here by making `query` throw the same error, because reproducing a
 * torn write in a test would mean reproducing the filesystem that causes it.
 */
test('a query that throws answers 503 rather than killing the server', async () => {
    const s = await withServer([ev(1, 'ncte'), ev(2, 'ncte')]);
    try {
        const boom = Object.assign(new Error('database disk image is malformed'), { errcode: 11 });
        const original = s.store.query.bind(s.store);
        s.store.query = () => { throw boom; };

        const failed = await s.get('/events/cessations');
        assert.equal(failed.status, 503, 'the caller gets an error, not a closed socket');
        const body = failed.json();
        assert.equal(body.errcode, 11, 'the sqlite code is reported');
        assert.match(body.error, /earlier cursor/, 'and what to do about it');
        assert.equal(typeof body.max_event, 'number', 'with a known-good watermark to fall back to');

        // The server is still serving. This is the part that used to fail: one bad
        // request killed the process for every subsequent reader.
        s.store.query = original;
        const ok = await s.get('/events/cessations');
        assert.equal(ok.status, 200, 'the next request is unaffected');
        assert.equal(ok.rows().length, 2);
    } finally {
        await s.close();
    }
});

test('an error building the error response does not throw', async () => {
    // safeMaxEvent reads the store too, and an error response that threw while
    // assembling itself would be no response at all.
    const s = await withServer([ev(1, 'ncte')]);
    try {
        s.store.query = () => { throw Object.assign(new Error('boom'), { errcode: 11 }); };
        s.store.maxEvent = () => { throw new Error('also broken'); };
        const res = await s.get('/events/cessations');
        assert.equal(res.status, 503);
        assert.equal(res.json().max_event, 0, 'a broken watermark reads as unknown, not a crash');
    } finally {
        await s.close();
    }
});
