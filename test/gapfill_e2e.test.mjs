// Gap-fill end to end, against a controlled world.
//
// The fixture is real: a window of events actually collected today, with random
// contiguous chunks removed to stand in for events that were missed. The fake NS
// API serves the world those events came from, so the whole chain runs for real
// - fetch, paginate, replay over SSE, reparse, enrich, merge - with the only
// substitution being the API and the data directory.
//
// What makes this worth having: the assertion is an exact set equality between
// the ids deliberately removed and the ids recovered. A test that only checked
// "some events came back" would have passed while the timestamp element was
// being read as TIME, which is precisely the bug that put 184937 events into the
// store with no timestamp.
//
// The reparse step needs the akari binary. Where it is absent the test says so
// and stops, rather than passing vacuously - a skip that looks like a pass is
// how a broken chain stays broken.
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';

import { GapFiller, GapState, scanFileForGaps, fetchHappeningRange } from '../dist/gapfill.js';
import { NsPacer } from '../dist/ns_client.js';
import { LawParser } from '../dist/lawParser.js';
import { startFakeNsFromXml } from '../dist/fake_ns_api.js';
import { buildFixture, toRanges } from '../dist/make_gapfill_fixture.js';

const FIXTURE_SEED = 20261005;

/** Where the akari binary is, if this machine has one. */
function findAkari() {
    const explicit = process.env.AKARI_BIN;
    if (explicit && fs.existsSync(explicit)) return explicit;
    if (process.platform === 'win32') {
        // The container's binary is a Linux ELF and cannot run here, so do not go
        // looking for it - the absence is expected, not a fault.
        try {
            const found = execFileSync('where', ['akari'], { encoding: 'utf8' }).split(/\r?\n/)[0].trim();
            if (found && fs.existsSync(found)) return found;
        } catch {
            // Not on PATH.
        }
        return null;
    }
    for (const candidate of ['akari', '/usr/local/bin/akari']) {
        if (fs.existsSync(candidate)) return candidate;
    }
    return null;
}

function makeFixtureDir() {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'akiraka-gapfix-'));
    return dir;
}

/**
 * Build the fixture from the live store.
 *
 * Skips when there is no store to read, because a fixture built from invented
 * events would only test the author's imagination - which is the thing this
 * whole approach exists to avoid.
 */
function loadFixture() {
    const storePath = process.env.GAPFILL_FIXTURE_STORE || path.resolve('data/events.db');
    if (!fs.existsSync(storePath)) return null;
    const dir = makeFixtureDir();
    const fixture = buildFixture({
        storePath,
        outDir: dir,
        count: 400,
        chunks: 4,
        minChunk: 4,
        maxChunk: 20,
        seed: FIXTURE_SEED,
    });
    return { dir, fixture };
}

test('gap detection reports exactly the chunks the fixture removed', async (t) => {
    const loaded = loadFixture();
    if (!loaded) {
        t.skip('no event store to build a fixture from (set GAPFILL_FIXTURE_STORE)');
        return;
    }
    const { dir, fixture } = loaded;
    try {
        const gaps = await scanFileForGaps(fixture.seedPath);
        assert.deepEqual(
            gaps.map(g => ({ start: g.start, end: g.end })),
            fixture.droppedRanges,
            'detection must agree with what was removed, run for run'
        );
        // And the ids themselves, not just the ranges.
        const detectedIds = fixture.seedIds.length
            ? allIdsBetween(fixture, gaps)
            : [];
        assert.deepEqual(detectedIds, fixture.droppedIds);
    } finally {
        fs.rmSync(dir, { recursive: true, force: true });
    }
});

function allIdsBetween(fixture, gaps) {
    const present = new Set(fixture.seedIds);
    const out = [];
    for (const g of gaps) for (let id = g.start; id <= g.end; id++) if (!present.has(id)) out.push(id);
    return out.sort((a, b) => a - b);
}

test('a filled gap returns every id that was missing, and nothing else', async (t) => {
    const akari = findAkari();
    if (!akari) {
        t.skip('no akari binary available; the reparse step cannot run');
        return;
    }
    const loaded = loadFixture();
    if (!loaded) {
        t.skip('no event store to build a fixture from (set GAPFILL_FIXTURE_STORE)');
        return;
    }
    const { dir, fixture } = loaded;
    const ns = await startFakeNsFromXml(fs.readFileSync(path.join(dir, 'world.xml'), 'utf8'));
    const workDir = fs.mkdtempSync(path.join(os.tmpdir(), 'akiraka-gapwork-'));
    try {
        const statePath = path.join(workDir, 'state.json');
        const state = new GapState(statePath);
        const filler = new GapFiller({
            lawParser: new LawParser(path.resolve('data/issues_cache.txt')),
            // Required by the fetch boundary even for a local mock, which is the
            // point of enforcing it there rather than in the CLI.
            nsUserAgent: 'hermetic-test',
            akariBinary: akari,
            replayPort: 0,
            apiUrl: ns.url,
            timeoutMs: 60_000,
            // Deliberately no CAS: the direct path is the one being tested, and a
            // CAS client would add a second moving part to blame on a failure.
            cas: null,
            // These talk to a local mock, and the pacer would space requests
            // a second apart for no benefit. Pacing has its own suite.
            pacer: new NsPacer({ disabled: true }),
        });

        const gaps = await scanFileForGaps(fixture.seedPath);
        assert.ok(gaps.length > 0, 'fixture must contain at least one gap');

        const recovered = [];
        for (const gap of gaps) {
            const result = await filler.fillRange(gap.start, gap.end);
            assert.equal(result.cutoffReached, false, `range ${gap.start}-${gap.end} hit the cutoff`);
            recovered.push(...result.events.map(e => e.id));
        }

        recovered.sort((a, b) => a - b);
        assert.deepEqual(
            recovered,
            fixture.droppedIds,
            'the recovered set must equal the dropped set exactly'
        );

        // Pagination: 63 dropped events across 5 gaps is under one page each, so
        // assert the mock was actually asked rather than assuming it was.
        assert.ok(ns.requests.length >= gaps.length, 'the fake API should have been called per gap');
    } finally {
        await ns.close();
        fs.rmSync(dir, { recursive: true, force: true });
        fs.rmSync(workDir, { recursive: true, force: true });
    }
});

test('a recovered event carries a real timestamp, not a zero', async (t) => {
    // The regression this harness exists to catch. TIMESTAMP versus TIME: the
    // element never matched, parseInt got an empty string, and the isNaN fallback
    // wrote 0 - so every recovered event looked like it had no timestamp at all.
    const akari = findAkari();
    if (!akari) {
        t.skip('no akari binary available');
        return;
    }
    const loaded = loadFixture();
    if (!loaded) {
        t.skip('no event store to build a fixture from');
        return;
    }
    const { dir, fixture } = loaded;
    const ns = await startFakeNsFromXml(fs.readFileSync(path.join(dir, 'world.xml'), 'utf8'));
    const workDir = fs.mkdtempSync(path.join(os.tmpdir(), 'akiraka-gapwork-'));
    try {
        const filler = new GapFiller({
            lawParser: new LawParser(path.resolve('data/issues_cache.txt')),
            // Required by the fetch boundary even for a local mock, which is the
            // point of enforcing it there rather than in the CLI.
            nsUserAgent: 'hermetic-test',
            akariBinary: akari,
            replayPort: 0,
            apiUrl: ns.url,
            timeoutMs: 60_000,
            cas: null,
            // These talk to a local mock, and the pacer would space requests
            // a second apart for no benefit. Pacing has its own suite.
            pacer: new NsPacer({ disabled: true }),
        });
        const gaps = await scanFileForGaps(fixture.seedPath);
        const first = gaps[0];
        const result = await filler.fillRange(first.start, first.end);

        assert.ok(result.events.length > 0, 'the gap should have been filled');
        for (const event of result.events) {
            const parsed = JSON.parse(event.line);
            assert.ok(
                parsed.time > 0,
                `recovered event ${parsed.event} has time ${parsed.time}; TIMESTAMP is not being read`
            );
            assert.ok(
                parsed.time >= fixture.minEvent * 0 + 1_000_000_000,
                `recovered event ${parsed.event} has an implausible time ${parsed.time}`
            );
        }
    } finally {
        await ns.close();
        fs.rmSync(dir, { recursive: true, force: true });
        fs.rmSync(workDir, { recursive: true, force: true });
    }
});

test('a range the world does not contain reports the retention cutoff', async (t) => {
    // The one-week retention window is a real operational limit, and the wrong
    // response to it is to keep retrying. An empty first page must surface as
    // cutoffReached so the caller can give up on that range.
    const loaded = loadFixture();
    if (!loaded) {
        t.skip('no event store to build a fixture from');
        return;
    }
    const { dir } = loaded;
    const ns = await startFakeNsFromXml(fs.readFileSync(path.join(dir, 'world.xml'), 'utf8'), {
        // Nothing at all is in retention, so the first page comes back empty.
        retentionFloor: Number.MAX_SAFE_INTEGER,
    });
    try {
        const filler = new GapFiller({
            lawParser: new LawParser(),
            nsUserAgent: 'hermetic-test',
            akariBinary: 'unused',
            replayPort: 0,
            apiUrl: ns.url,
            timeoutMs: 5_000,
            cas: null,
            // These talk to a local mock, and the pacer would space requests
            // a second apart for no benefit. Pacing has its own suite.
            pacer: new NsPacer({ disabled: true }),
        });
        const result = await filler.fillRange(1, 50);
        assert.equal(result.cutoffReached, true);
        assert.equal(result.fetched, 0);
        assert.deepEqual(result.events, []);
    } finally {
        await ns.close();
        fs.rmSync(dir, { recursive: true, force: true });
    }
});

test('a range wider than one page is paginated, not truncated', async (t) => {
    // fetchHappeningRange pages backwards with beforeid and treats a page shorter
    // than it asked for as the end of history. That second rule is an assumption
    // about NS, not a fact about the protocol, so it is pinned here explicitly:
    // pageSize matches the client's requested limit, which is the only way the
    // distinction between "a short page means done" and "a short page means the
    // API served less" can be told apart at all.
    const loaded = loadFixture();
    if (!loaded) {
        t.skip('no event store to build a fixture from');
        return;
    }
    const { dir, fixture } = loaded;
    const ns = await startFakeNsFromXml(fs.readFileSync(path.join(dir, 'world.xml'), 'utf8'), {
        // The client asks for 100 per request and compares against the same number.
        pageSize: 100,
    });
    try {
        const result = await fetchHappeningRange(ns.url, 'test-agent', fixture.minEvent, fixture.maxEvent);
        assert.equal(result.events.length, fixture.allEvents.length, 'every event in the range');
        assert.ok(
            ns.requests.length > 1,
            `a ${fixture.allEvents.length}-event range at 100 per page must take several requests, took ${ns.requests.length}`
        );
        // Each page must move backwards, or the pager would re-fetch one window.
        for (let i = 1; i < ns.requests.length; i++) {
            assert.ok(
                ns.requests[i].beforeid <= ns.requests[i - 1].beforeid,
                `request ${i} did not move backwards: ${ns.requests[i - 1].beforeid} -> ${ns.requests[i].beforeid}`
            );
        }
        const ids = result.events.map(e => e.id).sort((a, b) => a - b);
        assert.deepEqual(ids, fixture.allEvents.map(e => e.id).sort((a, b) => a - b));
    } finally {
        await ns.close();
        fs.rmSync(dir, { recursive: true, force: true });
    }
});

test('a page shorter than requested is taken as the end of history', async (t) => {
    // Documents the assumption the previous test relies on, and shows what it
    // costs: if NS ever serves a short page for a reason other than exhaustion,
    // fetchHappeningRange stops early and the missing events are not reported as
    // missing. Worth knowing before that happens in production.
    const loaded = loadFixture();
    if (!loaded) {
        t.skip('no event store to build a fixture from');
        return;
    }
    const { dir, fixture } = loaded;
    const ns = await startFakeNsFromXml(fs.readFileSync(path.join(dir, 'world.xml'), 'utf8'), {
        pageSize: 30,
    });
    try {
        const result = await fetchHappeningRange(ns.url, 'test-agent', fixture.minEvent, fixture.maxEvent);
        assert.equal(
            result.events.length,
            30,
            'a short first page ends the walk, so only one page is collected'
        );
    } finally {
        await ns.close();
        fs.rmSync(dir, { recursive: true, force: true });
    }
});

test('a 429 is retried rather than surfacing as a failure', async (t) => {
    // gap-fill retries on 429 with the retry-after delay. A mock that cannot
    // produce a 429 leaves that branch untested, and it is the branch that fires
    // against the real API.
    const loaded = loadFixture();
    if (!loaded) {
        t.skip('no event store to build a fixture from');
        return;
    }
    const { dir, fixture } = loaded;
    const ns = await startFakeNsFromXml(fs.readFileSync(path.join(dir, 'world.xml'), 'utf8'), {
        rateLimitFirst: 2,
        retryAfterSeconds: 1,
    });
    try {
        const result = await fetchHappeningRange(ns.url, 'test-agent', fixture.minEvent, fixture.minEvent + 20);
        assert.equal(result.events.length > 0, true, 'the retry should have succeeded');
        assert.ok(ns.requests.length >= 3, `two 429s then a success is three requests, saw ${ns.requests.length}`);
    } finally {
        await ns.close();
        fs.rmSync(dir, { recursive: true, force: true });
    }
});

test('an API error is reported, not swallowed', async (t) => {
    const loaded = loadFixture();
    if (!loaded) {
        t.skip('no event store to build a fixture from');
        return;
    }
    const { dir } = loaded;
    const ns = await startFakeNsFromXml(fs.readFileSync(path.join(dir, 'world.xml'), 'utf8'), {
        alwaysFail: true,
    });
    try {
        await assert.rejects(
            () => fetchHappeningRange(ns.url, 'test-agent', 1, 10),
            /status 500/,
            'a failing API must not look like an empty range'
        );
    } finally {
        await ns.close();
        fs.rmSync(dir, { recursive: true, force: true });
    }
});

test('the fixture is reproducible from its seed', async (t) => {
    // A fixture that changes between runs turns a real failure into a ghost.
    const loaded = loadFixture();
    if (!loaded) {
        t.skip('no event store to build a fixture from');
        return;
    }
    const { dir } = loaded;
    const storePath = process.env.GAPFILL_FIXTURE_STORE || path.resolve('data/events.db');
    try {
        const again = makeFixtureDir();
        const second = buildFixture({
            storePath, outDir: again, count: 400, chunks: 4, minChunk: 4, maxChunk: 20, seed: FIXTURE_SEED,
        });
        assert.deepEqual(second.droppedRanges, JSON.parse(fs.readFileSync(path.join(dir, 'fixture.json'), 'utf8')).droppedRanges);
        fs.rmSync(again, { recursive: true, force: true });
    } finally {
        fs.rmSync(dir, { recursive: true, force: true });
    }
});

test('a request without a user agent is refused, not sent', async (t) => {
    // NS requires every caller to name the nation asking. The CLI branches check
    // it, but this is the function that actually crosses to NS, so the rule is
    // enforced here: a caller added anywhere else would otherwise make anonymous
    // requests without tripping anything.
    const loaded = loadFixture();
    if (!loaded) {
        t.skip('no event store to build a fixture from');
        return;
    }
    const { dir } = loaded;
    const ns = await startFakeNsFromXml(fs.readFileSync(path.join(dir, 'world.xml'), 'utf8'));
    try {
        for (const bad of ['', '   ']) {
            await assert.rejects(
                () => fetchHappeningRange(ns.url, bad, 100, 110),
                /without a user agent/,
                `user agent ${JSON.stringify(bad)} should be refused`
            );
        }
        assert.equal(ns.requests.length, 0, 'nothing should have reached the API');
    } finally {
        await ns.close();
        fs.rmSync(dir, { recursive: true, force: true });
    }
});

test('a user agent reaches the API verbatim', async (t) => {
    // It goes in both the header and the userAgent query parameter, because NS
    // reads attribution from either and a name that is mangled in one of them is
    // an attribution that does not count.
    const loaded = loadFixture();
    if (!loaded) {
        t.skip('no event store to build a fixture from');
        return;
    }
    const { dir, fixture } = loaded;
    const ns = await startFakeNsFromXml(fs.readFileSync(path.join(dir, 'world.xml'), 'utf8'));
    try {
        const ua = 'Some Nation';
        await fetchHappeningRange(ns.url, ua, fixture.minEvent, fixture.minEvent + 10);
        assert.ok(ns.requests.length > 0, 'the request should have gone out');
        assert.ok(ns.lastUserAgent === ua, `expected ${JSON.stringify(ua)}, got ${JSON.stringify(ns.lastUserAgent)}`);
    } finally {
        await ns.close();
        fs.rmSync(dir, { recursive: true, force: true });
    }
});

test('ranges are grouped from a bare id list', () => {
    // The shape detection reports, exercised without a store or a server.
    assert.deepEqual(toRanges([1, 2, 3, 7, 8, 20]), [
        { start: 1, end: 3 },
        { start: 7, end: 8 },
        { start: 20, end: 20 },
    ]);
    assert.deepEqual(toRanges([]), []);
});
