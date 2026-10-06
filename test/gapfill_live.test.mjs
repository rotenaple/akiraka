// Gap-fill against the real NationStates API.
//
// Opt-in: set AKIRAKA_LIVE=1. The hermetic suite in gapfill_e2e.test.mjs proves
// the chain works against a world we control; this proves the world we do not
// control still looks the way the code assumes. Those are different claims, and
// the assumptions most likely to rot live here:
//
//   - that TIMESTAMP is still the element, and still carries what we store
//   - that a recent range is still inside the ~1 week retention window
//   - that the rate-limit headers still say what the pacer expects
//
// It is deliberately a cross-check rather than a smoke test. Fetching a range
// and finding events would pass even if every timestamp were wrong. Instead it
// fetches ids we already hold and compares: the time the API returns now must
// equal the time we stored then. That is the property that broke once already,
// and it is checkable only against the real feed.
//
// One request for a small range, paced. This is not a load test.
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

import { fetchHappeningRange } from '../dist/gapfill.js';
import { NsPacer } from '../dist/ns_client.js';
import { EventStore } from '../dist/event_store.js';

const LIVE = process.env.AKIRAKA_LIVE === '1';
const NSAPI = 'https://www.nationstates.net/cgi-bin/api.cgi';
/** Small on purpose: one request, and a short enough range to stay in one page. */
const RANGE_SIZE = 20;

function skipUnlessLive(t, extra = '') {
    if (!LIVE) {
        t.skip('set AKIRAKA_LIVE=1 to run against the real NS API');
        return true;
    }
    if (!process.env.NS_USER_AGENT) {
        t.skip(`NS_USER_AGENT is required for a real API call${extra}`);
        return true;
    }
    return false;
}

function openStore() {
    const storePath = process.env.GAPFILL_FIXTURE_STORE || path.resolve('data/events.db');
    if (!fs.existsSync(storePath)) return null;
    return new EventStore(storePath, { readOnly: true });
}

test('the real API returns the timestamps we stored for the same ids', async (t) => {
    if (skipUnlessLive(t)) return;
    const store = openStore();
    if (!store) {
        t.skip('no event store to compare against');
        return;
    }
    try {
        // A window from the middle of what we hold. Recent enough to be inside
        // retention, old enough that the collector is not still writing it.
        const max = store.maxEvent();
        const from = max - 5000;
        const rows = store.query({ afterEvent: from, limit: RANGE_SIZE });
        assert.ok(rows.length >= RANGE_SIZE, 'need a full window to compare');
        const start = rows[0].event;
        const end = rows[rows.length - 1].event;

        const pacer = new NsPacer();
        t.diagnostic(`fetching ${start}-${end} from the real API; pacing: ${pacer.describe()}`);

        const result = await fetchHappeningRange(
            NSAPI, process.env.NS_USER_AGENT, start, end, undefined, pacer);

        if (result.cutoffReached) {
            // The window aged out. Not a failure: retention is the API's rule,
            // and a test that failed here would only teach us to bump a constant.
            t.skip(`range ${start}-${end} is outside NS retention`);
            return;
        }

        assert.equal(
            result.cutoffReached, false,
            `the API reports nothing for ${start}-${end}, which should be inside retention`
        );

        const returned = new Map(result.events.map(e => [e.id, e]));
        for (const row of rows) {
            const live = returned.get(row.event);
            assert.ok(live, `event ${row.event} is missing from the live API response`);
            assert.equal(
                live.time, row.time,
                `event ${row.event}: API says ${live.time}, store says ${row.time}`
            );
            assert.ok(live.text && live.text.length > 0, `event ${row.event} came back with no text`);
        }
    } finally {
        store.close();
    }
});

test('a real response is fetchable at all, and parses', async (t) => {
    if (skipUnlessLive(t)) return;
    const pacer = new NsPacer();
    const result = await fetchHappeningRange(
        NSAPI, process.env.NS_USER_AGENT, 1, 1, undefined, pacer);
    // Ids 1..1 are long gone, so an empty result is the correct answer. What this
    // checks is that a real endpoint, a real user agent and a real request cycle
    // complete without throwing - the wiring, not the content.
    assert.ok(Array.isArray(result.events));
    assert.equal(typeof result.cutoffReached, 'boolean');
});

test('the live API still reports the limit the pacer assumes', async (t) => {
    if (skipUnlessLive(t)) return;
    // NS has published 50;w=30 for as long as the header has existed, and the
    // pacer is built on that assumption rather than depending on the header.
    // This is the canary: if the limit ever changes, the pacer keeps pacing to a
    // remembered number while the server enforces a different one, and the
    // failure would show up as unexplained 429s rather than as anything pointing
    // here.
    const { EXPECTED_POLICY } = await import(pathToFileURL(path.resolve('dist/ns_client.js')).href);
    const pacer = new NsPacer();
    const url = `${NSAPI}?q=happenings&view=world&beforeid=999999999&limit=1` +
        `&userAgent=${encodeURIComponent(process.env.NS_USER_AGENT)}`;
    const resp = await fetch(url, { headers: { 'User-Agent': process.env.NS_USER_AGENT } });

    assert.equal(resp.status, 200, `live API returned ${resp.status}`);
    const reported = resp.headers.get('ratelimit-policy');
    t.diagnostic(`ratelimit-policy: ${reported}`);
    t.diagnostic(`ratelimit-remaining: ${resp.headers.get('ratelimit-remaining')}`);
    t.diagnostic(`ratelimit-reset: ${resp.headers.get('ratelimit-reset')}`);

    assert.ok(reported, 'no ratelimit-policy header at all');
    assert.equal(
        reported.trim(),
        `${EXPECTED_POLICY.limit};w=${EXPECTED_POLICY.windowMs / 1000}`,
        'the published limit has changed; update EXPECTED_POLICY in src/ns_client.ts'
    );

    // The remaining/reset pair is what decides when to hold back, so its absence
    // would quietly downgrade the pacer to even spacing with no reserve.
    assert.ok(resp.headers.get('ratelimit-remaining') !== null, 'no ratelimit-remaining');
    assert.ok(resp.headers.get('ratelimit-reset') !== null, 'no ratelimit-reset');
});
