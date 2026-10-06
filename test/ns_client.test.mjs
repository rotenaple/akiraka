// The pacer is what keeps a collector without ns-cas inside the NS rate limit.
//
// It is timing code, so it is tested against an injected clock and an injected
// sleep rather than against real ones. A test that actually waited out a 30-second
// window would either be slow or would assert nothing, which is how pacing bugs
// survive: nothing looks wrong until the collector is throttled in production.
import test from 'node:test';
import assert from 'node:assert/strict';

import { NsPacer, parseRatePolicy, EXPECTED_POLICY } from '../dist/ns_client.js';

/** A clock the test moves by hand, and a sleep that advances it. */
function fakeClock(start = 1_700_000_000_000) {
    let now = start;
    const slept = [];
    return {
        now: () => now,
        sleep: async (ms) => {
            slept.push(ms);
            now += ms;
        },
        advance: (ms) => { now += ms; },
        slept,
        get current() { return now; },
    };
}

const headers = (obj) => new Headers(obj);

test('a policy header yields the limit and the window', () => {
    assert.deepEqual(parseRatePolicy('50;w=30'), { limit: 50, windowMs: 30_000 });
    assert.deepEqual(parseRatePolicy('30;w=60'), { limit: 30, windowMs: 60_000 });
});

test('spacing and casing in the policy header do not matter', () => {
    // NS is not obliged to be tidy, and a header that fails to parse falls back
    // to a guess, so being forgiving here is the difference between pacing to
    // the real limit and pacing to a default.
    assert.deepEqual(parseRatePolicy('50; W = 30'), { limit: 50, windowMs: 30_000 });
    assert.deepEqual(parseRatePolicy('50;w=30;foo=bar'), { limit: 50, windowMs: 30_000 });
});

test('an unreadable policy is refused rather than guessed at', () => {
    // Guessing the window from ratelimit-reset is the mistake worth avoiding:
    // reset is the countdown to the next refill, so treating it as the window
    // makes a client burst at the start of every window.
    for (const bad of [null, undefined, '', 'nonsense', '50', 'w=30', '0;w=30', '50;w=0']) {
        assert.equal(parseRatePolicy(bad), null, `should refuse ${JSON.stringify(bad)}`);
    }
});

test('pacing assumes the published limit, with no header needed', () => {
    // NS has published 50 per 30s for as long as the header has existed. The
    // pacer starts from that rather than waiting to be told, so it works on a
    // response that carries no headers at all.
    const clock = fakeClock();
    const pacer = new NsPacer({ now: clock.now, sleep: clock.sleep });
    assert.deepEqual(EXPECTED_POLICY, { limit: 50, windowMs: 30_000 });
    assert.match(pacer.describe(), /50 per 30s/, 'assumed from the known limit, not observed');
});

test('a policy header matching the published limit changes nothing', () => {
    const clock = fakeClock();
    const pacer = new NsPacer({ now: clock.now, sleep: clock.sleep });
    pacer.observe(headers({ 'ratelimit-policy': '50;w=30', 'ratelimit-remaining': '50' }), 200);
    assert.match(pacer.describe(), /50 per 30s/);
    assert.doesNotMatch(pacer.describe(), /penalty/, 'a matching policy is not an anomaly');
});

test('a policy header that disagrees is adopted and announced', async () => {
    // The canary. We would otherwise keep pacing to a remembered limit while the
    // server enforces a different one, which fails as mysterious 429s rather than
    // as anything pointing at this.
    const clock = fakeClock();
    const warnings = [];
    const realWarn = console.warn;
    console.warn = (msg) => warnings.push(msg);
    try {
        const pacer = new NsPacer({ now: clock.now, sleep: clock.sleep });
        pacer.observe(headers({ 'ratelimit-policy': '20;w=60' }), 200);
        assert.match(pacer.describe(), /20 per 60s/, 'the new value wins');
        assert.equal(warnings.length, 1, 'and it is said out loud');
        assert.match(warnings[0], /ratelimit-policy is now 20;w=60/);
    } finally {
        console.warn = realWarn;
    }
});

test('requests are spaced evenly once a policy is known', async () => {
    const clock = fakeClock();
    const pacer = new NsPacer({ now: clock.now, sleep: clock.sleep });
    pacer.observe(headers({ 'ratelimit-policy': '50;w=30', 'ratelimit-remaining': '50' }), 200);

    // 50 per 30s is one request per 600ms.
    for (let i = 0; i < 4; i++) await pacer.before();
    assert.deepEqual(clock.slept, [600, 600, 600], 'three gaps for four requests');
});

test('the last requests of the window are held back until the refill', async () => {
    const clock = fakeClock();
    const pacer = new NsPacer({ now: clock.now, sleep: clock.sleep });
    pacer.observe(headers({
        'ratelimit-policy': '10;w=10',
        'ratelimit-remaining': '1',
        'ratelimit-reset': '4',
    }), 200);
    // One request left and a refill four seconds away. Spending it is legal; the
    // point of the reserve is that the *next* one then has to wait, so with
    // remaining=1 the pacer waits rather than emptying the window.
    await pacer.before();
    assert.ok(clock.slept.length > 0, 'should have waited for the refill');
    assert.ok(clock.slept.some(ms => ms >= 4000), `expected a wait near the 4s refill, got ${clock.slept}`);
});

test('no ratelimit headers at all still paces to the published limit', async () => {
    // A response with no headers is not permission to send everything at once, and
    // it is not a reason to stop pacing either.
    const clock = fakeClock();
    const pacer = new NsPacer({ now: clock.now, sleep: clock.sleep });
    pacer.observe(new Headers(), 200);
    await pacer.before();
    await pacer.before();
    assert.ok(clock.slept.length > 0, 'should still space requests');
    assert.deepEqual(clock.slept, [600], 'and at the assumed 600ms for 50 per 30s');
});

test('a 429 backs off by retry-after and slows the spacing', async () => {
    const clock = fakeClock();
    const pacer = new NsPacer({ now: clock.now, sleep: clock.sleep });
    pacer.observe(headers({ 'ratelimit-policy': '50;w=30', 'retry-after': '7' }), 429);
    await pacer.before();
    const total = clock.slept.reduce((a, b) => a + b, 0);
    // retry-after 7s, plus the 2s the reference implementation adds.
    assert.ok(total >= 9000, `expected at least 9s of backoff, got ${total}ms`);
    assert.match(pacer.describe(), /penalty x2/, 'a 429 should also slow the spacing');
});

test('a later 429 with a shorter retry-after does not shorten the wait', async () => {
    // The backoff is a floor, not a suggestion. A 429 carrying retry-after: 1
    // after one that said 30 must not cut the wait to three seconds.
    const clock = fakeClock();
    const pacer = new NsPacer({ now: clock.now, sleep: clock.sleep });
    pacer.observe(headers({ 'ratelimit-policy': '50;w=30' }), 429);
    pacer.observe(headers({ 'retry-after': '1' }), 429);
    await pacer.before();
    const total = clock.slept.reduce((a, b) => a + b, 0);
    assert.ok(total >= 30_000, `the longer backoff should stand, got ${total}ms`);
});

test('a 429 without retry-after falls back to thirty seconds', async () => {
    // The reference behaviour from the single-consumer tools, kept deliberately:
    // a missing header means do not guess short.
    const clock = fakeClock();
    const pacer = new NsPacer({ now: clock.now, sleep: clock.sleep });
    pacer.observe(headers({}), 429);
    await pacer.before();
    const total = clock.slept.reduce((a, b) => a + b, 0);
    assert.ok(total >= 30_000, `expected at least 30s of backoff, got ${total}ms`);
});

test('repeated 429s deepen the penalty but do not explode', async () => {
    const clock = fakeClock();
    const pacer = new NsPacer({ now: clock.now, sleep: clock.sleep });
    pacer.observe(headers({ 'ratelimit-policy': '50;w=30' }), 429);
    const seen = [];
    // Deliberately not advancing the clock: recovery is tested separately, and
    // moving time forward here would clear the penalty between observations.
    for (let i = 0; i < 10; i++) {
        seen.push(Number(/x(\d+)/.exec(pacer.describe())?.[1] ?? '1'));
        clock.advance(1000);
        pacer.observe(headers({ 'ratelimit-policy': '50;w=30' }), 429);
    }
    assert.ok(seen.every(v => v >= 1), 'penalty is always at least 1');
    assert.ok(seen[0] < seen[seen.length - 1], `penalty should deepen, saw ${seen.join(',')}`);
    assert.ok(Math.max(...seen) <= 16, `penalty should be capped, saw ${Math.max(...seen)}`);
});

test('the penalty recovers on its own after a quiet minute', async () => {
    const clock = fakeClock();
    const pacer = new NsPacer({ now: clock.now, sleep: clock.sleep });
    pacer.observe(headers({ 'ratelimit-policy': '50;w=30' }), 429);
    assert.match(pacer.describe(), /penalty x2/);

    clock.advance(61_000);
    await pacer.before();
    assert.doesNotMatch(pacer.describe(), /penalty/, 'a quiet minute should clear the penalty');
});

test('pacing can be turned off, for tests that are not about pacing', async () => {
    const clock = fakeClock();
    const pacer = new NsPacer({ now: clock.now, sleep: clock.sleep, disabled: true });
    pacer.observe(headers({ 'ratelimit-policy': '1;w=1', 'ratelimit-remaining': '0' }), 429);
    for (let i = 0; i < 5; i++) await pacer.before();
    assert.deepEqual(clock.slept, [], 'a disabled pacer never waits');
});

test('describe reports the pacing in terms an operator can act on', () => {
    const clock = fakeClock();
    const pacer = new NsPacer({ now: clock.now, sleep: clock.sleep });
    pacer.observe(headers({ 'ratelimit-policy': '50;w=30' }), 200);
    const text = pacer.describe();
    assert.match(text, /50 per 30s/);
    assert.match(text, /600ms/, 'the spacing is the actionable number');
});
