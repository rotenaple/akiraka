// Argument parsing, which is where a container misconfiguration shows up first.
// The compose file writes `--store=/data/events.db`; anything that only accepts
// `--store /data/events.db` turns that into a restart loop, so both spellings
// have to work and the mismatch is worth pinning down in a test.
import test from 'node:test';
import assert from 'node:assert/strict';

import { parseArgs } from '../dist/query.js';

// The parser reads env defaults, so these tests pin the ones that would
// otherwise leak in from the developer's shell.
function cleanEnv(fn) {
    const saved = {};
    for (const key of ['AKIRAKA_STORE', 'ENDPOINTS_FILE', 'AKIRAKA_QUERY_TOKEN', 'QUERY_PORT', 'QUERY_HOST']) {
        saved[key] = process.env[key];
        delete process.env[key];
    }
    try {
        return fn();
    } finally {
        for (const [key, value] of Object.entries(saved)) {
            if (value === undefined) delete process.env[key];
            else process.env[key] = value;
        }
    }
}

test('the spaced form is accepted', () => {
    const o = cleanEnv(() => parseArgs(['--store', '/tmp/a.db', '--port', '9000', '--host', '0.0.0.0']));
    assert.equal(o.store, '/tmp/a.db');
    assert.equal(o.port, 9000);
    assert.equal(o.host, '0.0.0.0');
});

test('the equals form is accepted, which is what compose writes', () => {
    // This is the exact shape in docker-compose.yml. It has to work, because a
    // failure here is not a clear error but a container restarting forever.
    const o = cleanEnv(() => parseArgs(['--store=/data/events.db', '--config=/app/config/endpoints.json']));
    assert.equal(o.store, '/data/events.db');
    assert.equal(o.config, '/app/config/endpoints.json');
});

test('a value containing an equals sign survives', () => {
    // Splitting on the first '=' only: a path or token may contain more.
    const o = cleanEnv(() => parseArgs(['--token=abc=def==']));
    assert.equal(o.token, 'abc=def==');
});

test('an empty inline value is a value, not a missing flag', () => {
    const o = cleanEnv(() => parseArgs(['--token=']));
    assert.equal(o.token, '');
});

test('no flags at all means serve, on the container default store', () => {
    const o = cleanEnv(() => parseArgs([]));
    assert.equal(o.serve, true);
    assert.equal(o.stats, false);
    assert.equal(o.store, '/data/events.db');
    assert.equal(o.port, 8084);
    assert.equal(o.host, '127.0.0.1');
});

test('--stats means do not serve', () => {
    const o = cleanEnv(() => parseArgs(['--stats']));
    assert.equal(o.stats, true);
    assert.equal(o.serve, false);
});

test('a flag with no value at all says so, rather than swallowing the next flag', () => {
    // Consuming '--port' as the value of '--store' would produce a store named
    // "--port" and fail somewhere much later and less clearly.
    assert.throws(() => cleanEnv(() => parseArgs(['--store'])), /--store needs a value/);
    assert.throws(() => cleanEnv(() => parseArgs(['--store', '--port=99'])), /--store needs a value/);
});

test('a value that really starts with -- is still expressible', () => {
    // The guard above rejects a bare following flag, so this is the escape hatch.
    const o = cleanEnv(() => parseArgs(['--token=--not-a-flag']));
    assert.equal(o.token, '--not-a-flag');
});

test('a single-dash value is not mistaken for a flag', () => {
    const o = cleanEnv(() => parseArgs(['--token', '-abc']));
    assert.equal(o.token, '-abc');
});

test('an unknown flag is still rejected', () => {
    assert.throws(() => cleanEnv(() => parseArgs(['--nope'])), /unknown argument: --nope/);
    assert.throws(() => cleanEnv(() => parseArgs(['--nope=1'])), /unknown argument: --nope=1/);
});

test('an out-of-range port is rejected', () => {
    assert.throws(() => cleanEnv(() => parseArgs(['--port=99999'])), /invalid --port/);
    assert.throws(() => cleanEnv(() => parseArgs(['--port=nonsense'])), /invalid --port/);
});

test('flags can be mixed across both forms', () => {
    const o = cleanEnv(() => parseArgs(['--store=/x.db', '--port', '1234', '--host=0.0.0.0', '--stats']));
    assert.equal(o.store, '/x.db');
    assert.equal(o.port, 1234);
    assert.equal(o.host, '0.0.0.0');
    assert.equal(o.stats, true);
});

test('--help short-circuits, wherever it appears', () => {
    assert.equal(cleanEnv(() => parseArgs(['--help'])), 'help');
    assert.equal(cleanEnv(() => parseArgs(['--store=/x.db', '-h'])), 'help');
});

test('a single-dash arg is not treated as having an inline value', () => {
    assert.throws(() => cleanEnv(() => parseArgs(['-x=1'])), /unknown argument/);
});
