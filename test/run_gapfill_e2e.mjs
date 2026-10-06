// Run the gap-fill end-to-end tests where the real Akari binary exists.
//
// The reparse step spawns Akari, and the only Akari we have is the Linux binary
// inside the image. On a Windows or macOS host those two tests would otherwise
// skip, which is the one outcome a chain this long must not have: a skip reads
// like a pass, and the chain it guards is the one that silently produced 184937
// timestamp-less events once already.
//
// Written in Node rather than shell so `npm run test:gapfill` behaves the same
// on every platform - the alternative was a bash script that only ran where
// bash does, and on Windows `bash` resolves to WSL, which may not be installed.
//
// Everything else the tests need is already in the image: node with node:sqlite,
// and a baked-in law cache at /app/data/issues_cache.txt.
//
// The store is mounted read-only. These tests must not be able to write to the
// collector's data even by accident, and a fixture builder pointed at production
// is exactly the sort of thing that eventually does.
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.dirname(here);
const image = process.env.AKIRAKA_TEST_IMAGE || 'akiraka-akiraka:latest';
const store = process.env.GAPFILL_FIXTURE_STORE || path.join(root, 'data', 'events.db');

// Which suite to run. The live one is opt-in and hits the real API, so it is a
// separate invocation rather than a flag that could be left on by accident.
const suite = process.argv[2] || 'gapfill_e2e.test.mjs';
const live = suite === 'gapfill_live.test.mjs';

if (!fs.existsSync(store)) {
    console.error(`gapfill e2e: no event store at ${store}`);
    console.error('  the fixture is built from a real store, so one has to exist first -');
    console.error('  run the collector to produce one, or set GAPFILL_FIXTURE_STORE');
    process.exit(1);
}

const mount = (hostPath, containerPath, readOnly) =>
    `${hostPath}:${containerPath}${readOnly ? ':ro' : ''}`;

// The user agent. The hermetic suite needs only something non-empty, because the
// API it talks to is a local mock. The live suite needs the real nation name, and
// it must come from the environment rather than being baked in here: a nation
// name is an identity, not a build setting, and putting one in a committed file
// would attribute every request made from this repo.
const userAgent = process.env.NS_USER_AGENT || (live ? '' : 'gapfill-e2e-test');
if (live && !userAgent) {
    console.error('gapfill live: NS_USER_AGENT must be set in the environment');
    console.error('  it is the nation name NationStates identifies you by');
    process.exit(1);
}

const env = [
    '-e', 'AKARI_BIN=/usr/local/bin/akari',
    '-e', 'CACHE_FILE=/app/data/issues_cache.txt',
    '-e', `GAPFILL_FIXTURE_STORE=/data/events.db`,
    '-e', `NS_USER_AGENT=${userAgent}`,
];
if (live) env.push('-e', 'AKIRAKA_LIVE=1');

const args = [
    'run', '--rm',
    '--name', 'akiraka-gapfill-e2e',
    '--entrypoint', 'node',
    '-v', mount(path.join(root, 'dist'), '/app/dist', true),
    '-v', mount(path.join(root, 'test'), '/app/test', true),
    '-v', mount(path.join(root, 'data'), '/data', true),
    '-w', '/app',
    ...env,
    image,
    '--test', `/app/test/${suite}`,
];

console.log(`gapfill: suite ${suite}`);
console.log(`gapfill: image ${image}`);
console.log(`gapfill: store ${store} (read-only)`);
if (live) console.log('gapfill: LIVE - this calls the real NationStates API');

const child = spawn('docker', args, { stdio: 'inherit', shell: false });
child.on('error', err => {
    console.error(`gapfill e2e: could not run docker: ${err.message}`);
    process.exit(1);
});
// Forward the signals a container stop sends, so Ctrl-C does not orphan it.
for (const signal of ['SIGINT', 'SIGTERM']) {
    process.on(signal, () => child.kill(signal));
}
child.on('exit', (code, signal) => {
    if (signal) {
        console.error(`gapfill e2e: docker exited on ${signal}`);
        process.exit(1);
    }
    process.exit(code ?? 1);
});
