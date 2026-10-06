import * as fs from 'fs';
import { EventStore } from './event_store.js';

/**
 * Read-only report on a store: what it holds, and how complete it is.
 *
 * Completeness comes from the gap table, which the collector fills in as it sees
 * the id stream jump. Reading it is O(gaps); rebuilding it from the events table
 * is a full scan and minutes on a large store, so that is opt-in with --rescan.
 */

const USAGE = `Usage: npm run store:stats -- <db> [--rescan]

  <db>        Path to the SQLite event store. Opened read-only unless --rescan.
  --rescan    Rebuild the gap table from the events table first. Opens the store
              writable and is minutes on a large store, which is why it is opt-in.
`;

function main(): void {
    const args = process.argv.slice(2);
    const dbPath = args.find(a => !a.startsWith('--'));
    if (!dbPath || args.includes('--help')) {
        console.log(USAGE);
        process.exit(dbPath ? 0 : 2);
    }
    if (!fs.existsSync(dbPath)) {
        console.error(`No such store: ${dbPath}`);
        process.exit(1);
    }
    const rescan = args.includes('--rescan');

    const store = new EventStore(dbPath, { readOnly: !rescan });
    try {
        if (store.schemaTooNew) {
            console.error('This store was written by a newer build; the figures below may be wrong.');
        }
        if (rescan) {
            const rebuilt = store.rebuildGaps();
            console.error(
                `Rebuilt the gap table: ${rebuilt.gaps.toLocaleString()} runs, ` +
                `${rebuilt.missingIds.toLocaleString()} ids.`
            );
        }

        const stats = store.stats();
        const span = stats.maxEvent - stats.minEvent + 1;

        // From the gap table, which the collector maintains as it sees id jumps,
        // rather than a scan of every row.
        const gaps = store.gapTotals();

        console.log(`store: ${dbPath}`);
        console.log(`  rows            : ${stats.rows.toLocaleString()}`);
        console.log(`  event id range  : ${stats.minEvent.toLocaleString()} .. ${stats.maxEvent.toLocaleString()}`);
        console.log(`  id span         : ${span.toLocaleString()}`);
        console.log(`  law resolved    : ${stats.enriched.toLocaleString()}`);
        console.log(`  categories      : ${stats.categories.length}`);
        console.log(`  file size       : ${(fs.statSync(dbPath).size / 1e9).toFixed(2)} GB`);
        console.log('');
        console.log('  completeness');
        console.log(`    gaps          : ${gaps.gaps.toLocaleString()}`);
        console.log(`    ids missing   : ${gaps.missingIds.toLocaleString()}`);
        console.log(`    density       : ${span > 0 ? ((100 * stats.rows) / span).toFixed(4) : '0'}% of the id span`);
        console.log(`    largest gap   : ${gaps.largestGap.toLocaleString()} ids at ${gaps.largestGapAt.toLocaleString()}`);
        if (!rescan && gaps.gaps === 0 && stats.rows > 0) {
            console.log('    (nothing recorded yet; run with --rescan to build the table)');
        }
        console.log('');
        console.log(`  categories (${stats.categories.length}):`);
        for (const c of stats.categories) {
            console.log(`    ${c.category.padEnd(16)} ${c.rows.toLocaleString()}`);
        }
    } finally {
        store.close();
    }
}

main();
