import * as fs from 'fs';
import type * as http from 'http';
import { EventStore } from './event_store.js';
import { createQueryServer, loadEndpointConfig, unservableEndpoints, type EndpointConfig } from './query_api.js';

/**
 * Serve queries against the event store.
 *
 * Kept separate from the enricher, which writes. This process only ever opens the
 * store read-only, so it can be stopped, restarted or never run without affecting
 * collection, and two readers can run at once.
 */

const USAGE = `
akiraka-query -- serve queries over the event store

Usage:
  akiraka-query [--store <db>] [--config <endpoints.json>] [--stats]
  akiraka-query [--store <db>] [--port <n>] [--host <addr>]

Options:
  --store <db>     SQLite event store (default: /data/events.db, or AKIRAKA_STORE)
  --config <file>  Endpoint definitions (default: /app/config/endpoints.json)
  --token <t>      Require this bearer token. Prefer AKIRAKA_QUERY_TOKEN, which
                   keeps the secret out of the process list.
  --port <n>       Port to serve on (default: 8084, or QUERY_PORT)
  --host <addr>    Address to bind (default: 127.0.0.1, or QUERY_HOST)
  --stats          Print what the store holds and exit, without serving
  --help, -h       Show this help

Endpoints are requested as:
  GET /events/<name>?after_event=<id>&limit=<n>

Each response line is one projected record. after_event is exclusive, so page
with the highest id received and no row is ever returned twice. The
x-akiraka-last-event header carries the last id on the page.

Every event is stored. An endpoint's categories select from the store; they never
decide what is kept.
`;

/** Endpoint definitions to use when no config file exists yet. */
export function defaultEndpointConfig(): EndpointConfig {
    return {
        endpoints: {
            // Everything, so a fresh install has something to serve without a
            // config. Deliberately not a projection: which subsets are useful
            // depends entirely on what the reader is doing, and there is no
            // sensible guess to make on someone else's behalf.
            all: {
                fields: EVENT_COLUMNS,
                maxRows: 100_000,
            },
        },
    };
}

/** Every event column the query API serves. `data` is excluded: it is much the largest thing in the store, and no reader needs it to keep a copy current. */
const EVENT_COLUMNS = [
    'event', 'time', 'category', 'actor', 'receptor', 'origin', 'destination', 'law_issue_id', 'law_option',
];

interface Options {
    store: string;
    config: string;
    token: string;
    serve: boolean;
    stats: boolean;
    port: number;
    host: string;
}

export function parseArgs(argv: string[]): Options | 'help' {
    const options: Options = {
        store: process.env.AKIRAKA_STORE || '/data/events.db',
        config: process.env.ENDPOINTS_FILE || '/app/config/endpoints.json',
        token: process.env.AKIRAKA_QUERY_TOKEN || '',
        serve: false,
        stats: false,
        port: Number(process.env.QUERY_PORT || 8084),
        host: process.env.QUERY_HOST || '127.0.0.1',
    };

    for (let i = 0; i < argv.length; i++) {
        const arg = argv[i];
        // Both `--flag value` and `--flag=value` are accepted. Compose writes the
        // second form, and the only symptom of not supporting it is a service
        // that exits with "unknown argument" and restarts in a loop - which is
        // what happened, so it is worth being forgiving about here.
        const eq = arg.startsWith('--') ? arg.indexOf('=') : -1;
        const flag = eq === -1 ? arg : arg.slice(0, eq);
        const inline = eq === -1 ? null : arg.slice(eq + 1);

        const takeValue = (): string => {
            if (inline !== null) return inline;
            const next = argv[i + 1];
            // A following flag means this one was given no value. Without this,
            // `--store --port=99` sets the store to "--port=99" and fails later
            // and much less clearly. A value that really does begin with "--" is
            // still expressible as `--flag=--value`.
            if (next === undefined || next.startsWith('--')) {
                throw new Error(`${flag} needs a value`);
            }
            i++;
            return next;
        };

        if (flag === '--help' || flag === '-h') return 'help';
        else if (flag === '--store') options.store = takeValue();
        else if (flag === '--config') options.config = takeValue();
        else if (flag === '--token') options.token = takeValue();
        else if (flag === '--port') options.port = Number(takeValue());
        else if (flag === '--host') options.host = takeValue();
        else if (flag === '--serve') options.serve = true;
        else if (flag === '--stats') options.stats = true;
        else throw new Error(`unknown argument: ${arg}`);
    }

    // Neither flag given means serve, which is the common case in a container.
    if (!options.serve && !options.stats) options.serve = true;

    if (!Number.isFinite(options.port) || options.port < 0 || options.port > 65535) {
        throw new Error(`invalid --port ${options.port}`);
    }
    return options;
}

export async function main(argv: string[] = process.argv.slice(2)): Promise<void> {
    const parsed = parseArgs(argv);
    if (parsed === 'help') {
        process.stdout.write(USAGE);
        return;
    }
    const options = parsed;

    if (!fs.existsSync(options.store)) {
        throw new Error(
            `No store at ${options.store}. The collector writes it as it runs, so ` +
            'start that first, or point --store at a file it has already written.'
        );
    }

    const store = new EventStore(options.store, { readOnly: true });

    let config: EndpointConfig;
    if (fs.existsSync(options.config)) {
        config = loadEndpointConfig(options.config);
    } else {
        config = defaultEndpointConfig();
        console.log(`[query] no endpoint config at ${options.config}; using the built-in all endpoint`);
    }

    if (options.stats) {
        const stats = store.stats();
        console.log(`[query] ${options.store}`);
        console.log(`  rows        : ${stats.rows.toLocaleString()}`);
        console.log(`  event range : ${stats.minEvent.toLocaleString()} .. ${stats.maxEvent.toLocaleString()}`);
        console.log(`  categories  : ${stats.categories.length}`);
        store.close();
        return;
    }
    let stopping = false;
    let server: http.Server;
    const shutdown = () => {
        if (stopping) return;
        stopping = true;
        console.log('[query] stopping');
        server.close();
        store.close();
        process.exit(0);
    };

    server = createQueryServer(store, config, { token: options.token });
    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);

    try {
        await new Promise<void>((resolve, reject) => {
            server.once('error', reject);
            server.listen(options.port, options.host, () => resolve());
        });

        // Diagnostics come after the listener binds. Counting a store this size is
        // a full scan, and doing that first would leave the port closed for as
        // long as it takes - long enough for a health check to conclude the
        // service is down when it is merely starting.
        console.log(`[query] store ${options.store}, max event ${store.maxEvent().toLocaleString()}`);
        for (const p of unservableEndpoints(store, config)) console.warn(`[query] ${p}`);

        const auth = options.token ? ' (bearer token required)' : ' (no authentication)';
        console.log(`[query] serving ${Object.keys(config.endpoints).join(', ')} on http://${options.host}:${options.port}${auth}`);

        if (options.token === '' && options.host !== '127.0.0.1' && options.host !== 'localhost') {
            console.warn(
                `[query] serving on ${options.host} with no authentication. Set AKIRAKA_QUERY_TOKEN ` +
                'before exposing this beyond loopback.'
            );
        }

        await new Promise(() => { /* run until a signal arrives */ });
    } catch (err) {
        server.close();
        store.close();
        throw err;
    }
}

if (require.main === module) {
    main().catch(err => {
        console.error('Fatal:', err instanceof Error ? err.message : err);
        process.exit(1);
    });
}