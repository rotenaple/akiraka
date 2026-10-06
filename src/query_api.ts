import * as http from 'http';
import * as fs from 'fs';
import { timingSafeEqual } from 'crypto';
import { EventStore, type StoredEvent } from './event_store.js';

/**
 * Serve stored events as newline-delimited JSON over named endpoints.
 *
 * Endpoints are read-side filters over a store that holds every event: they select
 * from the data and never decide what is kept. Paging is on the event id, which is
 * the primary key, so it needs no separate cursor column and is stable across a
 * rebuild.
 */

/** A named projection over the store. */
export interface EndpointDefinition {
    /** Categories this endpoint serves. Omitted means every category. */
    categories?: string[];
    /** Fields to include. Omitted means the whole record. */
    fields?: string[];
    /** Hard cap on rows per request, so one call cannot ask for everything. */
    maxRows?: number;
}

export interface EndpointConfig {
    endpoints: Record<string, EndpointDefinition>;
}

export interface ServerOptions {
    /** Rows a single request may return before the client must page. */
    defaultMaxRows?: number;
    /**
     * Bearer token required on every request but /health. Empty or omitted means
     * no check at all, which is the right default for a loopback-only bind.
     *
     * Not a security boundary: the data is public and the secret is plaintext in
     * `.env`. It guards against a misconfigured bind and nothing more, and it is
     * advisory - a token-less bind off loopback warns and serves anyway.
     */
    token?: string;
}

export const DEFAULT_MAX_ROWS = 10_000;

interface ParsedRequest {
    endpoint: string;
    /** Exclusive: only events with a greater id are returned. */
    afterEvent: number;
    /** Absent when the caller did not ask for a page size. */
    limit?: number;
    error?: string;
}

/**
 * Read `?after_event=<id>&limit=<n>`.
 *
 * `after_event` is exclusive, which is what makes paging safe: the client sends
 * the last id it received and cannot receive that row twice.
 *
 * `limit` is left absent when the caller did not send one, and is not reduced or
 * capped here, because the effective page size belongs to the endpoint and this
 * function cannot see it. Clamping against the server default before the endpoint
 * was resolved capped every request at 10000 rows regardless of what it asked for.
 */
export function parseRequest(url: string): ParsedRequest {
    const parsed = new URL(url, 'http://placeholder');
    const endpoint = parsed.pathname.replace(/^\/events\//, '').replace(/\/+$/, '');

    if (!endpoint) return { endpoint, afterEvent: 0, error: 'no endpoint named' };

    // The cursor this service used before the store was keyed by event id was an
    // ingest-order `seq`, paged with `since`. A client written against it sends
    // `since=<seq>` and reads `_seq` and `x-akiraka-max-seq`, none of which exist
    // here. Left alone it does not fail: `since` is ignored, `after_event` falls
    // back to 0, and the client is handed the oldest rows in the store with a
    // cursor of NaN - so it asks for the same first page forever, receiving 200 and
    // well-formed rows throughout. Rejecting the old parameter turns that silent
    // non-progress into one clear message.
    if (parsed.searchParams.has('since')) {
        return {
            endpoint,
            afterEvent: 0,
            error: 'this service pages on the event id: use after_event=<id>, not since=<seq>',
        };
    }

    let afterEvent = 0;
    const rawAfter = parsed.searchParams.get('after_event');
    if (rawAfter !== null) {
        afterEvent = Number(rawAfter);
        if (!Number.isFinite(afterEvent) || afterEvent < 0 || !Number.isInteger(afterEvent)) {
            return { endpoint, afterEvent: 0, error: `after_event must be a non-negative integer, got ${rawAfter}` };
        }
    }

    let limit: number | undefined;
    const rawLimit = parsed.searchParams.get('limit');
    if (rawLimit !== null) {
        const value = Number(rawLimit);
        if (!Number.isFinite(value) || value <= 0 || !Number.isInteger(value)) {
            return { endpoint, afterEvent, error: `limit must be a positive integer, got ${rawLimit}` };
        }
        // Not clamped to anything here. This function cannot know the endpoint's
        // own cap, and clamping to the server default before the endpoint is
        // looked up meant `limit` was silently reduced to 10000 for every request
        // - so an endpoint configured with a larger cap never got it, and the
        // over-cap 400 could never fire because the value arrived already
        // reduced. The caller checks the ask against the endpoint and refuses it.
        limit = value;
    }

    return { endpoint, afterEvent, limit };
}

export function loadEndpointConfig(path: string): EndpointConfig {
    const raw = JSON.parse(fs.readFileSync(path, 'utf8')) as EndpointConfig;
    if (!raw || typeof raw !== 'object' || typeof raw.endpoints !== 'object' || raw.endpoints === null) {
        throw new Error(`${path}: expected an object with an "endpoints" map`);
    }
    for (const [name, def] of Object.entries(raw.endpoints)) {
        if (!/^[A-Za-z0-9_-]+$/.test(name)) {
            throw new Error(`${path}: endpoint name "${name}" must be letters, digits, - or _`);
        }
        if (def.categories !== undefined && !Array.isArray(def.categories)) {
            throw new Error(`${path}: endpoint "${name}" categories must be an array`);
        }
        if (def.fields !== undefined && !Array.isArray(def.fields)) {
            throw new Error(`${path}: endpoint "${name}" fields must be an array`);
        }
        // The config file is snake_case throughout, and the discovery response
        // reports max_rows, so that is the key it is written in. The TypeScript
        // field is camelCase, and only the file goes through here, so the two are
        // bridged at the edge rather than by renaming either.
        const snake = (def as Record<string, unknown>).max_rows;
        if (snake !== undefined) {
            if (typeof snake !== 'number' || !Number.isInteger(snake) || snake <= 0) {
                throw new Error(`${path}: endpoint "${name}" max_rows must be a positive integer`);
            }
            def.maxRows = snake;
        }
        if (def.maxRows !== undefined && (!Number.isInteger(def.maxRows) || def.maxRows <= 0)) {
            throw new Error(`${path}: endpoint "${name}" maxRows must be a positive integer`);
        }
    }
    return raw;
}

/**
 * Compare a presented bearer token against the expected one.
 *
 * Length is compared without returning early, so a wrong-length token costs the
 * same as a right-length wrong one and the length is not leaked by timing.
 */
function tokenMatches(expected: string, presented: string | null): boolean {
    if (!presented) return false;
    const want = Buffer.from(expected, 'utf8');
    const got = Buffer.from(presented, 'utf8');
    if (want.length !== got.length) {
        timingSafeEqual(want, want);
        return false;
    }
    return timingSafeEqual(want, got);
}

/**
 * The store's highest event id, or 0 if even that cannot be read.
 *
 * Reported alongside an error so a client can back off to a known-good cursor. It
 * is itself an index read that can fail at the write frontier, and an error
 * response that threw while building its own error message would be no response
 * at all - so this cannot throw.
 */
function safeMaxEvent(store: EventStore): number {
    try {
        return store.maxEvent();
    } catch {
        return 0;
    }
}

/**
 * Project a stored row down to the endpoint's fields.
 *
 * A null field is left out rather than written as null. SQLite returns every
 * column as a key whether or not the event had one, and Akari omits absent fields,
 * so emitting them would both bloat every page and diverge from the format the
 * rest of the pipeline expects. `event` and `time` are always present.
 */
function project(row: StoredEvent, fields: Set<string> | null): Record<string, unknown> {
    const out: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(row)) {
        if (value === null) continue;
        if (key === 'event' || key === 'time' || !fields || fields.has(key)) out[key] = value;
    }
    return out;
}

/**
 * Serve `GET /events/<endpoint>?after_event=<id>&limit=<n>` as newline-delimited JSON.
 *
 * Each line is the projected record. The id is echoed in the `x-akiraka-last-event`
 * header so a client never has to guess where it got to, and never has to parse
 * the body to find out whether the page was short.
 */
export function createQueryServer(
    store: EventStore,
    config: EndpointConfig,
    options: ServerOptions = {},
): http.Server {
    const defaultMaxRows = options.defaultMaxRows ?? DEFAULT_MAX_ROWS;
    const token = options.token ?? '';

    return http.createServer((req, res) => {
        const send = (status: number, body: unknown) => {
            const payload = JSON.stringify(body);
            res.writeHead(status, {
                'content-type': 'application/json; charset=utf-8',
                'content-length': Buffer.byteLength(payload),
            });
            res.end(payload);
        };

        if (req.method !== 'GET' && req.method !== 'HEAD') {
            send(405, { error: 'only GET and HEAD' });
            return;
        }

        const url = req.url ?? '/';

        /** Health is deliberately reachable without a token: a monitor holding the
 * secret just to learn the process is up defeats the point. */
        if (url === '/health') {
            if (req.method === 'HEAD') {
                res.writeHead(200, { 'content-type': 'application/json; charset=utf-8' });
                res.end();
                return;
            }
            // The cheapest true statements about the store, and nothing else.
            // The category list needs a scan of the whole store, and a probe that
            // costs that fails the compose health check's 3-second timeout on a
            // cold cache. maxEvent is an index seek and says more about whether the
            // collector is still writing than a row count would.
            send(200, {
                ok: true,
                maxEvent: store.maxEvent(),
                hasEvents: store.hasEvents(),
            });
            return;
        }

        if (token) {
            const header = req.headers.authorization;
            const presented = header && header.startsWith('Bearer ') ? header.slice(7).trim() : null;
            if (!tokenMatches(token, presented)) {
                res.writeHead(401, {
                    'content-type': 'application/json; charset=utf-8',
                    'www-authenticate': 'Bearer realm="akiraka"',
                });
                res.end(JSON.stringify({ error: 'a bearer token is required' }));
                return;
            }
        }

        if (req.method === 'HEAD') {
            send(405, { allow: 'GET' });
            return;
        }

        if (url === '/endpoints') {
            send(200, {
                endpoints: Object.fromEntries(
                    Object.entries(config.endpoints).map(([name, def]) => [
                        name,
                        {
                            categories: def.categories ?? null,
                            fields: def.fields ?? null,
                            max_rows: def.maxRows ?? defaultMaxRows,
                            path: `/events/${name}`,
                        },
                    ])
                ),
            });
            return;
        }

        if (!url.startsWith('/events/')) {
            send(404, { error: 'not found', try: ['/health', '/endpoints', '/events/<name>'] });
            return;
        }

        const request = parseRequest(url);
        if (request.error) {
            send(400, { error: request.error });
            return;
        }

        const definition = config.endpoints[request.endpoint];
        if (!definition) {
            send(404, { error: `no endpoint named "${request.endpoint}"`, known: Object.keys(config.endpoints) });
            return;
        }

        // The endpoint's own cap, defaulting to the server's. Not the other way
        // round: `Math.min(def.maxRows ?? d, d)` made the server default a ceiling,
        // so an endpoint asking for more was silently clamped and every
        // `max_rows` in the config file was decorative.
        const maxRows = definition.maxRows ?? defaultMaxRows;
        const limit = request.limit ?? maxRows;
        if (request.limit !== undefined && request.limit > maxRows) {
            // Clamping silently would make a client believe it had the whole page.
            send(400, {
                error: `limit ${request.limit} exceeds this endpoint's maximum of ${maxRows}`,
                max_rows: maxRows,
            });
            return;
        }

        // A failing query answers that one request; it must not take the process
        // with it. The throw is SQLITE_CORRUPT, from a cursor in the store's write
        // frontier, where the table b-tree's rightmost leaf is being written.
        let rows;
        try {
            rows = store.query({
                afterEvent: request.afterEvent,
                categories: definition.categories,
                limit,
            });
        } catch (err) {
            const code = (err as { errcode?: number }).errcode;
            send(503, {
                error: code === 11
                    ? 'the store is being written at that point; re-read from an earlier cursor'
                    : `query failed: ${(err as Error).message}`,
                errcode: code ?? null,
                after_event: request.afterEvent,
                max_event: safeMaxEvent(store),
            });
            return;
        }
        const fields = definition.fields?.length ? new Set(definition.fields) : null;

        // Worked out before the head is sent: headers cannot be set once the body
        // has started. An empty page reports the cursor it was given, so a client
        // that pages to the end is not told the cursor moved backwards.
        const lastEvent = rows.length > 0 ? rows[rows.length - 1].event : request.afterEvent;

        // Newline-delimited so a client can consume a large page incrementally
        // rather than buffering it, and so a truncated response is detectable.
        res.writeHead(200, {
            'content-type': 'application/x-ndjson; charset=utf-8',
            'x-akiraka-endpoint': request.endpoint,
            'x-akiraka-after-event': String(request.afterEvent),
            'x-akiraka-count': String(rows.length),
            'x-akiraka-max-event': String(store.maxEvent()),
            'x-akiraka-last-event': String(lastEvent),
        });
        for (const row of rows) {
            res.write(`${JSON.stringify(project(row, fields))}\n`);
        }
        res.end();
    });
}

/**
 * Endpoints that ask for categories this store holds nothing for.
 *
 * A typo in an endpoint name is otherwise invisible: it serves an empty page
 * forever, which reads as "none have happened". Uses the cached counts, because
 * this runs at startup and must not put a scan in front of the listener binding.
 */
export function unservableEndpoints(store: EventStore, config: EndpointConfig): string[] {
    // Deliberately not the row count: emptiness is an index seek, and a category
    // check must not put a full scan in front of the listener binding.
    if (!store.hasEvents()) {
        return ['store is empty; import events before serving'];
    }

    const present = new Set(store.categoriesCached(30_000).map(c => c.category));
    const problems: string[] = [];
    for (const [name, def] of Object.entries(config.endpoints)) {
        if (!def.categories?.length) continue;
        const missing = def.categories.filter(c => !present.has(c));
        if (missing.length) problems.push(`${name}: not in the store -> ${missing.join(', ')}`);
    }
    return problems;
}