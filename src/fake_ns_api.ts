import * as http from 'node:http';
import type { AddressInfo } from 'node:net';
import { parseHappeningsXml, type RawHappening } from './gapfill';

/**
 * A stand-in for the NS happenings API.
 *
 * Faithful about the parts gap-fill depends on, because those are what a
 * hand-rolled 200-response gets wrong: results are newest-first and `beforeid` is
 * exclusive; a page is capped at `limit`, so a wider range needs pagination; and a
 * short page means the end of history. It can produce a 429 too, because gap-fill
 * retries on one and a mock that cannot ask for that leaves the branch untested.
 */
export interface FakeNsOptions {
    /** Every event the world contains, any order. */
    events: RawHappening[];
    /** Serve at most this many per request, as NS does. */
    pageSize?: number;
    /** Reply 429 this many times before serving, to exercise the retry. */
    rateLimitFirst?: number;
    /** retry-after to send with a 429. Small by default so tests do not idle. */
    retryAfterSeconds?: number;
    /** Always answer 404, to exercise the failure path. */
    alwaysFail?: boolean;
    /** Cut history below this id, imitating the ~1 week retention window. */
    retentionFloor?: number;
}

export interface FakeNsServer {
    url: string;
    port: number;
    /** Requests seen, so a test can assert on pagination rather than guess. */
    requests: Array<{ beforeid: number; limit: number }>;
    /** User agent of the most recent request, header or query parameter. */
    lastUserAgent: string | null;
    close: () => Promise<void>;
}

export async function startFakeNsApi(options: FakeNsOptions): Promise<FakeNsServer> {
    const pageSize = options.pageSize ?? 100;
    const rateLimitFirst = options.rateLimitFirst ?? 0;
    const requests: Array<{ beforeid: number; limit: number }> = [];
    // NS reads attribution from the header and the userAgent parameter, so the
    // mock records both and a test can check neither is mangled.
    const state: { lastUserAgent: string | null } = { lastUserAgent: null };
    let rateLimits = 0;

    const ascending = [...options.events].sort((a, b) => a.id - b.id);

    const server = http.createServer((req, res) => {
        const url = new URL(req.url || '/', 'http://127.0.0.1');
        const beforeid = Number(url.searchParams.get('beforeid') || '0');
        const limit = Math.min(Number(url.searchParams.get('limit') || String(pageSize)), pageSize);
        requests.push({ beforeid, limit });
        state.lastUserAgent = req.headers['user-agent'] || url.searchParams.get('userAgent');

        if (options.alwaysFail) {
            res.writeHead(500, { 'Content-Type': 'text/plain' });
            res.end('deliberate failure');
            return;
        }

        if (rateLimits < rateLimitFirst) {
            rateLimits++;
            // The client reads `retry-after` and falls back to 30s when it is absent
            // or zero, so a mock that sends 0 would idle the suite for a minute.
            res.writeHead(429, {
                'retry-after': String(options.retryAfterSeconds ?? 1),
                'Content-Type': 'text/plain',
            });
            res.end('slow down');
            return;
        }

    const floor = options.retentionFloor ?? 0;
    // Newest-first, strictly below beforeid, honouring the retention floor.
    //
    // pageSize caps what we return, but `limit` is what the client asked for and
    // what it compares a short page against. Capping below the requested limit
    // therefore looks like end-of-history to the client - which is faithful to NS
    // only if NS never serves a short page for another reason. See the pagination
    // test, which pins that assumption rather than working around it.
    const page = ascending
        .filter(e => e.id < beforeid && e.id >= floor)
        .reverse()
        .slice(0, Math.min(limit, pageSize));

        const xml = renderXml(page);
        res.writeHead(200, { 'Content-Type': 'text/xml; charset=utf-8' });
        res.end(xml);
    });

    await new Promise<void>((resolve, reject) => {
        server.on('error', reject);
        server.listen(0, '127.0.0.1', resolve);
    });
    const port = (server.address() as AddressInfo).port;

    return {
        url: `http://127.0.0.1:${port}/api.cgi`,
        port,
        requests,
        get lastUserAgent() { return state.lastUserAgent; },
        close: () => new Promise<void>(resolve => {
            server.closeAllConnections?.();
            server.close(() => resolve());
        }),
    };
}

function renderXml(events: RawHappening[]): string {
    if (events.length === 0) {
        // NS answers an exhausted range with a well-formed document and no
        // events, not a 404. The client distinguishes the two, so the mock has to.
        return `<?xml version="1.0" encoding="UTF-8"?>\n<WORLD>\n<HAPPENINGS>\n</HAPPENINGS>\n</WORLD>\n`;
    }
    const body = events
        .map(e => `  <EVENT id="${e.id}">\n` +
            `  <TIMESTAMP>${e.time}</TIMESTAMP>\n` +
            `  <TEXT><![CDATA[${e.text.replace(/]]>/g, ']]]]><![CDATA[>')}]]></TEXT>\n` +
            `  </EVENT>`)
        .join('\n');
    return `<?xml version="1.0" encoding="UTF-8"?>\n<WORLD>\n<HAPPENINGS>\n${body}\n</HAPPENINGS>\n</WORLD>\n`;
}

/**
 * A double for fetchHappeningRange's view of the world.
 *
 * Served as the fixture's own world.xml rather than re-rendered from the events,
 * so the mock is exercising the same bytes the real API would have produced.
 */
export async function startFakeNsFromXml(xml: string, options: Omit<FakeNsOptions, 'events'> = {}) {
    return startFakeNsApi({ ...options, events: parseHappeningsXml(xml) });
}
