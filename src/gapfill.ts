import * as fs from 'fs';
import * as he from 'he';
import * as http from 'http';
import * as net from 'net';
import * as os from 'os';
import * as path from 'path';
import * as readline from 'readline';
import { spawn, ChildProcess } from 'child_process';
import fetch from 'node-fetch';
import * as cheerio from 'cheerio';
import { LawParser } from './lawParser';
import { NsPacer } from './ns_client';
import { AkariEvent } from './types';
import { CasClient } from './cas';

const NSAPI_URL = 'https://www.nationstates.net/cgi-bin/api.cgi';
const MAX_EVENTS_PER_REQUEST = 100;

const DEFAULT_MAX_GAP = 1000;
const DEFAULT_TIMEOUT = 180_000;

export interface RawHappening {
    id: number;
    time: number;
    text: string;
}

export interface EventGap {
    start: number;
    end: number;
}

/** One recovered event, carrying its id so callers can filter by it. */
export interface RecoveredEvent {
    id: number;
    /** Enriched line, destined for the enriched stream. */
    line: string;
    /** Un-enriched Akari line, destined for the raw stream. */
    raw: string;
}

export interface FillResult {
    start: number;
    end: number;
    fetched: number;
    written: number;
    /**
     * Recovered events in ascending id order, one entry per id.
     *
     * Ids are carried rather than left implicit in a pair of parallel arrays
     * because the caller has to decide which of these the stream already holds,
     * and cannot do that without knowing which line belongs to which id.
     */
    events: RecoveredEvent[];
/**
 * Whether the range ran past the start of what the API still holds.
 *
 * Optional so a caller written against the older shape still compiles; every path
 * sets it.
 */
    cutoffReached?: boolean;
}

export interface GapFillerOptions {
    lawParser: LawParser;
    nsUserAgent?: string;
    akariBinary?: string;
    replayPort?: number;
    timeoutMs?: number;
    apiUrl?: string;
    cas?: CasClient;
    /**
     * Pacing for the direct path, when there is no ns-cas. Created by default;
     * pass one in to share it across several GapFillers, or to inject a clock.
     */
    pacer?: NsPacer;
}

/**
 * Tracks committed/in-flight filled ranges as merging [start, end] pairs.
 */
export class RangeTracker {
    private ranges: Array<[number, number]> = [];

    constructor(ranges?: Array<[number, number]>) {
        if (ranges) this.ranges = [...ranges].sort((a, b) => a[0] - b[0]);
    }

    add(start: number, end: number): void {
        const merged: Array<[number, number]> = [];
        for (const [s, e] of this.ranges) {
            if (e < start - 1 || s > end + 1) {
                merged.push([s, e]);
            } else {
                start = Math.min(start, s);
                end = Math.max(end, e);
            }
        }
        merged.push([start, end]);
        merged.sort((a, b) => a[0] - b[0]);
        this.ranges = merged;
    }

    contains(id: number): boolean {
        for (const [s, e] of this.ranges) {
            if (id >= s && id <= e) return true;
            if (id < s) return false;
        }
        return false;
    }

    coversRange(start: number, end: number): boolean {
        for (const [s, e] of this.ranges) {
            if (s <= start && e >= end) return true;
            if (s > start) return false;
        }
        return false;
    }

    removeOverlap(start: number, end: number): void {
        const kept: Array<[number, number]> = [];
        for (const [s, e] of this.ranges) {
            if (e < start || s > end) {
                kept.push([s, e]);
            } else {
                if (s < start) kept.push([s, start - 1]);
                if (e > end) kept.push([end + 1, e]);
            }
        }
        this.ranges = kept;
    }

    toArray(): Array<[number, number]> {
        return this.ranges.map(r => [r[0], r[1]] as [number, number]);
    }
}

/**
 * Persistent gapfill state (filled ranges + last event id seen), stored beside the
 * output or the store so a restart does not re-fill or re-detect the same gaps.
 */
export class GapState {
    private tracker: RangeTracker;
    private inFlight: RangeTracker;
    private statePath: string;
    private lastEventId: number = 0;
    private tailOffset: number = 0;
    private saveTimer: NodeJS.Timeout | null = null;

    constructor(statePath: string, initialLastEventId = 0) {
        this.statePath = statePath;
        this.lastEventId = initialLastEventId;
        this.tracker = new RangeTracker();
        this.inFlight = new RangeTracker();
        this.load();
    }

    private load(): void {
        try {
            if (!fs.existsSync(this.statePath)) return;
            const data = JSON.parse(fs.readFileSync(this.statePath, 'utf-8')) as {
                filled?: unknown;
                lastEventId?: number;
                tailOffset?: number;
            };
            if (Array.isArray(data.filled)) {
                const ranges: Array<[number, number]> = [];
                for (const item of data.filled) {
                    if (Array.isArray(item) && item.length === 2 &&
                        typeof item[0] === 'number' && typeof item[1] === 'number') {
                        ranges.push([item[0], item[1]]);
                    }
                }
                this.tracker = new RangeTracker(ranges);
            }
            if (typeof data.lastEventId === 'number' && data.lastEventId > this.lastEventId) {
                this.lastEventId = data.lastEventId;
            }
            if (typeof data.tailOffset === 'number' && data.tailOffset > 0) {
                this.tailOffset = data.tailOffset;
            }
        } catch {
            console.warn(`[gapfill] Could not load state from ${this.statePath}`);
        }
    }

    save(): void {
        if (this.saveTimer) { clearTimeout(this.saveTimer); this.saveTimer = null; }
        try {
            const dir = path.dirname(this.statePath);
            if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
            fs.writeFileSync(this.statePath, JSON.stringify({
                lastEventId: this.lastEventId,
                tailOffset: this.tailOffset,
                filled: this.tracker.toArray()
            }));
        } catch (err: any) {
            console.warn(`[gapfill] Could not save state to ${this.statePath}: ${err.message}`);
        }
    }

    saveDebounced(delayMs = 15_000): void {
        if (this.saveTimer) return;
        this.saveTimer = setTimeout(() => { this.saveTimer = null; this.save(); }, delayMs);
        this.saveTimer.unref();
    }

    observeEventId(id: number): void {
        if (id > this.lastEventId) {
            this.lastEventId = id;
            this.saveDebounced();
        }
    }

    get lastId(): number {
        return this.lastEventId;
    }

    /**
     * How far into the tail file this run has read.
     *
     * Persisted so a restart resumes rather than re-reading the whole log.
     * Re-reading is harmless - the event id is the primary key - but it
     * re-enriches every event from the beginning on each restart, which is not
     * free.
     */
    getTailOffset(): number {
        return this.tailOffset;
    }

    /**
     * Record how far the tail has been read.
     *
     * Only ever moves forward. A stale save landing after a rotation reset would
     * otherwise leave an offset pointing into a file that no longer has that
     * content, and the next start would skip real events.
     */
    setTailOffset(offset: number): void {
        if (!Number.isFinite(offset) || offset <= this.tailOffset) return;
        this.tailOffset = offset;
        this.saveDebounced();
    }

    /**
     * Forget the offset, after the tail file was replaced.
     *
     * The offset means nothing against a different file, so rotation has to
     * clear it rather than leave it to look like a resume point.
     */
    resetTailOffset(): void {
        this.tailOffset = 0;
        this.saveDebounced();
    }

    isCovered(id: number): boolean {
        return this.tracker.contains(id) || this.inFlight.contains(id);
    }

    coversRange(start: number, end: number): boolean {
        return this.tracker.coversRange(start, end) || this.inFlight.coversRange(start, end);
    }

    markInFlight(start: number, end: number): void {
        this.inFlight.add(start, end);
    }

    unmarkInFlight(start: number, end: number): void {
        this.inFlight.removeOverlap(start, end);
    }

    markFilled(start: number, end: number): void {
        this.inFlight.removeOverlap(start, end);
        this.tracker.add(start, end);
        this.save();
    }
}

function sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Detect non-contiguous ranges in a set of event ids. System events (id <= 0)
 * are ignored. Returns gaps [start, end] where every id in the range is missing.
 */
export function detectGaps(ids: Iterable<number>): EventGap[] {
    const sorted = Array.from(new Set(ids)).filter(id => id > 0).sort((a, b) => a - b);
    const gaps: EventGap[] = [];
    for (let i = 1; i < sorted.length; i++) {
        const prev = sorted[i - 1];
        const cur = sorted[i];
        if (cur - prev > 1) {
            gaps.push({ start: prev + 1, end: cur - 1 });
        }
    }
    return gaps;
}

/**
 * The range a forwarded id implies is missing, or null when the ids are
 * contiguous.
 *
 * The stream is the source of truth for what was missed: a jump from the last id
 * seen to this one is a gap, whatever caused it - a dropped connection, a log
 * rotation, a restart into a file that was replaced while this was down.
 */
export function gapFromJump(prevId: number, id: number): { start: number; end: number } | null {
    if (!(prevId > 0) || !(id > prevId + 1)) return null;
    return { start: prevId + 1, end: id - 1 };
}

/**
 * Read the range out of a `connmiss` marker.
 *
 * Akari emits one when a reconnect finds that ids were missed, carrying
 * `data: [missed, lastId, currentId]`. That is its own statement of what the
 * connection lost, and it is worth reading even though the id jump in the stream
 * usually implies the same range.
 */
export function parseConnmiss(line: string): { start: number; end: number } | null {
    if (!line.includes('connmiss')) return null;
    let event: { category?: unknown; data?: unknown };
    try {
        event = JSON.parse(line) as { category?: unknown; data?: unknown };
    } catch {
        return null;
    }
    if (event.category !== 'connmiss' || !Array.isArray(event.data)) return null;
    const last = Number(event.data[1]);
    const current = Number(event.data[2]);
    if (!Number.isInteger(last) || !Number.isInteger(current) || current <= last + 1) return null;
    return { start: last + 1, end: current - 1 };
}

export const DEFAULT_CHUNK_SIZE = 1000;

/**
 * Split a gap range [startId, endId] into smaller contiguous chunks of at most chunkSize events.
 */
export function splitIntoChunks(startId: number, endId: number, chunkSize = DEFAULT_CHUNK_SIZE): Array<{ start: number; end: number }> {
    if (chunkSize <= 0) chunkSize = DEFAULT_CHUNK_SIZE;
    const chunks: Array<{ start: number; end: number }> = [];
    let cur = startId;
    while (cur <= endId) {
        const next = Math.min(cur + chunkSize - 1, endId);
        chunks.push({ start: cur, end: next });
        cur = next + 1;
    }
    return chunks;
}

/**
 * Scan a JSONL file of Akari events and report the gaps in its event id sequence.
 */
export async function scanFileForGaps(filePath: string): Promise<EventGap[]> {
    return (await scanFile(filePath)).gaps;
}

/**
 * Scan a JSONL file of Akari events, returning all event ids found and the
 * gaps in their id sequence. Id order in the file does not matter; a gap is
 * any contiguous id range between two present ids where no id is present.
 */
export async function scanFile(filePath: string): Promise<{ ids: Set<number>; gaps: EventGap[] }> {
    const ids: Set<number> = new Set();
    const rl = readline.createInterface({
        input: fs.createReadStream(filePath),
        crlfDelay: Infinity
    });
    for await (const line of rl) {
        const match = /"event"\s*:\s*(-?\d+)/.exec(line);
        if (match) ids.add(parseInt(match[1], 10));
    }
    return { ids, gaps: detectGaps(ids) };
}

/**
 * Collect the event ids already present in a JSONL stream that fall inside any of
 * the given ranges.
 *
 * Gap-fill detects gaps in the raw input but appends to the enriched output, and
 * those two files do not have to agree: an event can be in the enriched stream
 * without being in the raw one, so appending it again duplicates it.
 *
 * `ranges` must be sorted and non-overlapping, so membership is a binary search
 * and the result is bounded by the ranges rather than by the size of the file.
 */
export async function scanFileForIdsInRanges(
    filePath: string,
    ranges: ReadonlyArray<readonly [number, number]>
): Promise<Set<number>> {
    const found = new Set<number>();
    if (ranges.length === 0 || !fs.existsSync(filePath)) return found;

    const inside = (id: number): boolean => {
        let lo = 0;
        let hi = ranges.length - 1;
        while (lo <= hi) {
            const mid = (lo + hi) >> 1;
            if (id < ranges[mid][0]) hi = mid - 1;
            else if (id > ranges[mid][1]) lo = mid + 1;
            else return true;
        }
        return false;
    };

    const rl = readline.createInterface({
        input: fs.createReadStream(filePath),
        crlfDelay: Infinity
    });
    for await (const line of rl) {
        const match = /"event"\s*:\s*(-?\d+)/.exec(line);
        if (!match) continue;
        const id = parseInt(match[1], 10);
        if (id > 0 && inside(id)) found.add(id);
    }
    return found;
}

/**
 * Parse the XML response of the happenings API into raw happenings.
 * Only the fields we need (id, time, text) are extracted.
 */
export function parseHappeningsXml(xml: string): RawHappening[] {
    const $ = cheerio.load(xml, { xmlMode: true });
    let nodes = $('EVENT').toArray();
    if (nodes.length === 0) nodes = $('event').toArray();

    const out: RawHappening[] = [];
    for (const node of nodes) {
        const $node = $(node);
        const idRaw = $node.attr('id');
        if (!idRaw) continue;
        const id = parseInt(idRaw, 10);
        if (isNaN(id)) continue;
        // NationStates names this element TIMESTAMP. The code once looked for
        // TIME, which does not exist, so parseInt saw an empty string and the
        // isNaN fallback below wrote 0 - meaning every event recovered through
        // gap-fill entered the store with no timestamp. The attribute form is
        // accepted too so a future change of shape is tolerated rather than
        // silently zeroing a second time.
        const timeAttr = $node.attr('time');
        const timeText = $node.find('TIMESTAMP').first().text()
            || $node.find('TIME').first().text()
            || timeAttr
            || '';
        const time = parseInt(timeText, 10);
        let text = $node.find('TEXT').first().text();
        text = he.decode(text);
        out.push({ id, time: isNaN(time) ? 0 : time, text });
    }
    return out;
}

async function fetchHappeningPage(apiUrl: string, ua: string, beforeId: number, limit: number, cas?: CasClient, pacer?: NsPacer): Promise<RawHappening[]> {
    const url = `${apiUrl}?q=happenings&view=world&beforeid=${beforeId}&limit=${limit}&userAgent=${encodeURIComponent(ua)}`;

    // Every request to NS must name the nation asking. Enforced here rather than
    // only in the CLI branches that check it, because this is the one function
    // that crosses to NS - a future caller added anywhere else would otherwise
    // make anonymous requests without tripping anything. The tail-mode check warns
    // and disables auto-fill; --gapfill exits; this makes the rule unavoidable.
    if (!ua || !ua.trim()) {
        throw new Error(
            'refusing to call the NationStates API without a user agent: ' +
            'set NS_USER_AGENT to your nation name'
        );
    }

    const doFetch = async (): Promise<{ resp: import('node-fetch').Response; fetcher: string }> => {
        if (cas && cas.enabled) {
            const resp = await cas.execute(() => fetch(url, { headers: { 'User-Agent': ua } }));
            return { resp, fetcher: `cas (${cas.baseIdentifier})` };
        }
        if (pacer) await pacer.before();
        try {
            const resp = await fetch(url, { headers: { 'User-Agent': ua } });
            if (pacer) pacer.observe(resp.headers as unknown as Headers, resp.status);
            return { resp, fetcher: pacer ? `direct, ${pacer.describe()}` : 'direct' };
        } catch (err) {
            // Record the miss so the next request is spaced out. A thrown fetch
            // means we do not know what the server did with it.
            if (pacer) pacer.observe(new Headers(), 0);
            throw err;
        }
    };

    const { resp, fetcher } = await doFetch();

    if (resp.status === 429) {
        // With a pacer the penalty and the wait are already recorded, so this is
        // only the message. Without one the wait still has to happen here, which
        // is the bare-fetch behaviour the pacer exists to improve on.
        const retryAfter = parseFloat(resp.headers.get('retry-after') || '0');
        if (!pacer) {
            const waitSeconds = retryAfter > 0 ? retryAfter : 30;
            console.log(`[gapfill] NS API 429 (via ${fetcher}); retrying in ${Math.ceil(waitSeconds)}s`);
            await sleep(waitSeconds * 1000 + 2000);
        } else {
            console.log(`[gapfill] NS API 429 (via ${fetcher}); backing off before retrying`);
        }
        return fetchHappeningPage(apiUrl, ua, beforeId, limit, cas, pacer);
    }
    if (!resp.ok) {
        throw new Error(`Happenings API returned status ${resp.status} (via ${fetcher})`);
    }
    const xml = await resp.text();
    return parseHappeningsXml(xml);
}

export interface FetchRangeResult {
    events: RawHappening[];
    cutoffReached: boolean;
}

/**
 * Fetch all happenings with ids in [startId, endId] via the NS API, oldest first.
 * The API returns events newest-first, so this paginates backwards with beforeid.
 *
 * Exported so the pagination can be tested without spawning akari to reach it: it
 * walks backwards from endId and stops on a short page, which is the part most
 * likely to be subtly wrong.
 */
export async function fetchHappeningRange(apiUrl: string, ua: string, startId: number, endId: number, cas?: CasClient, pacer?: NsPacer): Promise<FetchRangeResult> {
    const events = new Map<number, RawHappening>();
    let beforeId = endId + 1;
    const targetCount = endId - startId + 1;
    const maxIterations = Math.ceil(targetCount / MAX_EVENTS_PER_REQUEST) + 5;
    let cutoffReached = false;

    for (let i = 0; i < maxIterations; i++) {
        const page = await fetchHappeningPage(apiUrl, ua, beforeId, MAX_EVENTS_PER_REQUEST, cas, pacer);
        if (page.length === 0) {
            if (i === 0) {
                cutoffReached = true;
            }
            break;
        }

        let minId = beforeId;
        for (const happening of page) {
            if (happening.id >= startId && happening.id <= endId) {
                events.set(happening.id, happening);
            }
            if (happening.id < minId) minId = happening.id;
        }

        if (minId >= beforeId) break; // no progress, avoid infinite loop
        beforeId = minId;
        if (page.length < MAX_EVENTS_PER_REQUEST) break;
        if (beforeId <= startId) break;
    }

    return {
        events: [...events.values()].sort((a, b) => a.id - b.id),
        cutoffReached
    };
}

interface ReplayServer {
    server: http.Server;
    port: number;
    close: () => Promise<void>;
}

/**
 * Serve happenings to Akari's SSE client in the exact NationStates wire format:
 *   id: <id>
 *   data: {"id":"<id>","time":"<time>","str":"<text>","buckets":["move","all"]}
 *
 * The connection is kept open with keepalives so the reparse Akari instance stays
 * connected until the caller decides it is done and terminates the process.
 */
export function startReplayServer(events: Array<RawHappening>, port = 0): Promise<ReplayServer> {
    return new Promise((resolve, reject) => {
        const sockets = new Set<net.Socket>();
        const server = http.createServer((req, res) => {
            if (!req.url || !req.url.startsWith('/replay')) {
                res.writeHead(404);
                res.end();
                return;
            }
            res.writeHead(200, {
                'Content-Type': 'text/event-stream',
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive'
            });
            res.write(': connected\n\n');
            for (const happening of events) {
                const data = JSON.stringify({
                    id: String(happening.id),
                    time: String(happening.time),
                    str: happening.text,
                    buckets: ['move', 'all']
                });
                res.write(`id: ${happening.id}\ndata: ${data}\n\n`);
            }
            const keepalive = setInterval(() => {
                try { res.write(': keepalive\n\n'); } catch { clearInterval(keepalive); }
            }, 15_000);
            res.on('close', () => clearInterval(keepalive));
        });
        server.on('connection', socket => {
            sockets.add(socket);
            socket.on('close', () => sockets.delete(socket));
        });
        server.on('error', reject);
        server.listen(port, '127.0.0.1', () => {
            const address = server.address();
            const resolvedPort = typeof address === 'object' && address ? address.port : port;
            resolve({
                server,
                port: resolvedPort,
                close: () => new Promise<void>(r => {
                    for (const socket of sockets) socket.destroy();
                    server.close(() => r());
                })
            });
        });
    });
}

function writeAkariConfig(tempDir: string, replayUrl: string, outFile: string): string {
    const configPath = path.join(tempDir, 'akari.toml');
    const configDir = path.join(tempDir, 'config');
    fs.mkdirSync(configDir, { recursive: true });
    const content = [
        '[input]',
        `url = "${replayUrl}"`,
        'workers = 2',
        '',
        '[output.console]',
        'enabled = false',
        '',
        '[output.file]',
        'enabled = true',
        `path = "${outFile}"`,
        'threshold = 2000000000',
        '',
        '[output.redis]',
        'enabled = false',
        '',
        '[output.postgres]',
        'enabled = false',
        '',
        '[output.rmq]',
        'enabled = false',
        ''
    ].join('\n');
    fs.writeFileSync(configDir + path.sep + 'akari.toml', content);
    fs.writeFileSync(configPath, content);
    return configPath;
}

function spawnAkari(akariBinary: string, tempDir: string, configPath: string): ChildProcess {
    const needsShell = akariBinary.endsWith('.cmd') || akariBinary.endsWith('.bat');
    const proc = spawn(akariBinary, ['--config', configPath], {
        cwd: tempDir,
        env: { ...process.env },
        stdio: ['ignore', 'pipe', 'pipe'],
        shell: needsShell
    });
    proc.stdout?.on('data', chunk => process.stderr.write(`[gapfill:akari] ${chunk}`));
    proc.stderr?.on('data', chunk => process.stderr.write(`[gapfill:akari] ${chunk}`));
    return proc;
}

function waitForExit(proc: ChildProcess, timeoutMs: number): Promise<void> {
    return new Promise(resolve => {
        if (proc.exitCode !== null) { resolve(); return; }
        const timer = setTimeout(() => {
            try { proc.kill('SIGKILL'); } catch { /* already dead */ }
            resolve();
        }, timeoutMs);
        proc.once('exit', () => {
            clearTimeout(timer);
            resolve();
        });
    });
}

/**
 * Enrich an already-parsed Akari JSON line and serialize it back to JSON.
 */
export function enrichAkariLine(line: string, lawParser: LawParser): string {
    const trimmed = line.trim();
    if (!trimmed) return '';
    try {
        const event = JSON.parse(trimmed) as AkariEvent;
        return JSON.stringify(lawParser.enrichEvent(event));
    } catch (err: any) {
        if (process.env.GAPFILL_DEBUG) console.error(`[gapfill:debug] enrich error: ${err.message}`);
        return '';
    }
}

/**
 * Append recovered RAW Akari lines back to the raw Akari output file.
 *
 * Akari opens its output file with O_APPEND semantics, so a plain append is always
 * safe against a live Akari - every writer's bytes land at the current end-of-file.
 * In-place insertion (rewrite + rename) would break a live writer, whose handle
 * keeps writing to the renamed-away inode. Gap detection is id-set based, so the
 * gap still closes permanently. Lines already present by id are skipped. Returns
 * the number of lines appended.
 */
export async function appendRecoveredToRaw(rawPath: string, rawLines: string[]): Promise<number> {
    if (rawLines.length === 0) return 0;

    const pending = new Map<number, string>();
    for (const line of rawLines) {
        const match = /"event"\s*:\s*(-?\d+)/.exec(line);
        if (match) pending.set(parseInt(match[1], 10), line);
    }
    if (pending.size === 0) return 0;

    if (fs.existsSync(rawPath)) {
        const existing = await scanFile(rawPath);
        for (const id of existing.ids) pending.delete(id);
    }
    if (pending.size === 0) return 0;

    let payload = '';
    let needsNewline = false;
    if (fs.existsSync(rawPath)) {
        const stat = fs.statSync(rawPath);
        if (stat.size > 0) {
            const fd = fs.openSync(rawPath, 'r');
            const last = Buffer.alloc(1);
            fs.readSync(fd, last, 0, 1, stat.size - 1);
            fs.closeSync(fd);
            needsNewline = last[0] !== 0x0A;
        }
    }
    if (needsNewline) payload += '\n';
    payload += [...pending.entries()].sort((a, b) => a[0] - b[0]).map(([, l]) => l).join('\n') + '\n';

    const dir = path.dirname(rawPath);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    await fs.promises.appendFile(rawPath, payload);
    return pending.size;
}

export class GapFiller {
    private lawParser: LawParser;
    private nsUserAgent: string;
    private akariBinary: string;
    private replayPort: number;
    private timeoutMs: number;
    private apiUrl: string;
    private cas: CasClient | null;
    private pacer: NsPacer | null;

    constructor(options: GapFillerOptions) {
        this.lawParser = options.lawParser;
        this.nsUserAgent = options.nsUserAgent || process.env.NS_USER_AGENT || '';
        this.akariBinary = options.akariBinary || process.env.AKARI_BIN || 'akari';
        this.replayPort = options.replayPort || 0;
        this.timeoutMs = options.timeoutMs || DEFAULT_TIMEOUT;
        this.apiUrl = options.apiUrl || NSAPI_URL;
        this.cas = options.cas || (process.env.NS_CAS_URL ? new CasClient() : null);

        // ns-cas is optional. Without it the pacer keeps this collector inside
        // the limit on its own, which is the only thing a single collector can
        // do and is enough for one. With it, the ticket governs the quota and the
        // pacer would only add delay the queue has already accounted for.
        this.pacer = options.pacer ?? (this.cas && this.cas.enabled ? null : new NsPacer());
        if (this.cas && this.cas.enabled && options.pacer) {
            this.pacer = null;
        }
    }

    get hasUserAgent(): boolean {
        return this.nsUserAgent.length > 0;
    }

    get hasCas(): boolean {
        return this.cas !== null;
    }

    /**
     * How API requests are being paced, for the startup log line.
     *
     * Worth printing because the answer differs by setup and neither is wrong: a
     * reader comparing two collectors should not assume one of them is
     * misbehaving.
     */
    describePacing(): string {
        if (this.cas && this.cas.enabled) return `ns-cas (${this.cas.baseIdentifier})`;
        return this.pacer ? `built-in pacer, ${this.pacer.describe()}` : 'unpaced';
    }

    /**
     * Fill a contiguous missing range [start, end] by:
     *  1. Fetching the raw happenings from the NS API
     *  2. Replaying them to a second Akari instance via a local SSE shim
     *  3. Collecting, enriching, and returning the parsed events in id order
     */
    async fillRange(startId: number, endId: number): Promise<FillResult> {
        const fetchHint = this.cas ? ` via CAS (${this.cas.baseIdentifier})` : '';
        console.log(`[gapfill] Fetching ${startId}-${endId} (${endId - startId + 1} events)${fetchHint}...`);
        const { events: fetched, cutoffReached } = await fetchHappeningRange(
            this.apiUrl, this.nsUserAgent, startId, endId, this.cas || undefined, this.pacer || undefined);
        if (fetched.length === 0) {
            const reason = cutoffReached ? 'older than NS API retention cutoff (~7-8 days)' : 'no matching events in world feed';
            console.log(`[gapfill] No happenings returned for range ${startId}-${endId} (${reason}), skipping`);
            return { start: startId, end: endId, fetched: 0, written: 0, events: [], cutoffReached };
        }

        const replay = await startReplayServer(fetched, this.replayPort);
        const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'akiraka-gap-'));
        const outFile = path.join(tempDir, 'gapfill_events.jsonl');
        const replayUrl = `http://127.0.0.1:${replay.port}/replay`;

        console.log(`[gapfill] Filling ${startId}-${endId} (${fetched.length} events) via replay at ${replayUrl}`);

        let proc: ChildProcess | null = null;
        try {
            const configPath = writeAkariConfig(tempDir, replayUrl, outFile);
            proc = spawnAkari(this.akariBinary, tempDir, configPath);

            const wanted = new Set(fetched.map(h => h.id));
            const seen = new Set<number>();
            let pos = 0;
            let partial = '';
            const deadline = Date.now() + this.timeoutMs;

            while (Date.now() < deadline && proc.exitCode === null) {
                if (fs.existsSync(outFile)) {
                    const stat = fs.statSync(outFile);
                    if (stat.size < pos) { pos = 0; partial = ''; }
                    if (stat.size > pos) {
                        const fd = fs.openSync(outFile, 'r');
                        const buf = Buffer.alloc(stat.size - pos);
                        fs.readSync(fd, buf, 0, buf.length, pos);
                        fs.closeSync(fd);
                        pos = stat.size;
                        const text = partial + buf.toString('utf-8');
                        const lines = text.split('\n');
                        partial = lines.pop() || '';
                        for (const line of lines) {
                            const match = /"event"\s*:\s*(-?\d+)/.exec(line);
                            if (match) {
                                const id = parseInt(match[1], 10);
                                if (id >= startId && id <= endId) seen.add(id);
                            }
                        }
                    }
                }
                let complete = true;
                for (const id of wanted) {
                    if (!seen.has(id)) { complete = false; break; }
                }
                if (complete) break;
                await sleep(250);
            }
        } finally {
            if (proc && proc.exitCode === null) {
                try { proc.kill('SIGTERM'); } catch { /* already dead */ }
                await waitForExit(proc, 10_000);
            }
            await replay.close();
        }

        // Read the reparse output, restrict to the requested range, enrich, and sort.
        const linesInRange: Array<{ id: number; line: string; raw: string }> = [];
        if (fs.existsSync(outFile)) {
            const content = fs.readFileSync(outFile, 'utf-8');
            for (const line of content.split('\n')) {
                const trimmed = line.trim();
                if (!trimmed) continue;
                const match = /"event"\s*:\s*(-?\d+)/.exec(trimmed);
                if (!match) continue;
                const id = parseInt(match[1], 10);
                if (id < startId || id > endId) continue;
                const enriched = enrichAkariLine(trimmed, this.lawParser);
                if (enriched) linesInRange.push({ id, line: enriched, raw: trimmed });
            }
        }

        linesInRange.sort((a, b) => a.id - b.id);
        const unique = new Map<number, { line: string; raw: string }>();
        for (const entry of linesInRange) unique.set(entry.id, { line: entry.line, raw: entry.raw });

        const events: RecoveredEvent[] = [...unique.entries()].map(([id, v]) => ({ id, line: v.line, raw: v.raw }));
        const written = events.length;
        console.log(`[gapfill] Filled ${startId}-${endId}: ${written}/${fetched.length} events parsed`);

        try { fs.rmSync(tempDir, { recursive: true, force: true }); } catch { /* best-effort cleanup */ }

        return { start: startId, end: endId, fetched: fetched.length, written, events, cutoffReached };
    }
}