import * as fs from 'fs';
import * as path from 'path';
import { LawParser } from './lawParser';
import { StoreSink } from './store_sink';
import { AkariEvent } from './types';

/**
 * Membership set for 32-bit ids that does not hit V8's ~16.7M Map/Set entry cap.
 * Ids are paged by their top bits; each page is a bitset of 2^20 ids (128 KiB).
 */
export class PagedBitset {
    private static readonly SHIFT = 20;
    private static readonly MASK = (1 << PagedBitset.SHIFT) - 1;
    private chunks = new Map<number, Uint8Array>();

    add(id: number): void {
        if (id < 0) return;
        const key = id >>> PagedBitset.SHIFT;
        let chunk = this.chunks.get(key);
        if (!chunk) {
            chunk = new Uint8Array(1 << (PagedBitset.SHIFT - 3));
            this.chunks.set(key, chunk);
        }
        const idx = id & PagedBitset.MASK;
        chunk[idx >>> 3] |= 1 << (idx & 7);
    }

    has(id: number): boolean {
        if (id < 0) return false;
        const chunk = this.chunks.get(id >>> PagedBitset.SHIFT);
        if (!chunk) return false;
        const idx = id & PagedBitset.MASK;
        return (chunk[idx >>> 3] & (1 << (idx & 7))) !== 0;
    }
}

/**
 * Serialize an object to a single-line JSONL record. JSON.stringify leaves
 * U+2028/U+2029 (and U+0085) raw, but many line-based readers treat them as line
 * terminators, which would split one record across "lines". Escape them so every
 * record is exactly one physical line.
 */
export function toJsonlLine(obj: unknown): string {
    return JSON.stringify(obj)
        .replace(/\u2028/g, '\\u2028')
        .replace(/\u2029/g, '\\u2029')
        .replace(/\u0085/g, '\\u0085') + '\n';
}

/**
 * Convert a PostgreSQL array literal (`{a,b,"c,d"}`) into the canonical Akari
 * `data` string array. Elements are only quoted when they contain a delimiter or
 * whitespace, so unquoted numbers (`{61461063}`) become strings (`["61461063"]`).
 * Returns null when the literal is not a `{...}` array.
 */
export function parsePgArrayLiteral(literal: string): string[] | null {
    const s = literal.trim();
    if (s === '') return [];
    if (!s.startsWith('{') || !s.endsWith('}')) return null;
    const inner = s.slice(1, -1);
    if (inner === '') return [];

    const out: string[] = [];
    let cur = '';
    let quoted = false;
    let i = 0;
    while (i < inner.length) {
        const c = inner[i];
        if (quoted) {
            if (c === '\\') {
                cur += inner[i + 1] ?? '';
                i += 2;
                continue;
            }
            if (c === '"') { quoted = false; i++; continue; }
            cur += c; i++;
        } else if (c === '"') {
            quoted = true; i++;
        } else if (c === ',') {
            out.push(cur); cur = ''; i++;
        } else {
            cur += c; i++;
        }
    }
    out.push(cur);
    return out;
}

/**
 * Streaming CSV record reader. Unlike a line-based reader it tracks quotes across
 * chunk boundaries, so fields containing embedded newlines (present in a handful
 * of `chfield` rows) are read as a single record.
 */
export async function* readCsvRecords(stream: NodeJS.ReadableStream): AsyncGenerator<string[]> {
    let inQuotes = false;
    let field = '';
    let fields: string[] = [];
    let pending = '';

    for await (const chunk of stream) {
        const s = pending + (Buffer.isBuffer(chunk) ? chunk.toString('utf8') : String(chunk));
        pending = '';
        let i = 0;
        while (i < s.length) {
            const c = s[i];
            if (inQuotes) {
                if (c === '"') {
                    if (i + 1 >= s.length) { pending = '"'; i++; break; }
                    if (s[i + 1] === '"') { field += '"'; i += 2; }
                    else { inQuotes = false; i++; }
                } else { field += c; i++; }
            } else if (c === '"') {
                inQuotes = true; i++;
            } else if (c === ',') {
                fields.push(field); field = ''; i++;
            } else if (c === '\n') {
                fields.push(field); field = '';
                yield fields;
                fields = [];
                i++;
            } else if (c === '\r') {
                i++;
            } else {
                field += c; i++;
            }
        }
    }
    if (field !== '' || fields.length > 0) { fields.push(field); yield fields; }
}

export interface IngestOptions {
    inPath: string;
    /** JSONL output. Null when only the store was asked for. */
    outPath: string | null;
    /** SQLite store to write. Null when only JSONL was asked for. */
    storePath: string | null;
    rawOutPath: string | null;
    dedupPath: string | null;
    cachePath: string | null;
    refreshCache: boolean;
    limit: number;
}

/** What a run did, returned rather than only logged so a caller can assert on it. */
export interface IngestStats {
    read: number;
    bad: number;
    skippedExisting: number;
    skippedDuplicate: number;
    written: number;
    lawTotal: number;
    lawMatched: number;
    lawNull: number;
    /** Rows the store committed. Zero when no --store was given. */
    stored: number;
}

function printHelp(): void {
    console.log(`
Akari CSV -> Enriched JSONL Ingester

Usage:
  node dist/ingest_csv.js --in <events.csv> [--store <events.db>] [--out <jsonl>] [options]

  Writes to the store, to JSONL, or both. With neither, JSONL defaults to
  <input>.enriched.jsonl.

Options:
  --in <path>        Input CSV (columns: event,time,actor,receptor,origin,destination,category,data)
  --store <path>     SQLite event store to write
  --out <path>       Enriched JSONL output
  --raw-out <path>   Optional canonical un-enriched Akari JSONL output
  --dedup <path>     Existing JSONL whose event ids should be skipped (avoid duplicates)
  --cache <path>     Path to issues_cache.txt
  --refresh-cache    Re-fetch the law outcomes cache before ingesting
  --limit <n>        Stop after n input records (for testing)
  --help, -h         Show this help

Environment:
  CACHE_FILE         Fallback path to issues_cache.txt
  EVENT_STORE_FILE   Fallback path for --store
`);
}

export function parseArgs(args: string[]): IngestOptions | null {
    let inPath = '';
    let outPath: string | null = null, storePath: string | null = null;
    let rawOutPath: string | null = null;
    let dedupPath: string | null = null, cachePath: string | null = null;
    let refreshCache = false, limit = Infinity;

    for (let i = 0; i < args.length; i++) {
        if (args[i] === '--in' && args[i + 1]) inPath = args[++i];
        else if (args[i] === '--out' && args[i + 1]) outPath = args[++i];
        else if (args[i] === '--store' && args[i + 1]) storePath = args[++i];
        else if (args[i] === '--raw-out' && args[i + 1]) rawOutPath = args[++i];
        else if (args[i] === '--dedup' && args[i + 1]) dedupPath = args[++i];
        else if (args[i] === '--cache' && args[i + 1]) cachePath = args[++i];
        else if (args[i] === '--refresh-cache') refreshCache = true;
        else if (args[i] === '--limit' && args[i + 1]) limit = parseInt(args[++i], 10);
        else if (!args[i].startsWith('-') && !inPath) inPath = args[i];
    }

    if (!inPath || !fs.existsSync(inPath)) {
        console.error(`Error: Input CSV not found: ${inPath || '(none)'}`);
        return null;
    }
    if (!storePath && process.env.EVENT_STORE_FILE) storePath = process.env.EVENT_STORE_FILE;
    // Only default a JSONL path when the store was not asked for. Defaulting one
    // anyway would write a file nobody requested.
    if (!outPath && !storePath) {
        const ext = path.extname(inPath);
        outPath = path.join(path.dirname(inPath), path.basename(inPath, ext) + '.enriched.jsonl');
    }
    if (isNaN(limit) || limit <= 0) limit = Infinity;
    return { inPath, outPath, storePath, rawOutPath, dedupPath, cachePath, refreshCache, limit };
}

/** Scan a JSONL file and record every positive `event` id. */
async function loadExistingIds(filePath: string, into: PagedBitset): Promise<void> {
    const readline = await import('readline');
    const rl = readline.createInterface({ input: fs.createReadStream(filePath), crlfDelay: Infinity });
    let count = 0;
    for await (const line of rl) {
        const match = /"event"\s*:\s*(-?\d+)/.exec(line);
        if (!match) continue;
        const id = parseInt(match[1], 10);
        if (id > 0) { into.add(id); count++; }
        if (count > 0 && count % 5_000_000 === 0) {
            console.log(`[ingest] Loaded ${count.toLocaleString()} existing ids from ${filePath}...`);
        }
    }
    console.log(`[ingest] Loaded ${count.toLocaleString()} existing ids from ${filePath}.`);
}

/** Enrich a CSV and write it to the store, to JSONL, or both. */
export async function ingestCsv(opts: IngestOptions): Promise<IngestStats> {
    const parser = new LawParser(opts.cachePath || undefined);
    if (opts.refreshCache) {
        console.log('[ingest] Refreshing law outcomes cache...');
        await parser.fetchAndUpdate();
    }

    const dedupIds = new PagedBitset();
    if (opts.dedupPath) {
        if (!fs.existsSync(opts.dedupPath)) {
            throw new Error(`--dedup file not found: ${opts.dedupPath}`);
        }
        await loadExistingIds(opts.dedupPath, dedupIds);
    }

    if (opts.outPath) {
        const outDir = path.dirname(opts.outPath);
        if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });
    }
    if (opts.rawOutPath) {
        const rawDir = path.dirname(opts.rawOutPath);
        if (!fs.existsSync(rawDir)) fs.mkdirSync(rawDir, { recursive: true });
    }
    if (opts.storePath) {
        const storeDir = path.dirname(opts.storePath);
        if (!fs.existsSync(storeDir)) fs.mkdirSync(storeDir, { recursive: true });
    }

    const outStream = opts.outPath ? fs.createWriteStream(opts.outPath, { flags: 'w' }) : null;
    const rawStream = opts.rawOutPath ? fs.createWriteStream(opts.rawOutPath, { flags: 'w' }) : null;
    // Same sink the live enricher uses, so an imported event is stored exactly as
    // one tailed from Akari - batched, and keyed so a repeat cannot become a row.
    const storeSink = opts.storePath ? new StoreSink(opts.storePath) : null;

    const write = async (stream: NodeJS.WritableStream, line: string): Promise<void> => {
        if (!stream.write(line)) await new Promise<void>(r => stream.once('drain', () => r()));
    };

    const inputSeen = new PagedBitset();
    let read = 0, bad = 0, skippedExisting = 0, skippedDuplicate = 0, written = 0;
    let lawTotal = 0, lawMatched = 0, lawNull = 0;

    const start = Date.now();
    const source = fs.createReadStream(opts.inPath, { encoding: 'utf8', highWaterMark: 1 << 20 });

    for await (const fields of readCsvRecords(source)) {
        if (fields.length < 8) { bad++; continue; }
        read++;

        const id = parseInt(fields[0], 10);
        const time = parseInt(fields[1], 10);
        if (!Number.isFinite(id)) { bad++; continue; }

        const data = parsePgArrayLiteral(fields[7]);
        if (data === null) { bad++; continue; }

        if (dedupIds.has(id)) { skippedExisting++; if (read % 1_000_000 === 0) logProgress(); continue; }
        if (inputSeen.has(id)) { skippedDuplicate++; if (read % 1_000_000 === 0) logProgress(); continue; }
        inputSeen.add(id);

        const event: AkariEvent = {
            event: id,
            time: Number.isFinite(time) ? time : 0,
            ...(fields[2] ? { actor: fields[2] } : {}),
            ...(fields[3] ? { receptor: fields[3] } : {}),
            ...(fields[4] ? { origin: fields[4] } : {}),
            ...(fields[5] ? { destination: fields[5] } : {}),
            category: fields[6],
            data
        };

        const enriched = parser.enrichEvent(event);
        if (event.category === 'law') {
            lawTotal++;
            if (enriched.law_issue_id != null) lawMatched++; else lawNull++;
        }

        const line = toJsonlLine(enriched);
        if (outStream) await write(outStream, line);
        if (storeSink) storeSink.write(line);
        if (rawStream) await write(rawStream, toJsonlLine(event));
        written++;

        if (read % 1_000_000 === 0) logProgress();

        if (read >= opts.limit) break;
    }

    function logProgress(): void {
        const secs = (Date.now() - start) / 1000;
        console.log(`[ingest] read ${read.toLocaleString()} | written ${written.toLocaleString()} | skipped(existing) ${skippedExisting.toLocaleString()} | ${(read / secs).toFixed(0)}/s`);
    }

    if (outStream) await new Promise<void>(resolve => outStream.end(resolve));
    if (rawStream) await new Promise<void>(resolve => rawStream.end(resolve));
    // Flushes the tail batch and commits it; a run that ends without this leaves
    // the last rows in an open transaction.
    storeSink?.close();

    const outSize = opts.outPath && fs.existsSync(opts.outPath) ? fs.statSync(opts.outPath).size : 0;
    const stored = storeSink?.written ?? 0;
    console.log('[ingest] Done.');
    console.log(`[ingest] Input records:      ${read.toLocaleString()}`);
    console.log(`[ingest] Malformed records:  ${bad.toLocaleString()}`);
    console.log(`[ingest] Skipped (existing): ${skippedExisting.toLocaleString()}`);
    console.log(`[ingest] Skipped (dup):      ${skippedDuplicate.toLocaleString()}`);
    console.log(`[ingest] Written:            ${written.toLocaleString()}`);
    console.log(`[ingest] Law enriched:       ${lawMatched.toLocaleString()} / ${lawTotal.toLocaleString()} (${lawTotal ? (100 * lawMatched / lawTotal).toFixed(2) : '0.00'}%, null ${lawNull.toLocaleString()})`);
    if (opts.outPath) console.log(`[ingest] Output:             ${opts.outPath} (${(outSize / 1024 / 1024).toFixed(1)} MB)`);
    if (opts.storePath) console.log(`[ingest] Store:              ${opts.storePath} (${stored.toLocaleString()} rows stored)`);
    if (opts.rawOutPath) console.log(`[ingest] Raw output:         ${opts.rawOutPath}`);

    return { read, bad, skippedExisting, skippedDuplicate, written, lawTotal, lawMatched, lawNull, stored };
}

async function main(): Promise<void> {
    const args = process.argv.slice(2);
    if (args.includes('--help') || args.includes('-h')) { printHelp(); process.exit(0); }

    const opts = parseArgs(args);
    if (!opts) { printHelp(); process.exit(1); }

    await ingestCsv(opts);
}

if (require.main === module) {
    main().catch(err => {
        console.error('[ingest] Failed:', err);
        process.exit(1);
    });
}
