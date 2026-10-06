import * as fs from 'fs';
import * as path from 'path';
import * as readline from 'readline';
import { LawParser } from './lawParser';
import { GapFiller, GapState, scanFileForGaps, appendRecoveredToRaw, splitIntoChunks, gapFromJump, parseConnmiss, DEFAULT_CHUNK_SIZE } from './gapfill';
import { StoreSink, teeToStore } from './store_sink';
export { LawParser } from './lawParser';
export { EventStore } from './event_store';
export { StoreSink, ENRICH_VERSION } from './store_sink';
export * from './types';
export * from './gapfill';
export { CasClient } from './cas';
export type { CasToken, CasOptions, CasTelemetry } from './cas';

function printHelp(): void {
    console.error(`
Akiraka - enrich and store the NationStates happenings stream

Usage:
  # Stream stdin to stdout (pipe mode):
  cat akari.jsonl | akiraka > enriched.jsonl

  # Batch process a file:
  akiraka --in /path/to/events.jsonl --out /path/to/enriched.jsonl

  # Live tail mode alongside Akari:
  akiraka --tail /data/akari_events.jsonl --out /path/to/akari_events.enriched.jsonl

  # Also write enriched events into the event store (add --out to keep the JSONL):
  akiraka --tail /data/akari_events.jsonl --store /data/events.db

  # Fill gaps in an existing file (fetch missing ranges via NS API + Akari reparse):
  akiraka --gapfill --in /path/to/events.jsonl --out /path/to/enriched.jsonl

  # Fill ALL detected gaps regardless of size (bounded by what the NS API still stores):
  akiraka --gapfill --fill-all --in /path/to/events.jsonl --out /path/to/enriched.jsonl

Options:
  --tail <file>     Follow an active file as Akari appends to it
  --in <file>       Process an existing file and exit
  --out <file>      Output file (defaults to stdout in pipe mode)
  --cte-out <file>  Also write only cessation/revival events to this file
  --gapfill         Scan the input for gaps and fill them (tail mode auto-fills)
  --fill-all        Fill every detected gap, ignoring --max-gap (closest to oldest retained by API)
  --chunk-size <n>  Max events per chunk when filling large gaps (default: 1000)
  --no-write-back   Do not append recovered raw events back to the Akari output file
  --cache <file>    Path to issues_cache.txt
  --akari-bin <exe> Path to the Akari binary used for gap reparse (default: akari)
  --max-gap <n>     Largest single gap to fill in --gapfill mode (default: 1000)
  --scan-interval <n> Minutes between periodic gap scans of the tail file (0 = disabled)
  --help, -h        Show this help message

Environment:
  AKARI_FILE        JSONL file Akari writes (fallback for --tail)
  ENRICHED_FILE     Output file (fallback for --out)
  EVENT_STORE_FILE  SQLite event store to write (fallback for --store)
  CTE_FILE          Cessation/revival sidecar file (fallback for --cte-out)
  NS_USER_AGENT     Required for gapfill; identifies you to the NS API
  NS_CAS_URL        Optional NS Coordinated Allocation Server base URL (e.g. http://host:8082)
  NS_CAS_APPLIANCE  CAS appliance id for this instance (default: akiraka)
  NS_CAS_CLASS      CAS priority class for gapfill requests (default: P3_LOW)
  GAPFILL_TIMEOUT   Max ms to wait for a gap reparsing run (default: 180000)
  GAPFILL_MAX_GAP   Largest single gap to auto-fill in tail mode (default: 1000)
  GAPFILL_CHUNK_SIZE Max events per chunk during gap filling (default: 1000)
  GAPFILL_SCAN_START  If "1", scan the existing tail file for gaps on startup
  GAPFILL_SCAN_INTERVAL Minutes between periodic background scans for gaps (default: 0)
  GAPFILL_WRITE_BACK If "0", disable writing recovered raw events back to the Akari file
`);
}

/**
 * Happenings that mark a nation ceasing to exist or being refounded.
 *
 * These are the only events a cessation/lifecycle reader needs, and they are
 * under 1% of the stream, so writing them to their own file turns an
 * eleven-gigabyte read into a few tens of megabytes. The main output is
 * unchanged and still carries everything.
 */
const CTE_CATEGORIES = ['ncte', 'cte', 'nrefound'];

/**
 * Forward every write to the primary output, and additionally to a sidecar when
 * the line is a cessation/revival event.
 *
 * On the stream rather than at the call sites, so a gap-fill recovery is routed
 * too.
 */
function attachCteSidecar(
    primary: NodeJS.WritableStream,
    ctePath: string
): NodeJS.WritableStream {
    const sidecar = fs.createWriteStream(ctePath, { flags: 'a' });
    const markers = CTE_CATEGORIES.flatMap(c => [`"category":"${c}"`, `"category": "${c}"`]);

    return {
        write(chunk: unknown, ...rest: unknown[]): boolean {
            const text = typeof chunk === 'string'
                ? chunk
                : Buffer.isBuffer(chunk) ? chunk.toString('utf8') : String(chunk ?? '');
            if (markers.some(m => text.includes(m))) {
                (sidecar.write as (...a: unknown[]) => boolean)(chunk, ...rest);
            }
            return (primary.write as (...a: unknown[]) => boolean)(chunk, ...rest);
        },
    } as unknown as NodeJS.WritableStream;
}

interface GapfillOptions {
    maxGap: number;
    chunkSize: number;
    scanOnStart: boolean;
    scanIntervalMinutes: number;
    fillAll: boolean;
    writeBack: boolean;
}

function makeGapfillConfig(args: string[]): GapfillOptions {
    let maxGap = parseInt(process.env.GAPFILL_MAX_GAP || '1000', 10);
    if (isNaN(maxGap) || maxGap <= 0) maxGap = 1000;
    let chunkSize = parseInt(process.env.GAPFILL_CHUNK_SIZE || String(DEFAULT_CHUNK_SIZE), 10);
    if (isNaN(chunkSize) || chunkSize <= 0) chunkSize = DEFAULT_CHUNK_SIZE;
    let scanOnStart = process.env.GAPFILL_SCAN_START === '1' || process.env.GAPFILL_SCAN_START === 'true';
    let scanIntervalMinutes = parseInt(process.env.GAPFILL_SCAN_INTERVAL || '0', 10);
    if (isNaN(scanIntervalMinutes) || scanIntervalMinutes < 0) scanIntervalMinutes = 0;
    let fillAll = args.includes('--fill-all') || process.env.GAPFILL_FILL_ALL === '1' || process.env.GAPFILL_FILL_ALL === 'true';
    const envWriteBack = process.env.GAPFILL_WRITE_BACK;
    let writeBack = !(envWriteBack === '0' || envWriteBack === 'false') && !args.includes('--no-write-back');

    for (let i = 0; i < args.length; i++) {
        if (args[i] === '--max-gap' && args[i + 1]) {
            const parsed = parseInt(args[++i], 10);
            if (!isNaN(parsed) && parsed > 0) maxGap = parsed;
        } else if (args[i] === '--chunk-size' && args[i + 1]) {
            const parsed = parseInt(args[++i], 10);
            if (!isNaN(parsed) && parsed > 0) chunkSize = parsed;
        } else if (args[i] === '--scan-interval' && args[i + 1]) {
            const parsed = parseInt(args[++i], 10);
            if (!isNaN(parsed) && parsed >= 0) scanIntervalMinutes = parsed;
        }
    }
    return { maxGap, chunkSize, scanOnStart, scanIntervalMinutes, fillAll, writeBack };
}

async function runPipeMode(parser: LawParser, outputStream: NodeJS.WritableStream): Promise<void> {
    const rl = readline.createInterface({
        input: process.stdin,
        crlfDelay: Infinity
    });

    for await (const line of rl) {
        if (!line.trim()) continue;
        const enriched = parser.enrichLine(line);
        outputStream.write(enriched + '\n');
    }
}

async function runBatchFile(parser: LawParser, inPath: string, outStream: NodeJS.WritableStream): Promise<void> {
    if (!fs.existsSync(inPath)) {
        console.error(`Error: Input file not found: ${inPath}`);
        process.exit(1);
    }

    const rl = readline.createInterface({
        input: fs.createReadStream(inPath),
        crlfDelay: Infinity
    });

    let count = 0;
    for await (const line of rl) {
        if (!line.trim()) continue;
        const enriched = parser.enrichLine(line);
        outStream.write(enriched + '\n');
        count++;
        if (count % 500_000 === 0) {
            console.error(`Processed ${count.toLocaleString()} events...`);
        }
    }
    console.error(`Completed: ${count.toLocaleString()} events enriched.`);
}

async function fillGapInChunks(
    filler: GapFiller,
    state: GapState,
    startId: number,
    endId: number,
    outStream: NodeJS.WritableStream,
    rawPath: string | null,
    options: { chunkSize: number; writeBack: boolean }
): Promise<{ fetched: number; written: number; cutoffReached: boolean }> {
    if (state.coversRange(startId, endId)) {
        return { fetched: 0, written: 0, cutoffReached: false };
    }

    // Process chunks newest-to-oldest so if a gap crosses the retention cutoff,
    // we stop fetching older chunks as soon as the cutoff is reached.
    const chunks = splitIntoChunks(startId, endId, options.chunkSize).reverse();
    let totalFetched = 0;
    let totalWritten = 0;
    let cutoffReached = false;

    for (let c = 0; c < chunks.length; c++) {
        const chunk = chunks[c];
        if (state.coversRange(chunk.start, chunk.end)) {
            continue;
        }

        if (chunks.length > 1) {
            console.log(`[gapfill] [chunk ${c + 1}/${chunks.length}] Range ${chunk.start}-${chunk.end} (${chunk.end - chunk.start + 1} events)...`);
        }

        state.markInFlight(chunk.start, chunk.end);
        try {
            const result = await filler.fillRange(chunk.start, chunk.end);
            totalFetched += result.fetched;

            if (result.events.length > 0) {
                for (const recovered of result.events) {
                    outStream.write(recovered.line + '\n');
                }
                totalWritten += result.written;

                if (options.writeBack && rawPath) {
                    // appendRecoveredToRaw already dedupes by id and no-ops on an
                    // empty list, so the mapping here needs no guard of its own.
                    const rawLines = result.events.map(recovered => recovered.raw);
                    try {
                        const appended = await appendRecoveredToRaw(rawPath, rawLines);
                        console.log(`[gapfill] Wrote back ${appended} raw event(s) to ${rawPath}`);
                    } catch (err: any) {
                        console.warn(`[gapfill] Write-back to ${rawPath} failed: ${err.message}`);
                    }
                }
            }

            if (result.cutoffReached) {
                cutoffReached = true;
                state.markFilled(chunk.start, chunk.end);
                // Mark all remaining older chunks in this gap as filled
                for (let rem = c + 1; rem < chunks.length; rem++) {
                    state.markFilled(chunks[rem].start, chunks[rem].end);
                }
                break;
            } else if (result.fetched === 0) {
                // Within 7 days, but no matching events in world feed
                state.markFilled(chunk.start, chunk.end);
            } else if (result.written === 0) {
                state.unmarkInFlight(chunk.start, chunk.end);
                console.warn(`[gapfill] No events recovered for chunk ${chunk.start}-${chunk.end} (fetched ${result.fetched})`);
            } else {
                state.markFilled(chunk.start, chunk.end);
            }
        } catch (err: any) {
            state.unmarkInFlight(chunk.start, chunk.end);
            console.error(`[gapfill] Failed to fill chunk ${chunk.start}-${chunk.end}: ${err.message}`);
        }
    }

    return { fetched: totalFetched, written: totalWritten, cutoffReached };
}

/**
 * Standalone gapfill: scan an existing JSONL for missing event id ranges, then
 * fetch each range from the NS API, reparse it with a second Akari instance,
 * enrich it, and append the recovered events to the output.
 */
async function runGapfillMode(
    parser: LawParser,
    inPath: string,
    outStream: NodeJS.WritableStream,
    outPath: string,
    options: GapfillOptions,
    rev: { akariBinary?: string; replayPort?: number }
): Promise<void> {
    if (!fs.existsSync(inPath)) {
        console.error(`Error: Input file not found: ${inPath}`);
        process.exit(1);
    }

    const filler = new GapFiller({
        lawParser: parser,
        akariBinary: rev.akariBinary,
        replayPort: rev.replayPort,
        apiUrl: process.env.GAPFILL_API_URL || undefined
    });

    if (!filler.hasUserAgent) {
        console.error('Error: NS_USER_AGENT environment variable is required for --gapfill.');
        process.exit(1);
    }

    const sizeLimit = options.fillAll ? Infinity : options.maxGap;

    console.log(`[gapfill] Scanning ${inPath} for gaps...`);
    const gaps = (await scanFileForGaps(inPath))
        .filter(gap => gap.end - gap.start + 1 <= sizeLimit);

    if (gaps.length === 0) {
        console.log('[gapfill] No gaps to fill.');
        return;
    }

    // Sort gaps newest-to-oldest so we recover recent gaps first and stop immediately
    // once we hit the NationStates API retention cutoff (~7-8 days limit).
    gaps.sort((a, b) => b.start - a.start);

    if (options.fillAll) {
        console.log(`[gapfill] --fill-all: filling all ${gaps.length} detected gaps (newest first, chunked at ${options.chunkSize} events).`);
    } else {
        console.log(`[gapfill] Found ${gaps.length} gap(s). Filling up to ${options.maxGap} events each (newest first, chunked at ${options.chunkSize})...`);
    }

    const statePath = makeStatePath(outPath);
    const state = new GapState(statePath);

    let totalWritten = 0;
    for (let i = 0; i < gaps.length; i++) {
        const gap = gaps[i];
        if (state.coversRange(gap.start, gap.end)) {
            continue;
        }

        console.log(`[gapfill] [${i + 1}/${gaps.length}] Gap ${gap.start}-${gap.end} (${gap.end - gap.start + 1} events)`);
        const result = await fillGapInChunks(filler, state, gap.start, gap.end, outStream, inPath, {
            chunkSize: options.chunkSize,
            writeBack: options.writeBack
        });
        totalWritten += result.written;

        if (result.cutoffReached) {
            const remaining = gaps.length - 1 - i;
            console.log(`[gapfill] Reached NationStates API retention cutoff at ID ${gap.end} (~7-8 days limit). Skipping ${remaining} older unrecoverable gap(s).`);
            for (let j = i + 1; j < gaps.length; j++) {
                state.markFilled(gaps[j].start, gaps[j].end);
            }
            break;
        }
    }

    console.log(`[gapfill] Done. Recovered ${totalWritten} events.`);
}

function makeStatePath(outPath: string): string {
    if (process.env.GAPFILL_STATE_FILE) return process.env.GAPFILL_STATE_FILE;
    return path.join(path.dirname(outPath), '.akiraka_gapfill_state.json');
}

async function runTailMode(
    parser: LawParser,
    tailPath: string,
    outStream: NodeJS.WritableStream,
    outPath: string,
    gapfillOptions: GapfillOptions,
    rev: { akariBinary?: string; replayPort?: number; cachePath?: string; gapfillEnabled: boolean },
    flushStore?: () => void,
    recordGap?: (start: number, end: number) => void
): Promise<void> {
    console.error(`[Akiraka] Starting tail mode on: ${tailPath}`);

    while (!fs.existsSync(tailPath)) {
        console.error(`[Akiraka] Waiting for ${tailPath} to appear...`);
        await new Promise(r => setTimeout(r, 2000));
    }

    const state = new GapState(makeStatePath(outPath));

    // The read offset is debounced, so a stop that is not graceful would lose it
    // and make the next start re-read the log. Tail mode never returns, so there
    // is no `finally` to hang this on; the signal is the only chance.
    const persistState = (): void => {
        try {
            state.save();
        } catch {
            // save() already warns; a failure here must not stop the service.
        }
    };
    process.on('SIGTERM', persistState);
    process.on('SIGINT', persistState);
    process.on('exit', persistState);
    const filler = new GapFiller({
        lawParser: parser,
        akariBinary: rev.akariBinary,
        replayPort: rev.replayPort,
        apiUrl: process.env.GAPFILL_API_URL || undefined
    });
    const autoFill = rev.gapfillEnabled && filler.hasUserAgent;
    if (rev.gapfillEnabled && !filler.hasUserAgent) {
        console.warn('[gapfill] NS_USER_AGENT not set; gap auto-fill disabled.');
    }

    // Resume where the last run stopped instead of re-reading from the start.
    // Re-reading is safe - the store keys on the event id - but it is not free:
    // it re-enriches the entire log on every restart.
    let position = 0;
    let partialLine = '';
    let totalProcessed = 0;

    const initialStat = fs.statSync(tailPath);
    const savedOffset = state.getTailOffset();
    if (savedOffset > 0) {
        if (savedOffset <= initialStat.size) {
            position = savedOffset;
            console.error(
                `[Akiraka] Resuming at byte ${position.toLocaleString()} of ${initialStat.size.toLocaleString()}.`
            );
        } else {
            // The file is shorter than where we stopped, so it was replaced
            // rather than appended to. Reading it from the start is the safe
            // answer; the offset would skip everything up to it.
            console.error(
                `[Akiraka] Saved position ${savedOffset.toLocaleString()} is past the end of ` +
                `${tailPath} (${initialStat.size.toLocaleString()}); the file was replaced, so ` +
                `reading it from the start.`
            );
        }
    }

    const gapQueue: Array<{ start: number; end: number; gapSize: number }> = [];
    let isProcessingQueue = false;

    const processQueue = async (): Promise<void> => {
        if (isProcessingQueue || !autoFill) return;
        isProcessingQueue = true;
        try {
            while (gapQueue.length > 0) {
                const item = gapQueue.shift()!;
                if (!gapfillOptions.fillAll && item.gapSize > gapfillOptions.maxGap) {
                    console.warn(`[gapfill] Gap ${item.start}-${item.end} (${item.gapSize} events) exceeds max gap ${gapfillOptions.maxGap}, skipping`);
                    continue;
                }
                if (state.coversRange(item.start, item.end)) continue;

                console.log(`[gapfill] Detected gap ${item.start}-${item.end} (${item.gapSize} events), filling...`);
                const result = await fillGapInChunks(filler, state, item.start, item.end, outStream, tailPath, {
                    chunkSize: gapfillOptions.chunkSize,
                    writeBack: gapfillOptions.writeBack
                });
                if (result.cutoffReached && gapQueue.length > 0) {
                    console.log(`[gapfill] Hit retention cutoff; clearing ${gapQueue.length} older queued gap(s).`);
                    while (gapQueue.length > 0) {
                        const skipped = gapQueue.shift()!;
                        state.markFilled(skipped.start, skipped.end);
                    }
                }
            }
        } finally {
            isProcessingQueue = false;
        }
    };

    const triggerGapfill = (startId: number, endId: number, gapSize: number): void => {
        if (!autoFill) return;
        if (!gapfillOptions.fillAll && gapSize > gapfillOptions.maxGap) {
            console.warn(`[gapfill] Gap ${startId}-${endId} (${gapSize} events) exceeds max gap ${gapfillOptions.maxGap}, skipping`);
            return;
        }
        if (state.coversRange(startId, endId)) return;

        gapQueue.push({ start: startId, end: endId, gapSize });
        processQueue().catch(err => {
            console.error(`[gapfill] Queue processing error: ${err.message}`);
        });
    };

    const noteGap = (start: number, end: number): void => {
        // A range already filled is not missing, and recording it would overstate
        // completeness until something happened to re-write every id in it.
        if (state.coversRange(start, end)) return;
        recordGap?.(start, end);
        triggerGapfill(start, end, end - start + 1);
    };

    const processEventLine = (line: string): void => {
        // Akari says outright when a reconnect missed ids. The jump below implies
        // the same range, but this is its own account of what the connection lost.
        const reported = parseConnmiss(line);
        if (reported) noteGap(reported.start, reported.end);

        const match = /"event"\s*:\s*(-?\d+)/.exec(line);
        if (match) {
            const id = parseInt(match[1], 10);
            if (id > 0) {
                if (state.isCovered(id)) return;
                const jumped = gapFromJump(state.lastId, id);
                if (jumped) noteGap(jumped.start, jumped.end);
                state.observeEventId(id);
            }
        }
        const enriched = parser.enrichLine(line);
        outStream.write(enriched + '\n');
        totalProcessed++;
    };

    const processBuffer = (buffer: Buffer) => {
        const text = partialLine + buffer.toString('utf-8');
        const lines = text.split('\n');
        partialLine = lines.pop() || '';

        for (const line of lines) {
            if (!line.trim()) continue;
            processEventLine(line);
        }
    };

    // Initial read of whatever this run has not seen yet
    try {
        if (initialStat.size > position) {
            const fd = fs.openSync(tailPath, 'r');
            const buf = Buffer.alloc(initialStat.size - position);
            fs.readSync(fd, buf, 0, buf.length, position);
            fs.closeSync(fd);
            position = initialStat.size;
            processBuffer(buf);
            state.setTailOffset(position);
            // Catch-up is a natural commit point. Without this a burst that then
            // goes quiet stays buffered indefinitely: the sink flushes on a write,
            // and there is no next write.
            flushStore?.();
            await new Promise(r => setTimeout(r, 0));
            console.error(`[Akiraka] Processed ${totalProcessed.toLocaleString()} existing events. Now watching for new events...`);
        }
    } catch (err: any) {
        console.error(`[Akiraka] Error reading initial content: ${err.message}`);
    }

    const scanAndQueueGaps = async (reason: string): Promise<void> => {
        if (!autoFill) return;
        try {
            console.error(`[gapfill] Scanning tail file for gaps (${reason})...`);
            const gaps = await scanFileForGaps(tailPath);
            // Recorded whether or not they are fillable: the table is a statement of
            // what the store lacks, and a hole too big to auto-fill is still a hole.
            // Merging keeps a repeat scan from growing it.
            for (const gap of gaps) recordGap?.(gap.start, gap.end);
            const fillable = gaps
                .filter(g => gapfillOptions.fillAll || g.end - g.start + 1 <= gapfillOptions.maxGap)
                .sort((a, b) => b.start - a.start);
            let queued = 0;
            for (const gap of fillable) {
                if (state.coversRange(gap.start, gap.end)) continue;
                triggerGapfill(gap.start, gap.end, gap.end - gap.start + 1);
                queued++;
            }
            if (queued > 0) {
                console.error(`[gapfill] Queued ${queued} gap(s) for filling (${reason}).`);
            }
        } catch (err: any) {
            console.error(`[gapfill] Scan failed (${reason}): ${err.message}`);
        }
    };

    if (gapfillOptions.scanOnStart && autoFill) {
        scanAndQueueGaps('startup');
    }

    if (gapfillOptions.scanIntervalMinutes > 0 && autoFill) {
        const intervalMs = gapfillOptions.scanIntervalMinutes * 60 * 1000;
        console.error(`[gapfill] Periodic gap check scheduled every ${gapfillOptions.scanIntervalMinutes} minute(s).`);
        setInterval(() => {
            scanAndQueueGaps('periodic check');
        }, intervalMs);
    }

    // Continuous poll (500ms)
    setInterval(() => {
        try {
            if (!fs.existsSync(tailPath)) return;
            const stat = fs.statSync(tailPath);

            // Handle file truncation/rotation
            if (stat.size < position) {
                console.error(`[Akiraka] Akari log rotation detected. Resetting cursor to follow new file.`);
                position = 0;
                partialLine = '';
                // The offset is a position in a file that is being replaced, so
                // keeping it would make the next start skip into the new file.
                state.resetTailOffset();
                // When Akari rotates files, check for any gaps that occurred across the rotation boundary
                if (autoFill) {
                    scanAndQueueGaps('log rotation');
                }
            }

            if (stat.size > position) {
                const bytesToRead = stat.size - position;
                const fd = fs.openSync(tailPath, 'r');
                const buf = Buffer.alloc(bytesToRead);
                fs.readSync(fd, buf, 0, bytesToRead, position);
                fs.closeSync(fd);
                position = stat.size;
                processBuffer(buf);
                state.setTailOffset(position);
                // Having drained everything on disk is the moment to commit. A
                // flush keyed only to incoming writes would strand the last batch
                // of a quiet period, which is most of them.
                flushStore?.();
            }
        } catch {
            // Ignore temporary file access errors during append
        }
    }, 500);

    // Keep process alive
    await new Promise(() => {});
}

export async function cli(): Promise<void> {
    const args = process.argv.slice(2);

    if (args.includes('--help') || args.includes('-h')) {
        printHelp();
        process.exit(0);
    }

    let tailPath: string | null = null;
    let inPath: string | null = null;
    let outPath: string | null = null;
    let storePath: string | null = null;
    let ctePath: string | null = null;
    let cachePath: string | null = null;
    let akariBinary: string | null = null;
    let replayPort: number | null = null;
    let gapfillMode = false;

    for (let i = 0; i < args.length; i++) {
        if (args[i] === '--tail' && args[i + 1]) tailPath = args[++i];
        else if (args[i] === '--in' && args[i + 1]) inPath = args[++i];
        else if (args[i] === '--out' && args[i + 1]) outPath = args[++i];
        else if (args[i] === '--store' && args[i + 1]) storePath = args[++i];
        else if (args[i] === '--cte-out' && args[i + 1]) ctePath = args[++i];
        else if (args[i] === '--cache' && args[i + 1]) cachePath = args[++i];
        else if (args[i] === '--akari-bin' && args[i + 1]) akariBinary = args[++i];
        else if (args[i] === '--replay-port' && args[i + 1]) replayPort = parseInt(args[++i], 10);
        else if (args[i] === '--gapfill') gapfillMode = true;
        else if (args[i] === '--max-gap' && args[i + 1]) i++;
        else if (args[i] === '--chunk-size' && args[i + 1]) i++;
        else if (args[i] === '--scan-interval' && args[i + 1]) i++;
        else if (!args[i].startsWith('-') && !inPath) inPath = args[i];
    }

    // Default paths from environment variables if set
    if (!tailPath && !inPath && process.env.AKARI_FILE) {
        tailPath = process.env.AKARI_FILE;
    }
    if (!outPath && process.env.ENRICHED_FILE) {
        outPath = process.env.ENRICHED_FILE;
    }
    if (!storePath && process.env.EVENT_STORE_FILE) {
        storePath = process.env.EVENT_STORE_FILE;
    }
    if (!ctePath && process.env.CTE_FILE) {
        ctePath = process.env.CTE_FILE;
    }

    if (gapfillMode) {
        if (!inPath && process.env.AKARI_FILE) inPath = process.env.AKARI_FILE;
        if (!inPath) {
            console.error('Error: --gapfill requires an input file (--in <file> or AKARI_FILE).');
            process.exit(1);
        }
        if (!outPath) outPath = process.env.ENRICHED_FILE || path.join(path.dirname(inPath), 'events.enriched.jsonl');
    }

    const parser = new LawParser(cachePath || undefined);

    let outputStream: NodeJS.WritableStream = process.stdout;
    if (outPath) {
        const outDir = path.dirname(outPath);
        if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });
        outputStream = fs.createWriteStream(outPath, { flags: 'a' });
    }

    // The store is a destination in its own right, so a run with --store and no
    // --out must not fall through to stdout. Tail mode would otherwise copy
    // millions of enriched lines into the container log, which reads as a full
    // disk with nothing to point at.
    const storeSink = storePath ? new StoreSink(storePath) : null;
    if (storeSink) {
        if (outPath) {
            console.error(`[store] also writing enriched events to ${storePath}`);
        } else {
            outputStream = { write: () => true } as unknown as NodeJS.WritableStream;
            console.error(`[store] writing enriched events to ${storePath}; no JSONL output`);
        }
        outputStream = teeToStore(outputStream, storeSink);

        // Tail mode never returns, so a `finally` around the dispatch would never
        // run. Docker stops a service with SIGTERM, and a buffered batch is the
        // only thing at risk, so it is flushed on the way out rather than left to
        // the next start to re-derive.
        let closing = false;
        const shutdown = (signal: string): void => {
            if (closing) return;
            closing = true;
            // Closed before logging: the counts only mean anything once the last
            // batch has been committed, and the flush happens inside close().
            try {
                storeSink.close();
            } catch (err: any) {
                console.error(`[store] flush on ${signal} failed: ${err.message}`);
            }
            console.error(`[store] ${signal}: ${storeSink.summary()}`);
            process.exit(0);
        };
        process.on('SIGTERM', () => shutdown('SIGTERM'));
        process.on('SIGINT', () => shutdown('SIGINT'));
    }

    // Attached last, so gap-fill and batch writes pass through it too. Only
    // tail mode runs this way under the container, and the sidecar is skipped
    // silently when the file is absent, so this is inert everywhere else.
    if (ctePath) {
        const cteDir = path.dirname(ctePath);
        if (!fs.existsSync(cteDir)) fs.mkdirSync(cteDir, { recursive: true });
        outputStream = attachCteSidecar(outputStream, ctePath);
        console.error(`[Akiraka] Also writing cessation/revival events to: ${ctePath}`);
    }

    const gapfillOptions = makeGapfillConfig(args);

    try {
        if (gapfillMode) {
            await runGapfillMode(parser, inPath as string, outputStream, outPath as string, gapfillOptions, {
                akariBinary: akariBinary || undefined,
                replayPort: replayPort || undefined
            });
        } else if (tailPath) {
            // The state file has to live beside something real. With --store and
            // no --out there is no output path, and defaulting it to 'data.jsonl'
            // would put the state file in the working directory - which, inside the
            // container, is the source tree rather than the data volume, so it
            // would not survive a rebuild and could not be inspected from outside.
            const stateAnchor = outPath || storePath || 'data.jsonl';
            await runTailMode(parser, tailPath, outputStream, stateAnchor, gapfillOptions, {
                akariBinary: akariBinary || undefined,
                replayPort: replayPort || undefined,
                cachePath: cachePath || undefined,
                gapfillEnabled: true
            }, storeSink ? () => storeSink.flush() : undefined,
               storeSink ? (start: number, end: number) => storeSink.recordGap(start, end) : undefined);
        } else if (inPath) {
            await runBatchFile(parser, inPath, outputStream);
        } else if (!process.stdin.isTTY) {
            await runPipeMode(parser, outputStream);
        } else {
            printHelp();
            process.exit(1);
        }
    } finally {
        // A mode that returns has finished its input, so the last partial batch
        // has to be committed here. Tail mode never returns and is covered by the
        // SIGTERM handler instead; without this a batch run committed only the
        // batches that happened to fill, and exited silently having stored less
        // than it read.
        if (storeSink) {
            storeSink.close();
            console.error(`[store] ${storeSink.summary()}`);
        }
    }
}

// Auto-run if executed directly as script
if (require.main === module) {
    cli().catch(err => {
        console.error('Fatal:', err);
        process.exit(1);
    });
}