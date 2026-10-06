import * as fs from 'node:fs';
import * as path from 'node:path';
import { EventStore, type StoredEventWithData } from './event_store';
import type { EnrichedAkariEvent } from './types';

/**
 * What the collector saw, and what it should have seen but did not.
 *
 * A controlled gap-fill test needs three things that have to agree with each
 * other: a stream with holes in it, the exact set of ids missing from that
 * stream, and an NS happenings feed that will hand those ids back. Deriving all
 * three from one pass over real events is what makes the test meaningful - a
 * hand-written fixture can only prove the parser handles the shape the author
 * imagined, and the shape that broke gap-fill before was not the imagined one.
 */
export interface GapFillFixture {
    /** The stream the collector received, holes and all. */
    seedPath: string;
    /** Ids deliberately removed from the stream, ascending. */
    droppedIds: number[];
    /** The contiguous holes, which is what detection should report. */
    droppedRanges: Array<{ start: number; end: number }>;
    /** Every event in the window, for rendering the fake NS feed. */
    allEvents: Array<FixtureEvent>;
    /** Ids the stream does contain, ascending. */
    seedIds: number[];
    minEvent: number;
    maxEvent: number;
}

export interface FixtureEvent {
    id: number;
    time: number;
    category: string;
    actor: string | null;
    /** NS TEXT for this event, reconstructed from the stored data tokens. */
    text: string;
}

export interface FixtureOptions {
    storePath: string;
    outDir: string;
    /** How many events to take, newest first from the store's head. */
    count?: number;
    /** How many separate chunks to remove. */
    chunks?: number;
    /** Smallest and largest chunk, in events. */
    minChunk?: number;
    maxChunk?: number;
    /** Fixes the drop pattern, so a failure can be reproduced exactly. */
    seed?: number;
}

/**
 * A small deterministic PRNG.
 *
 * mulberry32: four lines, no dependency, and the same seed gives the same gaps
 * on any machine. Math.random would make a failing run unrepeatable, which is
 * the one property a fixture must not lack.
 */
function makeRandom(seed: number): () => number {
    let a = seed >>> 0;
    return () => {
        a = (a + 0x6d2b79f5) >>> 0;
        let t = Math.imul(a ^ (a >>> 15), 1 | a);
        t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
        return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
}

/**
 * Rebuild the NS TEXT for an event from what the store holds.
 *
 * Akari splits the text into tokens in `data` and keeps the nation in `actor`.
 * Joining the tokens recovers the prose closely enough to exercise the parser: a
 * reconstruction, not the original, so a test asserting on exact wording would be
 * asserting on this rather than on gap-fill.
 */
export function reconstructText(event: EnrichedAkariEvent): string {
    const tokens = Array.isArray(event.data) ? event.data : [];
    let text = tokens.join(' ').replace(/\s+/g, ' ').trim();
    if (event.actor && !text.includes(`@@${event.actor}@@`)) {
        text = text.replace(new RegExp(`\\b${escapeRegExp(event.actor)}\\b`), `@@${event.actor}@@`);
    }
    return text;
}

function escapeRegExp(value: string): string {
    return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * Render events as the NS happenings feed the collector would have fetched.
 *
 * Newest first, because that is the order the API returns and `fetchHappeningRange`
 * pages backwards from it. Getting this backwards would still pass a test that
 * only checked that ids came back, which is why the fake server asserts on it.
 */
export function renderWorldFeed(events: FixtureEvent[]): string {
    const body = [...events]
        .sort((a, b) => b.id - a.id)
        .map(e => `  <EVENT id="${e.id}">\n` +
            `  <TIMESTAMP>${e.time}</TIMESTAMP>\n` +
            `  <TEXT><![CDATA[${e.text.replace(/]]>/g, ']]]]><![CDATA[>')}]]></TEXT>\n` +
            `  </EVENT>`)
        .join('\n');
    return `<?xml version="1.0" encoding="UTF-8"?>\n<WORLD>\n<HAPPENINGS>\n${body}\n</HAPPENINGS>\n</WORLD>\n`;
}

/** Contiguous ascending runs, which is the shape a detector should report. */
export function toRanges(ids: number[]): Array<{ start: number; end: number }> {
    const ranges: Array<{ start: number; end: number }> = [];
    for (const id of ids) {
        const last = ranges[ranges.length - 1];
        if (last && id === last.end + 1) last.end = id;
        else ranges.push({ start: id, end: id });
    }
    return ranges;
}

/**
 * Build the fixture.
 *
 * Reads the newest `count` events from a real store, removes `chunks` random
 * runs of them, and writes the result out. Everything the test asserts against
 * comes from the same read, so "what was dropped" cannot drift from "what the
 * fake API will return".
 */
export function buildFixture(options: FixtureOptions): GapFillFixture {
    const count = options.count ?? 400;
    const chunkCount = options.chunks ?? 4;
    const minChunk = options.minChunk ?? 3;
    const maxChunk = options.maxChunk ?? 25;
    const random = makeRandom(options.seed ?? 20261005);

    const store = new EventStore(options.storePath, { readOnly: true });
    let window: StoredEventWithData[];
    try {
        const maxEvent = store.maxEvent();
        const rows = store.query({ afterEvent: Math.max(0, maxEvent - count * 3), limit: count * 3 });
        // query() projects the event columns only; `data` lives in its own table and
        // the fixture needs it to rebuild the NS text. One point lookup per event is
        // fine at fixture sizes and avoids widening the query API for a test.
        window = rows
            .filter(r => r.event > 0)
            .sort((a, b) => a.event - b.event)
            .slice(-count)
            .map(r => store.get(r.event))
            .filter((r): r is StoredEventWithData => r !== null);
    } finally {
        store.close();
    }

    if (window.length < count) {
        throw new Error(
            `store holds only ${window.length} usable events in the requested window, need ${count}`
        );
    }

    const allEvents: FixtureEvent[] = window.map(row => {
        const enriched = row as unknown as EnrichedAkariEvent;
        return {
            id: row.event,
            time: row.time ?? 0,
            category: row.category,
            actor: row.actor,
            text: reconstructText(enriched),
        };
    });

    // Drop whole chunks, never the first or last few ids: a hole at either end of
    // the window is not a gap the detector can see, so including one would assert
    // on something gap-fill is not meant to do.
    const margin = 5;
    const droppable = allEvents.length - margin * 2;
    if (droppable <= minChunk) {
        throw new Error(`window of ${allEvents.length} is too small to drop chunks from`);
    }

    const dropped = new Set<number>();
    const droppedRanges: Array<{ start: number; end: number }> = [];
    let attempts = 0;
    while (droppedRanges.length < chunkCount && attempts < chunkCount * 50) {
        attempts++;
        const size = minChunk + Math.floor(random() * (maxChunk - minChunk + 1));
        const offset = margin + Math.floor(random() * (droppable - size));
        const slice = allEvents.slice(offset, offset + size);
        if (slice.length < size) continue;
        // Reject an overlapping chunk, so the expected gaps stay contiguous runs
        // and the detector's answer is unambiguous.
        if (slice.some(e => dropped.has(e.id))) continue;
        droppedRanges.push({ start: slice[0].id, end: slice[slice.length - 1].id });
        for (const e of slice) dropped.add(e.id);
    }
    if (droppedRanges.length < chunkCount) {
        throw new Error(
            `could only place ${droppedRanges.length} of ${chunkCount} chunks without overlap; ` +
            `widen the window or shrink the chunks`
        );
    }

    const seedEvents = allEvents.filter(e => !dropped.has(e.id));
    const droppedIds = [...dropped].sort((a, b) => a - b);
    droppedRanges.sort((a, b) => a.start - b.start);

    fs.mkdirSync(options.outDir, { recursive: true });
    const seedPath = path.join(options.outDir, 'seed.jsonl');
    fs.writeFileSync(
        seedPath,
        seedEvents
            .map(e => JSON.stringify({
                event: e.id,
                time: e.time,
                category: e.category,
                ...(e.actor ? { actor: e.actor } : {}),
                data: [e.text],
            }))
            .join('\n') + '\n'
    );

    const fixture: GapFillFixture = {
        seedPath,
        droppedIds,
        droppedRanges,
        allEvents,
        seedIds: seedEvents.map(e => e.id),
        minEvent: allEvents[0].id,
        maxEvent: allEvents[allEvents.length - 1].id,
    };

    fs.writeFileSync(path.join(options.outDir, 'fixture.json'), JSON.stringify({
        droppedIds,
        droppedRanges,
        minEvent: fixture.minEvent,
        maxEvent: fixture.maxEvent,
        seedCount: fixture.seedIds.length,
    }, null, 2));
    fs.writeFileSync(path.join(options.outDir, 'world.xml'), renderWorldFeed(allEvents));

    return fixture;
}
