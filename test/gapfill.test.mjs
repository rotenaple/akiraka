import test from 'node:test';
import assert from 'node:assert/strict';
import { parseHappeningsXml, detectGaps, splitIntoChunks, RangeTracker, gapFromJump, parseConnmiss } from '../dist/gapfill.js';

// Captured verbatim from the happenings API. The element is TIMESTAMP - reading
// TIME instead matches nothing and silently produced time 0 for every event
// recovered through gap-fill, which is how 184,937 events ended up in the store
// with no timestamp.
const REAL_WORLD_FEED = `<?xml version="1.0" encoding="UTF-8"?>
<WORLD>
<HAPPENINGS><EVENT id="374983691">
<TIMESTAMP>1791181306</TIMESTAMP>
<TEXT><![CDATA[Following new legislation in @@somewhere@@, a positive GPA is all it takes.]]></TEXT>
</EVENT>

<EVENT id="374983690">
<TIMESTAMP>1791181306</TIMESTAMP>
<TEXT><![CDATA[a second event, one second apart]]></TEXT>
</EVENT>
</HAPPENINGS>
</WORLD>
`;

test('the timestamp element is read', () => {
    const events = parseHappeningsXml(REAL_WORLD_FEED);
    assert.equal(events.length, 2);
    assert.equal(events[0].id, 374983691);
    assert.equal(events[0].time, 1791181306, 'TIMESTAMP must be read, not defaulted to 0');
    assert.equal(events[1].time, 1791181306);
    for (const e of events) {
        assert.notEqual(e.time, 0, 'a present timestamp must never come back as 0');
    }
});

test('event text is read and entities decoded', () => {
    const [first] = parseHappeningsXml(REAL_WORLD_FEED);
    assert.match(first.text, /somewhere/);
});

test('a time attribute is accepted as well', () => {
    const events = parseHappeningsXml(
        '<WORLD><HAPPENINGS><EVENT id="5" time="1700000000"><TEXT>x</TEXT></EVENT></HAPPENINGS></WORLD>'
    );
    assert.equal(events[0].time, 1700000000);
});

test('a genuinely absent timestamp becomes zero rather than NaN', () => {
    const events = parseHappeningsXml('<WORLD><HAPPENINGS><EVENT id="5"><TEXT>x</TEXT></EVENT></HAPPENINGS></WORLD>');
    assert.equal(events[0].time, 0);
    assert.ok(Number.isFinite(events[0].time));
});

test('an event with no id is skipped', () => {
    const events = parseHappeningsXml(
        '<WORLD><HAPPENINGS><EVENT><TIMESTAMP>1</TIMESTAMP><TEXT>x</TEXT></EVENT></HAPPENINGS></WORLD>'
    );
    assert.equal(events.length, 0);
});

test('html entities in text are decoded', () => {
    const events = parseHappeningsXml(
        '<WORLD><HAPPENINGS><EVENT id="7"><TIMESTAMP>5</TIMESTAMP>' +
        '<TEXT>a &amp; b &lt;c&gt;</TEXT></EVENT></HAPPENINGS></WORLD>'
    );
    assert.equal(events[0].text, 'a & b <c>');
});

test('gaps in an id sequence are detected', () => {
    assert.deepEqual(detectGaps([1, 2, 3, 7, 8, 12]), [
        { start: 4, end: 6 },
        { start: 9, end: 11 },
    ]);
});

test('system events with a non-positive id do not register as gaps', () => {
    assert.deepEqual(detectGaps([-1, 0, 1, 2]), []);
});

test('a chunked range covers every id exactly once', () => {
    const chunks = splitIntoChunks(1, 10, 4);
    const seen = new Set();
    for (const c of chunks) {
        for (let id = c.start; id <= c.end; id++) {
            assert.equal(seen.has(id), false, `id ${id} covered twice`);
            seen.add(id);
        }
    }
    assert.deepEqual([...seen].sort((a, b) => a - b), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
});

test('filled ranges merge when adjacent or overlapping', () => {
    const t = new RangeTracker();
    t.add(10, 20);
    t.add(30, 40);
    assert.deepEqual(t.toArray(), [[10, 20], [30, 40]], 'a gap between them stays a gap');
    t.add(21, 29);
    assert.deepEqual(t.toArray(), [[10, 40]], 'adjacency merges them');
    assert.equal(t.contains(25), true);
    assert.equal(t.contains(41), false);
});

// ---- missing-event detection ----

test('a forward jump is the range it skipped', () => {
    assert.deepEqual(gapFromJump(100, 105), { start: 101, end: 104 });
    assert.deepEqual(gapFromJump(100, 101), null, 'consecutive ids are not a gap');
    assert.deepEqual(gapFromJump(0, 50), null, 'nothing to measure a jump from');
    assert.deepEqual(gapFromJump(100, 100), null, 'a repeat is not a gap');
    assert.deepEqual(gapFromJump(100, 99), null, 'an older backfilled id is not a gap');
});

test('a connmiss marker reports its missing range', () => {
    const line = JSON.stringify({ event: -1, time: 1791181306, category: 'connmiss', data: ['150', '1000', '1151'] });
    assert.deepEqual(parseConnmiss(line), { start: 1001, end: 1150 });
});

test('a connmiss marker with nothing missed yields no range', () => {
    const line = JSON.stringify({ event: -1, time: 1791181306, category: 'connmiss', data: ['0', '1000', '1001'] });
    assert.equal(parseConnmiss(line), null);
});

test('a non-connmiss or malformed line yields no range', () => {
    assert.equal(parseConnmiss(JSON.stringify({ event: 5, category: 'law', data: [] })), null);
    assert.equal(parseConnmiss('{not json'), null);
    assert.equal(parseConnmiss(JSON.stringify({ event: -1, category: 'connmiss', data: ['x'] })), null);
});
