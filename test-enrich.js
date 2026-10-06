const { LawParser } = require('./dist/index.js');
const assert = require('assert');

console.log('Testing LawParser...');
const parser = new LawParser();

// Test 1: Law event with known pattern
const rawLaw = JSON.stringify({
    event: 1,
    time: 1000,
    category: 'law',
    actor: 'test_nation',
    data: ['following new legislation in @@test_nation@@, animal liberationists are regularly arrested']
});

const enriched = JSON.parse(parser.enrichLine(rawLaw));
console.log('Enriched event:', enriched);
assert.strictEqual(enriched.law_issue_id, 7);
assert.strictEqual(enriched.law_option, 1);
assert.strictEqual(enriched.actor, 'test_nation');
assert.strictEqual(enriched.category, 'law');

// Test 2: Non-law event
const rawMove = JSON.stringify({
    event: 2,
    time: 1001,
    category: 'move',
    actor: 'test_nation',
    origin: 'reg1',
    destination: 'reg2',
    data: []
});
const untouched = parser.enrichLine(rawMove);
assert.strictEqual(untouched, rawMove);

// Test 3: Gapfill splitIntoChunks
console.log('Testing splitIntoChunks...');
const { splitIntoChunks, detectGaps, RangeTracker } = require('./dist/index.js');

// Small gap <= chunkSize
assert.deepStrictEqual(splitIntoChunks(100, 150, 1000), [{ start: 100, end: 150 }]);

// Exact multiple
assert.deepStrictEqual(splitIntoChunks(1, 200, 100), [
    { start: 1, end: 100 },
    { start: 101, end: 200 }
]);

// Non-exact multiple
assert.deepStrictEqual(splitIntoChunks(1, 250, 100), [
    { start: 1, end: 100 },
    { start: 101, end: 200 },
    { start: 201, end: 250 }
]);

// Single element
assert.deepStrictEqual(splitIntoChunks(50, 50, 100), [{ start: 50, end: 50 }]);

// Test 4: RangeTracker incremental chunk merging
console.log('Testing RangeTracker merging with chunks...');
const tracker = new RangeTracker();
tracker.add(1, 100);
assert.strictEqual(tracker.coversRange(1, 100), true);
assert.strictEqual(tracker.coversRange(1, 200), false);
tracker.add(101, 200);
// Adjacent chunks should merge into [1, 200]
assert.strictEqual(tracker.coversRange(1, 200), true);
assert.deepStrictEqual(tracker.toArray(), [[1, 200]]);

console.log('ALL TESTS PASSED!');

