// Restart resume: the offset is what stops a restart re-reading the whole log.
// These cover the three cases that decide whether resuming is safe - the file
// grew, the file was replaced, and the offset was never written.
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { GapState } from '../dist/gapfill.js';

function tmpState() {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'akiraka-resume-'));
    return { dir, file: path.join(dir, 'state.json') };
}

test('an offset survives a save and reload', () => {
    const { dir, file } = tmpState();
    const first = new GapState(file);
    assert.equal(first.getTailOffset(), 0, 'a fresh state has nothing to resume from');

    first.setTailOffset(4096);
    first.save();

    const second = new GapState(file);
    assert.equal(second.getTailOffset(), 4096, 'this is the whole point: the offset is durable');
    fs.rmSync(dir, { recursive: true, force: true });
});

test('the offset only moves forward', () => {
    // A debounced save can land after a rotation reset. If it did, the next start
    // would carry an offset from the previous file and skip real events.
    const { dir, file } = tmpState();
    const state = new GapState(file);
    state.setTailOffset(9000);

    state.setTailOffset(100);
    assert.equal(state.getTailOffset(), 9000, 'a lower offset is ignored');

    state.resetTailOffset();
    assert.equal(state.getTailOffset(), 0, 'rotation clears it outright');

    // And a stale save cannot resurrect it.
    state.setTailOffset(50);
    assert.equal(state.getTailOffset(), 50, 'reading forward after a reset is normal again');
    fs.rmSync(dir, { recursive: true, force: true });
});

test('a nonsense offset is rejected rather than trusted', () => {
    const { dir, file } = tmpState();
    const state = new GapState(file);
    for (const bad of [Number.NaN, Number.POSITIVE_INFINITY, -1, 0]) {
        state.setTailOffset(bad);
    }
    assert.equal(state.getTailOffset(), 0);
    fs.rmSync(dir, { recursive: true, force: true });
});

test('a corrupt state file resumes from nothing instead of throwing', () => {
    // Losing the state file must cost a re-read, never a wrong offset.
    const { dir, file } = tmpState();
    fs.writeFileSync(file, '{ this is not json');
    const state = new GapState(file);
    assert.equal(state.getTailOffset(), 0);
    assert.equal(state.lastId, 0);
    fs.rmSync(dir, { recursive: true, force: true });
});

test('the saved offset sits alongside the gap state, not instead of it', () => {
    // Both are resume state for the same run, and losing either means redoing work.
    const { dir, file } = tmpState();
    const first = new GapState(file);
    first.observeEventId(500);
    first.markFilled(10, 20);
    first.setTailOffset(777);
    first.save();

    const second = new GapState(file);
    assert.equal(second.lastId, 500, 'gap tracking still resumes');
    assert.equal(second.coversRange(10, 20), true, 'filled ranges still resume');
    assert.equal(second.getTailOffset(), 777, 'and so does the read position');
    fs.rmSync(dir, { recursive: true, force: true });
});
