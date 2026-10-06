/**
 * Pacing for direct NationStates API calls, for collectors not running ns-cas.
 *
 * A bare fetch discovers the limit by being throttled, and being throttled against
 * NS delays everyone. `ratelimit-policy` states the allowance and the window
 * outright ("50;w=30"), which is enough to pace ahead of the limit rather than
 * after it and never spend the last few - a request made with nothing left is the
 * one that gets a 429.
 *
 * No shared state between processes: coordinating that is what ns-cas is for.
 */

/** Requests per window, as stated by ratelimit-policy, e.g. "50;w=30". */
export interface RatePolicy {
    limit: number;
    windowMs: number;
}

/**
 * Parse ratelimit-policy.
 *
 * Returns null for anything unrecognised rather than guessing: the value does not
 * matter to pacing, which starts from EXPECTED_POLICY, so this exists to notice a
 * change. `ratelimit-reset` is deliberately not read as a window - it is the
 * countdown to the next refill, and treating it as one makes a client burst at the
 * start of every window.
 */
export function parseRatePolicy(header: string | null | undefined): RatePolicy | null {
    if (!header) return null;
    const limitMatch = /(\d+)\s*;\s*w\s*=\s*(\d+)/i.exec(header);
    if (!limitMatch) return null;
    const limit = Number(limitMatch[1]);
    const windowSeconds = Number(limitMatch[2]);
    if (!Number.isFinite(limit) || limit <= 0) return null;
    if (!Number.isFinite(windowSeconds) || windowSeconds <= 0) return null;
    return { limit, windowMs: windowSeconds * 1000 };
}

/**
 * NationStates' rate limit: 50 requests per 30 seconds.
 *
 * The pacer starts from this and needs no header to work, so the header is a
 * canary rather than a dependency: if it disagrees, the new value is adopted and
 * said out loud.
 */
export const EXPECTED_POLICY: RatePolicy = { limit: 50, windowMs: 30_000 };

/**
 * Requests to keep in hand rather than spend.
 *
 * A refill is all-or-nothing, so spending the last request before the window
 * rolls means the next one has to wait for the refill. Holding a couple back is
 * what turns "usually fine" into "does not get throttled".
 */
const RESERVE = 2;

const MAX_PENALTY_MULTIPLIER = 16;
const PENALTY_RECOVERY_MS = 60_000;

export interface NsPacerOptions {
    /** Injected for tests; defaults to the real clock. */
    now?: () => number;
    /** Injected for tests; defaults to a real timer. */
    sleep?: (ms: number) => Promise<void>;
    /** Skip pacing entirely. Only for tests that are not about pacing. */
    disabled?: boolean;
}

export class NsPacer {
    private now: () => number;
    private sleep: (ms: number) => Promise<void>;
    private disabled: boolean;

    private policy: RatePolicy = { ...EXPECTED_POLICY };
    private remaining: number = EXPECTED_POLICY.limit;
    /** When the allowance refills, from ratelimit-reset. 0 when unknown. */
    private resetAt = 0;
    /** Earliest time the next request may go out. */
    private nextAllowedAt = 0;
    /** Multiplier applied to the minimum interval after a 429. */
    private penalty = 1;
    private penaltySince = 0;

    constructor(options: NsPacerOptions = {}) {
        this.now = options.now ?? (() => Date.now());
        this.sleep = options.sleep ?? (ms => new Promise(resolve => setTimeout(resolve, ms)));
        this.disabled = options.disabled ?? false;
    }

    /** What the client would be doing, for a log line or a status endpoint. */
    describe(): string {
        if (this.disabled) return 'pacing disabled';
        const spacing = Math.round(this.minimumIntervalMs());
        const penalty = this.penalty > 1 ? `, penalty x${this.penalty}` : '';
        return `${this.policy.limit} per ${Math.round(this.policy.windowMs / 1000)}s` +
            `, one request per ${spacing}ms${penalty}`;
    }

    /**
     * Even spacing across the window.
     *
     * Not a burst allowance: dividing the window by the limit is the rate the
     * limit is expressed at, and staying at or under it is what avoids a 429.
     */
    private minimumIntervalMs(): number {
        const base = this.policy.windowMs / Math.max(1, this.policy.limit);
        return Math.max(50, base * this.penalty);
    }

    private recoverPenalty(now: number): void {
        if (this.penalty === 1) return;
        if (now - this.penaltySince < PENALTY_RECOVERY_MS) return;
        this.penalty = 1;
    }

    /**
     * Read the limit from a response and schedule the next request.
     *
     * Called after every response, success or not, because the headers describe
     * the account's state rather than the outcome of one call.
     */
    observe(headers: Headers, status: number): void {
        if (this.disabled) return;
        const now = this.now();

        const policy = parseRatePolicy(headers.get('ratelimit-policy'));
        if (policy && (policy.limit !== EXPECTED_POLICY.limit || policy.windowMs !== EXPECTED_POLICY.windowMs)) {
            // Should not happen, and the whole reason this is checked is that we
            // would otherwise keep pacing to a remembered limit while the server
            // enforces a different one. Adopt what it says and say so.
            console.warn(
                `[ns] ratelimit-policy is now ${policy.limit};w=${policy.windowMs / 1000}, ` +
                `expected ${EXPECTED_POLICY.limit};w=${EXPECTED_POLICY.windowMs / 1000}. Pacing to the new value.`
            );
            this.policy = policy;
        }

        const remainingRaw = headers.get('ratelimit-remaining');
        if (remainingRaw !== null && remainingRaw !== '') {
            const parsed = Number(remainingRaw);
            if (Number.isFinite(parsed) && parsed >= 0) this.remaining = parsed;
        } else {
            // No allowance header. Assume nothing has been spent rather than
            // assuming the allowance is intact.
            this.remaining = EXPECTED_POLICY.limit;
        }

        const resetRaw = headers.get('ratelimit-reset');
        if (resetRaw !== null && resetRaw !== '') {
            const seconds = Number(resetRaw);
            if (Number.isFinite(seconds) && seconds >= 0) {
                // ratelimit-reset is a countdown, so convert to an instant once.
                this.resetAt = now + seconds * 1000;
            }
        }

        if (status === 429) {
            const retryAfter = Number(headers.get('retry-after'));
            const waitMs = Number.isFinite(retryAfter) && retryAfter > 0
                ? retryAfter * 1000 + 2000
                : 30_000;
            this.penalise(now, waitMs);
        }
    }

    private penalise(now: number, waitMs: number): void {
        // Halve the allowance rather than jump straight to a long pause: one 429
        // is often another consumer's doing, and doubling straight to a 16x
        // slowdown would starve a collector that did nothing wrong.
        this.penalty = Math.min(MAX_PENALTY_MULTIPLIER, this.penalty * 2);
        this.penaltySince = now;
        this.nextAllowedAt = Math.max(this.nextAllowedAt, now + waitMs);
    }

    /**
     * Wait until a request may go out, then account for it.
     *
     * Two reasons to wait: the even spacing between requests, and the case where
     * the allowance is spent and only a refill will help.
     */
    async before(): Promise<void> {
        if (this.disabled) return;
        let waited = 0;
        // Bounded: a mis-read header should not park the collector indefinitely.
        for (let guard = 0; guard < 10; guard++) {
            const now = this.now();
            this.recoverPenalty(now);

            let wait = Math.max(0, this.nextAllowedAt - now);

            const spacing = this.minimumIntervalMs();
            if (this.lastRequestAt) wait = Math.max(wait, this.lastRequestAt + spacing - now);

            if (this.remaining <= RESERVE && this.resetAt > now) {
                wait = Math.max(wait, this.resetAt - now);
            }

            if (wait <= 0) {
                this.lastRequestAt = now;
                // Spending down to the reserve locally keeps the pacing decision
                // in one place; the headers correct it on the next response.
                if (this.remaining > 0) this.remaining--;
                return;
            }

            // Cap a single sleep so a long refill wait stays interruptible and
            // the loop re-reads the clock rather than trusting one long sleep.
            const slice = Math.min(wait, 5000);
            if (waited + slice > 120_000) {
                // Give up on waiting and let the request try; NS is the authority
                // and a 429 with retry-after is handled above.
                this.lastRequestAt = now;
                return;
            }
            await this.sleep(slice);
            waited += slice;
        }
    }

    private lastRequestAt = 0;
}
