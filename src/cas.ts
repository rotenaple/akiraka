import fetch, { Response as FetchResponse } from 'node-fetch';

const ACQUIRE_TIMEOUT = 35_000;
const ACQUIRE_MAX_ATTEMPTS = 60;

function envStr(key: string, def: string): string {
    const v = process.env[key];
    return v && v.length > 0 ? v : def;
}

function toIso(date: Date): string {
    return date.toISOString();
}

function parseIntHeader(res: FetchResponse, names: string[]): number {
    for (const name of names) {
        const value = res.headers.get(name);
        if (value !== null) {
            const parsed = parseInt(value, 10);
            if (!isNaN(parsed)) return parsed;
        }
    }
    return 0;
}

/**
 * Read a header verbatim, for values that are not a bare integer.
 *
 * Returns '' when absent so the field can be omitted rather than sent as a
 * misleading zero.
 */
function stringHeader(res: FetchResponse, names: string[]): string {
    for (const name of names) {
        const value = res.headers.get(name);
        if (value !== null && value !== '') return value;
    }
    return '';
}

function sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

export interface CasTelemetry {
    nsRemaining: number;
    nsReset: number;
    nsRateLimit: number;
    /**
     * ratelimit-policy verbatim, e.g. "50;w=30". This is the only header that
     * states how long an allocation lasts: ratelimit-reset is the countdown to
     * the next refill, not the window length. CAS needs the policy because it
     * runs on the reported remaining rather than on a window of its own.
     */
    nsPolicy: string;
    statusCode: number;
    retryAfter: number;
}

export interface CasOptions {
    url?: string;
    applianceId?: string;
    priorityClass?: string;
}

export interface CasToken {
    token: string;
    queuedAt: Date;
    acquiredAt: Date;
}

export class CasClient {
    private url: string;
    private applianceId: string;
    private priorityClass: string;

    constructor(options: CasOptions = {}) {
        this.url = options.url || envStr('NS_CAS_URL', '');
        this.applianceId = options.applianceId || envStr('NS_CAS_APPLIANCE', 'akiraka');
        this.priorityClass = options.priorityClass || envStr('NS_CAS_CLASS', 'P3_LOW');
    }

    get enabled(): boolean {
        return this.url.length > 0;
    }

    get baseIdentifier(): string {
        return this.applianceId;
    }

    /**
     * Acquire a dispatch ticket from CAS. Retries on 409 (already active) and
     * 503 (queue full / long-poll timeout). Throws only after giving up.
     */
    async acquire(): Promise<CasToken> {
        if (!this.enabled) throw new Error('CAS is not enabled');

        const queuedAt = new Date();
        for (let attempt = 0; attempt < ACQUIRE_MAX_ATTEMPTS; attempt++) {
            try {
                const res = await fetch(`${this.url}/acquire`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        appliance_id: this.applianceId,
                        priority_class: this.priorityClass,
                        backlog: 0
                    }),
                    timeout: ACQUIRE_TIMEOUT
                });

                const acquiredAt = new Date();
                if (res.ok) {
                    const body = await res.json() as { token?: string };
                    if (body.token) return { token: body.token, queuedAt, acquiredAt };
                }

                if (res.status === 200) {
                    throw new Error('CAS acquire returned 200 without a token');
                }
                if (res.status === 409) {
                    throw new Error('CAS acquire: appliance already holds an active ticket');
                }
            } catch (err: any) {
                const isNetwork = !(err && err.status && typeof err.status === 'number');
                if (isNetwork) {
                    // Network failure to CAS - retry with backoff.
                    await sleep(Math.min(1000 * Math.pow(2, Math.min(attempt, 4)), 15_000));
                    continue;
                }
                if (err.status === 409) {
                    // Should be transient (prior ticket reaped or reported); back off briefly.
                    await sleep(1500);
                    continue;
                }
                throw err;
            }

            // 503 (queue full or timeout) - back off and retry.
            await sleep(2000);
        }

        throw new Error(`CAS acquire: exceeded ${ACQUIRE_MAX_ATTEMPTS} attempts`);
    }

    /**
     * Fire-and-forget telemetry report after an NS call completes.
     */
    async report(token: CasToken, meta: CasTelemetry, apiSentAt: Date, apiRecvAt: Date): Promise<void> {
        if (!this.enabled) return;
        try {
            await fetch(`${this.url}/report`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    token: token.token,
                    appliance_id: this.applianceId,
                    priority_class: this.priorityClass,
                    backlog: 0,
                    queued_at: toIso(token.queuedAt),
                    acquired_at: toIso(token.acquiredAt),
                    api_sent_at: toIso(apiSentAt),
                    api_recv_at: toIso(apiRecvAt),
                    ns_remaining: meta.nsRemaining,
                    ns_reset: meta.nsReset,
                    ns_rate_limit: meta.nsRateLimit,
                    ns_policy: meta.nsPolicy,
                    status_code: meta.statusCode,
                    retry_after: meta.retryAfter
                }),
                timeout: 10_000
            });
        } catch {
            // Telemetry is best-effort; never fail the NS call because we could not report.
        }
    }

    /**
     * Execute an NS API call under a CAS ticket: acquire, call, report.
     * The callback receives no args and returns the raw fetch Response.
     * A failed (throw) callback still reports a 429/error summary when possible.
     */
    async execute(call: () => Promise<FetchResponse>): Promise<FetchResponse> {
        const token = await this.acquire();
        const apiSentAt = new Date();
        let resp: FetchResponse | null = null;
        try {
            resp = await call();
            const telemetry: CasTelemetry = {
                nsRemaining: parseIntHeader(resp, ['x-ratelimit-remaining', 'ratelimit-remaining']),
                nsReset: parseIntHeader(resp, ['ratelimit-reset', 'x-ratelimit-reset']),
                nsRateLimit: parseIntHeader(resp, ['x-ratelimit-limit', 'ratelimit-limit']),
                nsPolicy: stringHeader(resp, ['x-ratelimit-policy', 'ratelimit-policy']),
                statusCode: resp.status,
                retryAfter: resp.status === 429 ? parseIntHeader(resp, ['retry-after']) : 0
            };
            await this.report(token, telemetry, apiSentAt, new Date());
            return resp;
        } catch (err: any) {
            // Report what we can (e.g. network error right after acquire) so the slot
            // is reclaimed and CAS never stalls on an un-reported ticket.
            const telemetry: CasTelemetry = {
                nsRemaining: resp ? parseIntHeader(resp, ['x-ratelimit-remaining', 'ratelimit-remaining']) : 0,
                nsReset: 0,
                nsRateLimit: 0,
                nsPolicy: resp ? stringHeader(resp, ['x-ratelimit-policy', 'ratelimit-policy']) : '',
                statusCode: resp ? resp.status : 0,
                retryAfter: 0
            };
            await this.report(token, telemetry, apiSentAt, new Date());
            throw err;
        }
    }
}