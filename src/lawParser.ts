import * as fs from 'fs';
import * as path from 'path';
import * as he from 'he';
import fetch from 'node-fetch';
import * as cheerio from 'cheerio';
import { EventEmitter } from 'events';
import { AkariEvent, EnrichedAkariEvent } from './types';

const ISSUES_URL = 'http://www.mwq.dds.nl/ns/results/issues.html';
const REFRESH_INTERVAL = 86_400_000; // 24 hours

export class LawParser extends EventEmitter {
    private issueResultMap = new Map<string, [number, number]>();
    private staticLookup = new Map<string, [number, number]>();
    private regexPatterns: Array<{ regex: RegExp; issueId: number; option: number }> = [];
    private lastUpdate: number = 0;
    private cachePath: string;

    constructor(cacheFilePath?: string) {
        super();
        this.cachePath = cacheFilePath || this.resolveCachePath();
        this.loadCacheSync();
    }

    private resolveCachePath(): string {
        if (process.env.CACHE_FILE && fs.existsSync(process.env.CACHE_FILE)) {
            return process.env.CACHE_FILE;
        }
        const candidates = [
            path.resolve('./data/issues_cache.txt'),
            path.resolve(__dirname, '../data/issues_cache.txt'),
            path.resolve(__dirname, '../../data/issues_cache.txt'),
            path.resolve('/data/issues_cache.txt')
        ];
        for (const candidate of candidates) {
            if (fs.existsSync(candidate)) return candidate;
        }
        return candidates[0];
    }

    public loadCacheSync(): boolean {
        if (!fs.existsSync(this.cachePath)) {
            return false;
        }
        try {
            const content = fs.readFileSync(this.cachePath, 'utf-8');
            const newMap = new Map<string, [number, number]>();
            const lines = content.split('\n');
            for (const line of lines) {
                const trimmed = line.trim();
                if (!trimmed || trimmed.startsWith('#')) continue;
                const parts = trimmed.split('|');
                if (parts.length !== 3) continue;
                const issueId = parseInt(parts[0], 10);
                const option = parseInt(parts[1], 10);
                let resultLine = parts[2].toLowerCase().trim();
                resultLine = resultLine.replace(/\s*\[[^\]]+\]\s*/g, '@@bracket@@').replace(/\s+/g, ' ').trim();

                if (!isNaN(issueId) && !isNaN(option) && !newMap.has(resultLine)) {
                    newMap.set(resultLine, [issueId, option]);
                }
            }
            this.issueResultMap = newMap;
            this.rebuildLookups();
            return true;
        } catch (err: any) {
            console.warn(`LawParser: Failed to load cache from ${this.cachePath}:`, err.message);
            return false;
        }
    }

    public rebuildLookups(): void {
        this.staticLookup.clear();
        this.regexPatterns = [];

        for (const [patternText, [issueId, option]] of this.issueResultMap.entries()) {
            if (!patternText.includes('@@') && !patternText.includes('<<BRACKET>>')) {
                if (!this.staticLookup.has(patternText)) {
                    this.staticLookup.set(patternText, [issueId, option]);
                }
            } else {
                try {
                    this.regexPatterns.push({
                        regex: new RegExp(this.patternToRegex(patternText)),
                        issueId,
                        option
                    });
                } catch { /* skip malformed patterns */ }
            }
        }
    }

    public async fetchAndUpdate(): Promise<void> {
        this.lastUpdate = Date.now();
        try {
            const response = await fetch(ISSUES_URL, {
                // @ts-ignore
                rejectUnauthorized: false
            });
            const htmlContent = await response.text();
            const $ = cheerio.load(htmlContent);
            const textContent = $('body').text();
            const newMap = new Map<string, [number, number]>();
            const lines = textContent.split('\n');
            let currentIssueId: number | null = null;

            for (const line of lines) {
                const trimmed = line.trim();
                if (!trimmed) continue;
                if (trimmed.startsWith('#')) {
                    const spacePos = trimmed.indexOf(' ');
                    if (spacePos > 0) {
                        const issueId = parseInt(trimmed.substring(1, spacePos), 10);
                        if (!isNaN(issueId)) currentIssueId = issueId;
                    }
                    continue;
                }
                if (currentIssueId !== null) {
                    const dotPos = trimmed.indexOf('.');
                    if (dotPos > 0) {
                        const option = parseInt(trimmed.substring(0, dotPos).trim(), 10);
                        if (!isNaN(option)) {
                            let resultText = trimmed.substring(dotPos + 1).trim().toLowerCase();
                            resultText = resultText.replace(/\s*\[[^\]]+\]\s*/g, '@@bracket@@').replace(/\s+/g, ' ').trim();
                            if (resultText && !newMap.has(resultText)) {
                                newMap.set(resultText, [currentIssueId, option]);
                            }
                        }
                    }
                }
            }

            if (newMap.size === 0) throw new Error('Parsed 0 issues from website');

            this.issueResultMap = newMap;
            this.saveToCache(newMap);
            this.rebuildLookups();
        } catch (error: any) {
            console.error('LawParser: Failed to fetch updates:', error.message);
        }
    }

    private saveToCache(map: Map<string, [number, number]>): void {
        try {
            const dir = path.dirname(this.cachePath);
            if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

            let content = `# Auto-generated cache from ${ISSUES_URL}\n# Format: issue_id|option|result_line\n#\n`;
            const entries = Array.from(map.entries()).sort((a, b) => {
                const [id1, opt1] = a[1], [id2, opt2] = b[1];
                return id1 !== id2 ? id1 - id2 : opt1 - opt2;
            });

            for (const [resultLine, [issueId, option]] of entries) {
                content += `${issueId}|${option}|${resultLine}\n`;
            }

            fs.writeFileSync(this.cachePath, content);
        } catch (err: any) {
            console.warn('LawParser: Failed to save cache:', err.message);
        }
    }

    public refreshIfNeeded(): void {
        if (this.lastUpdate > 0 && (Date.now() - this.lastUpdate) > REFRESH_INTERVAL) {
            this.fetchAndUpdate().catch(() => {});
        }
    }

    /**
     * Parses raw happening data text into { issueId, option }
     */
    public parseText(dataText: string): { issueId: number; option: number } | null {
        this.refreshIfNeeded();
        const resultText = this.extractResultLine(dataText);
        const normalizedLog = this.normalizeResultText(resultText);

        const staticMatch = this.staticLookup.get(normalizedLog);
        if (staticMatch) return { issueId: staticMatch[0], option: staticMatch[1] };

        for (const pattern of this.regexPatterns) {
            if (pattern.regex.test(normalizedLog)) {
                return { issueId: pattern.issueId, option: pattern.option };
            }
        }
        return null;
    }

    /**
     * Enriches an Akari event with law_issue_id and law_option if it is a law event.
     * Non-law events are returned unmodified.
     */
    public enrichEvent(event: AkariEvent): EnrichedAkariEvent {
        if (event.category !== 'law' || !event.data || event.data.length === 0) {
            return event;
        }

        const parsed = this.parseText(event.data[0]);
        if (parsed) {
            return {
                ...event,
                law_issue_id: parsed.issueId,
                law_option: parsed.option
            };
        }

        return {
            ...event,
            law_issue_id: null,
            law_option: null
        };
    }

    /**
     * Parses a single JSON line. If it's a law event, injects law_issue_id and law_option.
     * If not a law event or invalid JSON, returns the original line byte-for-byte.
     */
    public enrichLine(line: string): string {
        const trimmed = line.trim();
        if (!trimmed) return line;

        // Quick check before JSON parsing to avoid overhead on non-law lines
        if (!trimmed.includes('"category":"law"') && !trimmed.includes('"category": "law"')) {
            return line;
        }

        try {
            const event = JSON.parse(trimmed) as AkariEvent;
            const enriched = this.enrichEvent(event);
            return JSON.stringify(enriched);
        } catch {
            return line;
        }
    }

    private patternToRegex(pattern: string): string {
        const BR_TOKEN = '<<BRACKET>>';
        let withToken = pattern.replace(/\[[^\]]+\]/g, BR_TOKEN);
        let escaped = withToken.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        escaped = escaped.replace(new RegExp(BR_TOKEN.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'g'), '(?:\\s*.*?\\s*)?');
        escaped = escaped.replace(/\s*@@bracket@@\s*/g, '(?:\\s*.*?\\s*)?');
        escaped = escaped.replace(/@@[^@]+@@/g, '.*');
        return `^${escaped}$`;
    }

    private extractResultLine(happeningText: string): string {
        const regex = /following new legislation in [^,]+,\s*(.+)/i;
        const match = regex.exec(happeningText);
        if (match && match[1]) return match[1].trim();
        return happeningText.trim();
    }

    private normalizeResultText(text: string): string {
        let decoded = he.decode(text);
        decoded = decoded.replace(/\s*\[[^\]]+\]\s*/g, '@@bracket@@');
        return decoded.toLowerCase().trim().split(/\s+/).join(' ');
    }
}