// Canonical Akari event structure
export interface AkariEvent {
    event: number;
    time: number;
    actor?: string;
    receptor?: string;
    origin?: string;
    destination?: string;
    category: string;
    data: string[];
}

// Enriched Akari event: exact Akari schema with optional law resolution fields
export interface EnrichedAkariEvent extends AkariEvent {
    law_issue_id?: number | null;
    law_option?: number | null;
}
