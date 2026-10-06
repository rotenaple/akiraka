# akiraka

> *Building upon [Merethin's Akari](https://github.com/Merethin/Akari) ([light](https://en.wikipedia.org/wiki/Akari_(satellite))), named after the Japanese satellite; now there is Akiraka ([obvious](https://en.wiktionary.org/wiki/%E6%98%8E%E3%82%89%E3%81%8B)), as in, it is obviously time for a punnily-named derivative that makes what Akari shines light on, obvious.*

Collects the NationStates happenings stream, enriches it, stores it in SQLite, and
serves it over HTTP.

Akari collects happenings over SSE and writes them as JSONL. Akiraka runs alongside
it, resolves each event, and stores the results for querying.

## What it does

**Enriches law events.** Akari records a law decision as status text, such as
`"following new legislation in @@nation@@, ..."`. Akiraka matches that text against
about 6,800 known issue outcomes and adds the issue and option ids:

```json
{"event":581234,"time":1738540800,"category":"law","actor":"testlandia",
 "origin":"the_pacific","data":["..."],"law_issue_id":7,"law_option":1}
```

Non-law events pass through unchanged.

**Fills gaps.** Akari detects missed events but does not retrieve them. Akiraka fetches
missing ranges from the NationStates API and replays them through a short-lived Akari.
This parses recovered events the same way as live events. The API retains roughly a
week of events; older gaps are reported as skipped.

**Stores all events in SQLite**, keyed by event id. This avoids duplicate rows when gap
filling fetches an existing range or a replaced log is read from the start. It also
keeps every category and uses `ORDER BY event` for consistent ordering, without a
separate cursor to maintain.

**Serves read-only queries.** Readers can request specific categories and fields
instead of transferring the whole store.

## Setup

### Docker (recommended)

Akari, Akiraka and the query service run in one container.

```bash
echo 'NS_USER_AGENT=YourNation' >> .env
docker compose up -d
```

`NS_USER_AGENT` is **required**. Set it to your nation name; NationStates rejects
anonymous callers. All other settings have defaults.

Compose pulls the image from GHCR, so nothing is compiled. To build from source
instead - to run a change, or to update Akari - build the Akari base image first,
then the stack:

```bash
docker build -f Dockerfile.akari -t akiraka-akari:latest .   # builds Akari (Rust)
docker compose up -d --build
```

That starts one container, `akiraka`, which:

- tails Akari, enriches, fills gaps and writes `/data/events.db`;
- serves read-only HTTP queries over that store on port 8084.

The query service runs inside it as a separate process rather than a second
container: a bad query cannot stop collection, and the reader restarts without
bouncing ingest.

Data lives in `./data`. Akari writes `akari_events.jsonl`, rotating and compressing
older segments; `AKARI_THRESHOLD` and `AKARI_MAXFILES` bound that. Akiraka follows
the current file and writes `events.db`, the store that queries read. It also writes
a cessation/revival subset to `events.enriched.cte.jsonl` for reading those events
straight off disk; the same rows are in the store.

### Configuration

Set these in `.env` (see `.env.example`).

| Variable | Default | Purpose |
| --- | --- | --- |
| `NS_USER_AGENT` | - | **Required.** Your nation name |
| `NS_CAS_URL` | off | Optional shared-quota server; unset uses the built-in pacer |
| `AKIRAKA_QUERY_TOKEN` | off | Bearer token for the query service |
| `QUERY_BIND` | `127.0.0.1` | Host address the query port publishes on |
| `QUERY_PORT` | `8084` | Query port |
| `PUID` / `PGID` | `0` / `0` | Run as this user, for hosts that need it (Unraid) |
| `GAPFILL_MAX_GAP` | `1000` | Largest gap filled automatically |
| `GAPFILL_SCAN_START` | off | Scan for existing gaps on startup |
| `GAPFILL_SCAN_INTERVAL` | `0` | Minutes between background gap scans (0 = off) |
| `AKARI_THRESHOLD` | `500` | Rotate Akari's log every n x 1000 lines (minimum 100) |
| `AKARI_MAXFILES` | `168` | Rotated log segments to keep; older ones are deleted |

Akari rotates its own log. It keeps `maxfiles` segments and deletes the oldest, so
this is the one lever on the raw log's disk use. Akari's built-in defaults are a
500-line segment and no limit, which is how a log directory fills up; the values
above are in `config/akari.toml`, and the variables override them. Keep enough to
cover your rebuild horizon: the raw log is what re-enrichment replays against.

`AKARI_THRESHOLD` is floored at 100. Akiraka notices a rotation by the file getting
smaller, so a segment small enough to be refilled inside its half-second poll would
have its start skipped. 100 means 100,000 lines per segment, far above what the
stream can produce in that time.

The two files under `config/` serve different purposes.

**Edit `config/endpoints.json` to define query endpoints.** The container mounts this
file, so changes need the container restarted, not the image rebuilt. It includes one
endpoint, `all`; see *Reading the data* below.

**`config/akari.toml` is built into the image.** It connects Akari's output file to
the file Akiraka follows and sets the log rotation. A host copy has no effect; the
repository version is there for reference, and `AKARI_THRESHOLD` / `AKARI_MAXFILES`
override its rotation values.

The entrypoint checks that both programs use the same path and refuses to start if
they do not. Without this check, Akiraka would wait for its input file while the
container reported healthy, even though the store was not growing.

Gap filling follows NationStates' published limit of 50 requests per 30 seconds. The
pacer reads the limit from each response. If you run multiple collectors,
`NS_CAS_URL` coordinates their quota; a single collector does not need it.

### Reading the data

Endpoints in `config/endpoints.json` control which stored data a query returns; they
do not affect what the store retains. The default `all` endpoint returns the whole
store:

```bash
curl 'http://localhost:8084/events/all?after_event=0&limit=100'
```

To return a subset, such as one category or a few fields, add an endpoint to the file
and restart the container. The file is mounted from the host, so you do not need to
rebuild the image:

```json
"moves": {
  "categories": ["move"],
  "fields": ["event", "time", "actor", "origin", "destination"]
}
```

For pagination, pass `after_event=<highest event id received>`; the value is exclusive.
The `x-akiraka-last-event` response header returns the same value. `GET /endpoints`
lists the configured endpoints. `GET /health` uses an index seek and does not scan
the store.

Set `AKIRAKA_QUERY_TOKEN` before exposing the port beyond loopback. The token can
help catch a misconfigured bind, but it is not a security boundary: the data is public
and the token is stored in plaintext in `.env`. Access depends on the bind address
and network. The service has no rate limiting.

You can also read the store directly as a SQLite file. It uses WAL, so reads are safe
while the collector runs. Do not open it for writing; the collector is its only
writer.

### Unraid

Unraid needs two additional settings.

**Ownership.** Containers run as root by default, which leaves the store and its WAL
files owned by root. Set `PUID` and `PGID` to your Unraid uid and gid, and set
`QUERY_USER` to match. The entrypoint changes ownership of `/data` before dropping
to that identity.

**Keep the store off `/mnt/user`.** This FUSE-backed share does not support SQLite's
locking reliably. Use an appdata or disk path such as
`/mnt/disk1/appdata/akiraka/data`.

## Standalone CLI

```bash
# Pipe mode
cat akari.jsonl | akiraka > enriched.jsonl

# Batch a file
akiraka --in events.jsonl --out enriched.jsonl

# Tail alongside Akari, writing the store
akiraka --tail /data/akari_events.jsonl --store /data/events.db

# Fill gaps in an existing file, then exit
akiraka --gapfill --fill-all --in events.jsonl --store /data/events.db
```

| Flag | |
| --- | --- |
| `--tail <file>` | Follow a file Akari is appending to |
| `--in <file>` | Process a file and exit |
| `--out <file>` | Also write enriched JSONL (stdout in pipe mode) |
| `--store <file>` | SQLite store to write |
| `--cte-out <file>` | Also write cessation/revival events to this file |
| `--gapfill` | Scan for gaps and fill them |
| `--fill-all` | Fill every gap, not just those under `--max-gap` |
| `--max-gap <n>` | Largest gap to fill automatically (default 1000) |
| `--chunk-size <n>` | Events per chunk while filling (default 1000) |
| `--no-write-back` | Do not append recovered events back to the Akari file |
| `--cache <file>` | Path to `issues_cache.txt` |
| `--akari-bin <exe>` | Akari binary used to reparse (default `akari`) |
| `--scan-interval <n>` | Minutes between background gap scans (0 = off) |

By default, recovered events are appended to the Akari log so later scans no longer
report the gap. Appending is safe while Akari is running, but the events go at the end
of the file rather than in chronological order. Events whose ids are already present
are skipped. Downstream consumers should sort by `event` id.

## Other tools

```bash
npm run store:stats -- data/events.db      # what the store holds, and its gaps
npm run ingest:csv  -- --in recovered.csv --store data/events.db
```

- `store:stats` reports the row count, id range, and number of gaps. Gaps older than
the API's retention window cannot be filled through the API.
- `ingest:csv` enriches a CSV supplied by another operator, which can provide events
  older than the API's retention window. It can write to the store, to JSONL, or both.
  Use `--store`, `--out`, or both. If neither is set, JSONL output defaults to
  `<input>.enriched.jsonl`. Run `npm run ingest:csv -- --help` for the CSV format.

## Library

```typescript
import { LawParser } from 'akiraka';

const parser = new LawParser();          // loads issues_cache.txt
const enriched = parser.enrichEvent(rawEvent);   // gains law_issue_id, law_option
const line = parser.enrichLine(rawJsonLine);     // same, from a JSONL line
```

## Development

```bash
npm run build
npm test                    # unit tests
npm run test:gapfill        # gap-fill end-to-end, needs Docker
NS_USER_AGENT=YourNation npm run test:gapfill:live   # against the real API
```

`npm test` covers the store, enrichment sink, argument parsing, and gap arithmetic.
The end-to-end gap-fill tests run in the image because reparsing starts the Linux Akari
binary.

The end-to-end suite tests the full recovery path: detecting a gap, paging backward
through the API, replaying events over SSE, reparsing with Akari, enriching, and
merging. It uses a real window of collected events with chunks removed and a local
API simulation. The test checks that the recovered ids exactly match the removed ids.
A weaker check that only confirmed some events returned would miss a `TIMESTAMP`/
`TIME` mismatch that wrote `time: 0` on every recovered event.
