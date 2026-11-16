# Architecture

## Data flow

```
             CLI (cli.py)  /  Python API (api.py: KalshiData, LocalStore)
                         │                                │ (read-only, no network)
                         ▼                                ▼
                 Ingestor (ingest.py)  ── plans ──►  work_units
                   │      │      │
        routing.py │      │      │ normalize.py ──► validation issues (quarantine)
   (cutoff, tiers, │      │      ▼
     windows)      │      │   Store (storage.py, SQLite)  ◄── coverage.py, export.py
                   ▼      │      ▲
           KalshiClient   │      │ one transaction per response:
          (endpoints.py)  │      │ request row + rows + checkpoint
                   │      └──► RawStore (rawstore.py): raw/<run>/<request>.json.gz
                   ▼                      (written atomically BEFORE the transaction)
             Transport (http.py)
     pacing · bounded retries · budget · typed errors
                   │
                   ▼
           Kalshi REST API (public endpoints, no auth)
```

## Modules

| Module | Responsibility |
| --- | --- |
| `config.py` | Pydantic settings, TOML job spec, precedence (flags > env > file > defaults). |
| `timeutil.py` | Aware-UTC parsing, ISO formatting, Unix seconds, month helpers. Rejects naive times. |
| `decimals.py` | Exact decimal parsing (`json.loads(parse_float=Decimal)`, no float path). |
| `errors.py` | Typed exceptions (BadRequest/Auth/NotFound/RateLimit/Server/Transport/Budget/Pagination/CursorRejected...). |
| `http.py` | One `httpx.Client` (pooled), timeouts, pacing, retries with jitter + Retry-After, request budget, `ResponseRecord` with provenance. |
| `endpoints.py` | Endpoint adapters and the `paginate()` generator (cursor rules, repeat/restart detection, page caps). |
| `routing.py` | `Cutoff`, market tier (by settlement), trade window split (by trade time), deterministic window planning. |
| `normalize.py` | Raw items → records; errors vs warnings; explicit live/historical candle adapters. |
| `rawstore.py` | Atomic gzip raw envelopes with SHA-256; orphan detection. |
| `storage.py` | SQLite schema/migrations, idempotent upserts, conflict policies, work-unit checkpoints. |
| `lock.py` | Single-writer lock file (stale-PID detection). |
| `ingest.py` | Runs: cutoff → scope resolution → planning → execution → reconciliation → status. Resume. |
| `coverage.py` | Coverage and run-status reports. |
| `export.py` | CSV/Parquet from SQLite with provenance and exact decimals. |
| `api.py` | High-level Python interface; streaming iterators; polling. |
| `cli.py` | Typer commands, progress display, exit codes. |

## Key decisions

**Synchronous client, one shared pacer.** Downloads are I/O bound but rate-limited by
Kalshi; concurrency would mostly produce 429s and complicate checkpoints. Not needed so far.

**Routing behind one interface.** Callers ask for "trades of market M in [s, e)". The
ingestor splits the window at `trades_created_ts` (historical first, then live) and picks
the candle endpoint from the market's settlement time. Callers never choose endpoints.

**Tier handoff.** The cutoff moves forward over time. Within a run:
* after reading a live trade segment the cutoff is refreshed (at most every 5 minutes), and
  if it advanced into the window, the newly archived slice is read once from the
  historical tier (bounded reconciliation); duplicates are removed by `trade_id`;
* a candle request that 404s triggers one metadata refresh and one retry on the other tier;
* other errors (400/401/403/5xx after retries) are never converted into fallbacks or into
  "no data" – the unit is marked `failed`.

**Work units and keys.** Trades are planned in UTC calendar-month windows; candles in
epoch-aligned windows of 5,000 periods (the request limit). Windows are clipped to the
market's life (creation .. settlement + 1 day). The unit key is
`dataset|ticker|interval|window_start|window_end`, deterministic for identical requests,
which enables (a) resume within a run and (b) skipping *final* windows across runs.
A window that is not final but was completed earlier with the same start (the open,
most recent window) is only re-fetched from *(earlier completion − `overlap_seconds`)*
onward (`Ingestor._incremental_start`).

**Checkpoint transaction order.** For each response: raw file written atomically → one
SQLite transaction containing the `requests` row, normalized rows, validation issues and
the work-unit cursor/phase/status. A checkpoint therefore never moves past non-durable
data. A crash between the two steps leaves an orphan raw file (`doctor` reports it;
`reconcile --delete` removes it); the page is re-fetched on resume.

**Resume.** `resume RUN_ID` reloads the stored job (including the resolved ticker list),
re-reads the cutoff, re-plans the same unit keys and executes those not `complete`.
Trade units continue from the stored `(phase, cursor)`; if routing changed the phase id no
longer matches and the unit restarts (safe: idempotent inserts). A cursor rejected with 400,
or a cursor sequence that repeats/restarts, restarts the affected phase once.

**Single store.** SQLite (WAL, `synchronous=FULL`) is authoritative; exports are always
produced from it.

**Single writer.** `data/.writer.lock` is held by commands that write. Readers do not lock.

## Database tables

`runs`, `requests`, `series`, `events`, `markets`, `market_snapshots`, `trades`, `candles`,
`candle_revisions`, `orderbook_snapshots`, `orderbook_levels`, `work_units`,
`validation_issues`. See [data_dictionary.md](data_dictionary.md).

Schema version is stored in `PRAGMA user_version` (currently 1); `Store._migrate` applies
migrations and refuses databases newer than the code.
