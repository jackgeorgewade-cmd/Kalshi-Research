# Kalshi Research Data Engine

A local Python package and command-line tool (`kalshi-data`) for **academic research** on
Kalshi prediction markets. It discovers series, events and markets, then downloads, stores,
inspects and exports **historical and current public market data** through Kalshi's official
REST API – with no API key.

What it is for: a dependable, auditable historical dataset (trades and candlesticks) that
you can resume, update and trace back to the exact API responses.
What it is not: a trading bot. It places no orders and reads no account data.

**Main priorities:** historical correctness, recoverability, auditability.

* Historical data is routed automatically between Kalshi's *live* and *historical* tiers
  using `GET /historical/cutoff` (trades by trade time, candles by market settlement).
* Prices and quantities stay exact (`Decimal`; decimal text in SQLite; `decimal128` in
  Parquet). Missing values stay missing; nothing is zero-filled or forward-filled.
* Every normalized row links to the raw, checksummed API response it came from.
* Downloads are checkpointed per page; interrupted runs resume; re-runs do not duplicate.
* Coverage reports keep "no data returned", "request failed", "market not found" and
  "unfinished" apart.

See [docs/api_findings.md](docs/api_findings.md) for what was verified against the API on
2026-11-16, and what was not.

---

## 1. Install

Requirements: Python 3.11+ and either [uv](https://docs.astral.sh/uv/) (recommended) or pip.

```bash
git clone <this repository> kalshi-research
cd kalshi-research
uv sync                      # creates .venv with runtime + dev dependencies (incl. pandas)
uv run kalshi-data --help
```

With pip instead:

```bash
python -m venv .venv
source .venv/bin/activate    # Windows: .venv\Scripts\activate
pip install -e ".[research]" # [research] adds pandas for DataFrame helpers
kalshi-data --help
```

Check your setup (uses at most 3 API requests):

```bash
uv run kalshi-data doctor
```

If the API check fails because `external-api.kalshi.com` is blocked on your network, use the
other documented production host for every command:

```bash
uv run kalshi-data --base-url https://api.elections.kalshi.com/trade-api/v2 doctor
```

(or set `KALSHI_DATA_BASE_URL`, or `base_url` in a config file).

## 2. Quick start

```bash
# 1. Discover series (keyword match is local; --category is the server-side filter)
uv run kalshi-data discover --query "S&P 500" --category Financials

# 2. List markets of a series or an event, including archived ones
uv run kalshi-data markets --event INXD-23DEC29
uv run kalshi-data markets --series KXINXY --include-historical

# 3. Inspect one market (current metadata + quote)
uv run kalshi-data market INXD-23DEC29-B4787

# 4. Download history (UTC, half-open [start, end); dates mean midnight UTC)
uv run kalshi-data fetch trades  --ticker INXD-23DEC29-B4787 --start 2023-12-20 --end 2024-01-10
uv run kalshi-data fetch candles --ticker INXD-23DEC29-B4787 --start 2023-12-20 --end 2024-01-10 --interval 60

# 5. What was retrieved / what failed / what is uncertain
uv run kalshi-data runs
uv run kalshi-data coverage --ticker INXD-23DEC29-B4787

# 6. Export tidy files
uv run kalshi-data export --dataset candles --ticker INXD-23DEC29-B4787 --format parquet --output exports/candles.parquet
uv run kalshi-data export --dataset trades  --ticker INXD-23DEC29-B4787 --format csv     --output exports/trades.csv
```

The tickers above were real markets on 2026-11-16 (an S&P 500 range event from December
2023, now in Kalshi's historical tier). Use `discover`/`markets` to find your own.

Then in Python (no network):

```python
from kalshi_data import LocalStore

with LocalStore("data") as db:
    trades = db.to_dataframe("trades", tickers=["INXD-23DEC29-B4787"])            # exact Decimals
    candles = db.to_dataframe("candles", tickers=["INXD-23DEC29-B4787"], numeric=True)  # floats (lossy, explicit)
```

A full runnable tour (discover → select a real market → download → coverage → export →
pandas) is in [examples/research_walkthrough.py](examples/research_walkthrough.py).

## 3. Commands

Global options (before the command): `--config FILE`, `--data-dir DIR`, `--base-url URL`,
`--min-interval SECONDS`, `-v/--verbose`. Every command has `--help`.

| Command | What it does | Network |
| --- | --- | --- |
| `doctor [--offline] [--json]` | Checks Python, dependencies, config sources, storage, raw-file consistency, the writer lock, and API reachability (≤ 3 requests). | ≤ 3 requests |
| `discover [--query Q] [--category C] [--tags T] [--offline] [--json]` | Fetches `/series` (server-side `category`/`tags` only), caches it, filters by keywords **locally**. | 1 request |
| `markets --series S \| --event E [--include-historical/--live-only] [--json]` | Lists markets from both tiers merged by ticker; caches metadata. Refuses listings > `--max-markets`. | paged |
| `market TICKER [--json]` | Current metadata + quote (live tier, else historical). | 2–3 requests |
| `fetch trades --ticker T --start S --end E [--budget N] [--dry-run]` | Downloads trades in `[S, E)`; splits at the trade cutoff. | paged |
| `fetch candles --ticker T --start S --end E [--interval 1\|60\|1440] [--budget N] [--dry-run]` | Downloads candles whose period **end** is in `[S, E)`; ≤ 5,000 periods/request. | 1 per window |
| `fetch orderbook --ticker T [--depth N] [--json]` | One current order-book snapshot (bids only) with derived, labelled asks. | 4 requests |
| `collect --config FILE [--budget N] [--dry-run]` | Runs the `[job]` in a TOML file (tickers/series/events, range, datasets, interval). | paged |
| `poll --ticker T [--ticker T2] --interval SEC (--duration SEC \| --samples N) [--depth N]` | Bounded forward collection of book + quote snapshots; Ctrl-C stops cleanly. | per sample |
| `resume RUN_ID [--budget N]` | Continues an interrupted/incomplete run from its checkpoints. | as needed |
| `runs [--json]` | Recent runs. | none |
| `status RUN_ID [--json]` | Rows, requests, retries, unit states, unfinished work, validation issues. | none |
| `coverage --ticker T [--run RUN_ID] [--json]` | Requested vs covered windows, rows, tiers, empty/failed work, gaps. | none |
| `issues [--run RUN_ID] [--json]` | Validation errors (quarantined records) and warnings. | none |
| `export --dataset trades\|candles\|markets\|orderbook --output PATH [--format csv\|parquet] [--ticker T] [--start S] [--end E] [--interval N]` | Tidy export from SQLite with provenance columns and a `.meta.json` sidecar. | none |
| `reconcile [--delete]` | Lists (or deletes) raw files left without a DB record by a crash. | none |

**Exit codes:** `0` success · `1` run incomplete/failed (resumable) · `2` invalid input/config
(detected before any request) · `3` API error (e.g. not found) · `4` data directory locked by
another writer · `5` network failure after retries · `6` request budget spent · `130` interrupted.

## 4. Where things are stored

Everything lives under the data directory (default `./data`, change with `--data-dir`,
`KALSHI_DATA_DIR` or `[storage].data_dir`):

| Path | Contents |
| --- | --- |
| `data/kalshi.sqlite` | The single authoritative store: metadata, trades, candles, order books, runs, requests, work units, validation issues. |
| `data/raw/<run_id>/<request_id>.json.gz` | Raw API responses with request metadata and a SHA-256 of the exact body. |
| `data/.writer.lock` | Present only while a command is writing. |
| `exports/` (your choice) | CSV/Parquet exports plus `<file>.meta.json`. |

Reports (`status`, `coverage`, `issues`) are computed from the database on demand; add
`--json` to save them. Logs go to the terminal (stderr); use `-v` for debug logging.
`data/`, `exports/` and `logs/` are git-ignored.

## 5. Resume and update

* **Resume:** if a run is interrupted (Ctrl-C, crash, network loss) or stops because of
  `--budget`, it is recorded as `interrupted`/`incomplete`. `kalshi-data runs` shows it;
  `kalshi-data resume RUN_ID` continues from the last committed page.
* **Update:** re-run the same `fetch`/`collect` with a later `--end` (`--end now` works).
  Windows that were fetched *at least `overlap_seconds` (default 1 h) after they ended* are
  skipped as final. For the still-open recent window, only data from
  *(previous completion time − overlap)* onward is fetched again, so late trades and revised
  recent candles are picked up without re-downloading the whole window. Trades are de-duplicated by `trade_id`; changed candles are updated and the
  previous version is kept in `candle_revisions`.

## 6. Configuration

Precedence (highest first): command-line flags → environment variables → TOML file →
defaults. See the fully commented [configs/example.toml](configs/example.toml).

| Environment variable | Setting |
| --- | --- |
| `KALSHI_DATA_CONFIG` | path of the TOML file |
| `KALSHI_DATA_BASE_URL` | `api.base_url` |
| `KALSHI_DATA_MIN_REQUEST_INTERVAL` | `api.min_request_interval` (seconds; default 0.5 – our setting, not a Kalshi limit) |
| `KALSHI_DATA_MAX_RETRIES` | `api.max_retries` |
| `KALSHI_DATA_DIR` | `storage.data_dir` |
| `KALSHI_DATA_RAW_RETENTION` | `storage.raw_retention` (`keep`/`none`) |
| `KALSHI_DATA_REQUEST_BUDGET` | `collection.request_budget` |

No credentials are needed or read.

## 7. Development

```bash
uv run pytest                    # offline, deterministic (synthetic fixtures + mocked HTTP)
uv run ruff check src tests examples
uv run mypy                      # strict type checking of src/
KALSHI_LIVE_TESTS=1 uv run pytest tests/live -s   # opt-in live smoke test (<= 60 requests)
```

## 8. Limitations (read before publishing results)

* Kalshi makes no completeness promise for its archive and neither does this tool. Coverage
  reports say which windows were *requested and completed*, not that history is complete.
* Market metadata is a snapshot as of retrieval time; it is not proof of what was visible
  historically.
* Order books are only available as **current** snapshots. Polling collects snapshots going
  forward and cannot reconstruct books from before collection started or capture updates
  between polls.
* Derived asks/midpoints are labelled `derived_*`; they are not quotes and not executable
  prices.
* Unauthenticated rate limits are unpublished; large downloads take time at the default
  pacing.
* Authentication (API keys) is not implemented because no endpoint used here requires it.

More: [docs/research_usage.md](docs/research_usage.md),
[docs/troubleshooting.md](docs/troubleshooting.md),
[docs/data_dictionary.md](docs/data_dictionary.md),
[docs/architecture.md](docs/architecture.md),
[docs/implementation_status.md](docs/implementation_status.md).

No license file is included; choose one yourself if you intend to share the code.
