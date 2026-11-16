# Data dictionary

## Conventions

| Aspect | Rule |
| --- | --- |
| Timestamps | ISO 8601 UTC with microseconds and `Z` in SQLite/CSV (`2023-12-29T20:59:57.157280Z`); `timestamp[us, tz=UTC]` in Parquet; aware `datetime` in Python. |
| Prices | US **dollars per contract**, exact decimal text as sent by Kalshi (e.g. `0.4200`). `Decimal` in Python, `decimal128(38, s)` in Parquet. |
| Quantities | **Contracts**, exact decimal text (fractional allowed, e.g. `34.46`). |
| NULL | "Not supplied by the API". Never replaced by zero; never forward-filled. Empty field in CSV. |
| Booleans | 0/1 in SQLite, `true/false` in Parquet. |
| `tier` | `live` or `historical`: the API partition that served the row. |
| Provenance | `source_request_id` → `requests.request_id` → `requests.raw_path` (raw response file, SHA-256 in `requests.sha256`). `retrieved_at` = local receipt time of that response. |

## `trades` (key: `trade_id`)

| Column | Meaning |
| --- | --- |
| `trade_id` | Kalshi's unique trade identifier (primary key). |
| `ticker` | Market ticker. |
| `created_time` | Execution time reported by Kalshi (event time). |
| `count` | Contracts traded (`count_fp`). |
| `yes_price` | YES price of the trade, dollars (`yes_price_dollars`). |
| `no_price` | NO price, dollars (`no_price_dollars`); NULL if absent/unparseable (warning recorded). |
| `taker_outcome_side` | `yes`/`no`: outcome the taker became exposed to (buy-yes or sell-no → `yes`). |
| `taker_book_side` | `bid`/`ask`: same information in book vocabulary (bid ≡ yes). |
| `taker_side_deprecated` | Kalshi's deprecated `taker_side`, kept for comparison. |
| `is_block_trade` | True for off-book block trades. |
| `tier`, `retrieved_at`, `source_request_id`, `parser_version`, `content_hash` | Provenance. |

Conflict policy: trades are immutable. If a later download returns the same `trade_id`
with different content, the first stored version is kept and a `source_conflict` warning
(with both request ids and the new raw record) is written to `validation_issues`.

## `candles` (key: `ticker, interval_minutes, end_period_ts`)

A candle covers the period ending at `end_period_ts` (Kalshi: "inclusive end of the
period"); `interval_minutes` ∈ {1, 60, 1440}. Date filters apply to the **period end**.

| Column | Meaning |
| --- | --- |
| `end_period_ts`, `end_period_time` | Period end (Unix seconds / ISO). |
| `trade_open/high/low/close` | Traded YES price OHLC in the period. **NULL if no trade happened.** |
| `trade_mean` | Mean traded price (live docs: mean; historical docs: volume-weighted). |
| `trade_previous` | Last traded price *before* the period (carried value, not an observation in the period). |
| `trade_min`, `trade_max` | Documented for live candles only (not observed so far); NULL otherwise. |
| `yes_bid_open/high/low/close` | Best YES **bid** OHLC during the period. |
| `yes_ask_open/high/low/close` | Best YES **ask** OHLC during the period. |
| `volume` | Contracts traded in the period. |
| `open_interest` | Contracts outstanding at period end. |
| `source_schema` | `live` (`*_dollars` fields) or `historical` (plain field names) response shape. |
| `period_complete` | 1 if the period had ended when retrieved; 0 = partial (still in progress). |
| `revision` | Starts at 1; incremented when a re-download returns different values. |
| Provenance columns | as above. |

The three OHLC families are deliberately separate; there is no single "price" column.
`candle_revisions` keeps every superseded version (`payload_json`, its request id and when
it was superseded). Synthetic continuity candles (`include_latest_before_start`) are never
requested.

## `markets` (key: `ticker`) and `market_snapshots`

`markets` holds the **latest retrieved** version of each market. `market_snapshots` keeps
every distinct version of the non-quote metadata (plus every observation during `poll`),
with `retrieved_at`, `tier` and the normalized payload.

| Columns | Meaning |
| --- | --- |
| `ticker`, `event_ticker` | Identifiers. The series comes from the event (`events.series_ticker`), never from ticker text. |
| `title`, `subtitle`, `yes_sub_title`, `no_sub_title` | Display text. |
| `market_type`, `status`, `result` | `status` ∈ initialized, inactive, active, closed, determined, disputed, amended, finalized; `result` ∈ yes, no, scalar, ''. |
| `strike_type`, `floor_strike`, `cap_strike`, `functional_strike`, `custom_strike` | Contract definition (range bounds as exact decimals). |
| `rules_primary`, `rules_secondary` | Contract rules text. Settlement sources are on `series`/`events`. |
| `created_time`, `open_time`, `close_time`, `expected_expiration_time`, `latest_expiration_time`, `expiration_time` (undocumented), `settlement_ts`, `occurrence_datetime`, `updated_time` | Distinct lifecycle timestamps; not merged. |
| `settlement_value_dollars`, `expiration_value`, `notional_value_dollars` | Settlement outcome/value. |
| `price_level_structure`, `price_ranges` | Price grid (JSON of `{start,end,step}`). |
| `yes_bid_dollars` … `open_interest_fp` | Quote/volume **as of `retrieved_at`** (not historical). |
| `tier`, `first_seen_at`, `retrieved_at`, `source_request_id`, `content_hash` | Provenance. |

`series` and `events` hold the latest metadata (including `settlement_sources`, category,
tags, `strike_date`, `mutually_exclusive`).

## `orderbook_snapshots` (key: `snapshot_id`) and `orderbook_levels`

| Column | Meaning |
| --- | --- |
| `requested_at`, `received_at` | Local clock when the request was sent / response received. |
| `server_date` | HTTP `Date` header (second precision) – the only server time supplied. |
| `yes_levels`, `no_levels` | Number of levels returned per side. |
| `best_yes_bid`, `best_no_bid` | Highest bid on each side (quoted). |
| `derived_yes_ask` | **Derived**: notional (1.00) − `best_no_bid`. Not returned by the API. |
| `derived_no_ask` | **Derived**: 1.00 − `best_yes_bid`. |
| `derived_yes_spread` | **Derived**: `derived_yes_ask` − `best_yes_bid`. |

`orderbook_levels`: `side` (`yes` = bid to buy YES, `no` = bid to buy NO), `rank`
(1 = best), `price` (dollars), `quantity` (contracts). Kalshi returns bids only.

## Operational tables

* `runs`: `run_id`, `kind`, `status` (running/completed/incomplete/interrupted/failed),
  `spec_json` (job, incl. resolved tickers), `settings_json`, `cutoff_json` (the
  `/historical/cutoff` body read at run start), `requests_used`, `message`.
* `requests`: one row per HTTP response used (or failed lookup): path, exact params, status,
  timings, attempts, tier, `raw_path`, `sha256`, `parser_version`, `item_count`, `error`.
* `work_units`: one row per planned window: status (pending/running/complete/skipped/failed/
  not_found/incomplete), `phase` + `cursor` checkpoint, pages, rows received/new, tiers used,
  earliest/latest observation, error, note.
* `validation_issues`: `severity` (`error` = record quarantined and not stored; `warning`),
  `code` (missing_field, malformed_field, out_of_range, price_mismatch, schema_mismatch,
  unknown_field, source_conflict, wrong_market, ...), message, `raw_json`.

## Export columns

Exports contain the table columns above plus `raw_path` (joined from `requests`). The
`orderbook` export has one row per level, repeating the snapshot summary. Each export
writes `<file>.meta.json` with filters, row count, database path and generation time.
