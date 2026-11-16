# Kalshi API findings

**Verification date:** 2026-11-16 (UTC).

Labels used below:

* **Documented** – stated in the official docs or the OpenAPI spec (`openapi.yaml`, version 3.36.0).
* **Observed** – seen in a live, unauthenticated request made on the verification date.
* **Not verified** – neither documented clearly nor observed; treat as an open question.

## How verification was done (and its limits)

* Documentation was read through a web-fetch service because `docs.kalshi.com` was blocked
  by this build environment's network proxy (HTTP 403 on CONNECT). The pages read were the
  official ones listed below; nothing was taken from tutorials or memory.
* The **recommended** production host `https://external-api.kalshi.com/trade-api/v2` was
  **also blocked by the build environment's proxy**, so it could **not** be exercised live.
  All live observations were made against `https://api.elections.kalshi.com/trade-api/v2`,
  which the docs list as an "also supported" production host serving the same API.
  The engine defaults to the recommended host (configurable with `--base-url`,
  `KALSHI_DATA_BASE_URL` or `[api].base_url`). `kalshi-data doctor` tells you whether it is
  reachable from your machine.
* Live probing was deliberately small (well under 200 requests in total).

## Official references

* Index: <https://docs.kalshi.com/llms.txt>
* API environments: <https://docs.kalshi.com/getting_started/api_environments>
* Quick start, market data: <https://docs.kalshi.com/getting_started/quick_start_market_data>
* Historical data: <https://docs.kalshi.com/getting_started/historical_data>
* Rate limits: <https://docs.kalshi.com/getting_started/rate_limits>
* Fixed-point representation: <https://docs.kalshi.com/getting_started/fixed_point_migration>
* Order book responses: <https://docs.kalshi.com/getting_started/orderbook_responses>
* Pagination: <https://docs.kalshi.com/getting_started/pagination>
* OpenAPI spec: <https://docs.kalshi.com/openapi.yaml>
* Endpoint pages, e.g. <https://docs.kalshi.com/api-reference/market/get-trades>,
  <https://docs.kalshi.com/api-reference/historical/get-historical-trades>,
  <https://docs.kalshi.com/api-reference/market/get-market-candlesticks>,
  <https://docs.kalshi.com/api-reference/historical/get-historical-market-candlesticks>,
  <https://docs.kalshi.com/api-reference/historical/get-historical-cutoff-timestamps>

## Base URL

| Host | Status |
| --- | --- |
| `https://external-api.kalshi.com/trade-api/v2` | Documented as recommended. **Not verified live** (blocked in the build environment). Default in this project. |
| `https://api.elections.kalshi.com/trade-api/v2` | Documented as "also supported". **Observed** working for every endpoint below. |

## Authentication

* **Documented:** none of the endpoints below declares a security requirement in the OpenAPI
  spec; the market-data quick start and order-book page say no authentication is required.
* **Observed:** every endpoint below returned data without credentials.
* Therefore the engine has **no authentication code** and needs no API key. Kalshi documents
  that unauthenticated requests have *separate* (unpublished) rate limits.

## Endpoint capabilities used by this project

| Endpoint | Purpose | Key parameters | Page limit | Notes |
| --- | --- | --- | --- | --- |
| `GET /historical/cutoff` | tier boundaries | – | – | Returns `market_settled_ts`, `trades_created_ts`, `orders_updated_ts`, `market_positions_last_updated_ts` (RFC 3339). Observed all = `2026-09-17T00:00:00Z` except orders `2026-11-02`. |
| `GET /series` | list series | `category` (exact, case-sensitive, matches any entry of `categories`), `tags`, `include_volume`, `min_updated_ts` | none documented | **Observed:** no cursor; all 16,479 series in one response (1,200 for `category=Financials`). No text-search parameter exists → keyword search is done locally. |
| `GET /series/{series}` | one series | – | – | |
| `GET /events` | list events | `series_ticker`, `status` (`unopened/open/closed/settled`), `with_nested_markets`, `min_close_ts`, `cursor` | 200 (documented) | Documented: events are never archived. Nested markets omit archived ones. |
| `GET /events/{event}` | one event | `with_nested_markets`, `exclude_markets` | – | Top-level `markets` omits archived markets (documented). |
| `GET /markets` | live markets | `series_ticker`, `event_ticker`, `tickers`, `status` (`unopened/open/paused/closed/settled`), timestamp filters (mutually exclusive groups), `mve_filter`, `cursor` | 1000 | Documented: archived markets absent. Sorted by creation time desc by default. |
| `GET /historical/markets` | archived markets | `series_ticker` **or** `event_ticker` **or** `tickers` (mutually exclusive), `mve_filter`, `cursor` | 1000 | Observed newest-settled first. |
| `GET /markets/{t}` / `GET /historical/markets/{t}` | one market | – | – | 404 when absent from that tier (observed). |
| `GET /markets/trades` | live trades | `ticker`, `min_ts`, `max_ts` (Unix **seconds**), `is_block_trade`, `cursor` | 1000 | Newest first (observed). Unknown ticker → 200 with empty list (documented). |
| `GET /historical/trades` | archived trades | same as live | 1000 | Newest first (observed). |
| `GET /series/{s}/markets/{t}/candlesticks` | live candles | `start_ts`, `end_ts` (seconds), `period_interval` ∈ {1, 60, 1440}, `include_latest_before_start` | ≤ 5,000 periods | Requires the market's series ticker. |
| `GET /historical/markets/{t}/candlesticks` | archived candles | `start_ts`, `end_ts`, `period_interval` ∈ {1, 60, 1440} | ≤ 5,000 periods | Different field names from live (see below). |
| `GET /markets/{t}/orderbook` | current book | `depth` 0–100 (0 = all) | – | Bids only; unknown ticker → 200 with empty arrays (documented). |
| `GET /exchange/status` | reachability probe (`doctor`) | – | – | |

## Historical routing

* **Documented:** data is split into live and historical tiers. Markets *and their
  candlesticks* that **settled** before `market_settled_ts` are only available from
  `/historical/markets…`; **trades** created before `trades_created_ts` are only available
  from `/historical/trades`. Events and series are never archived.
* **Observed:** a December 2023 market (`INXD-23DEC29-B4787`) → live `/markets/{t}` 404,
  `/historical/markets/{t}` 200, candles and trades only from historical endpoints.
* **Observed (difference from a strict reading of the docs):** markets that settled
  shortly *before* the cutoff (e.g. on 2026-09-16) were served by **both** `/markets/{t}`
  and `/historical/markets/{t}` – an overlap during the live→historical handoff. The engine
  therefore routes candles by the documented *settlement-time rule*, not by "where the
  metadata was found", and falls back to the other tier once on a 404.
* **Observed:** a still-open yearly market's trades on 2026-09-16 came from
  `/historical/trades` and those after the cutoff from `/markets/trades`, while its candles
  for the same dates came from the live candle endpoint – confirming that candles and trades
  have different routing rules.

## Pagination

* **Documented:** cursor pagination; an empty/absent cursor ends the listing; page size
  `limit` max 1000 (200 for `/events`). An undecodable cursor is **silently ignored and the
  first page is returned**.
* **Observed:** `cursor=garbage` on `/historical/trades` returned the first page (no error).
  The engine detects a page that restarts at the first item and treats repeated cursors as
  errors.
* **Observed:** a single-filter `/markets?event_ticker=` request returns all markets with an
  empty cursor (documented "unpaged" case).
* **Observed:** an **empty** filter value (`event_ticker=`) is ignored and the *unfiltered*
  listing is returned. The client never sends empty parameters.

## Timestamps and boundaries

* **Documented:** all `*_ts` query parameters are Unix **seconds**; milliseconds → 400.
  **Observed:** `min_ts=1703883597000` → 400 `"min/max timestamp must be in seconds, not milliseconds"`.
* **Trades (observed):** `min_ts`/`max_ts` are compared with the full sub-second
  `created_time`. A trade at `20:59:57.157` was returned for `[min=…597, max=…598]` and not
  for `[min=…596, max=…597]`. Whether exact integer-second trades are included at each
  bound is **not verified** (none was available). The engine pads requests by 1 s on both
  sides and filters locally to the exact half-open window, so the answer does not matter.
* **Candles (observed):** `start_ts` and `end_ts` are both **inclusive** on
  `end_period_ts` (`start=end=1703804400` returned exactly that candle). The engine requests
  `[a, b-1]` for a window `[a, b)` so adjacent windows never overlap.
* **Candle period limit (documented and observed):** 5,000 periods per request;
  5,001 one-minute periods → 400 with
  `details: "requested time range with candlesticks: 5001.000000, max candlesticks: 5000"`;
  exactly 5,000 → 200.
* **Daily candles (observed):** `end_period_ts` at 05:00 UTC (midnight US Eastern standard
  time) in November 2026. Behaviour across daylight-saving changes is **not verified**.
* Response timestamps are RFC 3339 with up to microsecond precision (observed).

## Fields, units, precision and nullability

* **Documented:** prices are fixed-point dollar strings (`*_dollars`, ≤ 4 decimals for
  prices; some responses up to 6); contract quantities are fixed-point strings (`*_fp`,
  2 decimals, fractional allowed); the suffix alone does not determine the unit.
* **Observed:** fractional trade quantities (`"34.46"`, `"3.13"`); sub-cent prices
  (`"0.0010"`, `"0.9990"`); `floor_strike`/`cap_strike` are JSON **numbers**
  (e.g. `7949.9999`) – parsed as Decimal, never float.
* **Trades:** `trade_id`, `ticker`, `count_fp`, `yes_price_dollars`, `no_price_dollars`,
  `taker_outcome_side`, `taker_book_side`, `taker_side` (deprecated), `created_time`,
  `is_block_trade` (documented and observed).
* **Live candles:** `end_period_ts`, `volume_fp`, `open_interest_fp`, and OHLC objects
  `price` / `yes_bid` / `yes_ask` with `open_dollars` … `close_dollars`; `price` also
  `mean_dollars`, `previous_dollars` and documented `min_dollars`/`max_dollars`.
  **Observed:** when no trade occurred, `price` is an **empty object `{}`** (keys omitted)
  rather than keys set to null. `min_dollars`/`max_dollars` were **not observed** in responses.
* **Historical candles:** `volume`, `open_interest`, OHLC keys without `_dollars`
  (`open`, `high`, …, `mean`, `previous`), nulls when no trades (documented and observed).
* **Synthetic candles:** the documented `include_latest_before_start=true` option prepends
  an artificial candle; the engine never sends it.
* **Markets:** an undocumented `expiration_time` field was observed (kept separately).
* **Order book:** `orderbook_fp.yes_dollars` / `no_dollars` lists of `[price, quantity]`
  strings, ascending, best bid last; **bids only** (documented and observed).

## Errors and retry guidance

* **Documented:** errors use `{"error": {"code", "message", "details"}}`; 429 bodies are
  `too_many_requests`; 429 responses do **not** include `Retry-After`; apply exponential backoff.
* **Observed:** 404 codes differ by tier (`not_found` vs
  `failed_to_get_market_by_ticker:_not_found`); invalid `period_interval` returns a different
  shape `{"msg": "Parameter validation failed …"}` (both handled).
* **Observed:** unauthenticated HTTP 429 responses after short bursts of unpaced requests,
  and occasionally under 0.5 s pacing; the engine's retries recovered every time.
  Whether those 429s carried `Retry-After` was not checked; the engine honours it if present.

## Discovery quirks

* **Observed:** legacy event `INXD-23DEC29` reports `series_ticker = "KXINX"`. Listing
  `/events?series_ticker=INX` / `INXD` and `/historical/markets?series_ticker=INX` / `INXD`
  returned nothing. Series/event relationships therefore come from API metadata, never from
  ticker prefixes, and legacy markets are reliably reachable through their **event ticker**.
* Whether `/historical/markets?series_ticker=KXINX` also returns legacy `INXD-*` markets is
  **not verified** (it would require paging through thousands of archived markets).

## Remaining uncertainties

1. Unauthenticated rate limits are unpublished; our default pacing (0.5 s) is a guess
   that worked but still met occasional 429s.
2. How far back the historical archive goes for each series: a December 2023 market was
   available; nothing older was probed. **No completeness claim is made for any period.**
3. Inclusivity of trade `min_ts`/`max_ts` at exact integer seconds (mitigated by padding).
4. Daily candle alignment across daylight-saving time.
5. Whether `trade_id` is unique across tiers (assumed; it is documented as the trade's
   unique identifier and the engine keys on it).
6. Why some hourly candles are absent (observed gaps, e.g. 64 missing hours in a 10-day
   sample of an active market); presumably periods without any activity – reported as
   information, not filled.
7. Whether `external-api.kalshi.com` behaves identically to `api.elections.kalshi.com`
   (documented as equivalent; not verified here).
