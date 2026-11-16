# Research usage

## Notebook workflow

```python
from kalshi_data import KalshiData, LocalStore

# 1. Discover (network). Keyword matching is local; category is a server filter.
with KalshiData() as kd:                       # add a settings object for --base-url etc.
    series = kd.discover("S&P 500", category="Financials")
    events = kd.events("KXINX", status="settled")           # newest first
    mkts = kd.markets(event=events[0]["event_ticker"])      # live + archived, merged
    # 2. Persist a bounded download (resumable, de-duplicated, provenance kept)
    run = kd.collect(tickers=[m["ticker"] for m in mkts[:5]],
                     start="2026-11-01", end="2026-11-08",
                     datasets=["markets", "trades", "candles"], candle_interval=60)
    print(run.status, run.units)

# 3. Analyse offline (no network)
with LocalStore("data") as db:
    print(db.coverage(mkts[0]["ticker"]))
    trades = db.to_dataframe("trades", tickers=[mkts[0]["ticker"]])          # Decimal objects
    candles = db.to_dataframe("candles", interval=60, numeric=True)          # float64, lossy
```

Streaming without storing (useful for quick looks; nothing is persisted):

```python
with KalshiData() as kd:
    for t in kd.iter_trades("TICKER", "2026-11-01", "2026-11-02"):
        ...
    for c in kd.iter_candles("TICKER", "2026-11-01", "2026-11-08", interval=60):
        ...
```

`LocalStore` never touches the network; `KalshiData` methods always do.

## Precision

* Default DataFrames hold `decimal.Decimal` objects (dtype `object`). Sums/products stay
  exact; vectorised numpy operations will be slower.
* `numeric=True` converts decimal columns to `float64` explicitly. Do this only when exact
  cents/sub-cents do not matter (plots, regressions).
* Parquet exports use `decimal128`; pandas/pyarrow readers can keep them exact.

## Research caveats

1. **Coverage ≠ completeness.** A complete download means every planned request succeeded.
   It does not prove Kalshi's archive contains every trade or candle that ever existed.
   Read `coverage` (`uncovered`, `unfinished_or_failed`, `empty_windows`) before analysis.
2. **No trades ≠ zero price.** Candle `trade_*` columns are NULL when nothing traded. Do not
   fill them with zeros; forward-filling is a modelling choice you should make explicitly
   (e.g. using `trade_previous`, which is the last price *before* the period).
3. **Bid/ask vs traded price.** `yes_bid_*`/`yes_ask_*` describe quotes; `trade_*` describe
   executed trades. A quote midpoint is *derived* and is not an executable trade price.
   Near close, quotes can be extreme (e.g. bid 0.00 / ask 1.00 after trading stops), which
   makes midpoints meaningless.
4. **Order books** are only current snapshots. Polling yields a forward-looking sample at
   your polling interval; it misses intermediate updates and cannot recreate earlier books.
5. **Metadata is retrieved now.** Titles, rules, strikes and status in `markets` are as of
   `retrieved_at`. They are not proof that the same information was public historically
   (rules can be amended; status changes). `market_snapshots` shows only what *this tool*
   observed and when.
6. **Settlement vs close vs expiration.** `close_time` (trading stops), expiration times,
   and `settlement_ts` (payout determined) are different fields; choose deliberately.
7. **Time zones.** All stored times are UTC. Equity-index contracts are defined in US
   Eastern time; convert explicitly (e.g. `zoneinfo.ZoneInfo("America/New_York")`). Daily
   candles observed ending 05:00 UTC (midnight EST); check alignment across DST yourself.
8. **Fractional contracts and sub-cent prices** exist; never assume integer counts or
   whole-cent prices.
9. **Series identity.** Kalshi re-organised some products: e.g. the 2023 event
   `INXD-23DEC29` reports series `KXINX`. Use API relationships (`events.series_ticker`),
   not ticker prefixes. To reach older markets, list them by **event** ticker.
10. **Recent data changes.** The most recent candles can lag and be revised; re-run
    updates after the overlap period and check `revision`/`candle_revisions`.

## Equity-hedging use (scope note)

The engine provides the market data (e.g. S&P 500 range/threshold contracts with exact
strike bounds, settlement values and price histories). The hedging optimiser itself is out
of scope; build it on top of `LocalStore` queries or the Parquet exports.
