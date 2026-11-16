"""End-to-end research walkthrough (makes a small number of live API requests).

Steps:
  1. Discover candidate equity-index series (local keyword filter on /series).
  2. Select a real market returned by the API (no invented tickers).
  3. Download a bounded historical sample (trades + hourly candles).
  4. Inspect coverage and null counts.
  5. Export CSV and Parquet.
  6. Load locally stored data into pandas (no network).

Run:
    uv run python examples/research_walkthrough.py --data-dir data/demo
    uv run python examples/research_walkthrough.py --base-url \
        https://api.elections.kalshi.com/trade-api/v2   # alternate documented host

If no equity-related series is found, the script says so and falls back to a
clearly labelled general market (the most recent settled market listed).
"""

from __future__ import annotations

import argparse
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from kalshi_data import KalshiData, LocalStore
from kalshi_data.config import load_config
from kalshi_data.endpoints import Tier


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--data-dir", default="data/demo")
    ap.add_argument("--base-url", default=None)
    ap.add_argument("--budget", type=int, default=120, help="max HTTP requests")
    args = ap.parse_args()
    settings = load_config(
        overrides={"api": {"base_url": args.base_url}, "storage": {"data_dir": args.data_dir}}
    ).settings

    with KalshiData(settings, request_budget=args.budget) as kd:
        # 1. Discovery: server-side category filter + LOCAL keyword match.
        series = kd.discover("S&P 500", category="Financials")
        print(f"[1] {len(series)} series match 'S&P 500' in Financials")
        for s in series[:8]:
            print(f"     {s['ticker']:<16} {s['title']}  ({s['frequency']})")

        # 2. Pick a real market: a recently settled event of a daily range series.
        ticker = None
        daily = [
            s
            for s in series
            if s.get("frequency") == "daily" and "range" in (s.get("title") or "").lower()
        ]
        for s in daily[:3]:
            events = kd.events(s["ticker"], status="settled")
            if not events:
                continue
            ev = events[0]
            mkts = kd.markets(event=ev["event_ticker"])
            traded = [m for m in mkts if (m.get("volume_fp") or Decimal(0)) > 0]
            if traded:
                best = max(traded, key=lambda m: m["volume_fp"])
                ticker = best["ticker"]
                print(
                    f"[2] selected {ticker} from event {ev['event_ticker']} "
                    f"(series {s['ticker']}, volume {best['volume_fp']})"
                )
                break
        if ticker is None:
            print(
                "[2] No equity-range market with volume found; using a GENERAL market "
                "instead (most recent settled market in the live listing)."
            )
            page = next(kd.client.market_pages(Tier.LIVE, status="settled", max_pages=1))
            ticker = page.items[0]["ticker"]
            print(f"    general demo market: {ticker}")

        # 3. Bounded sample: the 3 days up to settlement (or now).
        rec, _tier = kd.market(ticker)
        end = rec["settlement_ts"] or rec["close_time"]
        start = end - timedelta(days=3)
        print(f"[3] downloading trades + 60-min candles for [{start}, {end}) ...")
        run = kd.collect(
            tickers=[ticker],
            start=start,
            end=end,
            datasets=["markets", "trades", "candles"],
            candle_interval=60,
        )
        print(
            f"    run {run.run_id}: {run.status}; units {run.units}; "
            f"{run.requests_used} HTTP attempts"
        )

    with LocalStore(Path(args.data_dir)) as db:
        # 4. Coverage and nulls.
        cov = db.coverage(ticker)
        for d in cov["datasets"]:
            print(
                f"[4] {d['dataset']}: rows={d.get('rows_stored')} tiers={d['tiers_used']} "
                f"earliest={d.get('earliest')} latest={d.get('latest')} "
                f"complete={d['all_planned_requests_completed']}"
            )
        candles = db.to_dataframe("candles", tickers=[ticker])
        if not candles.empty:
            print("    null counts (trade_* null = no trade in that period, not zero):")
            print(
                candles[["trade_close", "yes_bid_close", "yes_ask_close", "volume"]]
                .isna()
                .sum()
                .to_string()
            )

        # 5. Export.
        out = Path(args.data_dir) / "exports"
        n1 = db.export("trades", out / "trades.parquet", "parquet", tickers=[ticker])
        n2 = db.export("candles", out / "candles.csv", "csv", tickers=[ticker])
        print(
            f"[5] exported {n1} trades -> {out / 'trades.parquet'}; "
            f"{n2} candles -> {out / 'candles.csv'}"
        )

        # 6. pandas: exact Decimals by default; numeric=True converts (lossy) to float.
        trades = db.to_dataframe("trades", tickers=[ticker])
        print(
            f"[6] trades DataFrame: {len(trades)} rows; dtype of yes_price = "
            f"{trades['yes_price'].dtype if not trades.empty else 'n/a'} (Decimal objects)"
        )
        if not trades.empty:
            vwap = (trades["yes_price"] * trades["count"]).sum() / trades["count"].sum()
            print(f"    exact volume-weighted YES price over the sample: {vwap:.4f}")


if __name__ == "__main__":
    main()
