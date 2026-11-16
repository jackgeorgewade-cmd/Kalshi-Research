"""Load locally stored data into pandas (offline: no network requests).

Run after any download, e.g. the walkthrough:
    uv run python examples/load_into_pandas.py --data-dir data/demo
"""

from __future__ import annotations

import argparse

from kalshi_data import LocalStore


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="data")
    ap.add_argument("--ticker", default=None, help="limit to one market")
    args = ap.parse_args()
    tickers = [args.ticker] if args.ticker else None

    with LocalStore(args.data_dir) as db:
        markets = db.to_dataframe("markets", tickers=tickers)
        print(f"{len(markets)} markets cached locally")
        if not markets.empty:
            print(
                markets[["ticker", "status", "result", "close_time", "settlement_ts"]]
                .head()
                .to_string()
            )

        # Exact representation (Decimal objects) - the default.
        trades = db.to_dataframe("trades", tickers=tickers)
        print(f"\n{len(trades)} trades; columns: {list(trades.columns)[:8]} ...")

        # Explicit, lossy float conversion for statistics/plotting.
        candles = db.to_dataframe("candles", tickers=tickers, interval=60, numeric=True)
        if not candles.empty:
            candles = candles.set_index("end_period_time")
            # Quote midpoint: DERIVED from YES bid/ask closes; not a traded price.
            candles["derived_mid"] = (candles["yes_bid_close"] + candles["yes_ask_close"]) / 2
            print("\nhourly candles (float view):")
            print(
                candles[["trade_close", "yes_bid_close", "yes_ask_close", "derived_mid", "volume"]]
                .tail()
                .to_string()
            )


if __name__ == "__main__":
    main()
