"""CSV and Parquet exports from the normalized SQLite store.

Exports are always produced *from SQLite* (the single source of truth), never
from a separate store, so they cannot diverge.

Precision and nulls:

* CSV: decimals are written exactly as stored (``"0.4200"``); NULL is an
  empty field. Timestamps are ISO 8601 UTC strings.
* Parquet: decimal columns use ``decimal128(38, s)`` where ``s`` is the
  largest number of decimal places present in that column for the exported
  rows, so values round-trip exactly; NULL stays null. Timestamps are
  ``timestamp[us, tz=UTC]``.

Every exported row carries provenance (``source_request_id``, ``retrieved_at``,
``tier`` and the raw file path) so it can be traced back to the API response.
A ``<output>.meta.json`` sidecar records the filters and generation time.
"""

from __future__ import annotations

import csv
import json
from collections.abc import Iterator
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from . import __version__
from .errors import ConfigError
from .storage import CANDLE_VALUE_COLUMNS, MARKET_COLUMNS, Store
from .timeutil import iso, parse_api_time, utcnow

_MARKET_DEC = {
    "floor_strike",
    "cap_strike",
    "notional_value_dollars",
    "settlement_value_dollars",
    "settlement_floor_dollars",
    "last_price_dollars",
    "previous_price_dollars",
    "yes_bid_dollars",
    "yes_ask_dollars",
    "no_bid_dollars",
    "no_ask_dollars",
    "previous_yes_bid_dollars",
    "previous_yes_ask_dollars",
    "yes_bid_size_fp",
    "yes_ask_size_fp",
    "volume_fp",
    "volume_24h_fp",
    "open_interest_fp",
}
_MARKET_TIME = {
    "created_time",
    "open_time",
    "close_time",
    "expected_expiration_time",
    "latest_expiration_time",
    "expiration_time",
    "settlement_ts",
    "occurrence_datetime",
    "updated_time",
    "fee_waiver_expiration_time",
}

# dataset -> (SQL, decimal columns, timestamp columns, time column for filtering)
DATASETS: dict[str, tuple[str, set[str], set[str], str | None]] = {
    "trades": (
        "SELECT t.trade_id, t.ticker, t.created_time, t.count, t.yes_price, t.no_price, "
        "t.taker_outcome_side, t.taker_book_side, t.taker_side_deprecated, t.is_block_trade, "
        "t.tier, t.retrieved_at, t.source_request_id, r.raw_path, t.parser_version "
        "FROM trades t LEFT JOIN requests r ON r.request_id = t.source_request_id",
        {"count", "yes_price", "no_price"},
        {"created_time", "retrieved_at"},
        "t.created_time",
    ),
    "candles": (
        "SELECT c.ticker, c.interval_minutes, c.end_period_ts, c.end_period_time, "
        + ", ".join(f"c.{x}" for x in CANDLE_VALUE_COLUMNS)
        + ", c.period_complete, c.revision, c.source_schema, c.tier, c.retrieved_at, "
        "c.source_request_id, r.raw_path, c.parser_version "
        "FROM candles c LEFT JOIN requests r ON r.request_id = c.source_request_id",
        set(CANDLE_VALUE_COLUMNS),
        {"end_period_time", "retrieved_at"},
        "c.end_period_time",
    ),
    "markets": (
        "SELECT "
        + ", ".join(f"m.{x}" for x in MARKET_COLUMNS)
        + ", e.series_ticker, m.tier, m.first_seen_at, m.retrieved_at, m.source_request_id, "
        "r.raw_path FROM markets m LEFT JOIN events e ON e.event_ticker = m.event_ticker "
        "LEFT JOIN requests r ON r.request_id = m.source_request_id",
        _MARKET_DEC,
        _MARKET_TIME | {"first_seen_at", "retrieved_at"},
        None,
    ),
    "orderbook": (
        "SELECT s.snapshot_id, s.ticker, s.requested_at, s.received_at, s.server_date, "
        "l.side, l.rank, l.price, l.quantity, s.best_yes_bid, s.best_no_bid, "
        "s.derived_yes_ask, s.derived_no_ask, s.derived_yes_spread, s.source_request_id, "
        "r.raw_path FROM orderbook_snapshots s "
        "LEFT JOIN orderbook_levels l ON l.snapshot_id = s.snapshot_id "
        "LEFT JOIN requests r ON r.request_id = s.source_request_id",
        {
            "price",
            "quantity",
            "best_yes_bid",
            "best_no_bid",
            "derived_yes_ask",
            "derived_no_ask",
            "derived_yes_spread",
        },
        {"requested_at", "received_at"},
        "s.received_at",
    ),
}


# INTEGER 0/1 flags exported as booleans.
BOOL_COLUMNS = {"is_block_trade", "period_complete", "can_close_early", "mutually_exclusive"}


def query_rows(
    store: Store,
    dataset: str,
    *,
    tickers: list[str] | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    interval: int | None = None,
) -> Iterator[dict[str, Any]]:
    """Stream rows (raw SQLite representation) for a dataset with filters.

    ``start``/``end`` form a half-open UTC range on the dataset's time column
    (trade time, candle period end, snapshot receipt time).
    """
    if dataset not in DATASETS:
        raise ConfigError(f"unknown dataset {dataset!r}; choose from {sorted(DATASETS)}")
    sql, _dec, _ts, tcol = DATASETS[dataset]
    where: list[str] = []
    args: list[Any] = []
    alias = {"trades": "t", "candles": "c", "markets": "m", "orderbook": "s"}[dataset]
    if tickers:
        where.append(f"{alias}.ticker IN ({', '.join('?' * len(tickers))})")
        args += tickers
    if tcol and start is not None:
        where.append(f"{tcol} >= ?")
        args.append(iso(start))
    if tcol and end is not None:
        where.append(f"{tcol} < ?")
        args.append(iso(end))
    if interval is not None and dataset == "candles":
        where.append("c.interval_minutes = ?")
        args.append(interval)
    if where:
        sql += " WHERE " + " AND ".join(where)
    order = {
        "trades": " ORDER BY t.ticker, t.created_time, t.trade_id",
        "candles": " ORDER BY c.ticker, c.interval_minutes, c.end_period_ts",
        "markets": " ORDER BY m.ticker",
        "orderbook": " ORDER BY s.ticker, s.received_at, l.side, l.rank",
    }[dataset]
    cur = store.conn.execute(sql + order, args)
    cols = [d[0] for d in cur.description]
    for row in cur:
        yield dict(zip(cols, row, strict=True))


def _scale(values: list[str | None]) -> int:
    s = 0
    for v in values:
        if v is not None:
            exp = Decimal(v).as_tuple().exponent
            if isinstance(exp, int):
                s = max(s, -exp)
    return s


def export(store: Store, dataset: str, output: Path, fmt: str, **filters: Any) -> int:
    """Write a dataset to CSV or Parquet. Returns the number of rows written.

    Raises:
        ConfigError: unknown dataset/format.
    """
    if fmt not in ("csv", "parquet"):
        raise ConfigError("format must be 'csv' or 'parquet'")
    output.parent.mkdir(parents=True, exist_ok=True)
    rows = query_rows(store, dataset, **filters)
    if fmt == "csv":
        n = _write_csv(rows, output, dataset)
    else:
        n = _write_parquet(list(rows), output, dataset)
    meta = {
        "dataset": dataset,
        "format": fmt,
        "rows": n,
        "generated_at": iso(utcnow()),
        "database": str(store.path),
        "engine_version": __version__,
        "filters": {k: (iso(v) if isinstance(v, datetime) else v) for k, v in filters.items()},
        "notes": "Decimals exact; NULL = not supplied by API (empty in CSV). See "
        "docs/data_dictionary.md.",
    }
    Path(str(output) + ".meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return n


def _write_csv(rows: Iterator[dict[str, Any]], output: Path, dataset: str) -> int:
    n = 0
    with output.open("w", newline="", encoding="utf-8") as fh:
        writer: csv.DictWriter[str] | None = None
        for row in rows:
            if writer is None:
                writer = csv.DictWriter(fh, fieldnames=list(row))
                writer.writeheader()
            writer.writerow({k: "" if v is None else v for k, v in row.items()})
            n += 1
        if writer is None:  # empty export still gets a header
            sql = DATASETS[dataset][0]
            cols = _columns_of(sql)
            csv.writer(fh).writerow(cols)
    return n


def _columns_of(sql: str) -> list[str]:
    head = sql.split(" FROM ")[0].removeprefix("SELECT ")
    return [c.strip().split(".")[-1] for c in head.split(",")]


def _write_parquet(rows: list[dict[str, Any]], output: Path, dataset: str) -> int:
    import pyarrow as pa
    import pyarrow.parquet as pq

    _sql, dec_cols, ts_cols, _t = DATASETS[dataset]
    cols = list(rows[0]) if rows else _columns_of(DATASETS[dataset][0])
    arrays, fields = [], []
    for c in cols:
        values = [r[c] for r in rows]
        if c in dec_cols:
            scale = _scale(values)
            typ = pa.decimal128(38, scale)
            arr = pa.array([None if v is None else Decimal(v) for v in values], type=typ)
        elif c in BOOL_COLUMNS:
            typ = pa.bool_()
            arr = pa.array([None if v is None else bool(v) for v in values], type=typ)
        elif c in ts_cols:
            typ = pa.timestamp("us", tz="UTC")
            arr = pa.array([parse_api_time(v) for v in values], type=typ)
        else:
            arr = pa.array(values)
            typ = arr.type if rows else pa.string()
            if not rows:
                arr = pa.array([], type=typ)
        arrays.append(arr)
        fields.append(pa.field(c, typ))
    table = pa.Table.from_arrays(arrays, schema=pa.schema(fields))
    pq.write_table(table, output)
    return len(rows)
