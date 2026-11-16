"""SQLite storage: schema, migrations, idempotent writes and local queries.

Representation conventions (see docs/data_dictionary.md):

* Timestamps: TEXT, ISO 8601 UTC with microseconds and ``Z``
  (``2025-01-02T03:04:05.123456Z``). Fixed width, so text order == time order.
* Prices/quantities: TEXT holding the exact decimal as received (``"0.4200"``).
  Never REAL. NULL means "not supplied", never zero.
* Booleans: INTEGER 0/1 or NULL.

Keys and conflict policies:

* ``trades``: PRIMARY KEY ``trade_id`` (documented as the unique trade id).
  Trades are immutable: the first stored version is kept. If a later download
  returns the same id with different content, a ``source_conflict`` warning is
  written to ``validation_issues`` with both request ids.
* ``candles``: PRIMARY KEY ``(ticker, interval_minutes, end_period_ts)``.
  Recent candles may be revised (a period still in progress, or late data);
  a changed candle replaces the stored row, its ``revision`` is incremented and
  the superseded version is kept in ``candle_revisions``.
* ``markets``/``events``/``series``: one "latest" row per identifier
  (latest retrieval wins) plus ``market_snapshots`` holding each *distinct*
  observed market metadata version with its retrieval time.
* ``orderbook_snapshots``: one row per snapshot request (key ``snapshot_id`` =
  request id) and its levels in ``orderbook_levels``.

All writes for one API response happen inside one transaction together with
the ``requests`` row and the work-unit checkpoint (see ingest.py).
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from .decimals import dumps_exact
from .normalize import Issue, Record, content_hash, raw_json
from .timeutil import iso, utcnow

SCHEMA_VERSION = 1

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,                 -- fetch-trades, fetch-candles, collect, poll, ...
    status TEXT NOT NULL,               -- running, completed, incomplete, failed, interrupted
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    spec_json TEXT NOT NULL,            -- the job specification (for resume)
    settings_json TEXT,                 -- non-secret settings snapshot
    cutoff_json TEXT,                   -- /historical/cutoff body read at run start
    cutoff_request_id TEXT,
    requests_used INTEGER NOT NULL DEFAULT 0,
    message TEXT
);

CREATE TABLE IF NOT EXISTS requests (
    request_id TEXT PRIMARY KEY,
    run_id TEXT,
    path TEXT NOT NULL,
    params_json TEXT NOT NULL,
    url TEXT,
    status INTEGER,
    requested_at TEXT,
    received_at TEXT,
    attempts INTEGER,
    tier TEXT,
    raw_path TEXT,                      -- relative to <data_dir>/raw; NULL if not retained
    sha256 TEXT,
    body_bytes INTEGER,
    parser_version TEXT,
    item_count INTEGER,
    error TEXT
);
CREATE INDEX IF NOT EXISTS idx_requests_run ON requests(run_id);

CREATE TABLE IF NOT EXISTS series (
    ticker TEXT PRIMARY KEY, title TEXT, category TEXT, categories TEXT, tags TEXT,
    frequency TEXT, settlement_sources TEXT, contract_url TEXT, contract_terms_url TEXT,
    last_updated_ts TEXT, retrieved_at TEXT NOT NULL, source_request_id TEXT
);

CREATE TABLE IF NOT EXISTS events (
    event_ticker TEXT PRIMARY KEY, series_ticker TEXT, title TEXT, sub_title TEXT,
    category TEXT, strike_date TEXT, strike_period TEXT, mutually_exclusive INTEGER,
    collateral_return_type TEXT, settlement_sources TEXT, last_updated_ts TEXT,
    retrieved_at TEXT NOT NULL, source_request_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_series ON events(series_ticker);

CREATE TABLE IF NOT EXISTS markets (
    ticker TEXT PRIMARY KEY,
    event_ticker TEXT NOT NULL,
    market_type TEXT, title TEXT, subtitle TEXT, yes_sub_title TEXT, no_sub_title TEXT,
    status TEXT, result TEXT, strike_type TEXT, floor_strike TEXT, cap_strike TEXT,
    functional_strike TEXT, custom_strike TEXT, rules_primary TEXT, rules_secondary TEXT,
    price_level_structure TEXT, price_ranges TEXT, expiration_value TEXT,
    mve_collection_ticker TEXT, settlement_bounds_type TEXT, early_close_condition TEXT,
    created_time TEXT, open_time TEXT, close_time TEXT, expected_expiration_time TEXT,
    latest_expiration_time TEXT, expiration_time TEXT, settlement_ts TEXT,
    occurrence_datetime TEXT, updated_time TEXT, fee_waiver_expiration_time TEXT,
    notional_value_dollars TEXT, settlement_value_dollars TEXT, settlement_floor_dollars TEXT,
    last_price_dollars TEXT, previous_price_dollars TEXT, yes_bid_dollars TEXT,
    yes_ask_dollars TEXT, no_bid_dollars TEXT, no_ask_dollars TEXT,
    previous_yes_bid_dollars TEXT, previous_yes_ask_dollars TEXT, yes_bid_size_fp TEXT,
    yes_ask_size_fp TEXT, volume_fp TEXT, volume_24h_fp TEXT, open_interest_fp TEXT,
    can_close_early INTEGER,
    tier TEXT,                          -- endpoint tier that returned the latest version
    content_hash TEXT,
    first_seen_at TEXT NOT NULL,
    retrieved_at TEXT NOT NULL,
    source_request_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_markets_event ON markets(event_ticker);

CREATE TABLE IF NOT EXISTS market_snapshots (
    ticker TEXT NOT NULL,
    retrieved_at TEXT NOT NULL,
    tier TEXT,
    content_hash TEXT NOT NULL,
    source_request_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,         -- normalized record (decimals as exact strings)
    PRIMARY KEY (ticker, source_request_id)
);

CREATE TABLE IF NOT EXISTS trades (
    trade_id TEXT PRIMARY KEY,
    ticker TEXT NOT NULL,
    created_time TEXT NOT NULL,
    count TEXT NOT NULL,
    yes_price TEXT NOT NULL,
    no_price TEXT,
    taker_outcome_side TEXT,
    taker_book_side TEXT,
    taker_side_deprecated TEXT,
    is_block_trade INTEGER,
    tier TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    retrieved_at TEXT NOT NULL,
    source_request_id TEXT NOT NULL,
    parser_version TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_trades_ticker_time ON trades(ticker, created_time);

CREATE TABLE IF NOT EXISTS candles (
    ticker TEXT NOT NULL,
    interval_minutes INTEGER NOT NULL,
    end_period_ts INTEGER NOT NULL,
    end_period_time TEXT NOT NULL,
    trade_open TEXT, trade_high TEXT, trade_low TEXT, trade_close TEXT,
    trade_mean TEXT, trade_previous TEXT, trade_min TEXT, trade_max TEXT,
    yes_bid_open TEXT, yes_bid_high TEXT, yes_bid_low TEXT, yes_bid_close TEXT,
    yes_ask_open TEXT, yes_ask_high TEXT, yes_ask_low TEXT, yes_ask_close TEXT,
    volume TEXT, open_interest TEXT,
    source_schema TEXT NOT NULL,        -- live | historical (response shape)
    tier TEXT NOT NULL,
    period_complete INTEGER NOT NULL,   -- 1 if the period had ended when retrieved
    revision INTEGER NOT NULL DEFAULT 1,
    content_hash TEXT NOT NULL,
    retrieved_at TEXT NOT NULL,
    source_request_id TEXT NOT NULL,
    parser_version TEXT NOT NULL,
    PRIMARY KEY (ticker, interval_minutes, end_period_ts)
);

CREATE TABLE IF NOT EXISTS candle_revisions (
    ticker TEXT NOT NULL, interval_minutes INTEGER NOT NULL, end_period_ts INTEGER NOT NULL,
    revision INTEGER NOT NULL, content_hash TEXT NOT NULL, payload_json TEXT NOT NULL,
    retrieved_at TEXT NOT NULL, source_request_id TEXT NOT NULL, superseded_at TEXT NOT NULL,
    PRIMARY KEY (ticker, interval_minutes, end_period_ts, revision)
);

CREATE TABLE IF NOT EXISTS orderbook_snapshots (
    snapshot_id TEXT PRIMARY KEY,       -- = source request id
    run_id TEXT,
    ticker TEXT NOT NULL,
    requested_at TEXT NOT NULL,         -- local clock, request sent
    received_at TEXT NOT NULL,          -- local clock, response received
    server_date TEXT,                   -- HTTP Date header (second precision), if any
    depth_requested INTEGER,
    yes_levels INTEGER NOT NULL,
    no_levels INTEGER NOT NULL,
    best_yes_bid TEXT,
    best_no_bid TEXT,
    derived_yes_ask TEXT,               -- DERIVED: 1 - best_no_bid (not returned by API)
    derived_no_ask TEXT,                -- DERIVED: 1 - best_yes_bid
    derived_yes_spread TEXT,            -- DERIVED: derived_yes_ask - best_yes_bid
    source_request_id TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ob_ticker_time ON orderbook_snapshots(ticker, received_at);

CREATE TABLE IF NOT EXISTS orderbook_levels (
    snapshot_id TEXT NOT NULL,
    side TEXT NOT NULL,                 -- 'yes' = bid for YES, 'no' = bid for NO
    rank INTEGER NOT NULL,              -- 1 = best (highest) bid on that side
    price TEXT NOT NULL,
    quantity TEXT NOT NULL,
    PRIMARY KEY (snapshot_id, side, rank)
);

CREATE TABLE IF NOT EXISTS work_units (
    unit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    unit_key TEXT NOT NULL,             -- deterministic identity, see ingest.unit_key()
    dataset TEXT NOT NULL,
    ticker TEXT NOT NULL,
    interval_minutes INTEGER,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    status TEXT NOT NULL,               -- pending|running|complete|failed|not_found|incomplete
    phase TEXT,                         -- trades: which tier segment is in progress
    cursor TEXT,                        -- next cursor (checkpoint) within the phase
    first_item_key TEXT,
    pages INTEGER NOT NULL DEFAULT 0,
    rows_received INTEGER NOT NULL DEFAULT 0,
    rows_new INTEGER NOT NULL DEFAULT 0,
    tiers_used TEXT NOT NULL DEFAULT '',
    earliest_obs TEXT,
    latest_obs TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    note TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    UNIQUE (run_id, unit_key)
);
CREATE INDEX IF NOT EXISTS idx_units_key ON work_units(unit_key, status);

CREATE TABLE IF NOT EXISTS validation_issues (
    issue_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT,
    request_id TEXT,
    dataset TEXT,
    record_key TEXT,
    severity TEXT NOT NULL,             -- error (quarantined) | warning
    code TEXT NOT NULL,
    message TEXT NOT NULL,
    raw_json TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_issues_run ON validation_issues(run_id);
"""

MARKET_COLUMNS = (
    "ticker",
    "event_ticker",
    "market_type",
    "title",
    "subtitle",
    "yes_sub_title",
    "no_sub_title",
    "status",
    "result",
    "strike_type",
    "floor_strike",
    "cap_strike",
    "functional_strike",
    "custom_strike",
    "rules_primary",
    "rules_secondary",
    "price_level_structure",
    "price_ranges",
    "expiration_value",
    "mve_collection_ticker",
    "settlement_bounds_type",
    "early_close_condition",
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
    "can_close_early",
)
CANDLE_VALUE_COLUMNS = (
    "trade_open",
    "trade_high",
    "trade_low",
    "trade_close",
    "trade_mean",
    "trade_previous",
    "trade_min",
    "trade_max",
    "yes_bid_open",
    "yes_bid_high",
    "yes_bid_low",
    "yes_bid_close",
    "yes_ask_open",
    "yes_ask_high",
    "yes_ask_low",
    "yes_ask_close",
    "volume",
    "open_interest",
)
TRADE_COLUMNS = (
    "trade_id",
    "ticker",
    "created_time",
    "count",
    "yes_price",
    "no_price",
    "taker_outcome_side",
    "taker_book_side",
    "taker_side_deprecated",
    "is_block_trade",
)
# Volatile quote fields excluded when deciding whether market metadata changed.
MARKET_VOLATILE = (
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
)


def to_sql(value: Any) -> Any:
    """Python value -> SQLite value using the documented representations."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return iso(value)
    if isinstance(value, bool):
        return int(value)
    return value


class Store:
    """Connection wrapper around the research SQLite database."""

    def __init__(self, path: Path, *, readonly: bool = False) -> None:
        self.path = path
        if readonly:
            if not path.exists():
                raise FileNotFoundError(f"Database not found: {path}")
            uri = f"file:{path.as_posix()}?mode=ro"
            self.conn = sqlite3.connect(uri, uri=True, isolation_level=None)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            self.conn = sqlite3.connect(path, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        if not readonly:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=FULL")  # durability over speed
            self._migrate()

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> Store:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _migrate(self) -> None:
        version = self.conn.execute("PRAGMA user_version").fetchone()[0]
        if version > SCHEMA_VERSION:
            raise RuntimeError(
                f"Database schema v{version} is newer than this code (v{SCHEMA_VERSION})"
            )
        if version < 1:
            self.conn.executescript(SCHEMA)
            self.conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """``BEGIN IMMEDIATE`` ... ``COMMIT`` (``ROLLBACK`` on any exception)."""
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        self.conn.execute("COMMIT")

    # ------------------------------------------------------------------ runs
    def create_run(
        self, run_id: str, kind: str, spec: dict[str, Any], settings: dict[str, Any]
    ) -> None:
        now = iso(utcnow())
        self.conn.execute(
            "INSERT INTO runs(run_id, kind, status, created_at, updated_at, spec_json, "
            "settings_json) VALUES (?,?,?,?,?,?,?)",
            (
                run_id,
                kind,
                "running",
                now,
                now,
                dumps_exact(spec, sort_keys=True),
                dumps_exact(settings, sort_keys=True),
            ),
        )

    def set_run_cutoff(self, run_id: str, cutoff: dict[str, Any], request_id: str) -> None:
        self.conn.execute(
            "UPDATE runs SET cutoff_json=?, cutoff_request_id=?, updated_at=? WHERE run_id=?",
            (json.dumps(cutoff, sort_keys=True), request_id, iso(utcnow()), run_id),
        )

    def finish_run(
        self, run_id: str, status: str, requests_used: int, message: str | None = None
    ) -> None:
        self.conn.execute(
            "UPDATE runs SET status=?, requests_used=requests_used+?, message=?, updated_at=? "
            "WHERE run_id=?",
            (status, requests_used, message, iso(utcnow()), run_id),
        )

    def get_run(self, run_id: str) -> sqlite3.Row | None:
        row: sqlite3.Row | None = self.conn.execute(
            "SELECT * FROM runs WHERE run_id=?", (run_id,)
        ).fetchone()
        return row

    def list_runs(self, limit: int = 50) -> list[sqlite3.Row]:
        return list(
            self.conn.execute("SELECT * FROM runs ORDER BY created_at DESC LIMIT ?", (limit,))
        )

    # -------------------------------------------------------------- requests
    def insert_request(
        self,
        *,
        request_id: str,
        run_id: str | None,
        path: str,
        params: dict[str, str],
        url: str | None,
        status: int | None,
        requested_at: datetime | None,
        received_at: datetime | None,
        attempts: int | None,
        tier: str | None,
        raw_path: str | None,
        sha256: str | None,
        body_bytes: int | None,
        parser_version: str | None,
        item_count: int | None,
        error: str | None = None,
    ) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO requests VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                request_id,
                run_id,
                path,
                json.dumps(params, sort_keys=True),
                url,
                status,
                iso(requested_at),
                iso(received_at),
                attempts,
                tier,
                raw_path,
                sha256,
                body_bytes,
                parser_version,
                item_count,
                error,
            ),
        )

    def raw_paths(self) -> list[str]:
        return [
            r[0]
            for r in self.conn.execute("SELECT raw_path FROM requests WHERE raw_path IS NOT NULL")
        ]

    # ---------------------------------------------------------------- issues
    def add_issues(
        self, issues: Sequence[Issue], *, run_id: str | None, request_id: str | None, dataset: str
    ) -> None:
        now = iso(utcnow())
        self.conn.executemany(
            "INSERT INTO validation_issues(run_id, request_id, dataset, record_key, severity, "
            "code, message, raw_json, created_at) VALUES (?,?,?,?,?,?,?,?,?)",
            [
                (
                    run_id,
                    request_id,
                    dataset,
                    i.record_key,
                    i.severity,
                    i.code,
                    i.message,
                    None if i.raw is None else raw_json(i.raw),
                    now,
                )
                for i in issues
            ],
        )

    # -------------------------------------------------------------- metadata
    def upsert_series(self, rec: Record, request_id: str, retrieved_at: datetime) -> None:
        cols = (
            "ticker",
            "title",
            "category",
            "categories",
            "tags",
            "frequency",
            "settlement_sources",
            "contract_url",
            "contract_terms_url",
            "last_updated_ts",
        )
        vals = [to_sql(rec[c]) for c in cols] + [iso(retrieved_at), request_id]
        self.conn.execute(
            f"INSERT OR REPLACE INTO series({', '.join(cols)}, retrieved_at, source_request_id)"
            f" VALUES ({', '.join('?' * (len(cols) + 2))})",
            vals,
        )

    def upsert_event(self, rec: Record, request_id: str, retrieved_at: datetime) -> None:
        cols = (
            "event_ticker",
            "series_ticker",
            "title",
            "sub_title",
            "category",
            "strike_date",
            "strike_period",
            "mutually_exclusive",
            "collateral_return_type",
            "settlement_sources",
            "last_updated_ts",
        )
        vals = [to_sql(rec[c]) for c in cols] + [iso(retrieved_at), request_id]
        self.conn.execute(
            f"INSERT OR REPLACE INTO events({', '.join(cols)}, retrieved_at, source_request_id)"
            f" VALUES ({', '.join('?' * (len(cols) + 2))})",
            vals,
        )

    def upsert_market(
        self,
        rec: Record,
        *,
        tier: str,
        request_id: str,
        retrieved_at: datetime,
        always_snapshot: bool = False,
    ) -> bool:
        """Store the latest market metadata; append a snapshot if the
        non-volatile content changed (or always, for quote polling).
        Returns True if a snapshot row was added."""
        h = content_hash(rec, exclude=MARKET_VOLATILE)
        prev = self.conn.execute(
            "SELECT content_hash, first_seen_at, retrieved_at FROM markets WHERE ticker=?",
            (rec["ticker"],),
        ).fetchone()
        if prev is not None and prev["retrieved_at"] > iso(retrieved_at):
            return False  # never let an older observation overwrite a newer one
        first_seen = prev["first_seen_at"] if prev else iso(retrieved_at)
        vals = [to_sql(rec.get(c)) for c in MARKET_COLUMNS]
        self.conn.execute(
            f"INSERT OR REPLACE INTO markets({', '.join(MARKET_COLUMNS)}, tier, content_hash, "
            f"first_seen_at, retrieved_at, source_request_id) VALUES "
            f"({', '.join('?' * (len(MARKET_COLUMNS) + 5))})",
            [*vals, tier, h, first_seen, iso(retrieved_at), request_id],
        )
        if always_snapshot or prev is None or prev["content_hash"] != h:
            self.conn.execute(
                "INSERT OR IGNORE INTO market_snapshots VALUES (?,?,?,?,?,?)",
                (
                    rec["ticker"],
                    iso(retrieved_at),
                    tier,
                    h,
                    request_id,
                    dumps_exact({k: to_sql(v) for k, v in rec.items()}, sort_keys=True),
                ),
            )
            return True
        return False

    def get_market(self, ticker: str) -> sqlite3.Row | None:
        row: sqlite3.Row | None = self.conn.execute(
            "SELECT m.*, e.series_ticker FROM markets m LEFT JOIN events e "
            "ON e.event_ticker = m.event_ticker WHERE m.ticker=?",
            (ticker,),
        ).fetchone()
        return row

    def get_event(self, event_ticker: str) -> sqlite3.Row | None:
        row: sqlite3.Row | None = self.conn.execute(
            "SELECT * FROM events WHERE event_ticker=?", (event_ticker,)
        ).fetchone()
        return row

    # ---------------------------------------------------------------- trades
    def insert_trades(
        self,
        recs: Sequence[Record],
        *,
        tier: str,
        request_id: str,
        retrieved_at: datetime,
        parser_version: str,
        run_id: str | None,
    ) -> tuple[int, int, int]:
        """Insert trades idempotently. Returns (new, duplicate_same, conflicting)."""
        new = same = conflict = 0
        conflicts: list[Issue] = []
        for rec in recs:
            h = content_hash(rec)
            row = self.conn.execute(
                "SELECT content_hash, source_request_id FROM trades WHERE trade_id=?",
                (rec["trade_id"],),
            ).fetchone()
            if row is None:
                self.conn.execute(
                    f"INSERT INTO trades({', '.join(TRADE_COLUMNS)}, tier, content_hash, "
                    f"retrieved_at, source_request_id, parser_version) VALUES "
                    f"({', '.join('?' * (len(TRADE_COLUMNS) + 5))})",
                    [
                        *(to_sql(rec[c]) for c in TRADE_COLUMNS),
                        tier,
                        h,
                        iso(retrieved_at),
                        request_id,
                        parser_version,
                    ],
                )
                new += 1
            elif row["content_hash"] == h:
                same += 1
            else:
                conflict += 1
                conflicts.append(
                    Issue(
                        "warning",
                        "source_conflict",
                        f"trade {rec['trade_id']} differs from stored version (request "
                        f"{row['source_request_id']}); first-seen version kept",
                        rec["trade_id"],
                        {k: to_sql(v) for k, v in rec.items()},
                    )
                )
        if conflicts:
            self.add_issues(conflicts, run_id=run_id, request_id=request_id, dataset="trade")
        return new, same, conflict

    # --------------------------------------------------------------- candles
    def upsert_candles(
        self,
        recs: Sequence[Record],
        *,
        tier: str,
        request_id: str,
        retrieved_at: datetime,
        parser_version: str,
    ) -> tuple[int, int, int]:
        """Insert or revise candles. Returns (new, revised, unchanged)."""
        new = revised = same = 0
        now_ts = retrieved_at.timestamp()
        for rec in recs:
            h = content_hash(rec, exclude=("source_schema",))
            key = (rec["ticker"], rec["interval_minutes"], rec["end_period_ts"])
            row = self.conn.execute(
                "SELECT * FROM candles WHERE ticker=? AND interval_minutes=? AND end_period_ts=?",
                key,
            ).fetchone()
            complete = int(rec["end_period_ts"] <= now_ts)
            values = [to_sql(rec[c]) for c in CANDLE_VALUE_COLUMNS]
            if row is None:
                self.conn.execute(
                    f"INSERT INTO candles(ticker, interval_minutes, end_period_ts, "
                    f"end_period_time, {', '.join(CANDLE_VALUE_COLUMNS)}, source_schema, tier, "
                    f"period_complete, revision, content_hash, retrieved_at, source_request_id, "
                    f"parser_version) VALUES "
                    f"({', '.join('?' * (len(CANDLE_VALUE_COLUMNS) + 12))})",
                    [
                        *key,
                        iso(rec["end_period_time"]),
                        *values,
                        rec["source_schema"],
                        tier,
                        complete,
                        1,
                        h,
                        iso(retrieved_at),
                        request_id,
                        parser_version,
                    ],
                )
                new += 1
            elif row["content_hash"] == h:
                same += 1
                if complete and not row["period_complete"]:
                    self.conn.execute(
                        "UPDATE candles SET period_complete=1 WHERE ticker=? AND "
                        "interval_minutes=? AND end_period_ts=?",
                        key,
                    )
            elif row["retrieved_at"] > iso(retrieved_at):
                same += 1  # stale observation; keep the newer stored version
            else:
                old = {k: row[k] for k in row.keys()}  # noqa: SIM118 - sqlite3.Row
                self.conn.execute(
                    "INSERT INTO candle_revisions VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        *key,
                        row["revision"],
                        row["content_hash"],
                        json.dumps(old),
                        row["retrieved_at"],
                        row["source_request_id"],
                        iso(retrieved_at),
                    ),
                )
                sets = ", ".join(f"{c}=?" for c in CANDLE_VALUE_COLUMNS)
                self.conn.execute(
                    f"UPDATE candles SET {sets}, source_schema=?, tier=?, period_complete=?, "
                    f"revision=revision+1, content_hash=?, retrieved_at=?, source_request_id=?, "
                    f"parser_version=? WHERE ticker=? AND interval_minutes=? AND end_period_ts=?",
                    [
                        *values,
                        rec["source_schema"],
                        tier,
                        complete,
                        h,
                        iso(retrieved_at),
                        request_id,
                        parser_version,
                        *key,
                    ],
                )
                revised += 1
        return new, revised, same

    # ------------------------------------------------------------- orderbook
    def insert_orderbook(
        self,
        *,
        snapshot_id: str,
        run_id: str | None,
        ticker: str,
        requested_at: datetime,
        received_at: datetime,
        server_date: str | None,
        depth: int | None,
        levels: Sequence[Record],
        notional: Decimal | None,
    ) -> Record:
        """Store one snapshot; returns the summary row (with derived fields)."""
        yes = [lv for lv in levels if lv["side"] == "yes"]
        no = [lv for lv in levels if lv["side"] == "no"]
        best_yes = max((lv["price"] for lv in yes), default=None)
        best_no = max((lv["price"] for lv in no), default=None)
        # A NO bid at p is economically a YES ask at (notional - p); Kalshi binary
        # contracts have notional $1.00. These values are DERIVED, not quoted.
        one = notional if notional is not None else Decimal("1")
        d_yes_ask = None if best_no is None else one - best_no
        d_no_ask = None if best_yes is None else one - best_yes
        spread = None if d_yes_ask is None or best_yes is None else d_yes_ask - best_yes
        summary: Record = {
            "snapshot_id": snapshot_id,
            "run_id": run_id,
            "ticker": ticker,
            "requested_at": requested_at,
            "received_at": received_at,
            "server_date": server_date,
            "depth_requested": depth,
            "yes_levels": len(yes),
            "no_levels": len(no),
            "best_yes_bid": best_yes,
            "best_no_bid": best_no,
            "derived_yes_ask": d_yes_ask,
            "derived_no_ask": d_no_ask,
            "derived_yes_spread": spread,
            "source_request_id": snapshot_id,
        }
        cols = list(summary)
        self.conn.execute(
            f"INSERT OR IGNORE INTO orderbook_snapshots({', '.join(cols)}) VALUES "
            f"({', '.join('?' * len(cols))})",
            [to_sql(summary[c]) for c in cols],
        )
        self.conn.executemany(
            "INSERT OR IGNORE INTO orderbook_levels VALUES (?,?,?,?,?)",
            [
                (snapshot_id, lv["side"], lv["rank"], str(lv["price"]), str(lv["quantity"]))
                for lv in levels
            ],
        )
        return summary

    # ------------------------------------------------------------ work units
    def ensure_unit(
        self,
        run_id: str,
        unit_key: str,
        dataset: str,
        ticker: str,
        interval: int | None,
        start: datetime,
        end: datetime,
    ) -> sqlite3.Row:
        now = iso(utcnow())
        self.conn.execute(
            "INSERT OR IGNORE INTO work_units(run_id, unit_key, dataset, ticker, "
            "interval_minutes, window_start, window_end, status, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                run_id,
                unit_key,
                dataset,
                ticker,
                interval,
                iso(start),
                iso(end),
                "pending",
                now,
                now,
            ),
        )
        row: sqlite3.Row = self.conn.execute(
            "SELECT * FROM work_units WHERE run_id=? AND unit_key=?", (run_id, unit_key)
        ).fetchone()
        return row

    def update_unit(self, unit_id: int, **fields: Any) -> None:
        fields["updated_at"] = iso(utcnow())
        sets = ", ".join(f"{k}=?" for k in fields)
        self.conn.execute(
            f"UPDATE work_units SET {sets} WHERE unit_id=?",
            [to_sql(v) for v in fields.values()] + [unit_id],
        )

    def get_unit(self, unit_id: int) -> sqlite3.Row:
        row: sqlite3.Row = self.conn.execute(
            "SELECT * FROM work_units WHERE unit_id=?", (unit_id,)
        ).fetchone()
        return row

    def final_unit_elsewhere(
        self, unit_key: str, overlap_seconds: int, exclude_run: str
    ) -> sqlite3.Row | None:
        """A completed unit with the same key from another run that is *final*:
        it finished at least ``overlap_seconds`` after its window ended."""
        rows = self.conn.execute(
            "SELECT * FROM work_units WHERE unit_key=? AND status='complete' AND run_id<>? "
            "ORDER BY completed_at DESC",
            (unit_key, exclude_run),
        ).fetchall()
        for r in rows:
            end = datetime.fromisoformat(r["window_end"].replace("Z", "+00:00"))
            done = datetime.fromisoformat(r["completed_at"].replace("Z", "+00:00"))
            if (done - end).total_seconds() >= overlap_seconds:
                found: sqlite3.Row = r
                return found
        return None

    def units_for_run(self, run_id: str) -> list[sqlite3.Row]:
        return list(
            self.conn.execute("SELECT * FROM work_units WHERE run_id=? ORDER BY unit_id", (run_id,))
        )
