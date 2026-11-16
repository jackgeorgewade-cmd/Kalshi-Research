"""Coverage reports: what was requested, what came back, what is missing.

A report is computed from ``work_units`` (what we asked for and how each
request ended) and the data tables (what is stored). It deliberately keeps
these states apart:

* ``complete`` with rows        - all planned requests for the window finished.
* ``complete`` with zero rows   - "no data returned" (the API answered, empty).
* ``failed``                    - a request failed; nothing is implied about data.
* ``not_found``                 - the market was absent from both tiers.
* ``incomplete``/``running``/``pending`` - unfinished work (budget, interrupt).
* ``skipped``                   - an identical *final* window exists from another run.

First/last observation timestamps do not prove completeness; the report says
which time ranges were actually covered by completed requests.
"""

from __future__ import annotations

from datetime import datetime
from itertools import pairwise
from typing import Any

from .storage import Store
from .timeutil import parse_api_time

DONE = ("complete", "skipped")

LIMITATIONS = [
    "Coverage means 'requests for these windows completed', not that Kalshi's archive is "
    "complete. Kalshi does not publish completeness guarantees.",
    "Trades have no expected frequency; long gaps between trades can be genuine inactivity.",
    "Candles may be absent for periods without activity; missing periods are reported as "
    "information, not as errors.",
    "Market metadata reflects the time it was retrieved, not necessarily what was visible "
    "historically.",
]


def _merge(intervals: list[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    out: list[tuple[datetime, datetime]] = []
    for s, e in sorted(intervals):
        if out and s <= out[-1][1]:
            out[-1] = (out[-1][0], max(out[-1][1], e))
        else:
            out.append((s, e))
    return out


def _subtract(base: list[tuple[datetime, datetime]],
              cover: list[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    gaps = []
    for s, e in base:
        cur = s
        for cs, ce in cover:
            if ce <= cur or cs >= e:
                continue
            if cs > cur:
                gaps.append((cur, cs))
            cur = max(cur, ce)
        if cur < e:
            gaps.append((cur, e))
    return gaps


def _t(text: str) -> datetime:
    dt = parse_api_time(text)
    assert dt is not None
    return dt


def dataset_coverage(store: Store, ticker: str, dataset: str, interval: int | None = None,
                     run_id: str | None = None) -> dict[str, Any]:
    """Coverage of one dataset for one market (optionally limited to one run)."""
    q = "SELECT * FROM work_units WHERE ticker=? AND dataset=?"
    args: list[Any] = [ticker, dataset]
    if interval is not None:
        q += " AND interval_minutes=?"
        args.append(interval)
    if run_id is not None:
        q += " AND run_id=?"
        args.append(run_id)
    units = list(store.conn.execute(q + " ORDER BY window_start", args))
    requested = _merge([(_t(u["window_start"]), _t(u["window_end"])) for u in units])
    covered = _merge([(_t(u["window_start"]), _t(u["window_end"]))
                      for u in units if u["status"] in DONE])
    status_counts: dict[str, int] = {}
    for u in units:
        status_counts[u["status"]] = status_counts.get(u["status"], 0) + 1
    tiers = sorted({t for u in units for t in (u["tiers_used"] or "").split(",") if t})
    empty = [dict(window_start=u["window_start"], window_end=u["window_end"])
             for u in units if u["status"] == "complete" and u["rows_received"] == 0]
    unfinished = [dict(window_start=u["window_start"], window_end=u["window_end"],
                       status=u["status"], error=u["error"], note=u["note"], run_id=u["run_id"])
                  for u in units if u["status"] not in DONE]
    # Only windows never completed in any run count as uncovered.
    uncovered = _subtract(requested, covered)
    report: dict[str, Any] = {
        "ticker": ticker, "dataset": dataset, "interval_minutes": interval,
        "requested": [(s.isoformat(), e.isoformat()) for s, e in requested],
        "covered_by_completed_requests": [(s.isoformat(), e.isoformat()) for s, e in covered],
        "uncovered": [(s.isoformat(), e.isoformat()) for s, e in uncovered],
        "units": status_counts,
        "pages_completed": sum(u["pages"] for u in units),
        "tiers_used": tiers,
        "empty_windows": empty,
        "unfinished_or_failed": unfinished,
        "all_planned_requests_completed": bool(units) and not unfinished,
    }
    if dataset == "trades":
        row = store.conn.execute(
            "SELECT COUNT(*) n, MIN(created_time) a, MAX(created_time) b FROM trades "
            "WHERE ticker=?", (ticker,)).fetchone()
        report.update(rows_stored=row["n"], earliest=row["a"], latest=row["b"])
        times = [r[0] for r in store.conn.execute(
            "SELECT created_time FROM trades WHERE ticker=? ORDER BY created_time", (ticker,))]
        if len(times) > 1:
            gaps = [((_t(b) - _t(a)).total_seconds(), a, b) for a, b in pairwise(times)]
            g = max(gaps)
            report["largest_gap_between_trades"] = {"seconds": g[0], "from": g[1], "to": g[2]}
    elif dataset == "candles":
        q2 = ("SELECT COUNT(*) n, MIN(end_period_time) a, MAX(end_period_time) b, "
              "MIN(end_period_ts) ta, MAX(end_period_ts) tb, SUM(period_complete=0) partial, "
              "SUM(trade_close IS NULL) no_trade, SUM(revision>1) revised "
              "FROM candles WHERE ticker=?")
        args2: list[Any] = [ticker]
        if interval is not None:
            q2 += " AND interval_minutes=?"
            args2.append(interval)
        row = store.conn.execute(q2, args2).fetchone()
        report.update(rows_stored=row["n"], earliest=row["a"], latest=row["b"],
                      periods_without_trades=row["no_trade"] or 0,
                      incomplete_periods_at_retrieval=row["partial"] or 0,
                      revised_candles=row["revised"] or 0)
        if interval and row["n"]:
            expected = (row["tb"] - row["ta"]) // (interval * 60) + 1
            report["missing_periods_between_first_and_last"] = expected - row["n"]
    report["limitations"] = LIMITATIONS
    return report


def market_coverage(store: Store, ticker: str, run_id: str | None = None) -> dict[str, Any]:
    """Coverage for every dataset/interval stored for a market."""
    combos = list(store.conn.execute(
        "SELECT DISTINCT dataset, interval_minutes FROM work_units WHERE ticker=?"
        + (" AND run_id=?" if run_id else "") + " ORDER BY dataset, interval_minutes",
        (ticker, run_id) if run_id else (ticker,)))
    market = store.get_market(ticker)
    return {
        "ticker": ticker,
        "market_known_locally": market is not None,
        "market_status": market["status"] if market else None,
        "settlement_ts": market["settlement_ts"] if market else None,
        "datasets": [dataset_coverage(store, ticker, c["dataset"], c["interval_minutes"], run_id)
                     for c in combos if c["dataset"] != "orderbook"],
        "orderbook_snapshots": store.conn.execute(
            "SELECT COUNT(*) FROM orderbook_snapshots WHERE ticker=?", (ticker,)).fetchone()[0],
    }


def run_status(store: Store, run_id: str) -> dict[str, Any]:
    """Status of a run: units by state, issues, requests, and unfinished work."""
    run = store.get_run(run_id)
    if run is None:
        raise KeyError(run_id)
    units = store.units_for_run(run_id)
    counts: dict[str, int] = {}
    for u in units:
        counts[u["status"]] = counts.get(u["status"], 0) + 1
    issues = {f"{r['severity']}:{r['code']}": r["n"] for r in store.conn.execute(
        "SELECT severity, code, COUNT(*) n FROM validation_issues WHERE run_id=? "
        "GROUP BY severity, code", (run_id,))}
    req = store.conn.execute(
        "SELECT COUNT(*) n, SUM(error IS NOT NULL) failed, SUM(COALESCE(attempts,1)-1) retries "
        "FROM requests WHERE run_id=?", (run_id,)).fetchone()
    return {
        "run_id": run_id, "kind": run["kind"], "status": run["status"],
        "message": run["message"], "created_at": run["created_at"],
        "updated_at": run["updated_at"], "cutoff": run["cutoff_json"],
        "requests_logged": req["n"], "requests_failed": req["failed"] or 0,
        "retries": req["retries"] or 0, "http_attempts_counted": run["requests_used"],
        "units": counts,
        "rows_received": sum(u["rows_received"] for u in units),
        "rows_new": sum(u["rows_new"] for u in units),
        "empty_units": sum(1 for u in units if u["status"] == "complete"
                           and u["rows_received"] == 0),
        "unfinished": [dict(key=u["unit_key"], status=u["status"], error=u["error"],
                            note=u["note"]) for u in units if u["status"] not in DONE],
        "issues": issues,
    }
