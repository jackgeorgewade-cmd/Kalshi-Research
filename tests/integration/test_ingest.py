"""End-to-end ingestion against the SYNTHETIC FakeKalshi (no network)."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

from conftest import Engine, make_engine
from fixtures.synthetic import (
    CUTOFF_TS,
    FakeKalshi,
    hist_candle,
    live_candle,
    market,
    trade,
    ts,
)
from kalshi_data.coverage import market_coverage, run_status
from kalshi_data.ingest import JobRequest
from kalshi_data.rawstore import find_orphans, read_envelope
from kalshi_data.timeutil import utcnow

S, E = datetime(2026, 9, 1, tzinfo=UTC), datetime(2026, 10, 1, tzinfo=UTC)


def job(tickers: list[str], datasets: list[str], start: datetime = S,
        end: datetime = E, interval: int = 60) -> JobRequest:
    return JobRequest(kind="test", start=start, end=end, datasets=datasets, tickers=tickers,
                      candle_interval=interval)


def setup_markets(fake: FakeKalshi) -> None:
    # LIVEM: settles after the cutoff -> live metadata/candles; trades span both tiers.
    fake.add_market(market("LIVEM", "EV1", created="2026-09-01T00:00:00Z"))
    fake.hist_trades += [trade(f"h{i}", "LIVEM", ts("2026-09-10T00:00:00Z") + i * 60)
                         for i in range(5)]
    boundary = trade("edge", "LIVEM", CUTOFF_TS)  # exactly at the cutoff, served by both
    fake.hist_trades.append(boundary)
    fake.live_trades += [dict(boundary)] + [
        trade(f"l{i}", "LIVEM", ts("2026-09-20T00:00:00Z") + i * 60, "0.5000", "2.50")
        for i in range(5)]
    # Candles before AND after the trade cutoff, all served by the live endpoint.
    fake.live_candles[("LIVEM", 60)] = [live_candle(ts("2026-09-10T01:00:00Z")),
                                        live_candle(ts("2026-09-20T01:00:00Z"), None, "0.00")]
    # ARCH: settled before the cutoff -> historical metadata/candles/trades only.
    fake.add_market(market("ARCH", "EV0", created="2026-09-01T00:00:00Z",
                           settled="2026-09-05T00:00:00Z"), historical=True)
    fake.hist_trades += [trade(f"a{i}", "ARCH", ts("2026-09-02T00:00:00Z") + i)
                         for i in range(3)]
    fake.hist_candles[("ARCH", 60)] = [hist_candle(ts("2026-09-02T01:00:00Z"))]


def count(eng: Engine, sql: str, *args: Any) -> int:
    return int(eng.store.conn.execute(sql, args).fetchone()[0])


def test_trades_routed_across_tiers_with_boundary_dedupe(engine: Engine) -> None:
    setup_markets(engine.fake)
    res = engine.ingestor.run(job(["LIVEM"], ["markets", "trades"]))
    assert res.status == "completed"
    assert count(engine, "SELECT COUNT(*) FROM trades WHERE ticker='LIVEM'") == 11
    assert count(engine, "SELECT COUNT(*) FROM trades WHERE trade_id='edge'") == 1
    paths = {p for p, _ in engine.fake.calls}
    assert {"/historical/trades", "/markets/trades"} <= paths
    hist_calls = [q for p, q in engine.fake.calls if p == "/historical/trades"]
    assert int(hist_calls[0]["max_ts"]) == CUTOFF_TS + 1  # padded, then filtered locally
    cov = market_coverage(engine.store, "LIVEM")["datasets"][0]
    assert cov["tiers_used"] == ["historical", "live"]
    assert cov["all_planned_requests_completed"] and cov["uncovered"] == []


def test_candles_routed_by_market_archival_not_candle_date(engine: Engine) -> None:
    setup_markets(engine.fake)
    res = engine.ingestor.run(job(["LIVEM", "ARCH"], ["candles"]))
    assert res.status == "completed"
    candle_paths = [p for p, _ in engine.fake.calls if p.endswith("candlesticks")]
    assert any(p.startswith("/series/SYNSERIES/markets/LIVEM") for p in candle_paths)
    assert any(p.startswith("/historical/markets/ARCH") for p in candle_paths)
    # LIVEM candles before the trade cutoff were still requested from the live endpoint.
    assert not any("historical/markets/LIVEM" in p for p in candle_paths)
    rows = {r["ticker"]: r for r in engine.store.conn.execute("SELECT * FROM candles")}
    assert count(engine, "SELECT COUNT(*) FROM candles WHERE ticker='LIVEM'") == 2
    assert rows["ARCH"]["source_schema"] == "historical"
    no_trade = engine.store.conn.execute(
        "SELECT trade_close, volume FROM candles WHERE ticker='LIVEM' ORDER BY end_period_ts "
        "DESC").fetchone()
    assert no_trade["trade_close"] is None and no_trade["volume"] == "0.00"


def test_candle_404_falls_back_to_other_tier_once(engine: Engine) -> None:
    setup_markets(engine.fake)
    f = engine.fake

    def move_on_candles(path: str, params: dict[str, str]) -> httpx.Response | None:
        if path.startswith("/series/") and "LIVEM" in f.live_markets:
            m = f.live_markets.pop("LIVEM")  # market archived mid-run
            m.update(status="finalized", settlement_ts="2026-09-30T00:00:00Z")
            f.hist_markets["LIVEM"] = m
            f.hist_candles[("LIVEM", 60)] = [hist_candle(c["end_period_ts"])
                                             for c in f.live_candles[("LIVEM", 60)]]
        return None

    f.hooks.append(move_on_candles)
    res = engine.ingestor.run(job(["LIVEM"], ["candles"]))
    assert res.status == "completed"
    unit = engine.store.conn.execute("SELECT * FROM work_units").fetchone()
    assert unit["tiers_used"] == "historical" and "retried historical" in unit["note"]


def test_interruption_then_resume_without_duplicates(tmp_path: Path, fake: FakeKalshi) -> None:
    setup_markets(fake)
    eng = make_engine(tmp_path, fake, page_limit=2)
    calls = {"n": 0}

    def interrupt(path: str, params: dict[str, str]) -> httpx.Response | None:
        if path == "/markets/trades":
            calls["n"] += 1
            if calls["n"] == 2:
                raise KeyboardInterrupt
        return None

    fake.hooks.append(interrupt)
    with pytest.raises(KeyboardInterrupt):
        eng.ingestor.run(job(["LIVEM"], ["trades"]))
    run = eng.store.list_runs()[0]
    assert run["status"] == "interrupted"
    unit = eng.store.conn.execute("SELECT * FROM work_units").fetchone()
    assert unit["status"] == "incomplete" and unit["cursor"] == "c2"  # checkpoint kept
    before = count(eng, "SELECT COUNT(*) FROM trades")
    fake.hooks.clear()
    fake.calls.clear()
    req = JobRequest.from_json(json.loads(run["spec_json"]))
    res = eng.ingestor.run(req, run_id=run["run_id"])
    assert res.status == "completed"
    live_calls = [q for p, q in fake.calls if p == "/markets/trades"]
    assert live_calls[0].get("cursor") == "c2"  # resumed from the checkpoint
    assert not [q for p, q in fake.calls if p == "/historical/trades"]  # finished phase skipped
    assert count(eng, "SELECT COUNT(*) FROM trades") == 11 > before
    eng.close()


def test_invalid_cursor_restarts_bounded_phase(tmp_path: Path, fake: FakeKalshi) -> None:
    setup_markets(fake)
    eng = make_engine(tmp_path, fake, page_limit=2)
    state = {"done": False}

    def reject(path: str, params: dict[str, str]) -> httpx.Response | None:
        if path == "/historical/trades" and params.get("cursor") == "c2" and not state["done"]:
            state["done"] = True
            return httpx.Response(400, json={"error": {"code": "bad_request",
                                                       "message": "invalid cursor"}})
        return None

    fake.hooks.append(reject)
    res = eng.ingestor.run(job(["LIVEM"], ["trades"]))
    assert res.status == "completed"
    unit = eng.store.conn.execute("SELECT * FROM work_units").fetchone()
    assert "restarted from first page" in unit["note"]
    assert count(eng, "SELECT COUNT(*) FROM trades") == 11
    eng.close()


def test_budget_exhaustion_is_incomplete_then_resumable(tmp_path: Path,
                                                        fake: FakeKalshi) -> None:
    setup_markets(fake)
    eng = make_engine(tmp_path, fake, budget=5, page_limit=2)
    res = eng.ingestor.run(job(["LIVEM"], ["trades"]))
    assert res.status == "incomplete" and "budget" in (res.message or "")
    assert run_status(eng.store, res.run_id)["unfinished"]
    eng.close()
    eng2 = make_engine(tmp_path, fake, page_limit=2)
    row = eng2.store.get_run(res.run_id)
    assert row is not None
    res2 = eng2.ingestor.run(JobRequest.from_json(json.loads(row["spec_json"])),
                             run_id=res.run_id)
    assert res2.status == "completed"
    assert count(eng2, "SELECT COUNT(*) FROM trades") == 11
    eng2.close()


def test_failed_request_distinct_from_empty_and_not_found(engine: Engine) -> None:
    setup_markets(engine.fake)
    engine.fake.add_market(market("QUIET", "EV2", created="2026-09-01T00:00:00Z"))

    def fail_arch(path: str, params: dict[str, str]) -> httpx.Response | None:
        if path == "/historical/trades" and params.get("ticker") == "ARCH":
            return httpx.Response(503)
        return None

    engine.fake.hooks.append(fail_arch)
    res = engine.ingestor.run(job(["ARCH", "QUIET", "NOPE"], ["trades"]))
    assert res.status == "incomplete"
    st = {r["ticker"]: r for r in engine.store.conn.execute("SELECT * FROM work_units")}
    assert st["ARCH"]["status"] == "failed" and "503" in st["ARCH"]["error"]
    assert st["QUIET"]["status"] == "complete" and st["QUIET"]["rows_received"] == 0
    assert "no trades returned" in st["QUIET"]["note"]
    assert st["NOPE"]["status"] == "not_found"
    quiet = market_coverage(engine.store, "QUIET")["datasets"][0]
    assert quiet["empty_windows"] and quiet["all_planned_requests_completed"]
    arch = market_coverage(engine.store, "ARCH")["datasets"][0]
    assert arch["unfinished_or_failed"] and not arch["all_planned_requests_completed"]


def test_rerun_skips_final_windows_and_refetches_recent(engine: Engine) -> None:
    setup_markets(engine.fake)
    r1 = engine.ingestor.run(job(["LIVEM"], ["trades"]))
    r2 = engine.ingestor.run(job(["LIVEM"], ["trades"]))
    assert r1.status == r2.status == "completed"
    assert r2.units == {"skipped": 1}
    assert count(engine, "SELECT COUNT(*) FROM trades") == 11
    # A window that reaches into the future is never final: re-fetched, candle revised.
    now = utcnow().replace(minute=0, second=0, microsecond=0)
    engine.fake.live_candles[("LIVEM", 60)] = [live_candle(int(now.timestamp()), "0.4000")]
    recent = job(["LIVEM"], ["candles"], start=now - timedelta(hours=2),
                 end=now + timedelta(days=1))
    engine.ingestor.run(recent)
    engine.fake.live_candles[("LIVEM", 60)] = [live_candle(int(now.timestamp()), "0.4600")]
    r4 = engine.ingestor.run(recent)
    assert r4.units == {"complete": 1}
    row = engine.store.conn.execute("SELECT * FROM candles WHERE trade_close='0.4600'").fetchone()
    assert row["revision"] == 2
    assert count(engine, "SELECT COUNT(*) FROM candle_revisions") == 1


def test_crash_between_raw_write_and_commit(tmp_path: Path, fake: FakeKalshi,
                                           monkeypatch: pytest.MonkeyPatch) -> None:
    setup_markets(fake)
    eng = make_engine(tmp_path, fake)
    original = eng.store.insert_trades

    def boom(*a: Any, **k: Any) -> Any:
        raise RuntimeError("simulated crash")

    monkeypatch.setattr(eng.store, "insert_trades", boom)
    with pytest.raises(RuntimeError):
        eng.ingestor.run(job(["ARCH"], ["trades"]))
    unit = eng.store.conn.execute("SELECT * FROM work_units").fetchone()
    assert unit["pages"] == 0 and unit["cursor"] is None  # checkpoint not advanced
    assert count(eng, "SELECT COUNT(*) FROM trades") == 0
    rs = eng.ingestor.raw
    orphans = find_orphans(rs, eng.store.raw_paths())
    assert len(orphans) == 1 and read_envelope(orphans[0])["path"] == "/historical/trades"
    monkeypatch.setattr(eng.store, "insert_trades", original)
    run = eng.store.list_runs()[0]
    res = eng.ingestor.run(JobRequest.from_json(json.loads(run["spec_json"])),
                           run_id=run["run_id"])
    assert res.status == "completed" and count(eng, "SELECT COUNT(*) FROM trades") == 3
    eng.close()


def test_cutoff_advance_during_run_is_reconciled(engine: Engine) -> None:
    setup_markets(engine.fake)
    f = engine.fake
    new_cut = "2026-09-20T00:02:30Z"

    def archive(path: str, params: dict[str, str]) -> httpx.Response | None:
        if path == "/markets/trades" and f.cutoff["trades_created_ts"] != new_cut:
            f.cutoff["trades_created_ts"] = new_cut
            moved = [t for t in f.live_trades if t["created_time"] < "2026-09-20T00:02:30"]
            f.live_trades = [t for t in f.live_trades if t not in moved]
            f.hist_trades += [t for t in moved if t["trade_id"] != "edge"]
            engine.ingestor._cutoff_at = datetime(2000, 1, 1, tzinfo=UTC)  # force refresh
        return None

    f.hooks.append(archive)
    res = engine.ingestor.run(job(["LIVEM"], ["trades"]))
    assert res.status == "completed"
    assert count(engine, "SELECT COUNT(*) FROM trades") == 11  # nothing lost in the handoff
    unit = engine.store.conn.execute("SELECT note FROM work_units").fetchone()
    assert "reconciled historical" in unit["note"]


def test_provenance_chain_from_row_to_raw_file(engine: Engine) -> None:
    setup_markets(engine.fake)
    engine.ingestor.run(job(["ARCH"], ["markets", "trades"]))
    row = engine.store.conn.execute(
        "SELECT t.trade_id, r.raw_path, r.sha256 FROM trades t JOIN requests r "
        "ON r.request_id = t.source_request_id LIMIT 1").fetchone()
    env = read_envelope(engine.settings.storage.raw_dir / row["raw_path"])
    assert env["sha256"] == row["sha256"]
    assert row["trade_id"] in env["body"]
    assert env["params"]["ticker"] == "ARCH"
    cut = engine.store.list_runs()[0]["cutoff_json"]
    assert json.loads(cut)["trades_created_ts"] == "2026-09-17T00:00:00Z"


def test_malformed_record_quarantined_not_dropped(engine: Engine) -> None:
    setup_markets(engine.fake)
    bad = trade("bad", "ARCH", ts("2026-09-02T00:00:10Z"), count="NaN-ish")
    engine.fake.hist_trades.append(bad)
    engine.ingestor.run(job(["ARCH"], ["trades"]))
    issue = engine.store.conn.execute(
        "SELECT * FROM validation_issues WHERE severity='error'").fetchone()
    assert issue["record_key"] == "bad" and "NaN-ish" in issue["raw_json"]
    assert count(engine, "SELECT COUNT(*) FROM trades WHERE trade_id='bad'") == 0


def test_orderbook_snapshot_and_derived_fields(engine: Engine) -> None:
    setup_markets(engine.fake)
    engine.fake.orderbooks["LIVEM"] = {"orderbook_fp": {
        "yes_dollars": [["0.4100", "5.00"], ["0.4200", "1.25"]],
        "no_dollars": [["0.5500", "3.00"]]}}
    snap = engine.ingestor.snapshot_orderbook(None, "LIVEM")
    assert str(snap["best_yes_bid"]) == "0.4200"
    assert str(snap["derived_yes_ask"]) == "0.4500"
    assert str(snap["derived_yes_spread"]) == "0.0300"
    assert snap["server_date"] and snap["received_at"] >= snap["requested_at"]
    assert count(engine, "SELECT COUNT(*) FROM orderbook_levels") == 3


def test_market_in_both_tiers_routes_candles_by_settlement(engine: Engine) -> None:
    # Observed live: markets settled just before the cutoff are served by both tiers.
    f = engine.fake
    m = market("OVERLAP", "EV3", created="2026-09-10T00:00:00Z",
               settled="2026-09-16T23:00:00Z")
    f.add_market(m)
    f.add_market(dict(m), historical=True)
    f.hist_candles[("OVERLAP", 60)] = [hist_candle(ts("2026-09-16T12:00:00Z"))]
    f.live_candles[("OVERLAP", 60)] = [live_candle(ts("2026-09-16T12:00:00Z"))]
    res = engine.ingestor.run(job(["OVERLAP"], ["candles"]))
    assert res.status == "completed"
    candle_paths = [p for p, _ in f.calls if p.endswith("candlesticks")]
    assert candle_paths == ["/historical/markets/OVERLAP/candlesticks"]
    unit = engine.store.conn.execute("SELECT * FROM work_units").fetchone()
    assert unit["tiers_used"] == "historical" and "settlement time" in unit["note"]
