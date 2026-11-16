"""OPT-IN live smoke test against the real Kalshi API (public, read-only endpoints).

Run with::

    KALSHI_LIVE_TESTS=1 uv run pytest tests/live -v -s

Optional: ``KALSHI_DATA_BASE_URL`` to use another documented host.

Constraints: strict request budget (60), short timeouts, a ticker discovered
dynamically (never hard-coded), both live and archived paths attempted, no
exchange-wide crawl. A market with no trades is NOT a failure; unexpected
schemas/routing ARE failures. The exercised paths are printed and written to
``<tmp>/live_paths.json``.
"""

from __future__ import annotations

import json
import os
from datetime import timedelta
from pathlib import Path

import pytest

from kalshi_data.config import load_config
from kalshi_data.coverage import market_coverage
from kalshi_data.endpoints import KalshiClient, Tier
from kalshi_data.http import RequestBudget, Transport
from kalshi_data.ingest import Ingestor, JobRequest
from kalshi_data.rawstore import RawStore
from kalshi_data.routing import Cutoff
from kalshi_data.storage import Store
from kalshi_data.timeutil import utcnow

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        os.environ.get("KALSHI_LIVE_TESTS") != "1",
        reason="live API test; set KALSHI_LIVE_TESTS=1 to run",
    ),
]

BUDGET = 60


def test_live_smoke(tmp_path: Path) -> None:
    over = {
        "api": {
            "read_timeout": 15.0,
            "connect_timeout": 10.0,
            "max_retries": 3,
            "min_request_interval": 0.6,
        },
        "storage": {"data_dir": str(tmp_path)},
    }
    s = load_config(overrides=over).settings
    exercised: dict[str, object] = {"base_url": s.api.base_url}
    with Transport(s.api, budget=RequestBudget(BUDGET)) as t, Store(s.storage.db_path) as st:
        client = KalshiClient(t, 100)
        ing = Ingestor(s, st, RawStore(s.storage.raw_dir), client)
        cutoff = Cutoff.from_body(client.historical_cutoff().json())
        exercised["cutoff"] = cutoff.raw

        # Archived path: first archived market from /historical/markets (1 page only).
        hist = next(client.market_pages(Tier.HISTORICAL, max_pages=1))
        assert hist.items, "historical market listing returned nothing"
        archived = next(
            (m for m in hist.items if m.get("volume_fp") not in (None, "0.00")), hist.items[0]
        )
        # Live path: a settled-but-not-archived market from /markets (1 page only).
        live = next(client.market_pages(Tier.LIVE, status="settled", max_pages=1))
        assert live.items, "live settled listing returned nothing"
        recent = live.items[0]
        exercised["archived_ticker"] = archived["ticker"]
        exercised["live_ticker"] = recent["ticker"]

        for m in (archived, recent):
            settle = m.get("settlement_ts") or m.get("close_time")
            from kalshi_data.timeutil import parse_api_time

            end = parse_api_time(settle) or utcnow()
            req = JobRequest(
                kind="live-smoke",
                start=end - timedelta(days=2),
                end=end + timedelta(hours=1),
                datasets=["trades", "candles"],
                tickers=[m["ticker"]],
                candle_interval=60,
            )
            res = ing.run(req)
            assert res.status == "completed", (m["ticker"], res)
            cov = market_coverage(st, m["ticker"])
            for d in cov["datasets"]:
                assert d["all_planned_requests_completed"], d
            exercised[m["ticker"]] = {
                d["dataset"]: {"rows": d.get("rows_stored"), "tiers": d["tiers_used"]}
                for d in cov["datasets"]
            }
        issues = st.conn.execute(
            "SELECT code, COUNT(*) FROM validation_issues WHERE severity='error' GROUP BY code"
        ).fetchall()
        assert not issues, f"schema errors on live data: {issues}"
        exercised["requests_used"] = t.budget.used
    (tmp_path / "live_paths.json").write_text(json.dumps(exercised, indent=2, default=str))
    print(json.dumps(exercised, indent=2, default=str))
    arch = exercised[str(exercised["archived_ticker"])]
    assert isinstance(arch, dict)
    # An archived (settled before the cutoff) market must use the historical candle path.
    assert arch["candles"]["tiers"] == ["historical"], arch
    assert "historical" in arch["trades"]["tiers"], arch
