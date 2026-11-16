"""CLI behaviour against the SYNTHETIC FakeKalshi (served through respx)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import respx
from typer.testing import CliRunner

from fixtures.synthetic import BASE, FakeKalshi, market, trade, ts
from kalshi_data.cli import app

runner = CliRunner()


def cli(tmp_path: Path, *args: str) -> tuple[int, str]:
    res = runner.invoke(
        app,
        ["--data-dir", str(tmp_path / "data"), "--base-url", BASE, "--min-interval", "0", *args],
    )
    return res.exit_code, res.output


@pytest.fixture
def served() -> FakeKalshi:
    fake = FakeKalshi()
    fake.add_market(
        market("ARCH", "EV0", created="2026-09-01T00:00:00Z", settled="2026-09-05T00:00:00Z"),
        historical=True,
    )
    fake.hist_trades += [trade(f"a{i}", "ARCH", ts("2026-09-02T00:00:00Z") + i) for i in range(3)]
    fake.series = [
        {
            "ticker": "SYNINX",
            "title": "S&P 500 range (synthetic)",
            "category": "Financials",
            "tags": ["S&P 500"],
            "frequency": "daily",
        },
        {"ticker": "SYNWX", "title": "Weather (synthetic)", "category": "Climate"},
    ]
    return fake


def test_help_lists_every_command() -> None:
    res = runner.invoke(app, ["--help"])
    assert res.exit_code == 0
    for cmd in (
        "doctor",
        "discover",
        "markets",
        "market",
        "fetch",
        "collect",
        "poll",
        "resume",
        "runs",
        "status",
        "coverage",
        "issues",
        "export",
        "reconcile",
    ):
        assert cmd in res.output
    assert "trades" in runner.invoke(app, ["fetch", "--help"]).output


@pytest.mark.parametrize(
    "args",
    [
        ["fetch", "trades", "--ticker", "X", "--start", "2025-02-01", "--end", "2025-01-01"],
        ["fetch", "trades", "--ticker", "X", "--start", "2025-01-01T00:00", "--end", "2025-02-01"],
        [
            "fetch",
            "candles",
            "--ticker",
            "X",
            "--start",
            "2025-01-01",
            "--end",
            "2025-02-01",
            "--interval",
            "5",
        ],
        ["poll", "--ticker", "X"],
        ["markets"],
        ["export", "--dataset", "nope", "--output", "x.csv"],
        ["collect"],
    ],
)
def test_invalid_input_exit_2_without_network(tmp_path: Path, args: list[str]) -> None:
    with respx.mock(assert_all_called=False) as router:
        route = router.route(host="kalshi.test.invalid")
        code, out = cli(tmp_path, *args)
    assert code == 2, out
    assert route.call_count == 0


def test_dry_run_makes_no_requests(tmp_path: Path) -> None:
    with respx.mock(assert_all_called=False) as router:
        route = router.route(host="kalshi.test.invalid")
        code, out = cli(
            tmp_path,
            "fetch",
            "candles",
            "--ticker",
            "X",
            "--start",
            "2024-01-01",
            "--end",
            "2025-01-01",
            "--interval",
            "1",
            "--dry-run",
        )
    assert code == 0 and "Dry run" in out and route.call_count == 0


@respx.mock
def test_fetch_status_coverage_export_flow(tmp_path: Path, served: FakeKalshi) -> None:
    respx.route(host="kalshi.test.invalid").mock(side_effect=served.handler)
    code, out = cli(
        tmp_path,
        "fetch",
        "trades",
        "--ticker",
        "ARCH",
        "--start",
        "2026-09-01",
        "--end",
        "2026-10-01",
    )
    assert code == 0, out
    code, out = cli(tmp_path, "runs", "--json")
    run_id = json.loads(out)[0]["run_id"]
    code, out = cli(tmp_path, "status", run_id, "--json")
    assert code == 0 and json.loads(out)["rows_new"] == 3
    code, out = cli(tmp_path, "coverage", "--ticker", "ARCH", "--json")
    cov = json.loads(out)["datasets"][0]
    assert cov["rows_stored"] == 3 and cov["tiers_used"] == ["historical"]
    dest = tmp_path / "out" / "t.parquet"
    code, out = cli(
        tmp_path,
        "export",
        "--dataset",
        "trades",
        "--ticker",
        "ARCH",
        "--format",
        "parquet",
        "--output",
        str(dest),
    )
    assert code == 0 and dest.exists()


@respx.mock
def test_missing_market_exit_code(tmp_path: Path, served: FakeKalshi) -> None:
    respx.route(host="kalshi.test.invalid").mock(side_effect=served.handler)
    code, out = cli(tmp_path, "market", "NOPE")
    assert code == 3 and "not found" in out
    code, out = cli(
        tmp_path,
        "fetch",
        "trades",
        "--ticker",
        "NOPE",
        "--start",
        "2026-09-01",
        "--end",
        "2026-09-02",
    )
    assert code == 1  # run recorded as incomplete (not_found), not as "no data"


@respx.mock
def test_discover_filters_locally(tmp_path: Path, served: FakeKalshi) -> None:
    respx.route(host="kalshi.test.invalid").mock(side_effect=served.handler)
    code, out = cli(tmp_path, "discover", "--query", "s&p 500", "--json")
    assert code == 0
    assert [s["ticker"] for s in json.loads(out)] == ["SYNINX"]
    code, out = cli(tmp_path, "discover", "--query", "volcano", "--json")
    assert json.loads(out) == []


@respx.mock
def test_doctor_offline_and_lock(tmp_path: Path) -> None:
    code, out = cli(tmp_path, "doctor", "--offline", "--json")
    assert code == 0 and all(c["ok"] for c in json.loads(out))
    (tmp_path / "data" / ".writer.lock").write_text(
        json.dumps({"pid": 1, "host": "another-host", "since": "x"})
    )
    code, out = cli(tmp_path, "market", "ARCH")
    assert code == 4 and "locked" in out
