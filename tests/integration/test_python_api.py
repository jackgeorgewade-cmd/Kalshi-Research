"""Python API against the SYNTHETIC FakeKalshi: streaming iterators and LocalStore."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from conftest import make_settings
from fixtures.synthetic import CUTOFF_TS, FakeKalshi, hist_candle, live_candle, market, trade, ts
from kalshi_data import KalshiData, LocalStore
from kalshi_data.endpoints import KalshiClient
from kalshi_data.errors import ConfigError, NotFoundError
from kalshi_data.http import Transport


def api(tmp_path: Path, fake: FakeKalshi) -> KalshiData:
    settings = make_settings(tmp_path / "data")
    kd = KalshiData(settings)
    kd.transport.close()
    kd.transport = Transport(
        settings.api,
        client=httpx.Client(base_url=settings.api.base_url, transport=fake.transport()),
        sleep=lambda s: None,
    )
    kd.client = KalshiClient(kd.transport, settings.api.page_limit)
    return kd


@pytest.fixture
def served() -> FakeKalshi:
    f = FakeKalshi()
    f.add_market(market("LIVEM", "EV1", created="2026-09-01T00:00:00Z"))
    edge = trade("edge", "LIVEM", CUTOFF_TS)
    f.hist_trades += [trade("h1", "LIVEM", ts("2026-09-10T00:00:00Z")), edge]
    f.live_trades += [dict(edge), trade("l1", "LIVEM", ts("2026-09-20T00:00:00Z"))]
    f.live_candles[("LIVEM", 60)] = [live_candle(ts("2026-09-10T01:00:00Z"))]
    f.add_market(market("ARCH", "EV0", settled="2026-09-05T00:00:00Z"), historical=True)
    f.hist_candles[("ARCH", 60)] = [hist_candle(ts("2026-09-02T01:00:00Z"))]
    return f


def test_iter_trades_streams_across_tiers_without_storing(
    tmp_path: Path, served: FakeKalshi
) -> None:
    with api(tmp_path, served) as kd:
        trades = list(kd.iter_trades("LIVEM", "2026-09-01", "2026-10-01"))
    assert sorted(t["trade_id"] for t in trades) == ["edge", "h1", "l1"]  # edge once
    assert {t["tier"] for t in trades} == {"historical", "live"}
    assert not (tmp_path / "data" / "kalshi.sqlite").exists()  # nothing persisted


def test_iter_candles_routes_by_settlement(tmp_path: Path, served: FakeKalshi) -> None:
    with api(tmp_path, served) as kd:
        live = list(kd.iter_candles("LIVEM", "2026-09-01", "2026-10-01", 60))
        arch = list(kd.iter_candles("ARCH", "2026-09-01", "2026-10-01", 60))
        with pytest.raises(ConfigError):
            list(kd.iter_candles("ARCH", "2026-09-01", "2026-10-01", 5))
        with pytest.raises(NotFoundError):
            kd.market("NOPE")
    assert live[0]["tier"] == "live" and arch[0]["tier"] == "historical"
    assert arch[0]["trade_close"] == Decimal("0.4200")


def test_collect_then_local_store_is_offline(tmp_path: Path, served: FakeKalshi) -> None:
    with api(tmp_path, served) as kd:
        run = kd.collect(
            tickers=["LIVEM"],
            start="2026-09-01",
            end="2026-10-01",
            datasets=["markets", "trades", "candles"],
        )
    assert run.status == "completed"
    served.hooks.append(lambda p, q: pytest.fail("LocalStore must not use the network"))
    with LocalStore(tmp_path / "data") as db:
        rows = list(db.rows("trades", tickers=["LIVEM"]))
        assert isinstance(rows[0]["yes_price"], Decimal)
        df = db.to_dataframe("candles", numeric=True)
        assert df["yes_bid_close"].dtype.kind == "f"
        assert db.search("Synthetic")["markets"]
        assert db.coverage("LIVEM")["datasets"]
