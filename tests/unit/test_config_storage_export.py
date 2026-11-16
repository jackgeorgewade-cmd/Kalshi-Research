"""Configuration precedence, idempotent storage, atomic transactions, exports."""

from __future__ import annotations

import csv
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from fixtures.synthetic import live_candle, trade
from kalshi_data.config import load_config
from kalshi_data.errors import ConfigError
from kalshi_data.export import export
from kalshi_data.normalize import parse_candle, parse_trade
from kalshi_data.storage import Store

T0 = datetime(2026, 1, 1, tzinfo=UTC)


# ------------------------------------------------------------------ config
def test_config_precedence(tmp_path: Path) -> None:
    f = tmp_path / "c.toml"
    f.write_text(
        '[api]\nmin_request_interval = 2.0\nmax_retries = 7\n[storage]\ndata_dir = "from_file"\n'
    )
    env = {"KALSHI_DATA_MIN_REQUEST_INTERVAL": "3.0"}
    cfg = load_config(f, {"api": {"min_request_interval": None, "max_retries": 9}}, env=env)
    s = cfg.settings
    assert s.api.min_request_interval == 3.0  # env beats file
    assert s.api.max_retries == 9  # argument beats file
    assert s.storage.data_dir == Path("from_file")  # file beats default
    assert s.api.read_timeout == 30.0  # default
    assert cfg.sources[0] == "defaults" and "arguments" in cfg.sources


def test_config_rejects_bad_values_and_unknown_keys(tmp_path: Path) -> None:
    f = tmp_path / "c.toml"
    f.write_text("[api]\npage_limit = 5000\n")
    with pytest.raises(ConfigError):
        load_config(f, env={})
    f.write_text("[api]\nunknown = 1\n")
    with pytest.raises(ConfigError):
        load_config(f, env={})
    with pytest.raises(ConfigError):
        load_config(tmp_path / "missing.toml", env={})


def test_job_validation(tmp_path: Path) -> None:
    f = tmp_path / "j.toml"
    f.write_text(
        '[job]\ntickers=["A"]\nstart="2025-01-01"\nend="2025-02-01"\n'
        'datasets=["trades","candles"]\ncandle_interval=1440\n'
    )
    job = load_config(f, env={}).job
    assert job is not None and job.start == datetime(2025, 1, 1, tzinfo=UTC)
    f.write_text('[job]\ntickers=["A"]\nstart=2025-01-01T00:00:00\nend="2025-02-01"\n')
    with pytest.raises(ConfigError):  # naive TOML datetime
        load_config(f, env={})
    f.write_text('[job]\ntickers=["A"]\nstart="2025-01-01"\nend="2025-02-01"\ncandle_interval=5\n')
    with pytest.raises(ConfigError):
        load_config(f, env={})
    f.write_text('[job]\nstart="2025-01-01"\nend="2025-02-01"\n')
    with pytest.raises(ConfigError):  # no scope
        load_config(f, env={})


# ----------------------------------------------------------------- storage
def _trade(tid: str, price: str = "0.4200") -> dict[str, object]:
    rec, _ = parse_trade(trade(tid, "M", T0.timestamp() + 1, price))
    assert rec is not None
    return rec


def test_trades_idempotent_and_conflicts_recorded(tmp_path: Path) -> None:
    with Store(tmp_path / "db.sqlite") as st:
        with st.transaction():
            assert st.insert_trades(
                [_trade("a"), _trade("b")],
                tier="live",
                request_id="r1",
                retrieved_at=T0,
                parser_version="1",
                run_id="x",
            ) == (2, 0, 0)
        with st.transaction():
            assert st.insert_trades(
                [_trade("a"), _trade("b", "0.5000")],
                tier="live",
                request_id="r2",
                retrieved_at=T0,
                parser_version="1",
                run_id="x",
            ) == (0, 1, 1)
        row = st.conn.execute(
            "SELECT yes_price, source_request_id FROM trades WHERE trade_id='b'"
        ).fetchone()
        assert row["yes_price"] == "0.4200" and row["source_request_id"] == "r1"  # first kept
        issue = st.conn.execute("SELECT * FROM validation_issues").fetchone()
        assert issue["code"] == "source_conflict" and "r1" in issue["message"]


def test_candle_revisions_versioned(tmp_path: Path) -> None:
    def c(close: str) -> dict[str, object]:
        rec, _ = parse_candle(live_candle(int(T0.timestamp()), close), "M", 60, "live")
        assert rec is not None
        return rec

    t1, t2 = datetime(2025, 12, 31, 23, 30, tzinfo=UTC), datetime(2026, 1, 1, 2, tzinfo=UTC)
    with Store(tmp_path / "db.sqlite") as st:
        with st.transaction():
            assert st.upsert_candles(
                [c("0.4000")], tier="live", request_id="r1", retrieved_at=t1, parser_version="1"
            ) == (1, 0, 0)
        row = st.conn.execute("SELECT * FROM candles").fetchone()
        assert row["period_complete"] == 0  # retrieved before the period ended
        with st.transaction():
            assert st.upsert_candles(
                [c("0.4500")], tier="live", request_id="r2", retrieved_at=t2, parser_version="1"
            ) == (0, 1, 0)
        with st.transaction():
            assert st.upsert_candles(
                [c("0.4500")], tier="live", request_id="r3", retrieved_at=t2, parser_version="1"
            ) == (0, 0, 1)
        row = st.conn.execute("SELECT * FROM candles").fetchone()
        assert row["trade_close"] == "0.4500" and row["revision"] == 2
        assert row["period_complete"] == 1
        old = st.conn.execute("SELECT * FROM candle_revisions").fetchone()
        assert old["revision"] == 1 and old["source_request_id"] == "r1"


def test_transaction_rollback_keeps_checkpoint(tmp_path: Path) -> None:
    with Store(tmp_path / "db.sqlite") as st:
        with st.transaction():
            st.create_run("r", "test", {}, {})
            u = st.ensure_unit("r", "k", "trades", "M", None, T0, T0)
        with pytest.raises(RuntimeError), st.transaction():
            st.insert_trades(
                [_trade("a")],
                tier="live",
                request_id="q",
                retrieved_at=T0,
                parser_version="1",
                run_id="r",
            )
            st.update_unit(u["unit_id"], cursor="next", pages=1)
            raise RuntimeError("crash before commit")
        assert st.conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0] == 0
        assert st.get_unit(u["unit_id"])["cursor"] is None


# ------------------------------------------------------------------ export
def _seed(st: Store) -> None:
    recs = [_trade("a", "0.0001"), _trade("b", "0.9999")]
    recs[1]["no_price"] = None  # NULL must survive exports
    recs[0]["count"] = Decimal("34.46")
    with st.transaction():
        st.insert_request(
            request_id="r1",
            run_id="x",
            path="/markets/trades",
            params={},
            url=None,
            status=200,
            requested_at=T0,
            received_at=T0,
            attempts=1,
            tier="live",
            raw_path="x/r1.json.gz",
            sha256="0",
            body_bytes=1,
            parser_version="1",
            item_count=2,
        )
        st.insert_trades(
            recs, tier="live", request_id="r1", retrieved_at=T0, parser_version="1", run_id="x"
        )


def test_csv_and_parquet_preserve_precision_nulls_and_provenance(tmp_path: Path) -> None:
    with Store(tmp_path / "db.sqlite") as st:
        _seed(st)
        assert export(st, "trades", tmp_path / "t.csv", "csv") == 2
        assert export(st, "trades", tmp_path / "t.parquet", "parquet") == 2
    rows = list(csv.DictReader((tmp_path / "t.csv").open()))
    assert rows[0]["yes_price"] == "0.0001" and rows[0]["count"] == "34.46"
    assert rows[1]["no_price"] == ""  # NULL -> empty
    assert rows[0]["raw_path"] == "x/r1.json.gz" and rows[0]["source_request_id"] == "r1"
    table = pq.read_table(tmp_path / "t.parquet").to_pylist()
    assert table[0]["yes_price"] == Decimal("0.0001")
    assert table[0]["count"] == Decimal("34.46")
    assert table[1]["no_price"] is None
    assert table[0]["is_block_trade"] is False
    assert table[0]["created_time"].tzinfo is not None
    assert (tmp_path / "t.parquet.meta.json").exists()


def test_empty_export_has_header(tmp_path: Path) -> None:
    with Store(tmp_path / "db.sqlite") as st:
        assert export(st, "candles", tmp_path / "c.csv", "csv") == 0
        assert export(st, "candles", tmp_path / "c.parquet", "parquet") == 0
    assert "end_period_ts" in (tmp_path / "c.csv").read_text()
    assert "yes_bid_close" in pq.read_table(tmp_path / "c.parquet").schema.names
