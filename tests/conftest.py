"""Shared offline test helpers. All HTTP is served by the SYNTHETIC FakeKalshi."""

from __future__ import annotations

import sys
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).parent))

from fixtures.synthetic import BASE, FakeKalshi
from kalshi_data.config import Settings, load_config
from kalshi_data.endpoints import KalshiClient
from kalshi_data.http import RequestBudget, Transport
from kalshi_data.ingest import Ingestor
from kalshi_data.rawstore import RawStore
from kalshi_data.storage import Store


def make_settings(data_dir: Path, **api: object) -> Settings:
    over: dict[str, dict[str, object]] = {
        "api": {"base_url": BASE, "min_request_interval": 0.0, "backoff_initial": 0.01,
                "backoff_max": 0.02, "max_retries": 2, **api},
        "storage": {"data_dir": str(data_dir)},
    }
    return load_config(overrides=over, env={}).settings


@dataclass
class Engine:
    fake: FakeKalshi
    settings: Settings
    store: Store
    transport: Transport
    client: KalshiClient
    ingestor: Ingestor
    sleeps: list[float]

    def close(self) -> None:
        self.transport.close()
        self.store.close()


def make_engine(tmp_path: Path, fake: FakeKalshi, budget: int | None = None,
                **api: object) -> Engine:
    settings = make_settings(tmp_path / "data", **api)
    sleeps: list[float] = []
    http = httpx.Client(base_url=settings.api.base_url, transport=fake.transport())
    transport = Transport(settings.api, budget=RequestBudget(budget), client=http,
                          sleep=sleeps.append)
    client = KalshiClient(transport, settings.api.page_limit)
    store = Store(settings.storage.db_path)
    ing = Ingestor(settings, store, RawStore(settings.storage.raw_dir), client)
    return Engine(fake, settings, store, transport, client, ing, sleeps)


@pytest.fixture
def fake() -> FakeKalshi:
    return FakeKalshi()


@pytest.fixture
def engine(tmp_path: Path, fake: FakeKalshi) -> Iterator[Engine]:
    e = make_engine(tmp_path, fake)
    yield e
    e.close()
