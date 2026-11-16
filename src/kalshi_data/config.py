"""Configuration.

Precedence (highest wins):

1. Explicit arguments (CLI flags or Python keyword arguments).
2. Environment variables (``KALSHI_DATA_*``, see ``ENV_VARS``).
3. The TOML file passed with ``--config`` (or ``KALSHI_DATA_CONFIG``).
4. Built-in defaults below.

Pacing and retry defaults are *our* conservative settings for unauthenticated
public access, not a Kalshi guarantee. Kalshi documents that unauthenticated
requests have separate (unpublished) limits; we observed HTTP 429 after a
short unpaced burst.
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from .errors import ConfigError
from .timeutil import parse_user_time

DEFAULT_BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
# Documented by Kalshi as "also supported" for production.
ALTERNATE_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

VALID_INTERVALS = (1, 60, 1440)
DATASETS = ("markets", "trades", "candles", "orderbook")


class ApiSettings(BaseModel):
    """HTTP/API behaviour."""

    model_config = ConfigDict(extra="forbid")

    base_url: str = DEFAULT_BASE_URL
    connect_timeout: float = Field(10.0, gt=0, description="seconds")
    read_timeout: float = Field(30.0, gt=0, description="seconds")
    min_request_interval: float = Field(
        0.5, ge=0, description="Minimum seconds between request starts (our pacing setting)."
    )
    max_retries: int = Field(5, ge=0, le=20)
    backoff_initial: float = Field(1.0, gt=0, description="seconds before first retry")
    backoff_max: float = Field(60.0, gt=0, description="cap on a single retry delay, seconds")
    page_limit: int = Field(1000, ge=1, le=1000, description="Kalshi documents max 1000")

    @field_validator("base_url")
    @classmethod
    def _strip(cls, v: str) -> str:
        if not v.startswith("https://"):
            raise ValueError("base_url must start with https://")
        return v.rstrip("/")


class StorageSettings(BaseModel):
    """Local storage locations and retention."""

    model_config = ConfigDict(extra="forbid")

    data_dir: Path = Path("data")
    raw_retention: Literal["keep", "none"] = "keep"

    @property
    def db_path(self) -> Path:
        return self.data_dir / "kalshi.sqlite"

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def lock_path(self) -> Path:
        return self.data_dir / ".writer.lock"


class CollectionSettings(BaseModel):
    """Defaults that shape ingestion runs."""

    model_config = ConfigDict(extra="forbid")

    request_budget: int | None = Field(
        None, ge=1, description="Max HTTP requests per run; None = unlimited."
    )
    overlap_seconds: int = Field(
        3600,
        ge=0,
        description=(
            "A completed window is only treated as final if it was fetched at least this "
            "long after its end; otherwise it is re-fetched on the next run to pick up late "
            "trades and revised recent candles."
        ),
    )
    max_markets_per_series: int = Field(
        500, ge=1, description="Safety guard when expanding a series into markets."
    )


class Settings(BaseModel):
    """All engine settings."""

    model_config = ConfigDict(extra="forbid")

    api: ApiSettings = ApiSettings()
    storage: StorageSettings = StorageSettings()
    collection: CollectionSettings = CollectionSettings()


class JobSpec(BaseModel):
    """A collection job (the ``[job]`` table of a collection config)."""

    model_config = ConfigDict(extra="forbid")

    name: str = "collection"
    tickers: list[str] = []
    series: list[str] = []
    events: list[str] = []
    start: datetime
    end: datetime
    datasets: list[Literal["markets", "trades", "candles", "orderbook"]] = [
        "markets",
        "trades",
        "candles",
    ]
    candle_interval: int = 60

    @field_validator("start", "end", mode="before")
    @classmethod
    def _parse_time(cls, v: Any) -> datetime:
        if isinstance(v, datetime) and v.tzinfo is None:
            # TOML local datetimes are naive; refuse rather than guess.
            raise ValueError("datetime must include an offset, e.g. 2025-01-01T00:00:00Z")
        return parse_user_time(v)

    @field_validator("candle_interval")
    @classmethod
    def _interval(cls, v: int) -> int:
        if v not in VALID_INTERVALS:
            raise ValueError(f"candle_interval must be one of {VALID_INTERVALS}")
        return v

    @model_validator(mode="after")
    def _check(self) -> JobSpec:
        if self.end <= self.start:
            raise ValueError("end must be after start")
        if not (self.tickers or self.series or self.events):
            raise ValueError("job needs at least one of tickers, series or events")
        return self


# Environment variable -> (section, key, type)
ENV_VARS: dict[str, tuple[str, str, type]] = {
    "KALSHI_DATA_BASE_URL": ("api", "base_url", str),
    "KALSHI_DATA_MIN_REQUEST_INTERVAL": ("api", "min_request_interval", float),
    "KALSHI_DATA_MAX_RETRIES": ("api", "max_retries", int),
    "KALSHI_DATA_DIR": ("storage", "data_dir", str),
    "KALSHI_DATA_RAW_RETENTION": ("storage", "raw_retention", str),
    "KALSHI_DATA_REQUEST_BUDGET": ("collection", "request_budget", int),
}


@dataclass
class LoadedConfig:
    """Settings plus the optional job and where each came from."""

    settings: Settings
    job: JobSpec | None = None
    sources: list[str] = field(default_factory=list)


def _deep_merge(base: dict[str, Any], top: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for k, v in top.items():
        if isinstance(v, dict):
            # Recurse even if the section is new so that None leaves are dropped.
            base_sec = out.get(k)
            out[k] = _deep_merge(base_sec if isinstance(base_sec, dict) else {}, v)
        elif v is not None:
            out[k] = v
    return out


def load_config(
    config_path: Path | None = None,
    overrides: dict[str, dict[str, Any]] | None = None,
    env: dict[str, str] | None = None,
) -> LoadedConfig:
    """Load settings using the documented precedence.

    Args:
        config_path: TOML file; falls back to ``$KALSHI_DATA_CONFIG`` if unset.
        overrides: ``{"api": {"min_request_interval": 1.0}, ...}`` from CLI/kwargs;
            ``None`` values are ignored.
        env: environment mapping (defaults to ``os.environ``; injectable for tests).

    Raises:
        ConfigError: unreadable file or invalid values.
    """
    env = dict(os.environ) if env is None else env
    sources = ["defaults"]
    data: dict[str, Any] = {}
    job_data: dict[str, Any] | None = None
    env_path = env.get("KALSHI_DATA_CONFIG")
    path = config_path or (Path(env_path) if env_path else None)
    if path is not None:
        if not path.is_file():
            raise ConfigError(f"Config file not found: {path}")
        try:
            raw = tomllib.loads(path.read_text(encoding="utf-8"))
        except tomllib.TOMLDecodeError as exc:
            raise ConfigError(f"Invalid TOML in {path}: {exc}") from exc
        job_data = raw.pop("job", None)
        data = _deep_merge(data, raw)
        sources.append(f"file:{path}")
    env_part: dict[str, Any] = {}
    for name, (section, key, typ) in ENV_VARS.items():
        if name in env and env[name] != "":
            try:
                env_part.setdefault(section, {})[key] = typ(env[name])
            except ValueError as exc:
                raise ConfigError(f"Invalid value for {name}: {env[name]!r}") from exc
            sources.append(f"env:{name}")
    data = _deep_merge(data, env_part)
    if overrides:
        data = _deep_merge(data, overrides)
        if any(v is not None for sec in overrides.values() for v in sec.values()):
            sources.append("arguments")
    try:
        settings = Settings.model_validate(data)
        job = JobSpec.model_validate(job_data) if job_data is not None else None
    except ValidationError as exc:
        raise ConfigError(f"Invalid configuration:\n{exc}") from exc
    return LoadedConfig(settings=settings, job=job, sources=sources)
