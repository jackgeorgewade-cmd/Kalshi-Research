"""Reusable HTTP transport: pooling, timeouts, pacing, bounded retries.

Retry policy (all bounded by ``ApiSettings.max_retries``):

* Retried: connection errors and timeouts, HTTP 429, 500, 502, 503, 504.
* Not retried: every other status, notably 400 (bad request), 401/403
  (permission) and 404 (not found). Those are raised immediately as typed
  errors so they can never be mistaken for "no data".

Delay before retry *n* (1-based) is ``min(backoff_max, backoff_initial * 2**(n-1))``
scaled by a random jitter factor in [0.5, 1.0]. If the response carries a
``Retry-After`` header (Kalshi documents that 429s currently do *not*), its
value is used instead, capped at 300 s.

Every HTTP attempt, including retries, consumes one unit of the optional
request budget.
"""

from __future__ import annotations

import logging
import random
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from . import __version__
from .config import ApiSettings
from .decimals import loads_exact
from .errors import (
    APIError,
    AuthError,
    BadRequestError,
    BudgetExhaustedError,
    NotFoundError,
    RateLimitError,
    ResponseFormatError,
    ServerError,
    TransportError,
)
from .timeutil import utcnow

log = logging.getLogger("kalshi_data.http")

RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
# Response headers worth keeping for provenance (none of them are secret).
KEPT_HEADERS = ("date", "cache-control", "x-cache", "x-ratelimit-limit",
                "x-ratelimit-remaining", "x-ratelimit-reset", "x-ratelimit-cost")


class RequestBudget:
    """Counts HTTP attempts and stops collection when the limit is reached."""

    def __init__(self, limit: int | None) -> None:
        self.limit = limit
        self.used = 0

    @property
    def remaining(self) -> int | None:
        return None if self.limit is None else max(0, self.limit - self.used)

    def consume(self) -> None:
        if self.limit is not None and self.used >= self.limit:
            raise BudgetExhaustedError(f"Request budget of {self.limit} requests is spent")
        self.used += 1


@dataclass
class TransportStats:
    """Counters shown in progress displays and run summaries."""

    requests: int = 0
    retries: int = 0
    rate_limited: int = 0
    failures: int = 0


@dataclass
class ResponseRecord:
    """A successful response plus everything needed for provenance.

    ``path`` is relative to the base URL (e.g. ``/markets/trades``) and
    ``params`` are the exact non-secret query parameters sent.
    """

    request_id: str
    path: str
    params: dict[str, str]
    url: str
    status: int
    requested_at: datetime
    received_at: datetime
    body: bytes
    headers: dict[str, str]
    attempts: int
    _json: Any = field(default=None, repr=False)

    def json(self) -> Any:
        """Body decoded with Decimal for all non-integer numbers."""
        if self._json is None:
            try:
                self._json = loads_exact(self.body)
            except ValueError as exc:
                raise ResponseFormatError(f"Non-JSON body from {self.path}: {exc}") from exc
        return self._json


def clean_params(params: Mapping[str, Any] | None) -> dict[str, str]:
    """Drop None/empty values and render booleans as Kalshi expects.

    Dropping empty strings matters: we observed live that ``event_ticker=``
    (empty) is silently ignored and the *unfiltered* listing is returned.
    """
    out: dict[str, str] = {}
    for k, v in (params or {}).items():
        if v is None:
            continue
        if isinstance(v, bool):
            out[k] = "true" if v else "false"
        elif isinstance(v, str):
            if v.strip() == "":
                continue
            out[k] = v
        else:
            out[k] = str(v)
    return out


def _parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, (when - utcnow()).total_seconds())


def _error_from_response(status: int, path: str, body: bytes) -> APIError:
    code = message = details = None
    try:
        payload = loads_exact(body)
        err = payload.get("error", payload) if isinstance(payload, dict) else {}
        if isinstance(err, dict):
            code = err.get("code")
            message = err.get("message") or err.get("msg")
            details = err.get("details")
    except ValueError:
        message = body[:200].decode("utf-8", "replace") or None
    cls: type[APIError]
    if status == 400:
        cls = BadRequestError
    elif status in (401, 403):
        cls = AuthError
    elif status == 404:
        cls = NotFoundError
    elif status == 429:
        cls = RateLimitError
    elif status >= 500:
        cls = ServerError
    else:
        cls = APIError
    return cls(status, path, code=code, message=message,
               details=None if details is None else str(details))


class Transport:
    """Synchronous, paced, retrying HTTP GET client for the Kalshi REST API.

    Use as a context manager (or call :meth:`close`) to release pooled
    connections.
    """

    def __init__(
        self,
        settings: ApiSettings,
        *,
        budget: RequestBudget | None = None,
        client: httpx.Client | None = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        rng: random.Random | None = None,
    ) -> None:
        self.settings = settings
        self.budget = budget or RequestBudget(None)
        self.stats = TransportStats()
        self._sleep = sleep
        self._monotonic = monotonic
        self._rng = rng or random.Random()
        self._last_start: float | None = None
        self._client = client or httpx.Client(
            base_url=settings.base_url,
            timeout=httpx.Timeout(settings.read_timeout, connect=settings.connect_timeout),
            headers={"User-Agent": f"kalshi-data-research/{__version__}",
                     "Accept": "application/json"},
            limits=httpx.Limits(max_keepalive_connections=4, max_connections=4),
        )

    def __enter__(self) -> Transport:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    def _pace(self) -> None:
        """Enforce the minimum interval between request starts."""
        interval = self.settings.min_request_interval
        now = self._monotonic()
        if self._last_start is not None and interval > 0:
            wait = self._last_start + interval - now
            if wait > 0:
                self._sleep(wait)
                now = self._monotonic()
        self._last_start = now

    def _delay(self, attempt: int, retry_after: float | None) -> float:
        if retry_after is not None:
            return min(retry_after, 300.0)
        base = min(self.settings.backoff_max,
                   self.settings.backoff_initial * (2 ** (attempt - 1)))
        return float(base * self._rng.uniform(0.5, 1.0))

    def get(self, path: str, params: Mapping[str, Any] | None = None) -> ResponseRecord:
        """GET ``path`` (relative to the base URL) with bounded retries.

        Returns:
            ResponseRecord for a 2xx response.

        Raises:
            BudgetExhaustedError: before sending if the budget is spent.
            BadRequestError, AuthError, NotFoundError, APIError: non-retryable statuses.
            RateLimitError, ServerError: retryable statuses after retries ran out.
            TransportError: network failures after retries ran out.
        """
        sent = clean_params(params)
        request_id = uuid.uuid4().hex
        attempt = 0
        while True:
            attempt += 1
            self.budget.consume()
            self._pace()
            requested_at = utcnow()
            self.stats.requests += 1
            log.debug("GET %s %s (attempt %d)", path, sent, attempt)
            retry_after: float | None = None
            try:
                resp = self._client.get(path, params=sent)
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                if attempt > self.settings.max_retries:
                    self.stats.failures += 1
                    raise TransportError(
                        f"{type(exc).__name__} for {path} after {attempt} attempts: {exc}"
                    ) from exc
                delay = self._delay(attempt, None)
                log.warning("Network error on %s (%s); retry %d in %.1fs",
                            path, type(exc).__name__, attempt, delay)
                self.stats.retries += 1
                self._sleep(delay)
                continue
            received_at = utcnow()
            if 200 <= resp.status_code < 300:
                headers = {k: resp.headers[k] for k in KEPT_HEADERS if k in resp.headers}
                return ResponseRecord(
                    request_id=request_id, path=path, params=sent,
                    url=str(resp.request.url), status=resp.status_code,
                    requested_at=requested_at, received_at=received_at,
                    body=resp.content, headers=headers, attempts=attempt,
                )
            if resp.status_code == 429:
                self.stats.rate_limited += 1
            if resp.status_code in RETRYABLE_STATUS and attempt <= self.settings.max_retries:
                retry_after = _parse_retry_after(resp.headers.get("retry-after"))
                delay = self._delay(attempt, retry_after)
                log.warning("HTTP %d on %s; retry %d in %.1fs",
                            resp.status_code, path, attempt, delay)
                self.stats.retries += 1
                self._sleep(delay)
                continue
            self.stats.failures += 1
            raise _error_from_response(resp.status_code, path, resp.content)
