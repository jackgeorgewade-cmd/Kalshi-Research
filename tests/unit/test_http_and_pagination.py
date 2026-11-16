"""Transport retries/pacing/budget and cursor pagination (mocked with respx)."""

from __future__ import annotations

import httpx
import pytest
import respx

from kalshi_data.config import ApiSettings
from kalshi_data.endpoints import paginate
from kalshi_data.errors import (
    AuthError,
    BadRequestError,
    BudgetExhaustedError,
    NotFoundError,
    PaginationError,
    RateLimitError,
    ServerError,
    TransportError,
)
from kalshi_data.http import RequestBudget, Transport, clean_params

BASE = "https://kalshi.test.invalid/trade-api/v2"


def make(
    max_retries: int = 2, budget: int | None = None, interval: float = 0.0
) -> tuple[Transport, list[float]]:
    sleeps: list[float] = []
    s = ApiSettings(
        base_url=BASE,
        max_retries=max_retries,
        backoff_initial=1.0,
        backoff_max=8.0,
        min_request_interval=interval,
    )
    clock = iter(range(0, 10_000))
    t = Transport(
        s,
        budget=RequestBudget(budget),
        sleep=sleeps.append,
        monotonic=lambda: float(next(clock)) * 0.0,
    )
    return t, sleeps


ERR = {"error": {"code": "too_many_requests", "message": "too many requests"}}


@respx.mock
def test_rate_limit_retried_then_success() -> None:
    route = respx.get(f"{BASE}/x").mock(
        side_effect=[httpx.Response(429, json=ERR), httpx.Response(200, json={"ok": True})]
    )
    t, sleeps = make()
    rec = t.get("/x")
    assert rec.json() == {"ok": True} and rec.attempts == 2
    assert route.call_count == 2 and len(sleeps) == 1 and 0.5 <= sleeps[0] <= 1.0
    assert t.stats.retries == 1 and t.stats.rate_limited == 1


@respx.mock
def test_retry_after_header_honoured() -> None:
    respx.get(f"{BASE}/x").mock(
        side_effect=[
            httpx.Response(503, headers={"Retry-After": "7"}),
            httpx.Response(200, json={}),
        ]
    )
    t, sleeps = make()
    t.get("/x")
    assert sleeps == [7.0]


@respx.mock
def test_rate_limit_exhaustion_raises_typed_error() -> None:
    route = respx.get(f"{BASE}/x").mock(return_value=httpx.Response(429, json=ERR))
    t, sleeps = make(max_retries=2)
    with pytest.raises(RateLimitError):
        t.get("/x")
    assert route.call_count == 3  # 1 + 2 retries, bounded
    assert sleeps[1] > sleeps[0] * 0.9  # exponential growth (with jitter)


@respx.mock
def test_server_error_exhaustion() -> None:
    respx.get(f"{BASE}/x").mock(return_value=httpx.Response(500))
    with pytest.raises(ServerError):
        make(max_retries=1)[0].get("/x")


@pytest.mark.parametrize(
    "status,exc", [(400, BadRequestError), (401, AuthError), (403, AuthError), (404, NotFoundError)]
)
@respx.mock
def test_non_retryable_statuses_fail_fast(status: int, exc: type[Exception]) -> None:
    route = respx.get(f"{BASE}/x").mock(
        return_value=httpx.Response(
            status, json={"error": {"code": "c", "message": "m", "details": "d"}}
        )
    )
    t, sleeps = make()
    with pytest.raises(exc) as info:
        t.get("/x")
    assert route.call_count == 1 and sleeps == []
    assert "c" in str(info.value) and "d" in str(info.value)


@respx.mock
def test_validation_error_body_shape_parsed() -> None:
    respx.get(f"{BASE}/x").mock(return_value=httpx.Response(400, json={"msg": "bad interval"}))
    with pytest.raises(BadRequestError, match="bad interval"):
        make()[0].get("/x")


@respx.mock
def test_network_errors_retried_then_typed() -> None:
    route = respx.get(f"{BASE}/x").mock(side_effect=httpx.ConnectError("boom"))
    with pytest.raises(TransportError):
        make(max_retries=1)[0].get("/x")
    assert route.call_count == 2


@respx.mock
def test_budget_counts_every_attempt() -> None:
    respx.get(f"{BASE}/x").mock(side_effect=[httpx.Response(429), httpx.Response(200, json={})])
    t, _ = make(budget=1)
    with pytest.raises(BudgetExhaustedError):
        t.get("/x")
    assert t.budget.used == 1


def test_pacing_enforces_min_interval() -> None:
    sleeps: list[float] = []
    now = [100.0]
    s = ApiSettings(base_url=BASE, min_request_interval=0.5)
    t = Transport(
        s,
        sleep=lambda x: (sleeps.append(x), now.__setitem__(0, now[0] + x)),
        monotonic=lambda: now[0],
    )
    t._pace()
    now[0] += 0.1
    t._pace()
    assert sleeps == [pytest.approx(0.4)]


def test_clean_params_drops_empty_filters() -> None:
    # Observed live: an empty event_ticker is ignored server-side -> unfiltered results.
    assert clean_params({"event_ticker": "", "x": None, "flag": True, "n": 5}) == {
        "flag": "true",
        "n": "5",
    }


# ------------------------------------------------------------------ pagination
@respx.mock
def test_multi_page_with_empty_page_and_preserved_filters() -> None:
    route = respx.get(f"{BASE}/markets/trades").mock(
        side_effect=[
            httpx.Response(200, json={"trades": [{"trade_id": "a"}], "cursor": "c1"}),
            httpx.Response(200, json={"trades": [], "cursor": "c2"}),  # empty page, more follows
            httpx.Response(200, json={"trades": [{"trade_id": "b"}], "cursor": ""}),
        ]
    )
    t, _ = make()
    pages = list(
        paginate(
            t,
            "/markets/trades",
            {"ticker": "M", "limit": 2},
            "trades",
            identity=lambda x: x["trade_id"],
        )
    )
    assert [len(p.items) for p in pages] == [1, 0, 1]
    assert pages[-1].next_cursor is None
    for call in route.calls:
        assert call.request.url.params["ticker"] == "M"
        assert call.request.url.params["limit"] == "2"
    assert route.calls[2].request.url.params["cursor"] == "c2"


@respx.mock
def test_short_page_is_not_completion() -> None:
    respx.get(f"{BASE}/t").mock(
        side_effect=[
            httpx.Response(200, json={"items": [1], "cursor": "c1"}),
            httpx.Response(200, json={"items": [2], "cursor": ""}),
        ]
    )
    pages = list(paginate(make()[0], "/t", {"limit": 1000}, "items"))
    assert len(pages) == 2


@respx.mock
def test_repeated_cursor_detected() -> None:
    respx.get(f"{BASE}/t").mock(
        side_effect=[
            httpx.Response(200, json={"items": [1], "cursor": "c1"}),
            httpx.Response(200, json={"items": [2], "cursor": "c1"}),
        ]
    )
    with pytest.raises(PaginationError, match="repeated"):
        list(paginate(make()[0], "/t", {}, "items"))


@respx.mock
def test_restart_from_first_page_detected() -> None:
    # Kalshi documents that an undecodable cursor silently returns page 1.
    respx.get(f"{BASE}/t").mock(
        side_effect=[
            httpx.Response(200, json={"items": [{"id": "x"}], "cursor": "c1"}),
            httpx.Response(200, json={"items": [{"id": "x"}], "cursor": "c2"}),
        ]
    )
    with pytest.raises(PaginationError, match="restarted"):
        list(paginate(make()[0], "/t", {}, "items", identity=lambda i: i["id"]))


@respx.mock
def test_max_pages_leaves_continuation_visible() -> None:
    respx.get(f"{BASE}/t").mock(
        side_effect=[httpx.Response(200, json={"items": [1], "cursor": "c1"})]
    )
    pages = list(paginate(make()[0], "/t", {}, "items", max_pages=1))
    assert pages[-1].next_cursor == "c1"  # caller must record incomplete


@respx.mock
def test_missing_items_key_is_format_error() -> None:
    from kalshi_data.errors import ResponseFormatError

    respx.get(f"{BASE}/t").mock(return_value=httpx.Response(200, json={"oops": []}))
    with pytest.raises(ResponseFormatError):
        list(paginate(make()[0], "/t", {}, "items"))
