"""Typed exceptions used throughout the engine.

The hierarchy lets callers distinguish states that matter for research
integrity, for example "the market does not exist" (NotFoundError) versus
"the request failed" (TransientError after retries) versus "no rows were
returned" (not an exception at all: an empty, successful result).
"""

from __future__ import annotations


class KalshiDataError(Exception):
    """Base class for all errors raised by this package."""


class ConfigError(KalshiDataError):
    """Invalid configuration or user input detected before any network call."""


class InvalidTimestampError(ConfigError):
    """A timestamp was ambiguous (naive) or could not be parsed."""


class LockError(KalshiDataError):
    """Another process holds the single-writer lock on the data directory."""


class APIError(KalshiDataError):
    """An HTTP response with a non-success status from the Kalshi API.

    Attributes:
        status: HTTP status code.
        code: Kalshi error code from the JSON body (``error.code``), if any.
        message: Human-readable message from the body, if any.
        details: Additional details from the body, if any.
        path: Request path (no host, no secrets).
    """

    def __init__(
        self,
        status: int,
        path: str,
        code: str | None = None,
        message: str | None = None,
        details: str | None = None,
    ) -> None:
        self.status = status
        self.path = path
        self.code = code
        self.message = message
        self.details = details
        text = f"HTTP {status} for {path}"
        if code:
            text += f" [{code}]"
        if message:
            text += f": {message}"
        if details:
            text += f" ({details})"
        super().__init__(text)


class BadRequestError(APIError):
    """HTTP 400. Not retried: the request itself must change."""


class AuthError(APIError):
    """HTTP 401/403. Not retried and never converted into 'no data'."""


class NotFoundError(APIError):
    """HTTP 404, e.g. a market that is not present in the queried tier."""


class RateLimitError(APIError):
    """HTTP 429 after retries were exhausted."""


class ServerError(APIError):
    """HTTP 5xx after retries were exhausted."""


class TransportError(KalshiDataError):
    """Connection/timeout failure after retries were exhausted."""


class BudgetExhaustedError(KalshiDataError):
    """The explicit request budget for this run has been spent.

    Collection stops and the affected work is recorded as incomplete,
    never as complete.
    """


class PaginationError(KalshiDataError):
    """The API returned a cursor sequence that cannot be trusted.

    Raised when a cursor repeats (which would loop forever) or when the API
    appears to have restarted from the first page (Kalshi documents that an
    undecodable cursor is silently ignored and the first page returned).
    """


class CursorRejectedError(PaginationError):
    """The API rejected (HTTP 400) a request that carried a pagination cursor,
    e.g. an expired cursor. The bounded unit is restarted from its first page."""


class ResponseFormatError(KalshiDataError):
    """A successful response did not have the documented envelope shape."""
