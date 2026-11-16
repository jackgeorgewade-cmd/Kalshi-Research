"""Exact parsing of Kalshi fixed-point values.

Kalshi sends prices as dollar strings (``"0.1200"``, up to 4 decimals for
prices, up to 6 in some monetary responses) and contract quantities as
fixed-point strings (``"34.46"``). A few metadata fields (``floor_strike``,
``cap_strike``) are JSON numbers. We never route any of these through
``float``:

* Response bodies are decoded with ``json.loads(..., parse_float=Decimal)``
  (see :func:`loads_exact`) so JSON numbers become Decimal directly.
* Strings are converted with ``Decimal(str)``.

In SQLite we store the canonical decimal *text* (``str(Decimal)``), which is
lossless. ``None``/missing stays ``None``; it is never replaced with zero.
"""

from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation
from typing import Any


def loads_exact(data: bytes | str) -> Any:
    """Decode JSON, turning every non-integer number into a Decimal."""
    return json.loads(data, parse_float=Decimal)


def to_decimal(value: Any) -> Decimal | None:
    """Convert an API value into a Decimal without passing through float.

    Returns None for None. Raises ValueError for empty strings, booleans,
    floats (which would already have lost precision) and non-numeric text.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"Boolean is not a decimal value: {value!r}")
    if isinstance(value, Decimal):
        result = value
    elif isinstance(value, int):
        result = Decimal(value)
    elif isinstance(value, str):
        if value.strip() == "":
            raise ValueError("Empty string is not a decimal value")
        try:
            result = Decimal(value.strip())
        except InvalidOperation as exc:
            raise ValueError(f"Not a decimal string: {value!r}") from exc
    elif isinstance(value, float):
        raise ValueError(
            "Float input refused: decode JSON with loads_exact() so numbers arrive as Decimal"
        )
    else:
        raise ValueError(f"Unsupported decimal input type {type(value).__name__}")
    if not result.is_finite():
        raise ValueError(f"Non-finite decimal: {value!r}")
    return result


def dec_text(value: Decimal | None) -> str | None:
    """Canonical lossless text for storage (keeps trailing zeros as sent)."""
    return None if value is None else str(value)


def json_default(obj: Any) -> Any:
    """``json.dumps`` default that keeps Decimals exact (as strings)."""
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def dumps_exact(obj: Any, **kwargs: Any) -> str:
    """JSON-encode with Decimals rendered as exact strings."""
    return json.dumps(obj, default=json_default, **kwargs)
