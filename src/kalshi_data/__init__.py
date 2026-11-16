"""Kalshi Research Data Engine.

Discover Kalshi markets and retrieve, store, inspect and export public market
data (metadata, trades, candlesticks, order book snapshots) for research.

Main entry points:

* :class:`kalshi_data.api.KalshiData`   - remote retrieval + persistent collection.
* :class:`kalshi_data.api.LocalStore`   - offline queries of the local SQLite store.
"""

__version__ = "0.1.0"

__all__ = ["KalshiData", "LocalStore", "__version__"]


def __getattr__(name: str) -> object:
    # Lazy import keeps `import kalshi_data` cheap and avoids import cycles.
    if name in ("KalshiData", "LocalStore"):
        from . import api

        return getattr(api, name)
    raise AttributeError(name)
