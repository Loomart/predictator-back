"""Backend package exports."""

import os

from .ingest.market_source import (
    MarketSource,
    MarketWithSnapshot,
    NormalizedMarket,
    NormalizedSnapshot,
)
from .ingest.mock_source import MockMarketSource
from .ingest.polymarket_client import PolymarketClient
from .ingest.polymarket_source import PolymarketSource
from .ingest.semireal_source import SemiRealMarketSource
from .ingest.sync_markets import sync_market_data


def get_market_source() -> MarketSource:
    """Get market source based on MARKET_SOURCE environment variable.

    Supported values:
    - "mock": Use MockMarketSource (default if not set)
    - "semireal": Use SemiRealMarketSource
    - "polymarket": Use PolymarketSource

    Returns:
        MarketSource instance.

    Raises:
        ValueError: If MARKET_SOURCE has an invalid value.
    """
    source_type = os.getenv("MARKET_SOURCE", "mock").lower()

    if source_type == "mock":
        return MockMarketSource()
    elif source_type == "semireal":
        return SemiRealMarketSource()
    elif source_type == "polymarket":
        return PolymarketSource()
    else:
        raise ValueError(
            f"Invalid MARKET_SOURCE '{source_type}'. "
            "Supported values: 'mock', 'semireal', 'polymarket'"
        )


__all__ = [
    "MarketSource",
    "NormalizedMarket",
    "NormalizedSnapshot",
    "MarketWithSnapshot",
    "MockMarketSource",
    "PolymarketClient",
    "SemiRealMarketSource",
    "sync_market_data",
    "get_market_source",
]
