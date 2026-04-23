"""
Market data ingestion layer.

Provides normalized data structures and services for market data synchronization.
Decoupled from database/ORM concerns.
"""

from .market_source import (
    MarketSource,
    MarketWithSnapshot,
    NormalizedMarket,
    NormalizedSnapshot,
)
from .mock_source import MockMarketSource
from .polymarket_client import PolymarketClient
from .semireal_source import SemiRealMarketSource
from .sync_markets import sync_market_data

__all__ = [
    "MarketSource",
    "NormalizedMarket",
    "NormalizedSnapshot",
    "MarketWithSnapshot",
    "MockMarketSource",
    "PolymarketClient",
    "SemiRealMarketSource",
    "sync_market_data",
]
