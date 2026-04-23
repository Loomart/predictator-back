"""
Market data normalization layer.

Defines normalized data structures and protocols for market data ingestion.
This layer is agnostic to SQLAlchemy and database-specific details.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Protocol


@dataclass
class NormalizedMarket:
    """Normalized market data from any external source."""

    external_id: str
    """Unique identifier from the external source."""

    platform: str
    """Platform identifier (e.g., 'polymarket', 'manifold')."""

    title: str
    """Market title/question."""

    slug: str | None = None
    """Market slug/URL identifier."""

    category: str | None = None
    """Market category or theme."""

    status: str = "open"
    """Market status (open, closed, resolved, etc.)."""

    resolution_date: datetime | None = None
    """Expected resolution date."""

    metadata: dict = field(default_factory=dict)
    """Additional platform-specific metadata."""


@dataclass
class NormalizedSnapshot:
    """Normalized market snapshot (price/liquidity data at a point in time)."""

    yes_price: float | None = None
    """Price for YES outcome."""

    no_price: float | None = None
    """Price for NO outcome."""

    spread: float | None = None
    """Bid-ask spread."""

    volume_24h: float | None = None
    """24-hour trading volume."""

    liquidity: float | None = None
    """Current liquidity pool."""

    best_bid: float | None = None
    """Best bid price."""

    best_ask: float | None = None
    """Best ask price."""

    metadata: dict = field(default_factory=dict)
    """Additional platform-specific metrics."""


@dataclass
class MarketWithSnapshot:
    """Complete market data with current snapshot."""

    market: NormalizedMarket
    snapshot: NormalizedSnapshot
    captured_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    """Timestamp when this data was captured."""


class MarketSource(Protocol):
    """Protocol for market data sources.

    Implement this to create custom market data fetchers (APIs, databases, etc).
    """

    def fetch_markets(self) -> list[MarketWithSnapshot]:
        """Fetch all available markets with their current snapshots.

        Returns:
            List of markets with their current snapshots.

        Raises:
            Exception: If fetching fails.
        """
        ...
