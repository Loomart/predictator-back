"""
Polymarket real data source.

Fetches market data from Polymarket API using requests.
Implements MarketSource protocol with defensive error handling.
"""

import os
from datetime import datetime, timezone
from typing import Any

import requests

from .market_source import (
    MarketSource,
    MarketWithSnapshot,
    NormalizedMarket,
    NormalizedSnapshot,
)


class PolymarketSource(MarketSource):
    """Real Polymarket API data source using requests."""

    def __init__(self):
        """Initialize with environment variable for endpoint."""
        self.markets_url = os.getenv("POLYMARKET_MARKETS_URL")
        if not self.markets_url:
            placeholder = "https://api.polymarket.com/markets"
            raise ValueError(
                f"POLYMARKET_MARKETS_URL environment variable not set. "
                f"Please set it to the Polymarket API endpoint, e.g., {placeholder}"
            )

    def fetch_markets(self) -> list[MarketWithSnapshot]:
        """Fetch and normalize markets from Polymarket API.

        Returns:
            List of normalized markets with snapshots.
        """
        try:
            raw_data = self.fetch_raw_data()
        except Exception as e:
            print(f"Failed to fetch raw data from Polymarket API: {e}")
            return []

        normalized = []
        for item in raw_data:
            try:
                market_with_snapshot = self.normalize_item(item)
                normalized.append(market_with_snapshot)
            except Exception as e:
                print(f"Failed to normalize item {item.get('id', 'unknown')}: {e}")
                continue

        return normalized

    def fetch_raw_data(self) -> list[dict[str, Any]]:
        """Fetch raw market data from Polymarket API.

        Returns:
            List of raw market dictionaries.

        Raises:
            requests.RequestException: If the HTTP request fails.
        """
        response = requests.get(self.markets_url, timeout=30)
        response.raise_for_status()
        return response.json()

    def normalize_item(self, raw_item: dict[str, Any]) -> MarketWithSnapshot:
        """Normalize a single raw market item into MarketWithSnapshot.

        Args:
            raw_item: Raw market data from API.

        Returns:
            Normalized market with snapshot.
        """
        # Extract basic market info
        external_id = raw_item.get("id") or raw_item.get("slug", "unknown")
        title = raw_item.get("question") or raw_item.get("title", "Unknown Market")
        slug = raw_item.get("slug")
        category = raw_item.get("category")
        status = raw_item.get("status", "open") if raw_item.get("active", True) else "closed"

        # Parse resolution date
        resolution_date = None
        end_date_str = raw_item.get("endDate") or raw_item.get("end_date")
        if end_date_str:
            try:
                # Assume ISO format, adjust if needed
                resolution_date = datetime.fromisoformat(end_date_str.replace('Z', '+00:00'))
            except ValueError:
                print(f"Could not parse resolution date '{end_date_str}' for market {external_id}")

        # Extract prices
        prices = raw_item.get("prices", {})
        yes_price = prices.get("yes") or raw_item.get("lastTradePrice")
        if yes_price is not None:
            yes_price = float(yes_price)
            no_price = 1.0 - yes_price
        else:
            no_price = None

        # Extract spread, volume, liquidity
        spread = None
        best_bid = raw_item.get("best_bid")
        best_ask = raw_item.get("best_ask")
        if best_bid is not None and best_ask is not None:
            best_bid = float(best_bid)
            best_ask = float(best_ask)
            spread = best_ask - best_bid

        volume_24h = raw_item.get("volume24hr") or raw_item.get("volume24h") or raw_item.get("volume")
        if volume_24h is not None:
            volume_24h = float(volume_24h)

        liquidity = raw_item.get("liquidity")
        if liquidity is not None:
            liquidity = float(liquidity)

        # Create normalized objects
        market = NormalizedMarket(
            external_id=str(external_id),
            platform="polymarket",
            title=str(title),
            slug=slug,
            category=category,
            status=str(status),
            resolution_date=resolution_date,
        )

        snapshot = NormalizedSnapshot(
            yes_price=yes_price,
            no_price=no_price,
            spread=spread,
            volume_24h=volume_24h,
            liquidity=liquidity,
            best_bid=best_bid,
            best_ask=best_ask,
        )

        return MarketWithSnapshot(
            market=market,
            snapshot=snapshot,
            captured_at=datetime.now(timezone.utc),
        )