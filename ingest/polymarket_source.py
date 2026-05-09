"""
Polymarket real data source.

Fetches market data from Polymarket API using requests.
Implements MarketSource protocol with defensive error handling.
"""

import os
import json
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
        category = self._extract_category(raw_item)
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
        yes_price, no_price = self._extract_yes_no_prices(raw_item)

        # Extract spread, volume, liquidity
        spread = None
        best_bid, best_ask = self._extract_best_bid_ask(raw_item)
        if best_bid is not None and best_ask is not None:
            spread = best_ask - best_bid

        volume_24h = self._as_float(
            raw_item.get("volume24hr")
            or raw_item.get("volume24h")
            or raw_item.get("volume")
            or raw_item.get("volumeNum")
        )
        liquidity = self._as_float(raw_item.get("liquidity") or raw_item.get("liquidityNum"))

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

    @staticmethod
    def _as_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _extract_category(raw_item: dict[str, Any]) -> str | None:
        direct = raw_item.get("category") or raw_item.get("group")
        if isinstance(direct, str) and direct.strip():
            return direct.strip()

        tags = raw_item.get("tags")
        if isinstance(tags, list):
            for tag in tags:
                if isinstance(tag, str) and tag.strip():
                    return tag.strip()
                if isinstance(tag, dict):
                    name = tag.get("name") or tag.get("slug") or tag.get("label")
                    if isinstance(name, str) and name.strip():
                        return name.strip()
        return None

    def _extract_yes_no_prices(self, raw_item: dict[str, Any]) -> tuple[float | None, float | None]:
        prices = raw_item.get("prices")
        if isinstance(prices, dict):
            yes = self._as_float(prices.get("yes"))
            no = self._as_float(prices.get("no"))
            if yes is not None and no is None:
                no = max(0.0, min(1.0, 1.0 - yes))
            if no is not None and yes is None:
                yes = max(0.0, min(1.0, 1.0 - no))
            if yes is not None or no is not None:
                return yes, no

        outcome_prices = raw_item.get("outcomePrices")
        parsed = outcome_prices
        if isinstance(outcome_prices, str):
            try:
                parsed = json.loads(outcome_prices)
            except json.JSONDecodeError:
                parsed = None
        if isinstance(parsed, list) and len(parsed) >= 2:
            yes = self._as_float(parsed[0])
            no = self._as_float(parsed[1])
            return yes, no

        yes = self._as_float(raw_item.get("yes_price") or raw_item.get("yesPrice"))
        no = self._as_float(raw_item.get("no_price") or raw_item.get("noPrice"))
        last_trade = self._as_float(raw_item.get("lastTradePrice") or raw_item.get("last_trade_price"))
        if yes is None and no is None and last_trade is not None:
            yes = last_trade
            no = max(0.0, min(1.0, 1.0 - yes))
        return yes, no

    def _extract_best_bid_ask(self, raw_item: dict[str, Any]) -> tuple[float | None, float | None]:
        best_bid = self._as_float(
            raw_item.get("best_bid")
            or raw_item.get("bestBid")
            or raw_item.get("bid")
        )
        best_ask = self._as_float(
            raw_item.get("best_ask")
            or raw_item.get("bestAsk")
            or raw_item.get("ask")
        )
        return best_bid, best_ask
