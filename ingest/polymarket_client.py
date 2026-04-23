"""
Polymarket client adapter.

Implements MarketSource and simulates HTTP fetching for future API integration.
The current implementation returns a mocked raw response, then normalizes it into
NormalizedMarket and NormalizedSnapshot objects.
"""

from datetime import datetime, timezone
from typing import Any

from .market_source import (
    MarketSource,
    MarketWithSnapshot,
    NormalizedMarket,
    NormalizedSnapshot,
)


class PolymarketClient(MarketSource):
    """Adapter for a Polymarket-style API.

    This client is designed so the HTTP fetch layer stays separate from parsing
    and normalization. It is ready to swap the simulated fetch with requests or
    httpx in the future.
    """

    def fetch_markets(self) -> list[MarketWithSnapshot]:
        """Fetch markets from the Polymarket API and normalize them."""
        raw_payload = self.fetch_raw_data()
        return [self._normalize_market(raw_item) for raw_item in raw_payload]

    def fetch_raw_data(self) -> list[dict[str, Any]]:
        """Simulate an HTTP response from a market API.

        Returns sample raw JSON data with the structure expected from the parser.
        """
        now = datetime.now(timezone.utc)

        return [
            {
                "id": "pm_btc_2026_q2",
                "platform": "polymarket",
                "question": "Will Bitcoin close above $50k by the end of Q2 2026?",
                "slug": "btc-above-50k-q2-2026",
                "category": "Crypto",
                "status": "open",
                "resolution_time": (now.replace(microsecond=0)).isoformat(),
                "prices": {
                    "yes": 0.61,
                    "no": 0.39,
                    "spread": 0.02,
                    "best_bid": 0.60,
                    "best_ask": 0.62,
                },
                "volume_24h": 132000.0,
                "liquidity": 508000.0,
            },
            {
                "id": "pm_eth_2026_q2",
                "platform": "polymarket",
                "question": "Will Ethereum close above $3,000 by the end of Q2 2026?",
                "slug": "eth-above-3k-q2-2026",
                "category": "Crypto",
                "status": "open",
                "resolution_time": (now.replace(microsecond=0)).isoformat(),
                "prices": {
                    "yes": 0.57,
                    "no": 0.43,
                    "spread": 0.04,
                    "best_bid": 0.56,
                    "best_ask": 0.60,
                },
                "volume_24h": 82000.0,
                "liquidity": 275000.0,
            },
        ]

    def _normalize_market(self, raw_market: dict[str, Any]) -> MarketWithSnapshot:
        """Normalize a raw market payload into domain objects."""
        market = NormalizedMarket(
            external_id=str(raw_market["id"]),
            platform=str(raw_market.get("platform", "polymarket")),
            title=str(raw_market["question"]),
            slug=raw_market.get("slug"),
            category=raw_market.get("category"),
            status=str(raw_market.get("status", "open")),
            resolution_date=self._parse_datetime(raw_market.get("resolution_time")),
            metadata={
                "raw_source": "polymarket",
                "raw_payload_id": raw_market.get("id"),
            },
        )

        snapshot = NormalizedSnapshot(
            yes_price=self._parse_float(raw_market["prices"].get("yes")),
            no_price=self._parse_float(raw_market["prices"].get("no")),
            spread=self._parse_float(raw_market["prices"].get("spread")),
            volume_24h=self._parse_float(raw_market.get("volume_24h")),
            liquidity=self._parse_float(raw_market.get("liquidity")),
            best_bid=self._parse_float(raw_market["prices"].get("best_bid")),
            best_ask=self._parse_float(raw_market["prices"].get("best_ask")),
            metadata={"source": "polymarket_raw"},
        )

        return MarketWithSnapshot(market=market, snapshot=snapshot)

    @staticmethod
    def _parse_datetime(value: Any) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(str(value))
        except ValueError:
            return None

    @staticmethod
    def _parse_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
