"""
Semi-real market data source.

Loads market data from a local JSON file or configurable URL.
Prepared for easy transition to real API integration.
"""

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List
from urllib.request import urlopen

from .market_source import (
    MarketSource,
    MarketWithSnapshot,
    NormalizedMarket,
    NormalizedSnapshot,
)


class SemiRealMarketSource(MarketSource):
    """Market source that loads data from JSON file or URL.

    Configurable via environment variables:
    - SEMIREAL_DATA_URL: URL to fetch JSON data from
    - SEMIREAL_DATA_FILE: Local JSON file path (fallback if URL not set)

    If neither is set, uses 'example_payload.json' in the same directory.
    """

    def fetch_markets(self) -> list[MarketWithSnapshot]:
        """Fetch markets from configured source and normalize them."""
        try:
            raw_data = self.fetch_raw_data()
            markets_data = raw_data.get("markets", [])

            if not markets_data:
                print("[SEMIREAL] No markets found in data source")
                return []

            markets = []
            for raw_market in markets_data:
                try:
                    normalized_market = self.normalize_market(raw_market)
                    normalized_snapshot = self.normalize_snapshot(raw_market.get("snapshot", {}))
                    captured_at = self._parse_datetime(raw_market.get("captured_at"))

                    market_with_snapshot = MarketWithSnapshot(
                        market=normalized_market,
                        snapshot=normalized_snapshot,
                        captured_at=captured_at,
                    )
                    markets.append(market_with_snapshot)
                except Exception as e:
                    print(f"[SEMIREAL] Error processing market {raw_market.get('external_id', 'unknown')}: {e}")
                    continue

            print(f"[SEMIREAL] Successfully loaded {len(markets)} markets")
            return markets

        except Exception as e:
            print(f"[SEMIREAL] Failed to fetch markets: {e}")
            raise

    def fetch_raw_data(self) -> Dict[str, Any]:
        """Fetch raw JSON data from configured source."""
        data_url = os.getenv("SEMIREAL_DATA_URL")
        data_file = os.getenv("SEMIREAL_DATA_FILE", "example_payload.json")

        if data_url:
            print(f"[SEMIREAL] Fetching data from URL: {data_url}")
            try:
                with urlopen(data_url) as response:
                    return json.loads(response.read().decode('utf-8'))
            except Exception as e:
                print(f"[SEMIREAL] Failed to fetch from URL: {e}")
                raise
        else:
            # Use local file
            file_path = os.path.join(os.path.dirname(__file__), data_file)
            print(f"[SEMIREAL] Loading data from file: {file_path}")
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except FileNotFoundError:
                raise FileNotFoundError(f"Data file not found: {file_path}")
            except Exception as e:
                print(f"[SEMIREAL] Failed to load from file: {e}")
                raise

    def normalize_market(self, raw_market: Dict[str, Any]) -> NormalizedMarket:
        """Convert raw market dict to NormalizedMarket."""
        return NormalizedMarket(
            external_id=str(raw_market.get("external_id", "")),
            platform=str(raw_market.get("platform", "unknown")),
            title=str(raw_market.get("title", "")),
            slug=raw_market.get("slug"),
            category=raw_market.get("category"),
            status=str(raw_market.get("status", "open")),
            resolution_date=self._parse_datetime(raw_market.get("resolution_date")),
            metadata=raw_market.get("metadata", {}),
        )

    def normalize_snapshot(self, raw_snapshot: Dict[str, Any]) -> NormalizedSnapshot:
        """Convert raw snapshot dict to NormalizedSnapshot."""
        return NormalizedSnapshot(
            yes_price=self._safe_float(raw_snapshot.get("yes_price")),
            no_price=self._safe_float(raw_snapshot.get("no_price")),
            spread=self._safe_float(raw_snapshot.get("spread")),
            volume_24h=self._safe_float(raw_snapshot.get("volume_24h")),
            liquidity=self._safe_float(raw_snapshot.get("liquidity")),
            best_bid=self._safe_float(raw_snapshot.get("best_bid")),
            best_ask=self._safe_float(raw_snapshot.get("best_ask")),
            metadata=raw_snapshot.get("metadata", {}),
        )

    def _safe_float(self, value: Any) -> float | None:
        """Safely convert value to float, returning None if invalid."""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _parse_datetime(self, date_str: str | None) -> datetime | None:
        """Parse ISO datetime string to datetime object."""
        if not date_str:
            return None
        try:
            # Assume ISO format
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            return None