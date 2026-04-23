"""
Mock market data source for testing and development.

Provides realistic production-ready sample market data without requiring external API calls.
"""

from datetime import datetime, timedelta, timezone

from .market_source import (
    MarketSource,
    MarketWithSnapshot,
    NormalizedMarket,
    NormalizedSnapshot,
)


class MockMarketSource(MarketSource):
    """Mock implementation of MarketSource with realistic production data."""

    def fetch_markets(self) -> list[MarketWithSnapshot]:
        """Fetch realistic sample markets with coherent snapshots.

        Returns three distinct markets:
        - Bitcoin (BTC) price prediction
        - Ethereum (ETH) price prediction
        - Macro event (Fed interest rate decision)

        Returns:
            List of markets with realistic, coherent snapshots.
        """
        now = datetime.now(timezone.utc)

        return [
            self._create_btc_market(now),
            self._create_eth_market(now),
            self._create_fed_decision_market(now),
        ]

    @staticmethod
    def _create_btc_market(now: datetime) -> MarketWithSnapshot:
        """Create realistic BTC price prediction market.

        BTC: High liquidity, tight spread, balanced odds.
        """
        market = NormalizedMarket(
            external_id="btc_over_50k_q2_2026",
            platform="polymarket",
            title="Will Bitcoin (BTC) close above $50k by end of Q2 2026?",
            slug="btc-above-50k-q2-2026",
            category="Crypto",
            status="open",
            resolution_date=now + timedelta(days=68),
            metadata={
                "creator": "crypto_markets_bot",
                "underlying": "BTC",
                "strike_price": 50000,
                "pool_size": 500000,
            },
        )

        snapshot = NormalizedSnapshot(
            yes_price=0.62,
            no_price=0.38,
            spread=0.02,  # Tight spread - high liquidity
            volume_24h=145000.50,
            liquidity=520000.00,
            best_bid=0.61,
            best_ask=0.63,
            metadata={
                "source": "polymarket_api",
                "last_update": now.isoformat(),
                "trading_activity": "high",
            },
        )

        return MarketWithSnapshot(
            market=market,
            snapshot=snapshot,
            captured_at=now,
        )

    @staticmethod
    def _create_eth_market(now: datetime) -> MarketWithSnapshot:
        """Create realistic ETH price prediction market.

        ETH: Medium liquidity, moderate spread, slightly bullish.
        """
        market = NormalizedMarket(
            external_id="eth_over_3k_q2_2026",
            platform="polymarket",
            title="Will Ethereum (ETH) close above $3,000 by end of Q2 2026?",
            slug="eth-above-3k-q2-2026",
            category="Crypto",
            status="open",
            resolution_date=now + timedelta(days=68),
            metadata={
                "creator": "crypto_markets_bot",
                "underlying": "ETH",
                "strike_price": 3000,
                "pool_size": 250000,
            },
        )

        snapshot = NormalizedSnapshot(
            yes_price=0.58,
            no_price=0.42,
            spread=0.04,  # Moderate spread - medium liquidity
            volume_24h=78500.75,
            liquidity=265000.00,
            best_bid=0.56,
            best_ask=0.60,
            metadata={
                "source": "polymarket_api",
                "last_update": now.isoformat(),
                "trading_activity": "medium",
            },
        )

        return MarketWithSnapshot(
            market=market,
            snapshot=snapshot,
            captured_at=now,
        )

    @staticmethod
    def _create_fed_decision_market(now: datetime) -> MarketWithSnapshot:
        """Create realistic macro event market (Fed decision).

        Fed: Lower liquidity, higher spread, uncertain outcome.
        """
        next_fomc = now + timedelta(days=45)  # Next FOMC meeting

        market = NormalizedMarket(
            external_id="fed_rate_cut_may_2026",
            platform="polymarket",
            title="Will the Federal Reserve cut rates at the May 2026 FOMC meeting?",
            slug="fed-rate-cut-may-2026",
            category="Macro",
            status="open",
            resolution_date=next_fomc,
            metadata={
                "creator": "macro_events_bot",
                "event_type": "FOMC_Decision",
                "meeting_date": next_fomc.isoformat(),
                "pool_size": 85000,
            },
        )

        snapshot = NormalizedSnapshot(
            yes_price=0.45,
            no_price=0.55,
            spread=0.08,  # Wide spread - low liquidity, high uncertainty
            volume_24h=12500.25,
            liquidity=88000.00,
            best_bid=0.42,
            best_ask=0.50,
            metadata={
                "source": "polymarket_api",
                "last_update": now.isoformat(),
                "trading_activity": "low",
                "uncertainty": "high",
            },
        )

        return MarketWithSnapshot(
            market=market,
            snapshot=snapshot,
            captured_at=now,
        )
