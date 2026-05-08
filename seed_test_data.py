from datetime import datetime, timedelta, UTC

from backend.db import SessionLocal
from backend.models import Market, MarketSnapshot, Signal


def seed():
    db = SessionLocal()

    try:
        market = Market(
            external_id="test-btc-105k-eom",
            platform="polymarket",
            title="BTC above 105k by end of month?",
            slug="btc-above-105k-eom",
            category="crypto",
            status="open",
            resolution_date=datetime.now(UTC).replace(tzinfo=None) + timedelta(days=20),
        )
        db.add(market)
        db.commit()
        db.refresh(market)

        snapshot = MarketSnapshot(
            market_id=market.id,
            yes_price=0.62,
            no_price=0.38,
            spread=0.02,
            volume_24h=125000.0,
            liquidity=54000.0,
            best_bid=0.61,
            best_ask=0.63,
        )
        db.add(snapshot)

        signal = Signal(
            market_id=market.id,
            signal_type="ENTER",
            strategy_name="microstructure_v1",
            confidence=0.74,
            edge_estimate=0.035,
            reason="Spread acceptable, liquidity sufficient, short-term momentum favorable.",
            is_executed=False,
        )
        db.add(signal)

        db.commit()
        print("Datos de prueba insertados correctamente.")

    finally:
        db.close()


if __name__ == "__main__":
    seed()