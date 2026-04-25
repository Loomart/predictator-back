from db import SessionLocal
from models import Market, MarketSnapshot


def main():
    db = SessionLocal()

    try:
        markets = db.query(Market).all()
        snapshots = db.query(MarketSnapshot).all()

        latest_by_market = {}

        for snapshot in snapshots:
            current = latest_by_market.get(snapshot.market_id)
            if current is None or snapshot.captured_at > current.captured_at:
                latest_by_market[snapshot.market_id] = snapshot

        total_markets = len(markets)
        markets_with_snapshot = len(latest_by_market)

        yes_price_count = 0
        spread_count = 0
        liquidity_count = 0
        volume_count = 0
        bid_ask_count = 0

        missing_critical = []

        for market in markets:
            snapshot = latest_by_market.get(market.id)

            if snapshot is None:
                missing_critical.append((market.id, market.title, "no_snapshot"))
                continue

            if snapshot.yes_price is not None:
                yes_price_count += 1
            if snapshot.spread is not None:
                spread_count += 1
            if snapshot.liquidity is not None:
                liquidity_count += 1
            if snapshot.volume_24h is not None:
                volume_count += 1
            if snapshot.best_bid is not None and snapshot.best_ask is not None:
                bid_ask_count += 1

            if snapshot.yes_price is None or snapshot.spread is None:
                missing_critical.append((market.id, market.title, "missing_price_or_spread"))

        print("=" * 80)
        print("MARKET DATA QUALITY REPORT")
        print("=" * 80)
        print(f"Total markets: {total_markets}")
        print(f"Markets with latest snapshot: {markets_with_snapshot}")
        print(f"With yes_price: {yes_price_count}")
        print(f"With spread: {spread_count}")
        print(f"With liquidity: {liquidity_count}")
        print(f"With volume_24h: {volume_count}")
        print(f"With bid/ask: {bid_ask_count}")
        print(f"Missing critical: {len(missing_critical)}")
        print("=" * 80)

        for item in missing_critical[:20]:
            market_id, title, reason = item
            print(f"[MISSING] market_id={market_id} reason={reason} title={title}")

    finally:
        db.close()


if __name__ == "__main__":
    main()