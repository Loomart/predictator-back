from db import SessionLocal
from models import Market, MarketSnapshot


def main():
    db = SessionLocal()

    try:
        markets = db.query(Market).all()

        deleted = 0

        for market in markets:
            has_snapshot = (
                db.query(MarketSnapshot)
                .filter(MarketSnapshot.market_id == market.id)
                .first()
            )

            if not has_snapshot:
                print(f"[DELETE] market_id={market.id} title={market.title}")
                db.delete(market)
                deleted += 1

        db.commit()

        print("=" * 60)
        print(f"Deleted orphan markets: {deleted}")
        print("=" * 60)

    finally:
        db.close()


if __name__ == "__main__":
    main()