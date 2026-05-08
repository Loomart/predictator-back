from backend.db import SessionLocal
from backend.models import Market, MarketSnapshot


def get_latest_snapshots(snapshots):
    latest_by_market = {}
    for snapshot in snapshots:
        current = latest_by_market.get(snapshot.market_id)
        if current is None or snapshot.captured_at > current.captured_at:
            latest_by_market[snapshot.market_id] = snapshot
    return latest_by_market


def collect_market_quality(markets, latest_by_market):
    counts = {
        "yes_price_count": 0,
        "spread_count": 0,
        "liquidity_count": 0,
        "volume_count": 0,
        "bid_ask_count": 0,
    }
    missing_critical = []

    for market in markets:
        snapshot = latest_by_market.get(market.id)
        if snapshot is None:
            missing_critical.append((market.id, market.title, "no_snapshot"))
            continue

        _increment_market_counts(counts, snapshot)
        _append_missing_if_needed(missing_critical, market, snapshot)

    return counts, missing_critical


def _increment_market_counts(counts, snapshot):
    counts["yes_price_count"] += snapshot.yes_price is not None
    counts["spread_count"] += snapshot.spread is not None
    counts["liquidity_count"] += snapshot.liquidity is not None
    counts["volume_count"] += snapshot.volume_24h is not None
    counts["bid_ask_count"] += snapshot.best_bid is not None and snapshot.best_ask is not None


def _append_missing_if_needed(missing_critical, market, snapshot):
    if snapshot.yes_price is None or snapshot.spread is None:
        missing_critical.append((market.id, market.title, "missing_price_or_spread"))


def print_quality_report(total_markets, markets_with_snapshot, counts, missing_critical):
    print("=" * 80)
    print("MARKET DATA QUALITY REPORT")
    print("=" * 80)
    print(f"Total markets: {total_markets}")
    print(f"Markets with latest snapshot: {markets_with_snapshot}")
    print(f"With yes_price: {counts['yes_price_count']}")
    print(f"With spread: {counts['spread_count']}")
    print(f"With liquidity: {counts['liquidity_count']}")
    print(f"With volume_24h: {counts['volume_count']}")
    print(f"With bid/ask: {counts['bid_ask_count']}")
    print(f"Missing critical: {len(missing_critical)}")
    print("=" * 80)

    for market_id, title, reason in missing_critical[:20]:
        print(f"[MISSING] market_id={market_id} reason={reason} title={title}")


def main():
    db = SessionLocal()

    try:
        markets = db.query(Market).all()
        snapshots = db.query(MarketSnapshot).all()

        latest_by_market = get_latest_snapshots(snapshots)
        counts, missing_critical = collect_market_quality(markets, latest_by_market)

        print_quality_report(
            total_markets=len(markets),
            markets_with_snapshot=len(latest_by_market),
            counts=counts,
            missing_critical=missing_critical,
        )
    finally:
        db.close()


if __name__ == "__main__":
    main()