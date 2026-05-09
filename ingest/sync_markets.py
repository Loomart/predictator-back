"""
Market synchronization logic.

Handles fetching markets from sources and syncing to database via SQLAlchemy.
"""

import logging
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.models import Market, MarketSnapshot
from backend.market_filters import category_matches, external_id_matches, title_matches
from backend.runtime_switches import load_runtime_switches
from .market_source import MarketSource, MarketWithSnapshot, NormalizedSnapshot

logger = logging.getLogger("backend.sync")

SnapshotThresholds = dict[str, float]

DEFAULT_SNAPSHOT_THRESHOLDS: SnapshotThresholds = {
    "yes_price": 0.01,
    "no_price": 0.01,
    "spread": 0.005,
    "volume_24h": 1000.0,
    "liquidity": 1000.0,
}


def is_snapshot_meaningfully_different(
    last_snapshot: MarketSnapshot,
    normalized_snapshot: NormalizedSnapshot,
    thresholds: SnapshotThresholds,
) -> bool:
    """Return True if the new snapshot differs enough from the last snapshot to warrant insertion."""
    fields = [
        "yes_price",
        "no_price",
        "spread",
        "volume_24h",
        "liquidity",
    ]

    for field_name in fields:
        last_value = getattr(last_snapshot, field_name)
        current_value = getattr(normalized_snapshot, field_name)
        threshold = thresholds[field_name]

        if last_value is None or current_value is None:
            if last_value != current_value:
                return True
            continue

        if abs(current_value - last_value) > threshold:
            return True

    return False


def has_useful_snapshot_data(snapshot: NormalizedSnapshot) -> bool:
    """Return True when the snapshot contains at least one metric."""
    return any(
        value is not None
        for value in (
            snapshot.yes_price,
            snapshot.no_price,
            snapshot.spread,
            snapshot.volume_24h,
            snapshot.liquidity,
            snapshot.best_bid,
            snapshot.best_ask,
        )
    )


def sync_market_data(
    db: Session,
    source: MarketSource,
    snapshot_thresholds: SnapshotThresholds | None = None,
    category_allowlist: set[str] | None = None,
    title_terms: set[str] | None = None,
    external_id_allowlist: set[str] | None = None,
) -> dict[str, int]:
    """Synchronize market data from source to database.

    For each market from the source:
    - Create new Market record if external_id doesn't exist
    - Update existing Market record if it exists
    - Insert a new MarketSnapshot only if the change is significant

    Args:
        db: SQLAlchemy database session.
        source: MarketSource implementation to fetch data from.
        snapshot_thresholds: Optional thresholds for snapshot comparison.

    Returns:
        Dictionary with sync statistics:
        {
            "total_markets_received": total markets from source,
            "markets_created": number of new markets created,
            "markets_updated": number of existing markets updated,
            "snapshots_inserted": number of snapshots inserted,
            "snapshots_skipped_duplicate": number of snapshots skipped due to deduplication,
            "snapshots_skipped_empty": number of snapshots skipped due to empty metrics,
        }
    """
    thresholds = snapshot_thresholds or DEFAULT_SNAPSHOT_THRESHOLDS
    runtime_switches = load_runtime_switches()
    filter_switches = runtime_switches["filters"]

    category_filter = category_allowlist
    if category_filter is None:
        category_filter = set(filter_switches["market_category_allowlist"])
    title_filter = title_terms
    if title_filter is None:
        title_filter = set(filter_switches["market_title_include"])
    external_id_filter = external_id_allowlist
    if external_id_filter is None:
        external_id_filter = set(filter_switches["market_external_id_allowlist"])

    stats = {
        "total_markets_received": 0,
        "markets_created": 0,
        "markets_updated": 0,
        "markets_filtered_out": 0,
        "snapshots_inserted": 0,
        "snapshots_skipped_duplicate": 0,
        "snapshots_skipped_empty": 0,
    }

    try:
        markets_data = source.fetch_markets()
    except Exception as e:
        raise RuntimeError(f"Failed to fetch markets from source: {e}") from e

    stats["total_markets_received"] = len(markets_data)

    if not markets_data:
        logger.info("[SYNC] No markets to sync")
        return stats

    for market_data in markets_data:
        normalized_market = market_data.market
        normalized_snapshot = market_data.snapshot
        if not category_matches(normalized_market.category, category_filter):
            stats["markets_filtered_out"] += 1
            continue
        if not title_matches(normalized_market.title, title_filter):
            stats["markets_filtered_out"] += 1
            continue
        if not external_id_matches(normalized_market.external_id, external_id_filter):
            stats["markets_filtered_out"] += 1
            continue

        existing_market = db.scalars(
            select(Market).where(
                Market.external_id == normalized_market.external_id
            )
        ).first()

        if existing_market is None:
            db_market = Market(
                external_id=normalized_market.external_id,
                platform=normalized_market.platform,
                title=normalized_market.title,
                slug=normalized_market.slug,
                category=normalized_market.category,
                status=normalized_market.status,
                resolution_date=normalized_market.resolution_date,
            )
            db.add(db_market)
            db.flush()
            logger.debug("[NEW MARKET] %s - %s", normalized_market.external_id, normalized_market.title)
            stats["markets_created"] += 1
        else:
            existing_market.title = normalized_market.title
            existing_market.slug = normalized_market.slug
            existing_market.category = normalized_market.category
            existing_market.status = normalized_market.status
            existing_market.resolution_date = normalized_market.resolution_date
            db.flush()
            logger.debug("[UPDATED MARKET] %s - %s", normalized_market.external_id, normalized_market.title)
            stats["markets_updated"] += 1
            db_market = existing_market

        if not has_useful_snapshot_data(normalized_snapshot):
            logger.debug(
                "[SKIP SNAPSHOT EMPTY] %s - no data points",
                normalized_market.external_id,
            )
            stats["snapshots_skipped_empty"] += 1
            continue

        latest_snapshot = db.scalars(
            select(MarketSnapshot)
            .where(MarketSnapshot.market_id == db_market.id)
            .order_by(MarketSnapshot.captured_at.desc())
        ).first()

        if latest_snapshot is not None:
            if not is_snapshot_meaningfully_different(latest_snapshot, normalized_snapshot, thresholds):
                logger.debug(
                    "[SKIP SNAPSHOT DUPLICATE] %s - no cambios relevantes",
                    normalized_market.external_id,
                )
                stats["snapshots_skipped_duplicate"] += 1
                continue

        db_snapshot = MarketSnapshot(
            market_id=db_market.id,
            yes_price=normalized_snapshot.yes_price,
            no_price=normalized_snapshot.no_price,
            spread=normalized_snapshot.spread,
            volume_24h=normalized_snapshot.volume_24h,
            liquidity=normalized_snapshot.liquidity,
            best_bid=normalized_snapshot.best_bid,
            best_ask=normalized_snapshot.best_ask,
            captured_at=market_data.captured_at,
        )
        db.add(db_snapshot)
        logger.debug(
            "[SNAPSHOT] %s - Price: %s",
            normalized_market.external_id,
            normalized_snapshot.yes_price,
        )
        stats["snapshots_inserted"] += 1

    db.commit()
    logger.info(
        "[SYNC COMPLETE] Received: %s, Created: %s, Updated: %s, "
        "Filtered out: %s, Snapshots: %s, Skipped duplicate: %s, Skipped empty: %s",
        stats["total_markets_received"],
        stats["markets_created"],
        stats["markets_updated"],
        stats["markets_filtered_out"],
        stats["snapshots_inserted"],
        stats["snapshots_skipped_duplicate"],
        stats["snapshots_skipped_empty"],
    )

    return stats
