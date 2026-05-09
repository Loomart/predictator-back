from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterable

from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker

from backend.circuit_breaker import reset as reset_circuit
from backend.models import Base, Fill, Market, MarketSnapshot, Order, Position, Signal
from backend.paper_execution_adapter import PaperExecutionAdapter
from backend.reconciliation import rebuild_positions_from_fills
from backend.signal_confirmation_processor import process_all_signal_confirmations
from backend.trading_metrics import positions_detail, trading_summary
from backend.trading_orchestrator import execute_confirmed_signals
from backend.trading_state import enable_trading
from backend.scanner import run_market_scanner
from backend.scanner import get_recent_snapshots
from backend.market_regime_analyzer import analyze_market_regime
from backend.confirmation_config import DEFAULT_CONFIRMATION_CONFIG

from backend.replay.replay_models import HistoricalSnapshot, ReplayConfig, ReplayRunResult, ReplayStepResult


@contextmanager
def _frozen_utc_time(target: datetime):
    class _FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return target if tz else target.replace(tzinfo=None)

    import backend.confirmation_engine as confirmation_engine
    import backend.scanner as scanner
    import backend.signal_confirmation_processor as confirmation_processor
    import backend.trading_orchestrator as trading_orchestrator

    old_confirm = confirmation_engine.datetime
    old_scan = scanner.datetime
    old_conf_proc = confirmation_processor.datetime
    old_exec = trading_orchestrator.datetime
    try:
        confirmation_engine.datetime = _FixedDatetime  # type: ignore
        scanner.datetime = _FixedDatetime  # type: ignore
        confirmation_processor.datetime = _FixedDatetime  # type: ignore
        trading_orchestrator.datetime = _FixedDatetime  # type: ignore
        yield
    finally:
        confirmation_engine.datetime = old_confirm  # type: ignore
        scanner.datetime = old_scan  # type: ignore
        confirmation_processor.datetime = old_conf_proc  # type: ignore
        trading_orchestrator.datetime = old_exec  # type: ignore


def _naive_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def _compute_equity_and_exposure(detail_rows: list[dict[str, Any]]) -> tuple[float, float]:
    equity = 0.0
    exposure = 0.0
    for row in detail_rows:
        qty = float(row.get("quantity") or 0.0)
        realized = float(row.get("realized_pnl") or 0.0)
        unrealized = float(row.get("unrealized_pnl") or 0.0)
        equity += realized + unrealized
        exposure += abs(qty)
    return round(equity, 6), round(exposure, 6)


class ReplayEngine:
    def __init__(self, *, config: ReplayConfig, sqlite_url: str | None = None):
        self.config = config
        self.sqlite_url = sqlite_url or "sqlite+pysqlite:///:memory:"

        engine = create_engine(self.sqlite_url, future=True)
        Base.metadata.create_all(engine)
        self._session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

    def run(self, snapshots: Iterable[HistoricalSnapshot]) -> ReplayRunResult:
        run_id = self.config.stable_id()
        steps: list[ReplayStepResult] = []

        enable_trading()
        reset_circuit()

        peak_equity = 0.0

        session: Session = self._session_factory()
        try:
            ordered = sorted(snapshots, key=lambda s: (s.captured_at, s.external_id))
            for snap in ordered:
                now = _naive_utc(snap.captured_at)

                with _frozen_utc_time(now.replace(tzinfo=timezone.utc)):
                    market = session.scalars(select(Market).where(Market.external_id == snap.external_id).limit(1)).first()
                    if market is None:
                        market = Market(
                            external_id=snap.external_id,
                            platform=snap.platform,
                            title=snap.title,
                            status=snap.status,
                            created_at=now,
                            updated_at=now,
                        )
                        session.add(market)
                        session.flush()
                    else:
                        market.title = snap.title
                        market.status = snap.status
                        market.updated_at = now

                    session.add(
                        MarketSnapshot(
                            market_id=market.id,
                            yes_price=snap.yes_price,
                            no_price=snap.no_price,
                            spread=snap.spread,
                            volume_24h=snap.volume_24h,
                            liquidity=snap.liquidity,
                            best_bid=snap.best_bid,
                            best_ask=snap.best_ask,
                            captured_at=now,
                            created_at=now,
                        )
                    )
                    session.commit()

                    scanner_stats = run_market_scanner(
                        session,
                        strategy_name=self.config.strategy_name,
                        window_size=self.config.scanner_window_size,
                        market_limit=self.config.market_limit,
                    )
                    confirmation_stats = process_all_signal_confirmations(session)
                    adapter = PaperExecutionAdapter({"slippage_bps": 0.0, "fee_bps": 0.0})
                    execution_stats = execute_confirmed_signals(session, adapter)
                    recon_stats = rebuild_positions_from_fills(session)

                    totals = {
                        "signals": int(session.scalar(select(func.count(Signal.id))) or 0),
                        "signals_confirmed": int(session.scalar(select(func.count(Signal.id)).where(Signal.status == "CONFIRMED")) or 0),
                        "signals_invalidated": int(session.scalar(select(func.count(Signal.id)).where(Signal.status == "INVALIDATED")) or 0),
                        "signals_expired": int(session.scalar(select(func.count(Signal.id)).where(Signal.status == "EXPIRED")) or 0),
                        "orders": int(session.scalar(select(func.count(Order.id))) or 0),
                        "fills": int(session.scalar(select(func.count(Fill.id))) or 0),
                        "positions": int(session.scalar(select(func.count(Position.id))) or 0),
                    }

                    regime_counts: dict[str, int] = {}
                    markets = list(session.scalars(select(Market).order_by(Market.id.asc())))
                    for m in markets:
                        window = get_recent_snapshots(session, m.id, limit=self.config.scanner_window_size)
                        regime = analyze_market_regime(window, DEFAULT_CONFIRMATION_CONFIG).regime.value
                        regime_counts[regime] = regime_counts.get(regime, 0) + 1

                    summary = trading_summary(session)
                    detail = positions_detail(session)
                    equity, exposure = _compute_equity_and_exposure(detail)
                    peak_equity = max(peak_equity, equity)
                    drawdown = round(max(0.0, peak_equity - equity), 6)

                    steps.append(
                        ReplayStepResult(
                            captured_at=now,
                            snapshots_applied=1,
                            scanner=scanner_stats,
                            confirmations=confirmation_stats,
                            execution=execution_stats,
                            reconciliation=recon_stats,
                            summary=summary,
                            totals=totals,
                            regime_counts=regime_counts,
                            exposure=exposure,
                            equity=equity,
                            drawdown=drawdown,
                        )
                    )

            return ReplayRunResult(run_id=run_id, config=self.config, steps=steps)
        finally:
            session.close()
