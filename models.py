from datetime import UTC, datetime
from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


def utc_now_naive() -> datetime:
    """Return UTC wall-clock as naive datetime for legacy DateTime columns."""
    return datetime.now(UTC).replace(tzinfo=None)


class Market(Base):
    __tablename__ = "markets"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    external_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    platform: Mapped[str] = mapped_column(String(50), nullable=False, default="polymarket")
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    slug: Mapped[str | None] = mapped_column(String(255), nullable=True)
    category: Mapped[str | None] = mapped_column(String(100), nullable=True)
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="open")
    resolution_date: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now_naive, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=utc_now_naive,
        onupdate=utc_now_naive,
        nullable=False,
    )

    snapshots: Mapped[list["MarketSnapshot"]] = relationship(
        back_populates="market",
        cascade="all, delete-orphan",
    )
    signals: Mapped[list["Signal"]] = relationship(
        back_populates="market",
        cascade="all, delete-orphan",
    )


class MarketSnapshot(Base):
    __tablename__ = "market_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    market_id: Mapped[int] = mapped_column(ForeignKey("markets.id"), nullable=False, index=True)

    yes_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    no_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    spread: Mapped[float | None] = mapped_column(Float, nullable=True)
    volume_24h: Mapped[float | None] = mapped_column(Float, nullable=True)
    liquidity: Mapped[float | None] = mapped_column(Float, nullable=True)
    best_bid: Mapped[float | None] = mapped_column(Float, nullable=True)
    best_ask: Mapped[float | None] = mapped_column(Float, nullable=True)

    captured_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now_naive, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now_naive, nullable=False)

    market: Mapped["Market"] = relationship(back_populates="snapshots")


class Signal(Base):
    __tablename__ = "signals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    market_id: Mapped[int] = mapped_column(ForeignKey("markets.id"), nullable=False, index=True)

    signal_type: Mapped[str] = mapped_column(String(50), nullable=False)  # ENTER, SKIP, EXIT
    strategy_name: Mapped[str] = mapped_column(String(100), nullable=False, default="microstructure_v1")
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    edge_estimate: Mapped[float | None] = mapped_column(Float, nullable=True)
    status: Mapped[str | None] = mapped_column(String(20), nullable=True, index=True)  # WATCH, CONFIRMING, CONFIRMED, INVALIDATED, EXPIRED
    direction: Mapped[str | None] = mapped_column(String(10), nullable=True)  # UP, DOWN
    reference_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    reference_spread: Mapped[float | None] = mapped_column(Float, nullable=True)
    reference_liquidity: Mapped[float | None] = mapped_column(Float, nullable=True)
    confirmation_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    last_evaluated_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    confirmation_deadline: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_executed: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now_naive, nullable=False, index=True)

    market: Mapped["Market"] = relationship(back_populates="signals")


class JobRun(Base):
    __tablename__ = "job_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    job_type: Mapped[str] = mapped_column(String(50), nullable=False)  # sync, scanner, pipeline
    status: Mapped[str] = mapped_column(String(20), nullable=False)  # success, failed
    source_name: Mapped[str | None] = mapped_column(String(50), nullable=True)
    summary_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    finished_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    duration_seconds: Mapped[float] = mapped_column(Float, nullable=False)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now_naive, nullable=False)
    
class SignalEvaluation(Base):
    __tablename__ = "signal_evaluations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)

    signal_id: Mapped[int] = mapped_column(ForeignKey("signals.id"), nullable=False, index=True)
    market_id: Mapped[int] = mapped_column(ForeignKey("markets.id"), nullable=False, index=True)

    evaluation_horizon_minutes: Mapped[int] = mapped_column(Integer, default=15, nullable=False)

    entry_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    exit_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    price_change: Mapped[float | None] = mapped_column(Float, nullable=True)

    direction: Mapped[str | None] = mapped_column(String(20), nullable=True)
    is_success: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    evaluated_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now_naive, nullable=False)

    signal: Mapped["Signal"] = relationship()
    market: Mapped["Market"] = relationship()


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)

    signal_id: Mapped[int] = mapped_column(ForeignKey("signals.id"), nullable=False, index=True)
    market_id: Mapped[int] = mapped_column(ForeignKey("markets.id"), nullable=False, index=True)

    side: Mapped[str] = mapped_column(String(10), nullable=False)  # BUY/SELL
    order_type: Mapped[str] = mapped_column(String(12), nullable=False, default="MARKET")
    quantity: Mapped[float] = mapped_column(Float, nullable=False)
    limit_price: Mapped[float | None] = mapped_column(Float, nullable=True)

    status: Mapped[str] = mapped_column(String(20), nullable=False, index=True, default="PENDING")
    external_id: Mapped[str | None] = mapped_column(String(100), nullable=True, index=True, unique=True)

    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now_naive, nullable=False, index=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=utc_now_naive,
        onupdate=utc_now_naive,
        nullable=False,
    )

    signal: Mapped["Signal"] = relationship()
    market: Mapped["Market"] = relationship()
    fills: Mapped[list["Fill"]] = relationship(
        back_populates="order",
        cascade="all, delete-orphan",
    )


class Fill(Base):
    __tablename__ = "fills"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id"), nullable=False, index=True)

    price: Mapped[float] = mapped_column(Float, nullable=False)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)
    fee: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    filled_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now_naive, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now_naive, nullable=False)

    order: Mapped["Order"] = relationship(back_populates="fills")


class Position(Base):
    __tablename__ = "positions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    market_id: Mapped[int] = mapped_column(ForeignKey("markets.id"), nullable=False, unique=True, index=True)

    quantity: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    avg_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    realized_pnl: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now_naive, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=utc_now_naive,
        onupdate=utc_now_naive,
        nullable=False,
    )

    market: Mapped["Market"] = relationship()
