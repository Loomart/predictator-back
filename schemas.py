from datetime import datetime
from pydantic import BaseModel, ConfigDict


class MarketBase(BaseModel):
    id: int
    external_id: str
    platform: str
    title: str
    slug: str | None = None
    category: str | None = None
    status: str
    resolution_date: datetime | None = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class MarketSnapshotBase(BaseModel):
    id: int
    market_id: int
    yes_price: float | None = None
    no_price: float | None = None
    spread: float | None = None
    volume_24h: float | None = None
    liquidity: float | None = None
    best_bid: float | None = None
    best_ask: float | None = None
    captured_at: datetime
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class SignalBase(BaseModel):
    id: int
    market_id: int
    signal_type: str
    strategy_name: str
    confidence: float | None = None
    edge_estimate: float | None = None
    reason: str | None = None
    is_executed: bool
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class JobRunBase(BaseModel):
    id: int
    job_type: str
    status: str
    source_name: str | None = None
    summary_json: str | None = None
    started_at: datetime
    finished_at: datetime
    duration_seconds: float
    error_message: str | None = None
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class MarketDetail(MarketBase):
    snapshots: list[MarketSnapshotBase] = []
    signals: list[SignalBase] = []


class OrderBase(BaseModel):
    id: int
    signal_id: int
    market_id: int
    side: str
    order_type: str
    quantity: float
    limit_price: float | None = None
    status: str
    external_id: str | None = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class FillBase(BaseModel):
    id: int
    order_id: int
    price: float
    quantity: float
    fee: float
    filled_at: datetime
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class PositionBase(BaseModel):
    id: int
    market_id: int
    quantity: float
    avg_price: float | None = None
    realized_pnl: float
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)
