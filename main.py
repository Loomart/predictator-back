import logging
import os
import threading
import time
from datetime import datetime, timezone
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.responses import Response
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from backend import crud
from backend import schemas
from backend.db import ensure_schema_compatibility, get_db, test_connection
from backend.ingest import get_market_source, sync_market_data
from backend.models import JobRun, MarketSnapshot, Order, Signal
from backend.scanner import run_market_scanner
from backend.signal_confirmation_processor import process_all_signal_confirmations
from backend.execution_factory import execution_health, execution_status, get_execution_adapter
from backend.circuit_breaker import reset as reset_execution_circuit, status as execution_circuit_status
from backend.reconciliation import rebuild_positions_from_fills
from backend.execution_recovery import recover_trading_state
from backend.job_run_recorder import run_job
from backend.trading_metrics import positions_detail, trading_summary
from backend.trading_orchestrator import execute_confirmed_signals
from backend.trading_state import disable_trading, enable_trading, is_trading_enabled
from backend.market_filters import normalize_set, parse_csv_values
from backend.runtime_switches import load_runtime_switches

from backend.scheduler import main as scheduler_main
from backend.scheduler_state import is_running, set_running, set_thread, get_thread
from backend.logging_utils import bind_context, log_event
from backend.prom_metrics import observe_request, render_latest


app = FastAPI(title="Prediction System API")
logger = logging.getLogger("backend.api")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

LOG_TRACEBACKS = os.getenv("LOG_TRACEBACKS", "false").strip().lower() == "true"


def _parse_query_filter(values: str | None) -> set[str] | None:
    if values is None:
        return None
    return normalize_set(parse_csv_values(values))


def _build_scanner_threshold_overrides(
    wait_liquidity_threshold: float | None,
    wait_noise_threshold: float | None,
    wait_stability_threshold: float | None,
    strong_enter_score_threshold: float | None,
    strong_enter_momentum_threshold: float | None,
    strong_enter_change_threshold: float | None,
    enter_score_threshold: float | None,
    enter_momentum_threshold: float | None,
    enter_change_threshold: float | None,
    watch_score_threshold: float | None,
    watch_momentum_threshold: float | None,
    avoid_score_threshold: float | None,
) -> dict[str, float]:
    pairs = {
        "wait_liquidity_threshold": wait_liquidity_threshold,
        "wait_noise_threshold": wait_noise_threshold,
        "wait_stability_threshold": wait_stability_threshold,
        "strong_enter_score_threshold": strong_enter_score_threshold,
        "strong_enter_momentum_threshold": strong_enter_momentum_threshold,
        "strong_enter_change_threshold": strong_enter_change_threshold,
        "enter_score_threshold": enter_score_threshold,
        "enter_momentum_threshold": enter_momentum_threshold,
        "enter_change_threshold": enter_change_threshold,
        "watch_score_threshold": watch_score_threshold,
        "watch_momentum_threshold": watch_momentum_threshold,
        "avoid_score_threshold": avoid_score_threshold,
    }
    return {key: float(value) for key, value in pairs.items() if value is not None}


def _admin_auth_config() -> tuple[str | None, str, bool]:
    api_key = os.getenv("ADMIN_API_KEY")
    header = os.getenv("ADMIN_API_KEY_HEADER", "X-Admin-API-Key")
    allow_unauth = os.getenv("ADMIN_API_KEY_ALLOW_UNAUTH", "false").strip().lower() in {"1", "true", "yes", "on"}
    return api_key, header, allow_unauth


def _raise_admin_error(action: str, exc: Exception) -> None:
    # Never expose internals in responses; traceback logging is opt-in.
    if LOG_TRACEBACKS:
        logger.exception("[ADMIN][%s] failed", action, exc_info=exc)
    else:
        logger.error("[ADMIN][%s] failed: %s", action, exc.__class__.__name__)
    raise HTTPException(status_code=500, detail=f"{action.capitalize()} failed")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup_schema_checks():
    ensure_schema_compatibility()


@app.middleware("http")
async def request_logging_middleware(request: Request, call_next):
    request_id = str(uuid4())[:8]
    started_at = time.perf_counter()
    with bind_context(request_id=request_id):
        if request.url.path.startswith("/admin"):
            admin_api_key, admin_header, allow_unauth = _admin_auth_config()
            if request.method.upper() == "OPTIONS":
                pass
            elif allow_unauth:
                pass
            elif admin_api_key:
                provided = request.headers.get(admin_header) or request.headers.get(admin_header.lower())
                if not provided or provided != admin_api_key:
                    return JSONResponse(status_code=401, content={"detail": "Unauthorized"})
            else:
                # Backward-compatible behavior: if no admin key is configured,
                # do not block admin endpoints.
                pass

        log_event(
            logger,
            "http_request_start",
            method=request.method,
            path=request.url.path,
        )
        try:
            response = await call_next(request)
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - started_at) * 1000
            log_event(
                logger,
                "http_request_error",
                level="exception" if LOG_TRACEBACKS else "error",
                method=request.method,
                path=request.url.path,
                status=500,
                elapsed_ms=round(elapsed_ms, 2),
                error=exc.__class__.__name__,
            )
            return JSONResponse(status_code=500, content={"detail": "Internal server error"})

        elapsed = time.perf_counter() - started_at
        elapsed_ms = elapsed * 1000
        log_event(
            logger,
            "http_request_end",
            method=request.method,
            path=request.url.path,
            status=int(response.status_code),
            elapsed_ms=round(elapsed_ms, 2),
        )
        observe_request(method=request.method, path=request.url.path, status=int(response.status_code), elapsed_seconds=elapsed)
        return response


@app.get("/metrics")
def metrics():
    body, content_type = render_latest()
    return Response(content=body, media_type=content_type)


@app.get("/")
def root():
    return {"status": "ok", "service": "backend"}


@app.get("/health/db")
def health_db():
    result = test_connection()
    return {"database": "ok", "result": result}


@app.get("/health")
def health_overview(db: Session = Depends(get_db)):
    last_snapshot_at = db.scalar(select(func.max(MarketSnapshot.captured_at)))
    last_snapshot_age_seconds = None
    if last_snapshot_at is not None:
        delta = datetime.now(timezone.utc).replace(tzinfo=None) - last_snapshot_at
        last_snapshot_age_seconds = int(delta.total_seconds())

    last_pipeline = db.scalars(
        select(JobRun)
        .where(JobRun.job_type == "pipeline")
        .order_by(JobRun.started_at.desc())
        .limit(1)
    ).first()

    return {
        "status": "ok",
        "database": "ok",
        "scheduler_running": bool(is_running()),
        "last_snapshot_at": last_snapshot_at.isoformat() if last_snapshot_at else None,
        "last_snapshot_age_seconds": last_snapshot_age_seconds,
        "last_pipeline_run": {
            "id": int(last_pipeline.id),
            "status": str(last_pipeline.status),
            "started_at": last_pipeline.started_at.isoformat(),
            "finished_at": last_pipeline.finished_at.isoformat(),
            "duration_seconds": float(last_pipeline.duration_seconds),
        }
        if last_pipeline is not None
        else None,
    }


@app.get("/markets", response_model=list[schemas.MarketBase])
def list_markets(db: Session = Depends(get_db)):
    return crud.get_markets(db)


@app.get("/markets/{market_id}", response_model=schemas.MarketDetail)
def get_market(market_id: int, db: Session = Depends(get_db)):
    market = crud.get_market_by_id(db, market_id)
    if not market:
        raise HTTPException(status_code=404, detail="Market not found")
    return market


@app.delete("/markets/{market_id}", status_code=204)
def delete_market(market_id: int, db: Session = Depends(get_db)):
    deleted = crud.delete_market_cascade(db, market_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Market not found")
    return Response(status_code=204)


@app.get("/snapshots", response_model=list[schemas.MarketSnapshotBase])
def list_snapshots(db: Session = Depends(get_db)):
    return crud.get_snapshots(db)


@app.get("/signals", response_model=list[schemas.SignalBase])
def list_signals(db: Session = Depends(get_db)):
    return crud.get_signals(db)


@app.get("/orders", response_model=list[schemas.OrderBase])
def list_orders(db: Session = Depends(get_db)):
    return crud.get_orders(db)


@app.get("/orders/{order_id}", response_model=schemas.OrderBase)
def get_order(order_id: int, db: Session = Depends(get_db)):
    order = crud.get_order_by_id(db, order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order


@app.get("/fills", response_model=list[schemas.FillBase])
def list_fills(db: Session = Depends(get_db)):
    return crud.get_fills(db)


@app.get("/positions", response_model=list[schemas.PositionBase])
def list_positions(db: Session = Depends(get_db)):
    return crud.get_positions(db)


@app.get("/positions/market/{market_id}", response_model=schemas.PositionBase)
def get_position_by_market(market_id: int, db: Session = Depends(get_db)):
    position = crud.get_position_by_market_id(db, market_id)
    if not position:
        raise HTTPException(status_code=404, detail="Position not found")
    return position


@app.get("/admin/trading/summary")
def get_trading_summary(db: Session = Depends(get_db)):
    return trading_summary(db)


@app.get("/admin/metrics")
def metrics_overview(db: Session = Depends(get_db)):
    signals_total = int(db.scalar(select(func.count(Signal.id))) or 0)
    by_status_rows = db.execute(select(Signal.status, func.count(Signal.id)).group_by(Signal.status)).all()
    signals_by_status = {str(status): int(count) for status, count in by_status_rows}

    orders_total = int(db.scalar(select(func.count(Order.id))) or 0)
    order_rows = db.execute(select(Order.status, func.count(Order.id)).group_by(Order.status)).all()
    orders_by_status = {str(status): int(count) for status, count in order_rows}

    confirmed = float(signals_by_status.get("CONFIRMED", 0))
    invalidated = float(signals_by_status.get("INVALIDATED", 0))
    expired = float(signals_by_status.get("EXPIRED", 0))
    active = float(signals_by_status.get("WATCH", 0) + signals_by_status.get("CONFIRMING", 0))
    denom = max(1.0, confirmed + invalidated + expired + active)

    filled_orders = float(orders_by_status.get("FILLED", 0))
    rejected_orders = float(orders_by_status.get("REJECTED", 0))
    errored_orders = float(orders_by_status.get("ERROR", 0))
    total_orders_rate_denom = max(1.0, float(orders_total))

    return {
        "signals": {
            "total": signals_total,
            "by_status": signals_by_status,
            "confirmation_rate": round(confirmed / denom, 6),
            "invalidation_rate": round(invalidated / denom, 6),
            "expiration_rate": round(expired / denom, 6),
        },
        "execution": {
            "orders_total": orders_total,
            "by_status": orders_by_status,
            "filled_rate": round(filled_orders / total_orders_rate_denom, 6),
            "rejected_rate": round(rejected_orders / total_orders_rate_denom, 6),
            "error_rate": round(errored_orders / total_orders_rate_denom, 6),
        },
    }


@app.get("/admin/runtime-switches")
def runtime_switches():
    switches = load_runtime_switches()
    filters = switches["filters"]
    return {
        "filters": {
            "market_category_allowlist": sorted(filters["market_category_allowlist"]),
            "market_title_include": sorted(filters["market_title_include"]),
            "market_external_id_allowlist": sorted(filters["market_external_id_allowlist"]),
        },
        "scanner": switches["scanner"],
    }


@app.get("/admin/trading/positions")
def get_trading_positions(db: Session = Depends(get_db)):
    return {
        "positions": positions_detail(db),
    }


@app.get("/admin/trading/status")
def get_trading_status():
    return {
        "enabled": is_trading_enabled(),
    }


@app.post("/admin/trading/enable")
def enable_trading_endpoint():
    enable_trading()
    return {"enabled": is_trading_enabled()}


@app.post("/admin/trading/disable")
def disable_trading_endpoint():
    disable_trading()
    return {"enabled": is_trading_enabled()}


# Admin endpoints for manual execution
@app.post("/admin/run-sync")
def run_sync(
    db: Session = Depends(get_db),
    category: str | None = None,
    title_contains: str | None = None,
    external_ids: str | None = None,
    market_limit: int | None = None,
):
    """Manually trigger market data synchronization."""
    try:
        category_filter = _parse_query_filter(category)
        title_filter = _parse_query_filter(title_contains)
        external_id_filter = _parse_query_filter(external_ids)

        def _fn():
            source = get_market_source()
            sync_kwargs = {}
            if category_filter is not None:
                sync_kwargs["category_allowlist"] = category_filter
            if title_filter is not None:
                sync_kwargs["title_terms"] = title_filter
            if external_id_filter is not None:
                sync_kwargs["external_id_allowlist"] = external_id_filter
            stats = sync_market_data(db, source, **sync_kwargs)
            if market_limit is not None:
                stats["operator_market_limit"] = int(market_limit)
            return stats, stats

        stats = run_job(db, job_type="sync", source_name=str(os.getenv("MARKET_SOURCE", "mock")), fn=_fn)
        message = "Sync completed"
        return {
            "status": "ok",
            "action": "sync",
            "message": message,
            "summary": stats,
        }
    except Exception as e:
        _raise_admin_error("sync", e)


@app.post("/admin/run-scanner")
def run_scanner(
    db: Session = Depends(get_db),
    category: str | None = None,
    title_contains: str | None = None,
    external_ids: str | None = None,
    market_limit: int | None = None,
    min_history: int | None = None,
    wait_liquidity_threshold: float | None = None,
    wait_noise_threshold: float | None = None,
    wait_stability_threshold: float | None = None,
    strong_enter_score_threshold: float | None = None,
    strong_enter_momentum_threshold: float | None = None,
    strong_enter_change_threshold: float | None = None,
    enter_score_threshold: float | None = None,
    enter_momentum_threshold: float | None = None,
    enter_change_threshold: float | None = None,
    watch_score_threshold: float | None = None,
    watch_momentum_threshold: float | None = None,
    avoid_score_threshold: float | None = None,
):
    """Manually trigger market scanner."""
    try:
        category_filter = _parse_query_filter(category)
        title_filter = _parse_query_filter(title_contains)
        external_id_filter = _parse_query_filter(external_ids)
        threshold_overrides = _build_scanner_threshold_overrides(
            wait_liquidity_threshold,
            wait_noise_threshold,
            wait_stability_threshold,
            strong_enter_score_threshold,
            strong_enter_momentum_threshold,
            strong_enter_change_threshold,
            enter_score_threshold,
            enter_momentum_threshold,
            enter_change_threshold,
            watch_score_threshold,
            watch_momentum_threshold,
            avoid_score_threshold,
        )

        def _fn():
            scanner_kwargs = {}
            if category_filter is not None:
                scanner_kwargs["category_allowlist"] = category_filter
            if title_filter is not None:
                scanner_kwargs["title_terms"] = title_filter
            if external_id_filter is not None:
                scanner_kwargs["external_id_allowlist"] = external_id_filter
            if market_limit is not None:
                scanner_kwargs["market_limit"] = int(market_limit)
            if min_history is not None:
                scanner_kwargs["min_history"] = int(min_history)
            if threshold_overrides:
                scanner_kwargs["classifier_thresholds_override"] = threshold_overrides
            stats = run_market_scanner(db, **scanner_kwargs)
            return stats, stats

        stats = run_job(db, job_type="scanner", fn=_fn)
        message = "Scanner completed"
        return {
            "status": "ok",
            "action": "scanner",
            "message": message,
            "summary": stats,
        }
    except Exception as e:
        _raise_admin_error("scanner", e)


@app.post("/admin/run-pipeline")
def run_pipeline_endpoint(
    db: Session = Depends(get_db),
    category: str | None = None,
    title_contains: str | None = None,
    external_ids: str | None = None,
    market_limit: int | None = None,
    min_history: int | None = None,
    wait_liquidity_threshold: float | None = None,
    wait_noise_threshold: float | None = None,
    wait_stability_threshold: float | None = None,
    strong_enter_score_threshold: float | None = None,
    strong_enter_momentum_threshold: float | None = None,
    strong_enter_change_threshold: float | None = None,
    enter_score_threshold: float | None = None,
    enter_momentum_threshold: float | None = None,
    enter_change_threshold: float | None = None,
    watch_score_threshold: float | None = None,
    watch_momentum_threshold: float | None = None,
    avoid_score_threshold: float | None = None,
):
    """Manually trigger full pipeline: sync + scanner."""
    try:
        category_filter = _parse_query_filter(category)
        title_filter = _parse_query_filter(title_contains)
        external_id_filter = _parse_query_filter(external_ids)
        threshold_overrides = _build_scanner_threshold_overrides(
            wait_liquidity_threshold,
            wait_noise_threshold,
            wait_stability_threshold,
            strong_enter_score_threshold,
            strong_enter_momentum_threshold,
            strong_enter_change_threshold,
            enter_score_threshold,
            enter_momentum_threshold,
            enter_change_threshold,
            watch_score_threshold,
            watch_momentum_threshold,
            avoid_score_threshold,
        )

        def _fn():
            source = get_market_source()
            sync_kwargs = {}
            scanner_kwargs = {}
            if category_filter is not None:
                sync_kwargs["category_allowlist"] = category_filter
                scanner_kwargs["category_allowlist"] = category_filter
            if title_filter is not None:
                sync_kwargs["title_terms"] = title_filter
                scanner_kwargs["title_terms"] = title_filter
            if external_id_filter is not None:
                sync_kwargs["external_id_allowlist"] = external_id_filter
                scanner_kwargs["external_id_allowlist"] = external_id_filter
            if market_limit is not None:
                scanner_kwargs["market_limit"] = int(market_limit)
            if min_history is not None:
                scanner_kwargs["min_history"] = int(min_history)
            if threshold_overrides:
                scanner_kwargs["classifier_thresholds_override"] = threshold_overrides
            sync_stats = sync_market_data(db, source, **sync_kwargs)
            scan_stats = run_market_scanner(db, **scanner_kwargs)
            confirmation_stats = process_all_signal_confirmations(db)
            execution_stats = execute_confirmed_signals(db, get_execution_adapter())
            summary = {
                "sync": sync_stats,
                "scanner": scan_stats,
                "confirmations": confirmation_stats,
                "execution": execution_stats,
            }
            return summary, summary

        summary = run_job(db, job_type="pipeline", source_name=str(os.getenv("MARKET_SOURCE", "mock")), fn=_fn)
        message = "Pipeline completed"
        return {
            "status": "ok",
            "action": "pipeline",
            "message": message,
            "summary": summary,
        }
    except Exception as e:
        _raise_admin_error("pipeline", e)


@app.post("/admin/run-confirmations")
def run_confirmations(db: Session = Depends(get_db)):
    try:
        def _fn():
            stats = process_all_signal_confirmations(db)
            return stats, stats

        stats = run_job(db, job_type="confirmations", fn=_fn)
        message = "Confirmations completed"
        return {
            "status": "ok",
            "action": "confirmations",
            "message": message,
            "summary": stats,
        }
    except Exception as e:
        _raise_admin_error("confirmations", e)


@app.post("/admin/run-execution")
def run_execution(db: Session = Depends(get_db)):
    try:
        def _fn():
            stats = execute_confirmed_signals(db, get_execution_adapter())
            return stats, stats

        stats = run_job(db, job_type="execution", fn=_fn, source_name=str(os.getenv("EXECUTION_MODE", "paper")))
        message = "Execution completed"
        return {
            "status": "ok",
            "action": "execution",
            "message": message,
            "summary": stats,
        }
    except Exception as e:
        _raise_admin_error("execution", e)


@app.post("/admin/run-trading-step")
def run_trading_step(db: Session = Depends(get_db)):
    try:
        def _fn():
            stats = execute_confirmed_signals(db, get_execution_adapter())
            return stats, stats

        stats = run_job(db, job_type="trading_step", fn=_fn, source_name=str(os.getenv("EXECUTION_MODE", "paper")))
        message = "Trading step completed"
        return {
            "status": "ok",
            "action": "trading_step",
            "message": message,
            "summary": stats,
        }
    except Exception as e:
        _raise_admin_error("trading step", e)


@app.get("/admin/execution/status")
def get_execution_status():
    return execution_status()


@app.get("/admin/execution/health")
def get_execution_health():
    return execution_health()


@app.get("/admin/execution/circuit-breaker")
def get_execution_circuit_breaker():
    return execution_circuit_status()


@app.post("/admin/execution/circuit-breaker/reset")
def reset_execution_circuit_breaker():
    reset_execution_circuit()
    return execution_circuit_status()


@app.post("/admin/trading/reconcile")
def reconcile_positions(db: Session = Depends(get_db)):
    try:
        stats = rebuild_positions_from_fills(db)
        return {
            "status": "ok",
            "action": "reconcile",
            "summary": stats,
        }
    except Exception as e:
        _raise_admin_error("reconcile", e)


@app.post("/admin/trading/recover")
def recover_positions(db: Session = Depends(get_db)):
    try:
        stats = recover_trading_state(db)
        return {
            "status": "ok",
            "action": "recover",
            "summary": stats,
        }
    except Exception as e:
        _raise_admin_error("recover", e)


@app.get("/admin/runs", response_model=list[schemas.JobRunBase])
def list_pipeline_runs(db: Session = Depends(get_db)):
    """Get list of all pipeline runs, ordered by creation date (newest first)."""
    return crud.get_pipeline_runs(db)


@app.get("/admin/runs/{run_id}", response_model=schemas.JobRunBase)
def get_pipeline_run(run_id: int, db: Session = Depends(get_db)):
    """Get details of a specific pipeline run by ID."""
    run = crud.get_pipeline_run_by_id(db, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Pipeline run not found")
    return run

@app.get("/admin/scheduler/status")
def scheduler_status():
    return {
        "running": is_running()
    }


@app.post("/admin/scheduler/start")
def scheduler_start():
    if is_running():
        return {"status": "already_running"}

    def run():
        scheduler_main()

    thread = threading.Thread(target=run, daemon=True)
    set_thread(thread)
    set_running(True)

    thread.start()

    return {"status": "started"}


@app.post("/admin/scheduler/stop")
def scheduler_stop():
    if not is_running():
        return {"status": "not_running"}

    set_running(False)

    return {"status": "stopping"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
