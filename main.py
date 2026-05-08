from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from backend import crud
from backend import schemas
from backend.db import get_db, test_connection
from backend.ingest import get_market_source, sync_market_data
from backend.models import JobRun
from backend.scanner import run_market_scanner
from backend.signal_confirmation_processor import process_all_signal_confirmations
from backend.execution_factory import execution_health, execution_status, get_execution_adapter
from backend.circuit_breaker import reset as reset_execution_circuit, status as execution_circuit_status
from backend.reconciliation import rebuild_positions_from_fills
from backend.trading_metrics import positions_detail, trading_summary
from backend.trading_orchestrator import execute_confirmed_signals
from backend.trading_state import disable_trading, enable_trading, is_trading_enabled

import threading

from backend.scheduler import main as scheduler_main
from backend.scheduler_state import is_running, set_running, set_thread, get_thread


app = FastAPI(title="Prediction System API")

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


@app.get("/")
def root():
    return {"status": "ok", "service": "backend"}


@app.get("/health/db")
def health_db():
    result = test_connection()
    return {"database": "ok", "result": result}


@app.get("/markets", response_model=list[schemas.MarketBase])
def list_markets(db: Session = Depends(get_db)):
    return crud.get_markets(db)


@app.get("/markets/{market_id}", response_model=schemas.MarketDetail)
def get_market(market_id: int, db: Session = Depends(get_db)):
    market = crud.get_market_by_id(db, market_id)
    if not market:
        raise HTTPException(status_code=404, detail="Market not found")
    return market


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
def run_sync(db: Session = Depends(get_db)):
    """Manually trigger market data synchronization."""
    try:
        print("[API] Starting manual sync...")
        source = get_market_source()
        stats = sync_market_data(db, source)
        message = "Sync completed"
        print(f"[API] {message}: {stats}")
        return {
            "status": "ok",
            "action": "sync",
            "message": message,
            "summary": stats,
        }
    except Exception as e:
        error_msg = f"Sync failed: {str(e)}"
        print(f"[API ERROR] {error_msg}")
        raise HTTPException(status_code=500, detail=error_msg)


@app.post("/admin/run-scanner")
def run_scanner(db: Session = Depends(get_db)):
    """Manually trigger market scanner."""
    try:
        print("[API] Starting manual scanner...")
        stats = run_market_scanner(db)
        message = "Scanner completed"
        print(f"[API] {message}: {stats}")
        return {
            "status": "ok",
            "action": "scanner",
            "message": message,
            "summary": stats,
        }
    except Exception as e:
        error_msg = f"Scanner failed: {str(e)}"
        print(f"[API ERROR] {error_msg}")
        raise HTTPException(status_code=500, detail=error_msg)


@app.post("/admin/run-pipeline")
def run_pipeline_endpoint(db: Session = Depends(get_db)):
    """Manually trigger full pipeline: sync + scanner."""
    try:
        print("[API] Starting manual pipeline...")

        source = get_market_source()
        sync_stats = sync_market_data(db, source)
        print(f"[API] Sync done: {sync_stats}")

        scan_stats = run_market_scanner(db)
        print(f"[API] Scanner done: {scan_stats}")

        confirmation_stats = process_all_signal_confirmations(db)
        print(f"[API] Confirmations done: {confirmation_stats}")

        execution_stats = execute_confirmed_signals(db, get_execution_adapter())
        print(f"[API] Execution done: {execution_stats}")

        message = "Pipeline completed"
        print(f"[API] {message}")
        return {
            "status": "ok",
            "action": "pipeline",
            "message": message,
            "summary": {
                "sync": sync_stats,
                "scanner": scan_stats,
                "confirmations": confirmation_stats,
                "execution": execution_stats,
            },
        }
    except Exception as e:
        error_msg = f"Pipeline failed: {str(e)}"
        print(f"[API ERROR] {error_msg}")
        raise HTTPException(status_code=500, detail=error_msg)


@app.post("/admin/run-confirmations")
def run_confirmations(db: Session = Depends(get_db)):
    try:
        print("[API] Starting confirmations...")
        stats = process_all_signal_confirmations(db)
        message = "Confirmations completed"
        print(f"[API] {message}: {stats}")
        return {
            "status": "ok",
            "action": "confirmations",
            "message": message,
            "summary": stats,
        }
    except Exception as e:
        error_msg = f"Confirmations failed: {str(e)}"
        print(f"[API ERROR] {error_msg}")
        raise HTTPException(status_code=500, detail=error_msg)


@app.post("/admin/run-execution")
def run_execution(db: Session = Depends(get_db)):
    try:
        print("[API] Starting execution...")
        stats = execute_confirmed_signals(db, get_execution_adapter())
        message = "Execution completed"
        print(f"[API] {message}: {stats}")
        return {
            "status": "ok",
            "action": "execution",
            "message": message,
            "summary": stats,
        }
    except Exception as e:
        error_msg = f"Execution failed: {str(e)}"
        print(f"[API ERROR] {error_msg}")
        raise HTTPException(status_code=500, detail=error_msg)


@app.post("/admin/run-trading-step")
def run_trading_step(db: Session = Depends(get_db)):
    try:
        print("[API] Starting trading step...")
        stats = execute_confirmed_signals(db, get_execution_adapter())
        message = "Trading step completed"
        print(f"[API] {message}: {stats}")
        return {
            "status": "ok",
            "action": "trading_step",
            "message": message,
            "summary": stats,
        }
    except Exception as e:
        error_msg = f"Trading step failed: {str(e)}"
        print(f"[API ERROR] {error_msg}")
        raise HTTPException(status_code=500, detail=error_msg)


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
        error_msg = f"Reconcile failed: {str(e)}"
        raise HTTPException(status_code=500, detail=error_msg)


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
