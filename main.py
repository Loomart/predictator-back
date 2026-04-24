from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

import crud
import schemas
from db import get_db, test_connection
from ingest import MockMarketSource, sync_market_data
from models import JobRun
from scanner import run_market_scanner

import threading

from scheduler import main as scheduler_main
from scheduler_state import is_running, set_running, set_thread, get_thread


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


# Admin endpoints for manual execution
@app.post("/admin/run-sync")
def run_sync(db: Session = Depends(get_db)):
    """Manually trigger market data synchronization."""
    try:
        print("[API] Starting manual sync...")
        source = MockMarketSource()
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

        source = MockMarketSource()
        sync_stats = sync_market_data(db, source)
        print(f"[API] Sync done: {sync_stats}")

        scan_stats = run_market_scanner(db)
        print(f"[API] Scanner done: {scan_stats}")

        message = "Pipeline completed"
        print(f"[API] {message}")
        return {
            "status": "ok",
            "action": "pipeline",
            "message": message,
            "summary": {
                "sync": sync_stats,
                "scanner": scan_stats,
            },
        }
    except Exception as e:
        error_msg = f"Pipeline failed: {str(e)}"
        print(f"[API ERROR] {error_msg}")
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