from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

import crud
import schemas
from db import get_db, test_connection
from ingest import MockMarketSource, sync_market_data
from scanner import run_market_scanner

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
        message = (
            f"Sync completed: Created={stats['created']}, "
            f"Updated={stats['updated']}, Snapshots={stats['snapshots']}, "
            f"Skipped={stats.get('skipped_snapshots', 0)}"
        )
        print(f"[API] {message}")
        return {"status": "success", "message": message}
    except Exception as e:
        error_msg = f"Sync failed: {str(e)}"
        print(f"[API ERROR] {error_msg}")
        return {"status": "error", "message": error_msg}


@app.post("/admin/run-scanner")
def run_scanner(db: Session = Depends(get_db)):
    """Manually trigger market scanner."""
    try:
        print("[API] Starting manual scanner...")
        run_market_scanner(db)
        message = "Scanner completed successfully"
        print(f"[API] {message}")
        return {"status": "success", "message": message}
    except Exception as e:
        error_msg = f"Scanner failed: {str(e)}"
        print(f"[API ERROR] {error_msg}")
        return {"status": "error", "message": error_msg}


@app.post("/admin/run-pipeline")
def run_pipeline_endpoint(db: Session = Depends(get_db)):
    """Manually trigger full pipeline: sync + scanner."""
    try:
        print("[API] Starting manual pipeline...")

        # Sync
        source = MockMarketSource()
        sync_stats = sync_market_data(db, source)
        print("[API] Sync done")

        # Scanner
        run_market_scanner(db)
        print("[API] Scanner done")

        message = (
            f"Pipeline completed: Sync(Created={sync_stats['created']}, "
            f"Updated={sync_stats['updated']}, Snapshots={sync_stats['snapshots']}, "
            f"Skipped={sync_stats.get('skipped_snapshots', 0)}) + Scanner"
        )
        print(f"[API] {message}")
        return {"status": "success", "message": message}
    except Exception as e:
        error_msg = f"Pipeline failed: {str(e)}"
        print(f"[API ERROR] {error_msg}")
        return {"status": "error", "message": error_msg}