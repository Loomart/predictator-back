from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

import crud
import schemas
from db import get_db, test_connection

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