import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker

BACKEND_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BACKEND_DIR.parent

for dotenv_path in (PROJECT_ROOT / ".env", BACKEND_DIR / ".env"):
    if dotenv_path.exists():
        load_dotenv(dotenv_path=dotenv_path, override=False)

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL no esta configurada en .env (raiz o backend/.env)")


def _should_force_ssl(database_url: str) -> bool:
    lowered = database_url.lower()
    return not (
        "localhost" in lowered
        or "127.0.0.1" in lowered
        or "::1" in lowered
    )


def _is_postgres_url(database_url: str) -> bool:
    lowered = database_url.lower()
    return lowered.startswith("postgresql://") or lowered.startswith("postgres://")


db_sslmode = os.getenv("DB_SSLMODE")
connect_args = None

if not _is_postgres_url(DATABASE_URL):
    connect_args = None
elif db_sslmode:
    connect_args = {"sslmode": db_sslmode}
elif _should_force_ssl(DATABASE_URL):
    connect_args = {"sslmode": "require"}

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    connect_args=connect_args or {},
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def test_connection():
    with engine.connect() as connection:
        result = connection.execute(text("SELECT 1"))
        return result.scalar()


def ensure_schema_compatibility() -> None:
    """Apply tiny additive schema patches for backward-compatible startups."""
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())
    if "orders" not in tables:
        return

    columns = {col["name"] for col in inspector.get_columns("orders")}
    with engine.begin() as connection:
        if "retry_count" not in columns:
            connection.execute(text("ALTER TABLE orders ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0"))
        if "last_error" not in columns:
            connection.execute(text("ALTER TABLE orders ADD COLUMN last_error TEXT NULL"))


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
