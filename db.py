import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL no está configurada en el archivo .env")


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


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
