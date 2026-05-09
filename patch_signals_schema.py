from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / "backend" / ".env", override=True)

from backend.models import Base
from backend.db import engine


STATEMENTS = [
    "ALTER TABLE signals ADD COLUMN IF NOT EXISTS status VARCHAR(20)",
    "ALTER TABLE signals ADD COLUMN IF NOT EXISTS direction VARCHAR(10)",
    "ALTER TABLE signals ADD COLUMN IF NOT EXISTS reference_price DOUBLE PRECISION",
    "ALTER TABLE signals ADD COLUMN IF NOT EXISTS reference_spread DOUBLE PRECISION",
    "ALTER TABLE signals ADD COLUMN IF NOT EXISTS reference_liquidity DOUBLE PRECISION",
    "ALTER TABLE signals ADD COLUMN IF NOT EXISTS confirmation_score DOUBLE PRECISION DEFAULT 0",
    "ALTER TABLE signals ADD COLUMN IF NOT EXISTS last_evaluated_at TIMESTAMP",
    "ALTER TABLE signals ADD COLUMN IF NOT EXISTS confirmation_deadline TIMESTAMP",
    "CREATE INDEX IF NOT EXISTS ix_signals_status ON signals(status)",
]


def main() -> None:
    # Ensure base tables exist before adding incremental columns.
    Base.metadata.create_all(bind=engine)
    with engine.begin() as conn:
        for statement in STATEMENTS:
            conn.execute(text(statement))
    print("[OK] signals schema patch applied.")


if __name__ == "__main__":
    main()
