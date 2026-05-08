from sqlalchemy import text

from backend.db import engine


CREATE_SIGNAL_EVALUATIONS_TABLE = """
CREATE TABLE IF NOT EXISTS signal_evaluations (
    id SERIAL PRIMARY KEY,

    signal_id INTEGER NOT NULL,
    market_id INTEGER NOT NULL,

    evaluation_horizon_minutes INTEGER NOT NULL DEFAULT 15,

    entry_price DOUBLE PRECISION,
    exit_price DOUBLE PRECISION,
    price_change DOUBLE PRECISION,

    direction VARCHAR(20),
    is_success BOOLEAN,

    evaluated_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),

    CONSTRAINT fk_signal_evaluations_signal
        FOREIGN KEY (signal_id)
        REFERENCES signals(id)
        ON DELETE CASCADE,

    CONSTRAINT fk_signal_evaluations_market
        FOREIGN KEY (market_id)
        REFERENCES markets(id)
        ON DELETE CASCADE
);
"""

CREATE_INDEXES = [
    """
    CREATE INDEX IF NOT EXISTS idx_signal_evaluations_signal_id
    ON signal_evaluations(signal_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_signal_evaluations_market_id
    ON signal_evaluations(market_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_signal_evaluations_evaluated_at
    ON signal_evaluations(evaluated_at);
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_signal_evaluation_signal_horizon
    ON signal_evaluations(signal_id, evaluation_horizon_minutes);
    """,
]


def main() -> None:
    with engine.begin() as conn:
        conn.execute(text(CREATE_SIGNAL_EVALUATIONS_TABLE))

        for index_sql in CREATE_INDEXES:
            conn.execute(text(index_sql))

    print("[OK] signal_evaluations table is ready.")


if __name__ == "__main__":
    main()