"""add signal confirmation lifecycle fields

Revision ID: 20260508_add_signal_confirmation_fields
Revises: 
Create Date: 2026-05-08 00:00:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "20260508_add_signal_confirmation_fields"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("signals", sa.Column("status", sa.String(length=20), nullable=True))
    op.add_column("signals", sa.Column("direction", sa.String(length=10), nullable=True))
    op.add_column("signals", sa.Column("reference_price", sa.Float(), nullable=True))
    op.add_column("signals", sa.Column("reference_spread", sa.Float(), nullable=True))
    op.add_column("signals", sa.Column("reference_liquidity", sa.Float(), nullable=True))
    op.add_column(
        "signals",
        sa.Column("confirmation_score", sa.Float(), nullable=False, server_default=sa.text("0")),
    )
    op.add_column("signals", sa.Column("last_evaluated_at", sa.DateTime(), nullable=True))
    op.add_column("signals", sa.Column("confirmation_deadline", sa.DateTime(), nullable=True))

    op.create_index("ix_signals_status", "signals", ["status"], unique=False)

    op.alter_column("signals", "confirmation_score", server_default=None)


def downgrade() -> None:
    op.drop_index("ix_signals_status", table_name="signals")

    op.drop_column("signals", "confirmation_deadline")
    op.drop_column("signals", "last_evaluated_at")
    op.drop_column("signals", "confirmation_score")
    op.drop_column("signals", "reference_liquidity")
    op.drop_column("signals", "reference_spread")
    op.drop_column("signals", "reference_price")
    op.drop_column("signals", "direction")
    op.drop_column("signals", "status")
