from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260508_execution_hardening"
down_revision = "20260508_add_signal_confirmation_fields"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("orders") as batch:
        batch.add_column(sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"))
        batch.add_column(sa.Column("last_error", sa.Text(), nullable=True))
        batch.create_unique_constraint("uq_orders_external_id", ["external_id"])


def downgrade() -> None:
    with op.batch_alter_table("orders") as batch:
        batch.drop_constraint("uq_orders_external_id", type_="unique")
        batch.drop_column("last_error")
        batch.drop_column("retry_count")
