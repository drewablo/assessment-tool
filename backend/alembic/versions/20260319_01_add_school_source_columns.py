"""add school source and NAIS reconciliation columns

Revision ID: 20260319_01
Revises: 
Create Date: 2026-03-19
"""

from alembic import op
import sqlalchemy as sa


revision = "20260319_01"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("competitors_schools", sa.Column("data_source", sa.String(length=20), nullable=False, server_default="pss"))
    op.add_column("competitors_schools", sa.Column("also_in_nais", sa.Boolean(), nullable=False, server_default=sa.false()))
    op.add_column("competitors_schools", sa.Column("nais_id", sa.String(length=30), nullable=True))


def downgrade() -> None:
    op.drop_column("competitors_schools", "nais_id")
    op.drop_column("competitors_schools", "also_in_nais")
    op.drop_column("competitors_schools", "data_source")
