"""Rename chunk_number to chunk_sequence

Revision ID: b3b9ab9ae808
Revises: 19b0eacf387f
Create Date: 2024-10-03 18:34:21.172792

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "b3b9ab9ae808"
down_revision: Union[str, None] = "19b0eacf387f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    # Rename column in the chunks table
    op.alter_column("chunks", "chunk_number", new_column_name="chunk_sequence")


def downgrade():
    # Reverse the change if downgrading
    op.alter_column("chunks", "chunk_sequence", new_column_name="chunk_number")
