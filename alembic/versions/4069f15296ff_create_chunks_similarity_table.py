"""Create chunks similarity table

Revision ID: 4069f15296ff
Revises: 88288c5e04ba
Create Date: 2024-10-17 15:43:00.223259

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '4069f15296ff'
down_revision: Union[str, None] = '88288c5e04ba'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade():
    op.create_table(
        'chunk_similarity',
        sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),
        sa.Column('user_query', sa.Text, nullable=False),
        sa.Column('embed_model', sa.String(100), nullable=False),
        sa.Column('annotation_model', sa.String(100), nullable=False),
        sa.Column('chunking_strategy', sa.String(100), nullable=False),
        sa.Column('annotation_placement_strategy', sa.String(100), nullable=False),
        sa.Column('contains_summary', sa.String(3), nullable=False),  # "Yes" or "No"
        sa.Column('chunk_sequence', sa.String(10), nullable=False),
        sa.Column('similarity_score', sa.Float, nullable=False),
        sa.Column('chunk_file', sa.String(200), nullable=False),
    )

def downgrade():
    op.drop_table('chunk_similarity')
