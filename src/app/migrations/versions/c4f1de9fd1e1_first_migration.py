"""First migration

Revision ID: c4f1de9fd1e1
Revises:
Create Date: 2022-05-09 15:15:19.083272

"""
import uuid

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "c4f1de9fd1e1"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "test_table",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            default=uuid.uuid4,
        ),
        sa.Column(
            "name",
            sa.Text,
            nullable=False,
        ),
    )


def downgrade():
    op.drop_table("test_table")
