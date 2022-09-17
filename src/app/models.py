"""Models for the database"""
from uuid import uuid4

from sqlalchemy import text as sqlalchemy_text
from sqlalchemy import Column, Text
from sqlalchemy.dialects.postgresql import UUID

from app.database import db_instance

Base = db_instance.base


def string_uuid():
    return str(uuid4())


class ExampleTable(Base):
    __tablename__ = "test_table"

    id = Column(
        UUID,
        primary_key=True,
        default=string_uuid,
        server_default=sqlalchemy_text("uuid_generate_v4()"),
    )
    name = Column(Text, nullable=False)
