from snowflake.sqlalchemy import VARIANT
from sqlalchemy import JSON, Column
from sqlalchemy.sql.sqltypes import Indexable

from db import Base


class Custom(Indexable, VARIANT):
    comparator_factory = JSON.Comparator


class EmailDataRaw(Base):
    __tablename__ = "email_data_raw"

    V = Column(Custom, primary_key=True)
