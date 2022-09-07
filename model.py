from snowflake.sqlalchemy import VARIANT
from sqlalchemy import Column
from sqlalchemy import JSON
from sqlalchemy.sql.sqltypes import Indexable

from db import Base


# TODO This does not work yet
class VARIANT_(Indexable, VARIANT):
    comparator_factory = JSON.Comparator


class EmailDataRaw(Base):
    __tablename__ = "email_data_raw"

    V = Column(VARIANT, primary_key=True)
