#!/usr/bin/env python
import os
from contextlib import contextmanager

from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

SNOWFLAKE_USER = os.environ.get("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_DATABASE = os.environ.get("SNOWFLAKE_DATABASE")
SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")

engine = create_engine(
    URL(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        warehouse=SNOWFLAKE_WAREHOUSE,
    )
)

@contextmanager
def connection():
    connection = engine.connect()
    try:
        yield connection
    finally:
        connection.close()
        engine.dispose()


Base = declarative_base()

Session = sessionmaker(engine)
