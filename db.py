#!/usr/bin/env python
import os
from contextlib import contextmanager

from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.elements import BinaryExpression
from sqlalchemy.sql.operators import json_getitem_op, json_path_getitem_op

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

def visit_getitem_binary(compiler, binary, **kw):
    return "%s[%s]" % (
        compiler.process(binary.left, **kw),
        compiler.process(binary.right, **{**kw, "literal_binds": True})
    )

JSON_OPERATORS = [json_getitem_op, json_path_getitem_op]

@compiles(BinaryExpression)
def compile_binary(binary, compiler, override_operator=None, **kw):
    operator = override_operator or binary.operator

    if operator in JSON_OPERATORS:
        return visit_getitem_binary(
            compiler, binary, override_operator=override_operator, **kw
        )

    return compiler.visit_binary(binary, override_operator=override_operator, **kw)
