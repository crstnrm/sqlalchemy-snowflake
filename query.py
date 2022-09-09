import json

from sqlalchemy import func, select, desc, Integer
from sqlalchemy.sql.expression import cast
from sqlalchemy.orm import aliased

import db
from model import EmailDataRaw


def get_version():
    with db.connection() as conn:
        [version] = conn.execute('select current_version()').fetchone()
        return version


def get_first_5_rows_legacy():
    with db.connection() as conn:
        results = conn.execute(select(EmailDataRaw).limit(5))
    return results


def get_first_5_rows():
    with db.Session() as session:
        results = session.query(EmailDataRaw).limit(5)
    return results


def filter_rows_legacy_by_v():
    statement = (
        select(EmailDataRaw)
        .filter(EmailDataRaw.V["thread_id"] == "178eb206326b9e85")
        .limit(5)
    )

    print(statement.compile(compile_kwargs={"literal_binds": True}))

    with db.connection() as conn:
        results = conn.execute(statement)
    return results


def filter_rows_legacy_by_v_oid():
    statement = (
        select(EmailDataRaw)
        .filter(EmailDataRaw.V["_id"]["$oid"] == "607dba7862de111f4f18d9d2")
        .limit(1)
    )

    print(statement.compile(compile_kwargs={"literal_binds": True}))

    with db.connection() as conn:
        results = conn.execute(statement)
    return results

def filter_rows_legacy_group_by():
    statement = (
        select(EmailDataRaw.V["thread_id"], func.count(EmailDataRaw.V).label('counter'))
        .group_by(EmailDataRaw.V["thread_id"])
        .order_by(desc(EmailDataRaw.V["thread_id"]))
        .limit(5)
    )

    print(statement.compile(compile_kwargs={"literal_binds": True}))

    with db.connection() as conn:
        results = conn.execute(statement)
    return results


def filter_rows_legacy_joins():
    Reflex = aliased(EmailDataRaw)

    statement = (
        select(func.sum(cast(EmailDataRaw.V["topics_discussed"][0]["num_segments"], Integer)).label('total'))
        .join(Reflex, Reflex.V["company"]["$oid"] == EmailDataRaw.V["company"]["$oid"])
        .filter(Reflex.V["company"]["$oid"] == "5a31b9f37e9beb6b6c1da122")
    )

    print(statement.compile(compile_kwargs={"literal_binds": True}))

    with db.connection() as conn:
        results = conn.execute(statement)
    return results


def print_info(rows):
    print("cursor type: %s" % type(rows))

    rows = list(rows)
    print("row type: %s" % type(rows[0]))
    print("column type (variant): %s" % type(rows[0].V))

    print("rows length: %s" % len(rows))

    column = json.loads(rows[0].V)
    print("column type (variant) using json.loads: %s" % type(column))
    print(column)


if __name__ == "__main__":
    # print(get_version())

    # print("############# Retrieve variant type (Legacy) #############")

    # print_info(get_first_5_rows_legacy())

    # print("\n############# Retrieve variant type #############")

    # print_info(get_first_5_rows())

    # print("\n############# Filter by variant type #############")

    # print_info(filter_rows_legacy_by_v())

    # print("\n############# Filter by variant type - filter by #############")

    # print_info(filter_rows_legacy_by_v_oid())

    # print("\n############# Filter by variant type - group by #############")

    # print(list(filter_rows_legacy_group_by()))

    print("\n############# Filter by variant type - joins #############")

    print(list(filter_rows_legacy_joins()))
