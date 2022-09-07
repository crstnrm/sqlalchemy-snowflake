import json

from sqlalchemy import select, text
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
        # This does not work yet -
        # .filter(EmailDataRaw.V["thread_id"] == "178eb206326b9e85")
        # This works
        .filter(EmailDataRaw.V.op(':')(text('thread_id')) == "178eb206326b9e85")
        .limit(5)
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

    column = json.loads(rows[0].V)
    print("column type (variant) using json.loads: %s" % type(column))
    print(column)


if __name__ == "__main__":
    # print(get_version())

    # print("############# Retrieve variant type (Legacy) #############")

    # print_info(get_first_5_rows_legacy())

    # print("\n############# Retrieve variant type #############")

    # print_info(get_first_5_rows())

    print("\n############# Filter by variant type #############")

    print(list(filter_rows_legacy_by_v()))
