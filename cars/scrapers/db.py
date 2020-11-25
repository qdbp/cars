import sqlite3 as sql

from pandas import DataFrame, read_sql

from cars.util import CAR_DB


def get_conn() -> sql.Connection:
    return sql.connect(CAR_DB)


def get_dealerships() -> DataFrame:

    return read_sql(
        """
        SELECT * FROM truecar_dealerships
        """,
        get_conn(),
    )


def get_ymmt_attrs() -> DataFrame:

    return read_sql()
