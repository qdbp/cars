import sqlite3 as sql

import pandas as pd

from cars.util import CAR_DB

if __name__ == "__main__":
    df = pd.read_sql(
        """
        SELECT substr(vin, 0, 14) as svin, tya.*
        FROM truecar_listings
        JOIN truecar_ymms_attrs tya on tya.ymmt_id = truecar_listings.ymmt_id
        """,
        sql.connect(CAR_DB),
    ).drop_duplicates()

    key = ["svin", "ymmt_id"]

    duped = df[key].groupby("svin").filter(lambda x: len(x) > 1)

    merged = pd.merge(duped, df, on=key, how="left")

    print("foo")
