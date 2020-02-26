import sqlite3 as sql

import matplotlib.pyplot as plt
import pandas as pd

from src.util import CAR_DB
from src.util.plot import hexbin_pairplot


def load_latest_listings() -> pd.DataFrame:

    conn = sql.connect(CAR_DB)
    listings = pd.read_sql_query(
        # TODO this is probably unnecessarily complicated...
        """
        SELECT
            tla.vin, mileage, price_listing, price_fees, days_in_inventory,
            year, make, model, trim, style,
            color_rgb, color_interior, mpg_city, mpg_hwy 
        FROM (
            SELECT
                vin, MAX(timestamp) AS latest_timestamp
            FROM
                truecar_listings AS tl
            GROUP BY
                vin
        )
            AS latest_records
        INNER JOIN (
            truecar_listings AS tl
            LEFT JOIN truecar_attrs AS ta
                ON tl.vin = ta.vin
        ) AS tla
            ON latest_records.latest_timestamp = tla.timestamp
            AND latest_records.vin = tla.vin
        """,
        conn,
    )

    listings.set_index("vin", inplace=True)
    listings = listings[listings["days_in_inventory"] >= 0]
    return listings


def show_pairplot():

    listings = load_latest_listings()

    print(listings)
    print(listings.shape)
    print(listings.columns)

    fig = hexbin_pairplot(
        listings[["price_listing", "mileage", "year", "days_in_inventory"]]
    )
    plt.show()


def inspect_nhtsa():

    with sql.connect(CAR_DB) as conn:
        df = pd.read_sql_query(
            """
            SELECT * FROM nhtsa_attributes
            """,
            conn,
        )

    keep_set = set()
    for col in df.columns:
        c = df[col]

        na_ratio = (c == "").sum() / c.count()
        if na_ratio < 0.9:
            print("=" * 100)
            print(col)
            keep_set.add(col)
            print(na_ratio)
            print(df[col].value_counts())

    print(sorted(keep_set))

    # print('na ratio:', f'{na_ratio:.3f}')
    # print('value counts:')
