import sqlite3 as sql
import time
from datetime import timedelta

import pandas as pd
import reverse_geocoder as rg
from geopy import distance as gd

from src.util import CAR_DB


def load_recent_listings_and_dealerships(
    min_price: float = 1.0,
    max_price: float = 100_000,  # this is a shopping site for the sensible
    min_mileage: int = 0,
    max_mileage: int = 1_000_000,
    min_mpg: float = 10,
    max_mpg: float = 1e3,
):
    conn = sql.Connection(CAR_DB)

    listings = pd.read_sql_query(
        f"""
        SELECT
            ta.vin, mileage,
            (price_listing + price_fees) AS price,
            ta.year, ta.make, ta.model, ta.trim,
            (0.45 * ta.mpg_hwy + 0.55 * ta.mpg_city) AS mpg,
            ta.fuel_type,
            td.pos_lat AS lat, td.pos_lon as lon,
            tl.dealer_id, td.name AS dealer_name,
            ('#' || ta.color_rgb) as color_hex
        FROM truecar_listings_snapshot tl
        JOIN truecar_attrs ta ON tl.vin = ta.vin
        JOIN truecar_dealerships td on tl.dealer_id = td.dealer_id
        WHERE mpg >= {min_mpg}
        AND mpg <= {max_mpg}
        AND year >= 1900
        AND mileage >= {min_mileage}
        AND mileage <= {max_mileage}
        AND (price_fees + price_listing) >= {min_price}
        AND (price_fees + price_listing) <= {max_price}
        """,
        conn,
    )

    conn.close()

    return listings


def filter_listings_by_distance(
    listings: pd.DataFrame, max_miles: int, lat: float, lon: float
):
    def dist(xy_arr) -> float:
        return gd.geodesic(xy_arr, (lat, lon)).mi

    distances = listings[["lat", "lon"]].apply(dist, raw=True, axis=1)

    return listings[distances < max_miles]


def filter_listings_by_state(listings: pd.DataFrame, states: str):

    rcoded = rg.search(tuple(listings[["lat", "lon"]].itertuples(index=False)))
    mask = [rc["admin1"] in states for rc in rcoded]
    return listings[mask]