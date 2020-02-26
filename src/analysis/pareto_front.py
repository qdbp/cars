import sqlite3 as sql
import time
from datetime import timedelta
import scipy.spatial as sss
import os
import plotly as plt
import plotly.express as px
import plotly.io

import numpy as np
import pandas as pd
from matplotlib.patches import Rectangle
from tqdm import tqdm

from src.util import CAR_DB

plotly.io.renderers.default = "firefox"


def load_recent_listings():
    conn = sql.Connection(CAR_DB)
    cutoff = time.time() - timedelta(days=7).total_seconds()

    listings = pd.read_sql_query(
        f"""
        SELECT
            ta.vin, mileage,
            (price_listing + price_fees) AS price,
            ta.year, ta.make, ta.model, ta.trim,
            (0.45 * ta.mpg_hwy + 0.55 * ta.mpg_city) AS mpg
        FROM truecar_listings tl
        JOIN (
            SELECT vin, MAX(timestamp) latest
            FROM truecar_listings
            WHERE timestamp > {cutoff}
            GROUP BY vin
        ) AS tf
        ON tl.vin = tf.vin AND tl.timestamp = tf.latest
        JOIN truecar_attrs ta ON tl.vin = ta.vin;
        """,
        conn,
    )

    return listings.set_index("vin")


def calculate_listing_pareto_front(listings: pd.DataFrame):

    listings = listings.copy()

    # basic filtering
    listings = listings[listings["mpg"] >= 35]
    listings = listings[listings["year"] >= 2015]
    listings = listings[listings["price"] <= 20000]
    listings = listings[listings["mileage"] >= 1000]

    makes = ["honda", "foyota", "ford", "hyundai"]
    models = ["camry", "accord", "fusion", "sonata"]

    listings = listings[listings["make"].str.lower().isin(makes)]
    listings = listings[listings["model"].str.lower().isin(models)]

    # invert "good" attributes for lexsort to be consistent -> smaller dominates
    listings["inv_mpg"] = 1 / listings["mpg"]
    listings["inv_year"] = 1 / listings["year"]

    points = listings[["mileage", "price", "inv_year", "inv_mpg"]].dropna()
    points = (points - points.mean(axis=0)) / points.std(axis=0)

    qhull = sss.ConvexHull(points)

    eqs = qhull.equations
    pareto_side = np.where(eqs @ np.array([-1, -1, -1, -1, 0]) > 0.25)

    pareto_vertices = np.unique(qhull.simplices[pareto_side].ravel())

    return listings.drop(["inv_mpg", "inv_year"], axis=1).iloc[pareto_vertices]


def display_listings(listings):

    for listing in listings.itertuples():
        print(listing)

    plt = px.scatter(
        listings.reset_index(),
        x="mileage",
        y="price",
        text="vin",
        hover_data=['mpg', 'year', "make", "model", "trim"],
    )
    plt.show()


if __name__ == "__main__":

    listings = load_recent_listings()
    pareto_listings = calculate_listing_pareto_front(listings)
    display_listings(pareto_listings)
