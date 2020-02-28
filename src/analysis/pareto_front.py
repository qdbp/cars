import sqlite3 as sql
import time
from datetime import timedelta

import dash
import dash.dependencies as dd
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import geopy.distance as gd
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.io
import reverse_geocoder as rg
import scipy.spatial as sss
from shapely.geometry.polygon import Polygon
from shapely.geometry.point import Point
import plotly.graph_objects as go

from src.util import CAR_DB, csvfile

plotly.io.renderers.default = "chromium"


def load_recent_listings_and_dealerships(
    min_price: float = 0.0,
    max_price: float = 1e9,
    min_mileage: int = 0,
    max_mileage: int = 1000000,
    min_mpg: float = 1.0,
    max_mpg: float = 1e3,
):
    conn = sql.Connection(CAR_DB)
    cutoff = time.time() - timedelta(days=7).total_seconds()

    listings = pd.read_sql_query(
        f"""
        SELECT
            ta.vin, mileage,
            (price_listing + price_fees) AS price,
            ta.year, ta.make, ta.model, ta.trim,
            (0.45 * ta.mpg_hwy + 0.55 * ta.mpg_city) AS mpg,
            td.pos_lat AS lat,  td.pos_lon as lon,
            tl.dealer_id, td.name AS dealer_name
        FROM truecar_listings tl
        JOIN (
            SELECT vin, MAX(timestamp) latest
            FROM truecar_listings
            WHERE timestamp > {cutoff}
            AND mileage >= {min_mileage}
            AND mileage <= {max_mileage}
            GROUP BY vin
        ) AS tf
        ON tl.vin = tf.vin AND tl.timestamp = tf.latest
        JOIN truecar_attrs ta ON tl.vin = ta.vin
        JOIN truecar_dealerships td on tl.dealer_id = td.dealer_id
        WHERE mpg >= {min_mpg}
        AND mpg <= {max_mpg}
        AND price >= {min_price}
        AND price <= {max_price}
        """,
        conn,
    )

    all_dealerships = pd.read_sql_query(
        f"""
        SELECT * FROM truecar_dealerships
        """,
        conn,
    ).set_index("dealer_id")

    dealerships = all_dealerships.join(
        listings["dealer_id"].drop_duplicates(), how="inner"
    )

    conn.close()

    return listings  # , dealerships


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


def calculate_listing_pareto_front(
    listings: pd.DataFrame, max_miles: int = 150
):

    # FIXME this is an approximation -- can we do it for realsies?

    # invert "good" attributes for lexsort to be consistent -> smaller dominates
    listings["inv_mpg"] = 1 / listings["mpg"]
    listings["inv_year"] = 1 / listings["year"]

    points = listings[["mileage", "price", "inv_year", "inv_mpg"]].dropna()
    points = (points - points.mean(axis=0)) / points.std(axis=0)

    qhull = sss.ConvexHull(points)

    eqs = qhull.equations
    pareto_side = np.where(eqs @ np.array([-1, -1, -1, -1, 0]) > 0.25)

    pareto_vertices = np.unique(qhull.simplices[pareto_side].ravel())

    listings.drop(["inv_mpg", "inv_year"], axis=1, inplace=True)
    return listings.iloc[pareto_vertices]


def setup_dash_layout(all_listings: pd.DataFrame,) -> dash.Dash:

    graph = dcc.Graph(
        id="scatter-price-mileage", config=dict(displayModeBar=False)
    )

    year_slider = dcc.RangeSlider(
        "cars-year-slider",
        min=(mn := all_listings["year"].min()),
        max=(mx := all_listings["year"].max()),
        value=[2012, 2018],
        marks={y: str(y) for y in range(mn, mx + 1)},
        vertical=True,
    )

    mileage_slider = dcc.RangeSlider(
        "cars-mileage-slider",
        min=(mn := all_listings["mileage"].min()),
        max=(mx := all_listings["mileage"].max()),
        value=[10000, 70000],
        marks={y: f"{y//1000}k" for y in range(0, mx, 25000)},
        step=1,
        vertical=True,
    )

    price_slider = dcc.RangeSlider(
        "cars-price-slider",
        min=(mn := all_listings["price"].min()),
        max=(mx := all_listings["price"].max()),
        value=[10000, 35000],
        marks={int(y): f"{y//1000}k" for y in range(0, int(mx) + 5000, 5000)},
        step=1,
        vertical=True,
    )

    mpg_slider = dcc.RangeSlider(
        "cars-mpg-slider",
        min=(mn := all_listings["mpg"].min()),
        max=(mx := all_listings["mpg"].max()),
        value=[20, mx],
        marks={int(y): f"{y:.0f}" for y in range(10, int(mx) + 1, 10)},
        step=1,
        vertical=True,
    )

    # cities = pd.read_csv(csvfile('2014_us_cities')).iloc[:1000]
    dealerships = all_listings[~all_listings["dealer_id"].duplicated()]

    geo_plot = px.scatter_geo(
        dealerships,
        lat="lat",
        lon="lon",
        text="dealer_name",
        height=500,
        scope="usa",
        locationmode="USA-states",
    )
    geo_plot.update_layout(
        margin=dict(l=0, r=0, b=0, t=0),
        clickmode="event+select",
        mapbox_style="open-street-map",
    )

    loc_picker = dcc.Graph(
        "cars-loc-picker",
        figure=geo_plot,
        config={"modeBarButtonsToRemove": ["hoverClosestGeo", "toImage"]}
        # config=dict(displayModeBar=False),
    )

    app = dash.Dash(
        "cars, cars, cars!", external_stylesheets=[dbc.themes.BOOTSTRAP]
    )

    input_content = dbc.Row(
        [
            dbc.Col(
                [html.P("💰", style=dict(margin=0)), price_slider],
                width=1,
                style={"text-align": "center"},
            ),
            dbc.Col(
                [html.P("📅", style=dict(margin=0)), year_slider],
                width=1,
                style={"text-align": "center"},
            ),
            dbc.Col(
                [html.P("🏁️", style=dict(margin=0)), mileage_slider],
                width=1,
                style={"text-align": "center"},
            ),
            dbc.Col(
                [html.P("⛽️️", style=dict(margin=0)), mpg_slider],
                width=1,
                style={"text-align": "center"},
            ),
            dbc.Col(
                [html.P("📍 ️️", style=dict(margin=0)), loc_picker],
                width=8,
                style={"text-align": "center"},
            ),
        ]
    )
    output_content = dbc.Row(
        [
            dbc.Col(graph, width=9),
            dbc.Col(
                dbc.Alert(id="output-link", style={"text-align": "center"}),
                width=3,
            ),
        ]
    )

    app.layout = dbc.Container([input_content, output_content])

    return app


def plot_listings(listings):

    plt = go.Figure(
        go.Scatter(
            x=listings["mileage"],
            y=listings["price"],
            customdata=listings[
                ["vin", "make", "model", "trim", "mpg", "dealer_name"]
            ],
            hovertemplate="<b>%{customdata}</b>",
            text=listings["year"] % 100,
            marker={"color": listings["mpg"], "colorbar": dict(title="mpg"),},
            mode="markers+text",
            # hover_data=["mpg", "year", "make", "model", "trim"],
        ),
        layout={"height": 430, "clickmode": "event"},
    )

    return plt


def setup_callbacks(
    app: dash.Dash, listings_universe: pd.DataFrame,
):

    ###
    # this callback sets up slider, make and model inputs
    ###
    @app.callback(
        dd.Output("scatter-price-mileage", "figure"),
        [
            dd.Input("cars-year-slider", "value"),
            dd.Input("cars-mileage-slider", "value"),
            dd.Input("cars-price-slider", "value"),
            dd.Input("cars-mpg-slider", "value"),
            dd.Input("cars-loc-picker", "selectedData"),
        ],
    )
    def generate_filtered_graph(
        year_limits, mileage_limits, price_limits, mpg_limits, selected
    ):

        listings = listings_universe.copy()

        for col, range_limit in {
            "year": year_limits,
            "mileage": mileage_limits,
            "price": price_limits,
            "mpg": mpg_limits,
        }.items():

            mn, mx = sorted(range_limit)
            listings = listings[
                (mn <= listings_universe[col]) & (listings_universe[col] <= mx)
            ]

        if selected is not None and "lassoPoints" in selected:
            poly = Polygon(selected["lassoPoints"]["geo"])

            def contained_in_poly(xy_raw):
                p = Point(xy_raw)
                return p.within(poly)

            mask = listings[["lon", "lat"]].apply(
                contained_in_poly, raw=True, axis=1
            )
            listings = listings[mask]

        listings = calculate_listing_pareto_front(listings)

        return plot_listings(listings)

    @app.callback(
        dd.Output("output-link", "children"),
        [dd.Input("scatter-price-mileage", "clickData")],
    )
    def fill_in_link(click_data) -> str:
        if click_data is not None:
            data = click_data["points"][0]["customdata"]
            vin, make, model, trim, *_ = data

            return html.A(
                f"{make} {model} [{vin}] on Truecar",
                href=f"https://www.truecar.com/used-cars-for-sale/listing/{vin}/",
            )
        else:
            return "Click a point on the graph to see the purchase link."


def prefilter_listings(listings):
    """
    Eliminate obvious garbage listings.

    Args:
        listings:

    Returns:

    """

    listings = listings[listings["mpg"] > 0]
    listings = listings[listings["price"] <= 100000]
    return listings


def run_server(app: dash.Dash):
    app.run_server(debug=True)


def main():

    listings = load_recent_listings_and_dealerships()
    listings = prefilter_listings(listings)

    app = setup_dash_layout(listings)
    setup_callbacks(app, listings)
    run_server(app)


if __name__ == "__main__":
    main()
