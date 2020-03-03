from collections import defaultdict
from functools import lru_cache, partial
from typing import Dict, Optional, List

import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import numpy as np
import pandas as pd
import zipcodes as zp
from dash import dependencies as dd
from numba import jit
from plotly import graph_objects as go

from src.analysis.etl import load_recent_listings_and_dealerships
from src.analysis.geo import great_circle_miles
from src.analysis.pareto_front import calculate_listing_pareto_front

SLIDER_MPG_ID = "cars-slider-mpg"
SLIDER_MILEAGE_ID = "cars-slider-mileage"
SLIDER_PRICE_ID = "cars-slider-price"
SLIDER_YEAR_ID = "cars-slider-year"

SLIDER_ICON_MPG_ID = "cars-slider-mpg-icon"
SLIDER_ICON_MILEAGE_ID = "cars-slider-mileage-icon"
SLIDER_ICON_PRICE_ID = "cars-slider-price-icon"
SLIDER_ICON_YEAR_ID = "cars-slider-year-icon"


MMT_MM_PICKER_ID = "mm_picker"
MMT_MATRIX_ID = "mmt_matrix"

SCATTER_ID = "scatter_box"
LOC_PICKER_ID = "loc_picker_box"

INFO_A_ID = "cars-info-a"
INFO_B_ID = "cars-info-b"
INFO_C_ID = "cars-info-c"
INFO_D_ID = "cars-info-d"

CACHED_DATA = "cached-data"


def concat_mmt_label(mmt_arr: np.ndarray) -> str:
    return f"{mmt_arr[0].upper()} - {mmt_arr[1].upper()} - {mmt_arr[2]}"


class DivSkeleton(Dict[str, html.Div]):
    """
    A dict of named ids to Divs. These divs should be "holes" expecting a single
    interactive input or output component.
    """

    def fill(self, key: str, object):
        # this check has saved a LOT of time. don't doubt defensive programming
        # especially in string-stew frontend code
        if len(self[key].children) == 0:
            self[key].children = object
        else:
            print(self[key].children)
            raise ValueError(f"Container {key} already filled")


def create_div_skeleton() -> DivSkeleton:
    """
    Returns:
        the core div skeleton of the app. Specifically, the root, and
        a dictionary mapping interactive div ID named

    """

    # top level:
    # +--------------+----------------------------------+------------+
    # |              |                                  |            |
    # |              |                                  |            |
    # |              |                                  |            |
    # |              |                                  |            |
    # |   sliders    |         location                 |  info      |
    # |              |           map                    |            |
    # |              |                                  |            |
    # |              |                                  |            |
    # |              |                                  |            |
    # |              |                                  |            |
    # +--------------------------------------------------------------+
    # |                     |                                        |
    # |                     |                                        |
    # |   make              |                                        |
    # |   model             |                                        |
    # |                     |          output chart                  |
    # |                     |                                        |
    # |                     |                                        |
    # |                     |                                        |
    # |                     |                                        |
    # |                     |                                        |
    # |                     |                                        |
    # |                     |                                        |
    # +--------------+----------------------------------+------------+

    # sliders:
    # +----+----+----+----+
    # |    |    |    |    |
    # |icon|    |    |    |
    # +-------------------+
    # |    |    |    |    |
    # |    |    |    |    |
    # | s  |    |    |    |
    # | l  |    |    |    |
    # | i  |    |    |    |
    # | d  |    |    |    |
    # | e  |    |    |    |
    # | r  |    |    |    |
    # |    |    |    |    |
    # |    |    |    |    |
    # |    |    |    |    |
    # |    |    |    |    |
    # |    |    |    |    |
    # |    |    |    |    |
    # |    |    |    |    |
    # |    |    |    |    |
    # |    |    |    |    |
    # |    |    |    |    |
    # |    |    |    |    |
    # +----+----+----+----+

    all_divs = {}

    def D(div_id, /, *args, **kwargs):
        if "className" in kwargs:
            kwargs["className"] += " sk_div"
        else:
            kwargs["className"] = "sk_div"
        out = html.Div(id="_sk_div_" + div_id, children=args, **kwargs)
        all_divs[div_id] = out
        return out

    def mk_slider_divs(slider_id: str, icon_id: str):
        return D(
            f"{slider_id}_box",
            D(icon_id, className="slider_icon"),
            D(slider_id, className="slider"),
            className="slider_box",
        )

    sliders = [
        mk_slider_divs(SLIDER_MPG_ID, SLIDER_ICON_MPG_ID),
        mk_slider_divs(SLIDER_MILEAGE_ID, SLIDER_ICON_MILEAGE_ID),
        mk_slider_divs(SLIDER_PRICE_ID, SLIDER_ICON_PRICE_ID),
        mk_slider_divs(SLIDER_YEAR_ID, SLIDER_ICON_YEAR_ID),
    ]

    sliders_box = D("slider_box", *sliders)

    loc_picker_box = D(LOC_PICKER_ID)

    mmt_box = D("mmt_box", D(MMT_MM_PICKER_ID), D(MMT_MATRIX_ID),)

    scatter_box = D(SCATTER_ID)

    alert_boxes = [
        D(INFO_A_ID, className="info_box"),
        D(INFO_B_ID, className="info_box"),
        D(INFO_C_ID, className="info_box"),
        D(INFO_D_ID, className="info_box"),
    ]

    info_box = D("info_box", *alert_boxes)

    D(
        "root",
        sliders_box,
        loc_picker_box,
        mmt_box,
        scatter_box,
        info_box,
        D(CACHED_DATA, className="hidden"),
    )

    return DivSkeleton(all_divs)


def setup_dash_layout(all_listings: pd.DataFrame, sk: DivSkeleton) -> dash.Dash:

    ### SLIDERS
    # FIXME tie to layout callbacks

    def create_sliders():
        slider_height = 460

        year_slider = dcc.RangeSlider(
            # FIXME variablefy all these stupid ids
            "input-year",
            min=(mn := all_listings["year"].min()),
            max=(mx := all_listings["year"].max()),
            value=[2012, 2018],
            marks={y: str(y) for y in range(mn, mx + 1)},
            vertical=True,
            updatemode="mouseup",
            verticalHeight=slider_height,
            persistence=True,
            persistence_type="session",
        )

        mileage_slider = dcc.RangeSlider(
            "input-mileage",
            min=(mn := all_listings["mileage"].min()),
            max=(mx := all_listings["mileage"].max()),
            value=[10000, 70000],
            marks={y: f"{y//1000}k" for y in range(0, mx, 25000)},
            step=1,
            vertical=True,
            updatemode="mouseup",
            verticalHeight=slider_height,
            persistence=True,
            persistence_type="session",
        )

        price_slider = dcc.RangeSlider(
            "input-price",
            min=(mn := all_listings["price"].min()),
            max=(mx := all_listings["price"].max()),
            value=[10000, 35000],
            marks={
                int(y): f"{y//1000}k" for y in range(0, int(mx) + 5000, 5000)
            },
            step=1,
            vertical=True,
            updatemode="mouseup",
            verticalHeight=slider_height,
            persistence=True,
            persistence_type="session",
        )

        mpg_slider = dcc.RangeSlider(
            "input-mpg",
            min=(mn := all_listings["mpg"].min()),
            max=(mx := all_listings["mpg"].max()),
            value=[20, mx],
            marks={int(y): f"{y:.0f}" for y in range(10, int(mx) + 1, 10)},
            step=1,
            vertical=True,
            updatemode="mouseup",
            verticalHeight=slider_height,
            persistence=True,
            persistence_type="session",
        )

        return year_slider, mileage_slider, price_slider, mpg_slider

    year_slider, mileage_slider, price_slider, mpg_slider = create_sliders()

    # everything in its right place
    sk.fill(SLIDER_ICON_MILEAGE_ID, html.P("🏁️", style=dict(margin=0)))
    sk.fill(SLIDER_MILEAGE_ID, mileage_slider)

    sk.fill(SLIDER_ICON_PRICE_ID, html.P("💰", style=dict(margin=0)))
    sk.fill(SLIDER_PRICE_ID, price_slider)

    sk.fill(SLIDER_ICON_YEAR_ID, html.P("📅️️", style=dict(margin=0)))
    sk.fill(SLIDER_YEAR_ID, year_slider)

    sk.fill(SLIDER_ICON_MPG_ID, html.P("⛽️️", style=dict(margin=0)))
    sk.fill(SLIDER_MPG_ID, mpg_slider)

    ### SCATTER
    scatter_graph = html.Div(id="scatter_box")

    sk.fill(SCATTER_ID, scatter_graph)

    loc_picker = html.Div(
        id="loc_picker_box",
        children=[
            dbc.Alert(
                "Select your location to see cars.",
                id="alert-map",
                color="primary",
                style={"text-align": "center"},
            ),
            dbc.InputGroup(
                id="zip_picker_box",
                children=[
                    dbc.InputGroupAddon("📍"),
                    dcc.Input(
                        id="input-zipcode",
                        # options=[
                        #     {"label": (code := z["zip_code"]), "value": code}
                        #     for z in zp.list_all()
                        #     if z["zip_code_type"] == "STANDARD"
                        # ],
                        placeholder="Zip code",
                        # searchable=False,
                        # clearable=False,
                        # list = [z['zip_code']  for z in zp.list_all() if z['zip_code_type'] == 'STANDARD'],
                        persistence=True,
                        persistence_type="session",
                    ),
                ],
            ),
            dbc.InputGroup(
                id="dist_picker_box",
                children=[
                    dbc.InputGroupAddon("↔"),
                    dcc.Input(
                        id="input-max-distance",
                        placeholder="Maximum distance, miles",
                        type="number",
                        persistence=True,
                        persistence_type="session",
                        debounce=True,
                    ),
                ],
            ),
        ],
    )

    sk.fill(LOC_PICKER_ID, loc_picker)

    ### MMT refinement
    # N.B. these are pre-filtered by selection
    mm_picker_menu = dcc.Dropdown(
        id="input-mm-picker",
        # options by callback
        multi=True,
        placeholder="Select makes",
        persistence=False,
        persistence_type="session",
        clearable=False,
    )

    sk.fill(MMT_MM_PICKER_ID, mm_picker_menu)

    ### ALERTS

    alert_link = dbc.Alert(
        id="output-link",
        style={"text-align": "center"},
        children="Click on a plot point to see details.",
    )

    app = dash.Dash(
        "cars, cars, cars!",
        external_stylesheets=[dbc.themes.CERULEAN],
        suppress_callback_exceptions=True,
    )
    # app.wsgi_app = ProfilerMiddleware(app.wsgi_app, restrictions=[50])

    # sk.fill(INFO_A_ID, alert_map)
    sk.fill(INFO_A_ID, alert_link)

    ### CACHES
    lasso_cache = html.Div(id="cache-lasso")
    sk.fill(CACHED_DATA, [lasso_cache])

    app.layout = sk["root"]
    return app


def plot_listings(listings):

    fig = go.Figure(
        go.Scattergl(
            x=listings["mileage"],
            y=listings["price"],
            customdata=listings[
                [
                    "vin",
                    "make",
                    "model",
                    "trim",
                    "mpg",
                    "dealer_name",
                    "distance",
                ]
            ],
            hovertemplate=(
                "<i>%{customdata[0]}</i><br>"
                "<b>%{customdata[1]} %{customdata[2]} %{customdata[3]}</b><br>"
                "Dealer: %{customdata[5]}<br>"
                "<b>About %{customdata[6]:.0f} miles from you.</b>"
            ),
            text=listings["year"] % 100,
            textposition="bottom center",
            marker={
                "color": listings["mpg"],
                "colorbar": {"title": "mpg",},
                "size": 10,
                "cmin": 20,
                "cmax": 50,
                "line": {
                    "color": listings["color_hex"].apply(
                        lambda x: x or "#000000"
                    ),
                    # no line if color is unknown, 3 otherwise
                    "width": 3
                    * listings["color_hex"]
                    .apply(lambda x: x is not None)
                    .astype(int),
                },
            },
            mode="markers+text",
        ),
        layout={
            "height": 430,
            "clickmode": "event",
            "xaxis": {"title": "Mileage, mi",},
            "yaxis": {"title": "Price, $",},
            "margin": dict(b=0, t=0, l=0, r=0, pad=0),
        },
    )

    graph = dcc.Graph(
        id="scatter-price-mileage",
        config=dict(displayModeBar=False),
        figure=fig,
    )

    return graph


def setup_data_callbacks(
    app: dash.Dash, listings_universe: pd.DataFrame,
) -> None:
    """
    This function runs once at server startup and configures callbacks between
    app components.

    Pre-aggregations of various data are also computed by this call. These are
    computed from the set of all car listings passed to this function. This set
    will form the universe of all accessible listings to the app, as no
    subsequent database calls are made.

    Args:
        app: the app to configure
        listings_universe: the dataframe of all listings accessible to the app

    """

    slider_inputs = {
        "year": dd.Input("input-year", "value"),
        "mileage": dd.Input("input-mileage", "value"),
        "price": dd.Input("input-price", "value"),
        "mpg": dd.Input("input-mpg", "value"),
    }

    ###
    # cache various aggregates in a fast-lookup form for the callbacks
    ###
    years_mm_available = {
        (make, model): set(df["year"].unique())
        for (make, model), df in listings_universe.groupby(["make", "model"])
    }
    mm_set = set(years_mm_available.keys())

    @lru_cache(maxsize=1 << 10)
    def check_mm_in_year_bounds(make, model, min_y, max_y):
        return years_mm_available[(make, model)] & set(range(min_y, max_y + 1))

    years_mmt_available = {
        (make, model, trim): set(df["year"].unique())
        for (make, model, trim), df in listings_universe.groupby(
            ["make", "model", "trim"]
        )
    }

    @lru_cache(maxsize=1 << 10)
    def check_mmt_in_year_bounds(make, model, trim, min_y, max_y):
        return years_mmt_available[(make, model, trim)] & set(
            range(min_y, max_y + 1)
        )

    mpg_bounds_by_mm = {
        (make, model): (min_mpg, max_mpg)
        for (make, model), df in listings_universe.groupby(["make", "model"])
        for (min_mpg, max_mpg) in [df["mpg"].agg(["min", "max"])]
    }

    @lru_cache(maxsize=1 << 10)
    def check_mm_in_mpg_bounds(
        make: str, model: str, min_mpg: float, max_mpg: float
    ) -> bool:
        mmin, mmax = mpg_bounds_by_mm[(make, model)]
        return mmin <= max_mpg and min_mpg <= mmax

    mpg_bounds_by_mmt = {
        (make, model, trim): (min_mpg, max_mpg)
        for (make, model, trim), df in listings_universe.groupby(
            ["make", "model", "trim"]
        )
        for (min_mpg, max_mpg) in [df["mpg"].agg(["min", "max"])]
    }

    @lru_cache(maxsize=1 << 10)
    def check_mmt_in_mpg_bounds(
        make: str, model: str, trim, min_mpg: float, max_mpg: float
    ) -> bool:
        mmin, mmax = mpg_bounds_by_mmt[(make, model, trim)]
        return mmin <= max_mpg and min_mpg <= mmax

    trims_by_mm = {
        (make, model): {trim for trim in df["trim"].unique()}
        for (make, model), df in listings_universe.groupby(["make", "model"])
    }

    ###
    # this callbacks restricts visible makes by the year range selected
    ###
    @app.callback(
        dd.Output("input-mm-picker", "options"),
        [slider_inputs["year"], slider_inputs["mpg"]],
    )
    def restrict_make_options(year_range, mpg_range):
        return [
            {"label": f"{make} {model}", "value": ";".join((make, model))}
            for make, model in mm_set
            if check_mm_in_year_bounds(make, model, *year_range)
            if check_mm_in_mpg_bounds(make, model, *mpg_range)
        ]

    # FIXME test
    @app.callback(
        dd.Output("_sk_div_mmt_matrix", "children"),
        [
            dd.Input("input-mm-picker", "value"),
            dd.Input("input-mm-picker", "options"),
            slider_inputs["year"],
            slider_inputs["mpg"],
        ],
    )
    def generate_trim_year_refinement_menu(
        selected_values: Optional[List[str]],
        options: Optional[Dict[str, str]],
        year_range,
        mpg_range,
    ) -> List:  # todo finish type

        if selected_values is None or options is None:
            return []

        visible_values = {opt["value"] for opt in options}

        # selected values that are hidden by external action (e.g. when the year
        # slider excludes a previously-valid choice, are not actually deselected
        # because of `persistence`. However, we do want to exclude them.
        include_values = visible_values & set(selected_values)

        out = []
        for val in include_values:
            make, model = val.split(";")
            trims = trims_by_mm[(make, model)]

            trims = [
                trim
                for trim in trims
                if check_mmt_in_year_bounds(make, model, trim, *year_range)
            ]
            trims = [
                trim
                for trim in trims
                if check_mmt_in_mpg_bounds(make, model, trim, *mpg_range)
            ]

            years = years_mm_available[(make, model)] & set(
                range(year_range[0], year_range[1] + 1)
            )

            toast = dbc.Toast(
                header=f"{make} {model}:",
                id=f"mmt_refine|{make};{model}",
                className="refine_container",
            )

            if len(trims) == 0:
                toast.children = (
                    "No trims available for your year and mpg limits."
                )
                out.append(toast)
                continue

            checklist_year = dbc.Checklist(
                f"input-mmty-refine-year_{make}_{model}",
                options=(
                    opts := [
                        # reverse sort to match slider
                        {"label": year, "value": year}
                        for year in sorted(years)[::-1]
                    ]
                ),
                value=[opt["value"] for opt in opts],
                persistence=True,
                persistence_type="memory",
                inputClassName="year-refine-input",
                labelClassName="year-refine-label",
                className="year-refine",
            )

            checklist_trim = dbc.Checklist(
                f"input-mmty-refine-trim_{make}_{model}",
                options=(
                    opts := [
                        {"label": trim, "value": f"{make};{model};{trim}"}
                        for trim in sorted(trims)
                    ]
                ),
                value=[opt["value"] for opt in opts],
                persistence=True,
                persistence_type="memory",
                inputClassName="trim-refine-input",
                labelClassName="trim-refine-label",
                className="trim-refine",
            )

            toast.children = [
                html.Div(checklist_year, className="checklist-container-year",),
                html.Div(checklist_trim, className="checklist-container-trim",),
            ]
            out.append(toast)

        return out

    latlong_by_zip = {
        z["zip_code"]: (float(z["lat"]), float(z["long"]))
        for z in zp.list_all()
        if z["zip_code_type"] == "STANDARD"
    }

    @jit
    def metric(dealer_latlong: np.ndarray, lat: float, long: float) -> float:
        print(dealer_latlong, lat, long)
        out = great_circle_miles(
            lat0=dealer_latlong[0], lon0=dealer_latlong[1], lat1=lat, lon1=long
        )
        print(out)
        return out

    def filter_listings_by_location(listings, zipcode: str, max_miles: float):

        lat, long = latlong_by_zip[zipcode]
        this_metric = partial(metric, lat=lat, long=long)

        dealerships = listings[~listings["dealer_id"].duplicated()].copy()
        dealerships["distance"] = dealerships[["lat", "lon"]].apply(
            this_metric, raw=True, axis=1
        )

        close_dealerships = dealerships[
            dealerships["distance"] <= max_miles
        ].set_index("dealer_id")

        listings = listings.join(
            close_dealerships[["distance"]], on="dealer_id", how="inner"
        )

        return listings

    ###
    # this callback filters the scatterplot output based on all relevant inputs
    ###
    @app.callback(
        dd.Output("scatter_box", "children"),
        [
            slider_inputs["price"],
            slider_inputs["mileage"],
            dd.Input("_sk_div_mmt_matrix", "children"),
            dd.Input("input-zipcode", "value"),
            dd.Input("input-max-distance", "value"),
        ],
    )
    def generate_filtered_graph(
        price_limits, mileage_limits, matrix_children, zipcode, max_distance,
    ):

        listings = listings_universe

        if zipcode is None or max_distance is None:
            return dbc.Alert(
                "Please select your location and acceptable distance.",
                color="warning",
            )

        if (
            len(it := zp.matching(zipcode)) != 1
            and it[0]["zip_code_type"] != "STANDARD"
        ):
            return dbc.Alert("Invalid zipcode.", color="danger",)

        listings = listings[listings["price"].between(*price_limits)]
        listings = listings[listings["mileage"].between(*mileage_limits)]

        def get_valid_values_from_checklist(cl_dict):
            return set(cl_dict["value"]) & {
                opt["value"] for opt in cl_dict["options"]
            }

        mmt_filter_by_year = defaultdict(set)

        for toast in matrix_children:

            # TODO dirty dirty dirty... but let's not get hung up on
            # trivialities for now
            make, model = toast["props"]["id"].split("|")[1].split(";")

            year_div, trim_div = toast["props"]["children"]

            year_checklist = year_div["props"]["children"]["props"]
            trim_checklist = trim_div["props"]["children"]["props"]

            year_valid_opts = get_valid_values_from_checklist(year_checklist)
            trim_valid_opts = get_valid_values_from_checklist(trim_checklist)

            for year in year_valid_opts:
                for option in trim_valid_opts:
                    trim = option.split(";")[-1]
                    mmt_filter_by_year[year].add((make, model, trim))

        if len(mmt_filter_by_year) == 0:
            return dbc.Alert(
                "Select the makes and models you are interested in.",
                color="warning",
            )

        def filter_by_mmt(ymmt):
            y, ma, mo, t = ymmt
            return (for_year := mmt_filter_by_year.get(y)) is not None and (
                ma,
                mo,
                t,
            ) in for_year

        listings = listings[
            listings[["year", "make", "model", "trim"]].apply(
                filter_by_mmt, raw=True, axis=1
            )
        ]

        print(zipcode, max_distance)

        listings = filter_listings_by_location(listings, zipcode, max_distance)

        if len(listings) == 0:
            return dbc.Alert(
                "No cars that meet your requirements available in your area.",
                color="danger",
            )

        if len(listings) > 10000:
            n_peel = 1
        elif len(listings) > 1000:
            n_peel = 2
        else:
            n_peel = 3

        listings = calculate_listing_pareto_front(listings, n_peel=n_peel)

        return plot_listings(listings)

    @app.callback(
        dd.Output("output-link", "children"),
        [dd.Input("scatter-price-mileage", "clickData")],
    )
    def fill_in_link(click_data):
        if click_data is not None:
            data = click_data["points"][0]["customdata"]
            vin, make, model, trim, mpg, dealer, distance = data

            return [
                html.A(
                    f"{make} {model} [{vin}] on Truecar",
                    href=f"https://www.truecar.com/used-cars-for-sale/listing/{vin}/",
                ),
                html.Br(),
                html.I(f"Around {round(distance, -1)} miles away."),
            ]
        else:
            return "Click a point on the graph to see the purchase link."


def start_app():

    listings = load_recent_listings_and_dealerships()
    skeleton = create_div_skeleton()

    app = setup_dash_layout(listings, skeleton)

    setup_data_callbacks(app, listings)

    app.run_server(debug=True, host="192.168.1.10")


if __name__ == "__main__":
    start_app()
