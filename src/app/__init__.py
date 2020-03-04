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
from src.analysis.geo import great_circle_miles, LATLONG_BY_ZIP
from src.analysis.pareto_front import calculate_listing_pareto_front

SLIDER_MPG_ID = "slider-mpg"
SLIDER_MILEAGE_ID = "slider-mileage"
SLIDER_PRICE_ID = "slider-price"
SLIDER_YEAR_ID = "slider-year"

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


ERR_BAD_ZIP = "bad_zip"
ERR_NO_ZIP = "err_no_zip"


def concat_mmt_label(mmt_arr: np.ndarray) -> str:
    return f"{mmt_arr[0].upper()} - {mmt_arr[1].upper()} - {mmt_arr[2]}"


class DivSkeleton(Dict[str, html.Div]):
    """
    A dict of named ids to Divs. These divs should be "holes" expecting a single
    interactive input or output component.
    """

    def fill(self, key: str, children):
        if len(self[key].children) == 0:
            self[key].children = children
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
    # |              |           picker                 |            |
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

    loc_picker_box = D("loc_picker_container", D(LOC_PICKER_ID))

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
    sk.fill(SLIDER_ICON_MILEAGE_ID, html.P("🏁️"))
    sk.fill(SLIDER_MILEAGE_ID, mileage_slider)

    sk.fill(SLIDER_ICON_PRICE_ID, html.P("💰"))
    sk.fill(SLIDER_PRICE_ID, price_slider)

    sk.fill(SLIDER_ICON_YEAR_ID, html.P("📅️️"))
    sk.fill(SLIDER_YEAR_ID, year_slider)

    sk.fill(SLIDER_ICON_MPG_ID, html.P("⛽️️"))
    sk.fill(SLIDER_MPG_ID, mpg_slider)

    ### SCATTER
    scatter_graph = html.Div(id="scatter_box", children=plot_listings(None))
    sk.fill(SCATTER_ID, scatter_graph)

    loc_picker = [
        dbc.Alert(
            "Select your location.",
            id="alert-loc-picker",
            color="primary",
            style={"text-align": "center"},
        ),
        dbc.InputGroup(
            id="zip_picker_box",
            children=[
                dbc.InputGroupAddon("📍"),
                dbc.Input(
                    id="input-zipcode",
                    placeholder="Zip code",
                    persistence=True,
                    persistence_type="session",
                    debounce=True,
                ),
            ],
        ),
        dbc.InputGroup(
            id="dist_picker_box",
            children=[
                dbc.InputGroupAddon("↔"),
                dbc.Input(
                    id="input-max-distance",
                    placeholder="Maximum distance, miles",
                    type="number",
                    persistence=True,
                    persistence_type="session",
                    debounce=True,
                ),
            ],
        ),
        html.Hr(),
        dbc.Alert(
            "Limit dealership states.",
            id="alert-state-picker",
            color="primary",
            style={"text-align": "center"},
        ),
        dcc.Dropdown(
            id="input-state-picker",
            # options by callback
            multi=True,
            persistence=True,
            persistence_type="session",
        ),
    ]

    sk.fill(LOC_PICKER_ID, loc_picker)

    ### MMT refinement
    # N.B. these are pre-filtered by selection
    mm_picker_menu = [
        dcc.Dropdown(
            id="input-mm-picker",
            # options by callback
            multi=True,
            placeholder="Select makes",
            persistence=True,
            persistence_type="session",
            clearable=False,
        ),
        dbc.Alert(
            "Select makes and models you are interested in.", color="primary",
            style={"text-align": "center"},
        ),
        dbc.Alert(
            "Refine trims and years per model on the left.", color="primary",
            style={"text-align": "center"},
        ),
    ]

    sk.fill(MMT_MM_PICKER_ID, mm_picker_menu)

    matrix_menus = [
        dbc.Button("Plot Options", id="input-matrix-button", color="success",),
        html.Div(id="input-matrix-toasts"),
    ]

    sk.fill(MMT_MATRIX_ID, matrix_menus)

    ### ALERTS

    alert_link = dbc.Alert(
        id="output-link",
        style={"text-align": "center"},
        children="Click on a plot point to see details.",
    )

    app = dash.Dash(
        "cars, cars, cars!", external_stylesheets=[dbc.themes.CERULEAN],
    )
    # app.wsgi_app = ProfilerMiddleware(app.wsgi_app, restrictions=[50])

    # sk.fill(INFO_A_ID, alert_map)
    sk.fill(INFO_A_ID, alert_link)

    ### CACHES
    dealership_cache = html.Div(id="cache-dealerships")
    sk.fill(CACHED_DATA, [dealership_cache])

    app.layout = sk["root"]
    return app


def plot_listings(listings):

    if listings is None:
        return dcc.Graph(id="scatter-price-mileage")

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
                    "year",
                ]
            ],
            hovertemplate=(
                "<i>%{customdata[0]}</i><br>"
                '<b style="font-size:16">%{customdata[7]}</b><br>'
                "<b>%{customdata[1]} %{customdata[2]} %{customdata[3]}</b><br>"
                "Dealer: %{customdata[5]}<br>"
                "<b>About %{customdata[6]:.0f} miles from you.</b>"
            ),
            marker={
                "color": listings["color_hex"].apply(lambda x: x or "#000000"),
                "opacity": 1
                - 0.75
                * listings["color_hex"].apply(lambda x: x is None).astype(int),
                "size": 10,
                "line": {
                    # "color": listings["color_hex"].apply(
                    #     lambda x: x or "#000000"
                    # ),
                    "width": 0,
                },
            },
            mode="markers+text",
        ),
        layout={
            "title": "Available Cars",
            # "height": 430,
            "clickmode": "event",
            "xaxis": {"title": "Mileage, mi",},
            "yaxis": {"title": "Price, $",},
            "margin": dict(b=0, t=0, l=0, r=0, pad=0),
        },
    )

    fig.update_yaxes(automargin=True)

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

    dealerships_universe = listings_universe[
        ~listings_universe["dealer_id"].duplicated()
    ][["dealer_id", "lat", "lon", "dealer_state"]].set_index("dealer_id")

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

    ###
    # this callback creates year and trim refinement cards for the selected
    # makes and models
    ###
    @app.callback(
        dd.Output("input-matrix-toasts", "children"),
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
                f"input-mmty-refine-year-{make}-{model}",
                options=(
                    opts := [
                        # reverse sort to match slider
                        {"label": year, "value": year}
                        for year in sorted(years)[::-1]
                    ]
                ),
                value=[opt["value"] for opt in opts],
                persistence=True,
                persistence_type="session",
                inputClassName="year-refine-input",
                labelClassName="year-refine-label",
                className="year-refine",
            )

            checklist_trim = dbc.Checklist(
                f"input-mmty-refine-trim-{make}-{model}",
                options=(
                    opts := [
                        {"label": trim, "value": f"{make};{model};{trim}"}
                        for trim in sorted(trims)
                    ]
                ),
                value=[opt["value"] for opt in opts],
                persistence=True,
                persistence_type="session",
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

    @jit
    def metric(dealer_latlong: np.ndarray, lat: float, long: float) -> float:
        out = great_circle_miles(
            lat0=dealer_latlong[0], lon0=dealer_latlong[1], lat1=lat, lon1=long
        )
        return out

    def filter_dealers_by_location(zipcode: str, max_miles: float):

        lat, long = LATLONG_BY_ZIP[zipcode]
        this_metric = partial(metric, lat=lat, long=long)

        distance = dealerships_universe[["lat", "lon"]].apply(
            this_metric, raw=True, axis=1
        )
        distance.name = "distance"

        return dealerships_universe[distance <= max_miles].join(distance)

    ###
    # this callback stores the valid dealerships
    ###
    @app.callback(
        dd.Output("cache-dealerships", "children"),
        [
            dd.Input("input-zipcode", "value"),
            dd.Input("input-max-distance", "value"),
        ],
    )
    def store_valid_dealerships(zipcode, max_miles):

        if zipcode is None or max_miles is None:
            return ERR_NO_ZIP

        if (
            len(it := zp.matching(zipcode)) != 1
            or (print(it))
            or it[0]["zip_code_type"] != "STANDARD"
        ):
            return ERR_BAD_ZIP

        return filter_dealers_by_location(zipcode, max_miles).to_json()

    ###
    # this callback generates the state refinement menu
    ###
    @app.callback(
        dd.Output("input-state-picker", "options"),
        [dd.Input("cache-dealerships", "children")],
    )
    def populate_state_options(close_dealers_json: str):
        try:
            close_dealerships = pd.read_json(close_dealers_json)
        except ValueError:
            return []
        return [
            {"label": state.upper(), "value": state.upper()}
            for state in close_dealerships["dealer_state"].unique()
        ]

    ###
    # this callback filters the scatterplot output based on all relevant inputs
    ###
    @app.callback(
        dd.Output("scatter_box", "children"),
        [
            dd.Input("input-matrix-button", "n_clicks"),
            slider_inputs["price"],
            slider_inputs["mileage"],
            dd.Input("input-state-picker", "value"),
        ],
        [
            dd.State("cache-dealerships", "children"),
            dd.State("input-matrix-toasts", "children"),
        ],
    )
    def generate_filtered_graph(
        n_clicked,
        price_limits,
        mileage_limits,
        picked_states,
        dealers_json,
        matrix_children,
    ):

        listings = listings_universe

        # filter by price and mileage
        listings = listings[listings["price"].between(*price_limits)]
        listings = listings[listings["mileage"].between(*mileage_limits)]

        if len(listings) == 0:
            return dbc.Alert(
                "There are no cars witin your price and mileage constraints.",
                color="danger",
            )

        if dealers_json == ERR_NO_ZIP:
            return dbc.Alert(
                "Enter location information to see listings.", color="warning"
            )
        elif dealers_json == ERR_BAD_ZIP:
            return dbc.Alert("Invalid zipcode.", color="danger",)

        elif not dealers_json:
            return dbc.Alert("Ready to plot", color="primary")

        # filter by dealerships and states
        close_dealers = pd.read_json(dealers_json)

        if picked_states is not None:

            if len(picked_states) == 0:
                return dbc.Alert("No states selected.", color="warning")

            dealers_in_states = close_dealers[
                close_dealers["dealer_state"].isin(picked_states)
            ]

        else:
            dealers_in_states = close_dealers

        assert len(dealers_in_states) > 0

        listings = listings.join(
            dealers_in_states["distance"], on="dealer_id", how="inner"
        )

        assert len(listings) > 0

        # filter by make and model
        def get_valid_values_from_checklist(cl_dict):
            return set(cl_dict["value"]) & {
                opt["value"] for opt in cl_dict["options"]
            }

        mmt_filter_by_year = defaultdict(set)
        for toast in matrix_children:

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

        if len(listings) == 0:
            return dbc.Alert(
                "No cars that meet your requirements available in your area.",
                color="danger",
            )

        if len(listings) > 10000:
            n_peel = 1
        elif len(listings) > 1000:
            n_peel = 2
        elif len(listings) > 100:
            n_peel = 3
        else:
            n_peel = 0

        listings = calculate_listing_pareto_front(
            listings, n_peel=n_peel, eliminate_dominated=True
        )

        return plot_listings(listings)

    ###
    # this callback generates the output link when a point is clicked
    ###
    @app.callback(
        dd.Output("output-link", "children"),
        [dd.Input("scatter-price-mileage", "clickData")],
    )
    def fill_in_link(click_data):
        if click_data is not None:
            data = click_data["points"][0]["customdata"]
            vin, make, model, trim, mpg, dealer, distance, year = data

            return [
                html.A(
                    f"{make} {model} [{vin}] on Truecar",
                    href=f"https://www.truecar.com/used-cars-for-sale/listing/{vin}/",
                ),
                html.Br(),
                html.I(f"Around {round(distance, -1):.0f} miles away."),
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
