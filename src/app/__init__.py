from typing import Dict
import json

import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
from dash import dependencies as dd
from plotly import graph_objects as go
from shapely.geometry import Polygon, Point

from src.analysis.etl import load_recent_listings_and_dealerships
from src.analysis.pareto_front import calculate_listing_pareto_front


SLIDER_MPG_ID = "cars-slider-mpg"
SLIDER_MILEAGE_ID = "cars-slider-mileage"
SLIDER_PRICE_ID = "cars-slider-price"
SLIDER_YEAR_ID = "cars-slider-year"

SLIDER_ICON_MPG_ID = "cars-slider-mpg-icon"
SLIDER_ICON_MILEAGE_ID = "cars-slider-mileage-icon"
SLIDER_ICON_PRICE_ID = "cars-slider-price-icon"
SLIDER_ICON_YEAR_ID = "cars-slider-year-icon"

SCATTER_ID = "scatter_box"
LOC_PICKER_ID = "loc_picker_box"

INFO_A_ID = "cars-info-a"
INFO_B_ID = "cars-info-b"
INFO_C_ID = "cars-info-c"
INFO_D_ID = "cars-info-d"

CACHED_DATA = "cached-data"


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
    # |   sliders    |         location                 |  reserved  |
    # |              |           map                    |            |
    # |              |                                  |            |
    # |              |                                  |            |
    # |              |                                  |            |
    # |              |                                  |            |
    # +--------------------------------------------------------------+
    # |              |                                  |            |
    # |              |                                  |            |
    # |   make       |                                  |  info      |
    # |   model      |                                  |            |
    # |              |          output chart            |            |
    # |              |                                  |            |
    # |              |                                  |            |
    # |              |                                  |            |
    # |              |                                  |            |
    # |              |                                  |            |
    # |              |                                  |            |
    # |              |                                  |            |
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

    def D(id, /, *args, **kwargs):
        if "className" in kwargs:
            kwargs["className"] += " sk_div"
        else:
            kwargs["className"] = "sk_div"
        out = html.Div(id="_sk_div_" + id, children=args, **kwargs)
        all_divs[id] = out
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
    reserved_box = D("reserved_box")
    mm_box = D("mm_box")
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
        reserved_box,
        mm_box,
        scatter_box,
        info_box,
        D(CACHED_DATA, className='hidden'),
    )

    return DivSkeleton(all_divs)


def setup_dash_layout(all_listings: pd.DataFrame, sk: DivSkeleton) -> dash.Dash:

    ### SLIDERS
    # FIXME tie to layout callbacks
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
    )

    price_slider = dcc.RangeSlider(
        "input-price",
        min=(mn := all_listings["price"].min()),
        max=(mx := all_listings["price"].max()),
        value=[10000, 35000],
        marks={int(y): f"{y//1000}k" for y in range(0, int(mx) + 5000, 5000)},
        step=1,
        vertical=True,
        updatemode="mouseup",
        verticalHeight=slider_height,
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
    )

    # everything in its right place
    sk.fill(SLIDER_ICON_MILEAGE_ID, html.P("🏁️", style=dict(margin=0)))
    sk.fill(SLIDER_MILEAGE_ID, mileage_slider)

    sk.fill(SLIDER_ICON_PRICE_ID, html.P("$", style=dict(margin=0)))
    sk.fill(SLIDER_PRICE_ID, price_slider)

    sk.fill(SLIDER_ICON_YEAR_ID, html.P("📅️️", style=dict(margin=0)))
    sk.fill(SLIDER_YEAR_ID, year_slider)

    sk.fill(SLIDER_ICON_MPG_ID, html.P("⛽️️", style=dict(margin=0)))
    sk.fill(SLIDER_MPG_ID, mpg_slider)

    ### SCATTER
    scatter_graph = dcc.Graph(
        id="scatter-price-mileage", config=dict(displayModeBar=False)
    )

    sk.fill(SCATTER_ID, scatter_graph)

    dealerships = all_listings[~all_listings['dealer_id'].duplicated()]
    geo_plot = go.Figure(
        go.Scattergeo(
            locationmode="USA-states",
            lat=dealerships["lat"],
            lon=dealerships["lon"],
            text=dealerships["dealer_name"],
            opacity=1,
            marker=dict(size=5, opacity=1),
        ),
        layout=dict(
            height=500,
            margin=dict(l=0, r=0, b=0, t=0),
            clickmode="select",
            mapbox_style="open-street-map",
            geo=dict(scope="usa", ),
        ),
    )

    ### LOC PICKER
    loc_picker = dcc.Graph(
        "input-loc-picker",
        config={
            "modeBarButtonsToRemove": ["hoverClosestGeo", "toImage", "select2d"]
        },
        figure=geo_plot
    )

    sk.fill(LOC_PICKER_ID, loc_picker)

    ### ALERTS
    alert_map = dbc.Alert(
        "Restrict dealerships by selecting them with the lasso on the map.",
        id="alert-map",
        color="primary",
        style={"text-align": "center"},
    )

    alert_link = dbc.Alert(id="output-link", style={"text-align": "center"})

    app = dash.Dash(
        "cars, cars, cars!", external_stylesheets=[dbc.themes.BOOTSTRAP]
    )

    sk.fill(INFO_A_ID, alert_map)
    sk.fill(INFO_B_ID, alert_link)

    ### CACHES
    lasso_cache = html.Div(id='cache-lasso')
    sk.fill(CACHED_DATA, [lasso_cache])

    app.layout = sk["root"]
    return app


def plot_listings(listings):

    print(listings["price"].max())

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


def setup_data_callbacks(
    app: dash.Dash, listings_universe: pd.DataFrame,
):
    def filter_listings_with_sliders(
        listings_universe, year_limits, mileage_limits, price_limits, mpg_limits
    ) -> pd.DataFrame:

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

        return listings

    # @app.callback(
    #     dd.Output("input-loc-picker", "figure",),
    #     [
    #         dd.Input("input-year", "value"),
    #         dd.Input("input-mileage", "value"),
    #         dd.Input("input-price", "value"),
    #         dd.Input("input-mpg", "value"),
    #     ],
    # )
    # def restrict_dealerships(
    #     year_limits, mileage_limits, price_limits, mpg_limits
    # ):

    #     # TODO this call is duplicated, we probably want to share it
    #     # however, this is not a large relative performance burden
    #     listings = filter_listings_with_sliders(
    #         listings_universe,
    #         year_limits,
    #         mileage_limits,
    #         price_limits,
    #         mpg_limits,
    #     )

    #     return geo_plot

    # @app.callback(
    #     dd.Output('cache-lasso', 'children'),
    #     [dd.Input('input-loc-picker', 'selectedData')]
    # )
    # def cache_lasso(data):
    #     return json.dumps(data)

    ###
    # this callback sets up slider, make and model inputs
    ###
    @app.callback(
        dd.Output("scatter-price-mileage", "figure"),
        [
            dd.Input("input-year", "value"),
            dd.Input("input-mileage", "value"),
            dd.Input("input-price", "value"),
            dd.Input("input-mpg", "value"),
            dd.Input("input-loc-picker", "selectedData"),
        ],
    )
    def generate_filtered_graph(
        year_limits, mileage_limits, price_limits, mpg_limits, selected
    ):

        listings = filter_listings_with_sliders(
            listings_universe,
            year_limits,
            mileage_limits,
            price_limits,
            mpg_limits,
        )

        if selected is not None and "lassoPoints" in selected:
            poly = Polygon(selected["lassoPoints"]["geo"])

            def contained_in_poly(xy_raw):
                p = Point(xy_raw)
                return p.within(poly)

            mask = listings[["lon", "lat"]].apply(
                contained_in_poly, raw=True, axis=1
            )
            listings = listings[mask]

        listings = calculate_listing_pareto_front(listings, n_peel=2)

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


def setup_layout_callbacks(app: dash.Dash):

    slider_ids = [f"input-{x}" for x in ("year", "mileage", "mpg", "price")]

    # FIXME find out how to get viewport height into callbacks
    # @app.callback(
    #     [dd.Output(sid, "verticalHeight") for sid in slider_ids],
    #     [dd.Input("_sk_div_slider_box", "stylefsdf")],
    # )
    # def adjust_slider_heights(div_height):
    #     return (div_height,) * len(slider_ids)


def start_app():

    listings = load_recent_listings_and_dealerships()
    skeleton = create_div_skeleton()

    app = setup_dash_layout(listings, skeleton)

    setup_data_callbacks(app, listings)
    setup_layout_callbacks(app)

    app.run_server(debug=True, host="192.168.1.10")


if __name__ == "__main__":
    start_app()
