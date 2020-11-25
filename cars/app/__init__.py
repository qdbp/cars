from functools import reduce
from itertools import product
from operator import or_
from typing import Any, Dict, List, NoReturn
from typing import Optional as Opt
from typing import Set, Tuple, Union

import dash
import dash_bootstrap_components as dbc
import dash_html_components as html
import plotly
from dash import dependencies as dd
from dash.dependencies import ALL, MATCH
from dash.development.base_component import Component
from dash.exceptions import PreventUpdate
from dash_bootstrap_components import Alert
from dash_core_components import Dropdown, Graph, RangeSlider, Slider
from dash_html_components import Div
from pandas import DataFrame
from pandas import IndexSlice as X
from plotly import graph_objects as go

import cars.analysis.etl as etl
from cars.analysis.etl import (
    filter_cars_by_year_mpg,
    get_dealers_in_range,
    get_states_in_range,
)
from cars.analysis.geo import LATLONG_BY_ZIP

plotly.io.renderers.default = "chromium"

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
CAR_OPTS_ID = "car_opts_box"

INFO_A_ID = "cars-info-a"
INFO_B_ID = "cars-info-b"
INFO_C_ID = "cars-info-c"
INFO_D_ID = "cars-info-d"

CACHED_DATA = "cached-data"


ERR_BAD_ZIP = "bad_zip"
ERR_NO_ZIP = "err_no_zip"
ERR_NO_DEALERS = "err_no_dealers"


class DivSkeleton(Dict[str, html.Div]):
    """
    A dict of named ids to Divs.

    These divs should be "holes" expecting a single interactive input or
    output component.
    """

    def fill(self, key: str, children: Any) -> None:
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

    all_divs = {}

    # noinspection PyPep8Naming
    def D(div_id: str, /, *children: Component, **kwargs: Any) -> Div:
        if "className" in kwargs:
            kwargs["className"] += " sk_div"
        else:
            kwargs["className"] = "sk_div"
        out = html.Div(id="_sk_div_" + div_id, children=children, **kwargs)
        all_divs[div_id] = out
        return out

    def mk_slider_divs(slider_id: str, icon_id: str) -> Div:
        return D(
            f"{slider_id}_box",
            D(icon_id, className="slider_icon"),
            D(slider_id, className="slider"),
            className="slider_box",
        )

    # top left box
    sliders = [
        mk_slider_divs(SLIDER_MPG_ID, SLIDER_ICON_MPG_ID),
        mk_slider_divs(SLIDER_MILEAGE_ID, SLIDER_ICON_MILEAGE_ID),
        mk_slider_divs(SLIDER_PRICE_ID, SLIDER_ICON_PRICE_ID),
        mk_slider_divs(SLIDER_YEAR_ID, SLIDER_ICON_YEAR_ID),
    ]
    sliders_box = D("slider_box", *sliders)

    # top-middle box
    top_middle_container = D(
        "top_middle_container",
        D(LOC_PICKER_ID),
        D(CAR_OPTS_ID),
    )

    # bottom-left box
    mmt_box = D(
        "mmt_box",
        D(MMT_MM_PICKER_ID),
        D(MMT_MATRIX_ID),
    )
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
        top_middle_container,
        mmt_box,
        scatter_box,
        info_box,
        D(CACHED_DATA, className="hidden"),
    )

    return DivSkeleton(all_divs)


def setup_dash_layout(sk: DivSkeleton) -> dash.Dash:
    def create_sliders() -> Tuple[
        RangeSlider, RangeSlider, RangeSlider, RangeSlider
    ]:
        slider_height = 460

        year_slider = RangeSlider(
            "input-year",
            min=(mn := etl.ATTRS.index.get_level_values("year").min()),
            max=(mx := etl.ATTRS.index.get_level_values("year").max()),
            value=[2012, 2018],
            marks={y: str(y) for y in range(mn, mx + 1)},
            vertical=True,
            updatemode="mouseup",
            verticalHeight=slider_height,
            persistence=True,
            persistence_type="session",
        )

        mileage_slider = RangeSlider(
            "input-mileage",
            min=etl.LISTINGS["mileage"].min(),
            max=(mx := etl.LISTINGS["mileage"].max()),
            value=[10000, 70000],
            marks={y: f"{y//1000}k" for y in range(0, mx, 25000)},
            step=1,
            vertical=True,
            updatemode="mouseup",
            verticalHeight=slider_height,
            persistence=True,
            persistence_type="session",
        )

        price_slider = RangeSlider(
            "input-price",
            min=etl.LISTINGS["price"].min(),
            max=(mx := etl.LISTINGS["price"].max()),
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

        mpg_slider = RangeSlider(
            "input-mpg",
            min=etl.ATTRS["mpg"].min(),
            max=(mx := etl.ATTRS["mpg"].max()),
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
        Alert(
            "Select your location.",
            id="alert-loc-picker",
            color="primary",
            style={"text-align": "center"},
        ),
        Dropdown(
            # dbc.Input(
            id="input-zipcode",
            placeholder="Zipcode",
            persistence=True,
            persistence_type="session",
            clearable=False,
            options=[
                dict(value=zp, label=zp) for zp in etl.LATLONG_BY_ZIP.keys()
            ],
        ),
        Slider(
            id="input-max-distance",
            className="form-control",
            min=10,
            max=250,
            marks={
                mark: dict(label=str(mark) + ("mi." if mark == 10 else ""))
                for mark in [10, 50, 100, 150, 200, 250]
            },
            persistence=True,
            persistence_type="session",
        ),
        Alert(
            "Limit dealership states.",
            id="alert-state-picker",
            color="primary",
            style={"text-align": "center"},
        ),
        Dropdown(
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
        Alert(
            "Select makes and models you are interested in. "
            "Filtered by sliders.",
            color="primary",
            style={"text-align": "center"},
        ),
        Dropdown(
            id="input-mm-picker",
            # options by callback
            multi=True,
            placeholder="Select makes",
            persistence=True,
            persistence_type="session",
            clearable=False,
        ),
        Alert(
            "Refine trims and years per model on the right.",
            color="primary",
            style={"text-align": "center"},
        ),
    ]

    sk.fill(MMT_MM_PICKER_ID, mm_picker_menu)

    matrix_menus = [
        dbc.Button(
            "Plot Cars Now!",
            id="input-matrix-button",
            color="success",
        ),
        html.Div(id="input-matrix-toasts"),
    ]

    sk.fill(MMT_MATRIX_ID, matrix_menus)

    ### ALERTS

    alert_link = Alert(
        id="output-link",
        style={"text-align": "center"},
        children="Click on a plot point to see details.",
    )

    app = dash.Dash(
        "cars, cars, cars!",
        external_stylesheets=[dbc.themes.CERULEAN],
    )
    # app.wsgi_app = ProfilerMiddleware(app.wsgi_app, restrictions=[50])

    sk.fill(INFO_A_ID, alert_link)

    ### CACHES
    dealership_cache = html.Div(id="cache-dealerships")
    sk.fill(CACHED_DATA, [dealership_cache])

    app.layout = sk["root"]
    return app


def setup_data_callbacks(app: dash.Dash) -> None:
    """
    This function runs once at server startup and configures callbacks between
    app components.

    Pre-aggregations of various data are also computed by this call. These are
    computed from the set of all car listings passed to this function. This set
    will form the universe of all accessible listings to the app, as no
    subsequent database calls are made.

    Args:
        app: the app to configure
    """

    sliders = {
        "year": dd.Input("input-year", "value"),
        "mileage": dd.Input("input-mileage", "value"),
        "price": dd.Input("input-price", "value"),
        "mpg": dd.Input("input-mpg", "value"),
    }

    slider_states = {
        "year": dd.State("input-year", "value"),
        "mileage": dd.State("input-mileage", "value"),
        "price": dd.State("input-price", "value"),
        "mpg": dd.State("input-mpg", "value"),
    }

    ###
    # this callback generates the state refinement menu
    ###
    @app.callback(  # type: ignore
        dd.Output("input-state-picker", "options"),
        [
            dd.Input("input-zipcode", "value"),
            dd.Input("input-max-distance", "value"),
        ],
    )
    def populate_state_options(
        zipcode: str, max_miles: int
    ) -> List[Dict[str, str]]:
        if zipcode not in etl.LATLONG_BY_ZIP:
            raise PreventUpdate
        return [
            dict(label=state.upper(), value=state.upper())
            for state in get_states_in_range(zipcode, max_miles)
        ]

    ###
    # this callbacks restricts visible makes by the year range selected
    ###
    @app.callback(  # type: ignore
        dd.Output("input-mm-picker", "options"),
        [
            sliders["year"],
            sliders["mpg"],
        ],
    )
    def restrict_make_options(
        year_range: Tuple[int, int], mpg_range: Tuple[int, int]
    ) -> List[Dict[str, str]]:

        return [
            dict(label=f"{make} {model}", value=f"{make};;;{model}")
            for make, model in filter_cars_by_year_mpg(
                *year_range,
                *mpg_range,
            )
            .reset_index()[["make", "model"]]
            .drop_duplicates()
            .values
        ]

    ###
    # this callback creates year and trim refinement cards for the selected
    # makes and models
    ###
    @app.callback(  # type: ignore
        dd.Output("input-matrix-toasts", "children"),
        [
            dd.Input("input-mm-picker", "value"),
            dd.Input("input-mm-picker", "options"),
            sliders["year"],
            sliders["mpg"],
        ],
    )
    def generate_year_refinement_toasts(
        selected_mms: Opt[List[str]],
        options: Opt[List[Dict[str, str]]],
        year_range: Tuple[int, int],
        mpg_range: Tuple[int, int],
    ) -> List[dbc.Toast]:

        if selected_mms is None or options is None:
            return []

        visible_mms = {option["value"] for option in options}

        # selected values that are hidden by external action (e.g. when the year
        # slider excludes a previously-valid choice, are not actually deselected
        # because of `persistence`. However, we do want to exclude them.
        include_mms = visible_mms & set(selected_mms)

        avail_cars = filter_cars_by_year_mpg(*year_range, *mpg_range)

        toasts = [
            Alert(
                "Select the years and trims you are interestd in.",
                style={"text-align": "center"},
            )
        ]
        for val in include_mms:
            make, model = val.split(";;;")

            mm_cars = avail_cars.loc[X[:, make, model, :]]
            if len(mm_cars) == 0:
                continue

            years = mm_cars.index.get_level_values("year").unique()
            trims = (
                mm_cars.index.get_level_values("trim_slug")
                .str.upper()
                .value_counts()
                .index
            )

            toast = dbc.Toast(
                header=f"{make} {model}:",
                id=dict(id="yt_refine", make=make, model=model),
                className="refine_container",
            )
            toast.children = [
                html.Div(
                    dbc.Checklist(
                        id=dict(id="y_refine", make=make, model=model),
                        options=[
                            # reverse sort to match slider
                            dict(label=year, value=year)
                            for year in sorted(years)[::-1]
                        ],
                        persistence=True,
                        persistence_type="session",
                        inputClassName="year-refine-input",
                        labelClassName="year-refine-label",
                        className="year-refine",
                    ),
                    className="checklist-container-year",
                ),
                html.Div(
                    dbc.Checklist(
                        id=dict(id="t_refine", make=make, model=model),
                        options=[],
                        value=sorted(trims),
                        persistence=True,
                        persistence_type="session",
                        inputClassName="trim-refine-input",
                        labelClassName="trim-refine-label",
                        className="trim-refine",
                    ),
                    className="checklist-container-trim",
                ),
            ]
            toasts.append(toast)

        return toasts

    ###
    # this callback populates trim refinement options based on selected years
    ###
    @app.callback(  # type: ignore
        dd.Output(dict(id="t_refine", make=MATCH, model=MATCH), "options"),
        [
            dd.Input(dict(id="y_refine", make=MATCH, model=MATCH), "value"),
            dd.Input(dict(id="y_refine", make=MATCH, model=MATCH), "options"),
        ],
        dd.State(dict(id="y_refine", make=MATCH, model=MATCH), "id"),
    )
    def create_trim_refinement_options(
        year_values: List[int],
        year_options: List[Dict[str, int]],
        y_id: Dict[str, str],
    ) -> List[Dict[str, str]]:
        return [
            dict(label=trim_slug.upper(), value=trim_slug)
            for trim_slug in (
                etl.TRIMS_BY_MM[(y_id["make"], y_id["model"])]
                & reduce(
                    or_,
                    (
                        etl.TRIMS_BY_YEAR[year]
                        for year in (
                            set(year_values or [])
                            & set(opt["value"] for opt in year_options)
                        )
                    ),
                    set(),
                )
            )
        ]

    ###
    # this callback filters the scatterplot output based on all relevant inputs
    ###
    @app.callback(  # type: ignore
        dd.Output("scatter_box", "children"),
        [
            dd.Input("input-matrix-button", "n_clicks"),
        ],
        [
            dd.State("input-zipcode", "value"),
            dd.State("input-max-distance", "value"),
            dd.State("input-state-picker", "value"),
            dd.State({"id": "yt_refine", "make": ALL, "model": ALL}, "id"),
            dd.State({"id": "y_refine", "make": ALL, "model": ALL}, "value"),
            dd.State({"id": "y_refine", "make": ALL, "model": ALL}, "options"),
            dd.State({"id": "t_refine", "make": ALL, "model": ALL}, "value"),
            dd.State({"id": "t_refine", "make": ALL, "model": ALL}, "options"),
            slider_states["price"],
            slider_states["mileage"],
        ],
    )
    def generate_filtered_graph(
        n_clicks: Opt[int],
        zipcode: Opt[str],
        max_miles: int,
        picked_states: Opt[List[str]],
        refine_id: List[Dict[str, str]],
        years_val: List[List[int]],
        years_opt: List[List[Dict[str, str]]],
        trims_val: List[List[str]],
        trims_opt: List[List[Dict[str, str]]],
        price_limits: Tuple[int, int],
        mileage_limits: Tuple[int, int],
    ) -> Component:

        if n_clicks is None:
            # this is the init call
            return Alert("Ready to plot.")
        if zipcode is None or max_miles is None:
            return Alert("Please enter location information.")
        elif zipcode not in LATLONG_BY_ZIP:
            return Alert("Invalid zipcode.", color="danger")

        # filter by dealerships and states
        dealers = get_dealers_in_range(zipcode, max_miles)

        # if not None or empty
        if picked_states:
            dealers = dealers[dealers["state"].isin(set(picked_states))]

        if len(dealers) == 0:
            return Alert("No dealerships within range!", color="warning")

        # filter by selected attributes
        years: List[Set[int]] = [
            set(val) & {o["label"] for o in opt}
            for val, opt in zip(years_val, years_opt)
            if val and opt
        ]
        trims: List[Set[str]] = [
            set(val) & {o["label"].lower() for o in opt}
            for val, opt in zip(trims_val, trims_opt)
            if val and opt
        ]
        ymms_selector: List[Tuple[int, str, str, str]] = [
            (y, rid["make"], rid["model"], t)
            for rid, mm_years, mm_trims, in zip(refine_id, years, trims)
            for y, t in product(mm_years, mm_trims)
        ]

        if len(ymms_selector) == 0:
            return Alert("Please choose some years and trims.", color="primary")

        print(len(etl.LISTINGS))
        want_cars = etl.get_specific_cars(ymms_selector)

        lst = etl.select_listings(
            want_cars,
            dealers,
            *mileage_limits,
            *price_limits,
        )
        if isinstance(lst, str):
            return Alert(lst, color="warning")

        return plot_listings(lst.reset_index())

    ###
    # this callback generates the output link when a point is clicked
    ###
    @app.callback(  # type: ignore
        dd.Output("output-link", "children"),
        [dd.Input("scatter-price-mileage", "clickData")],
    )
    def fill_in_link(
        click_data: Opt[Dict[str, Any]]
    ) -> Union[str, List[Component]]:

        if click_data is None:
            return "Click a point on the graph to see the listing link."

        data = click_data["points"][0]["customdata"]
        vin, make, model, trim, mpg, dealer, distance, *_ = data

        return [
            html.A(
                f"{make} {model} [{vin}] on Truecar",
                href=(
                    f"https://www.truecar.com/used-cars-for-sale/listing/{vin}/"
                ),
            ),
            html.Br(),
            html.I(f"Around {round(distance, -1):.0f} miles away."),
        ]


def plot_listings(listings: Opt[DataFrame]) -> Graph:

    if listings is None:
        return Graph(id="scatter-price-mileage")

    fig = go.Figure(
        go.Scattergl(
            x=listings["mileage"],
            y=listings["price"],
            customdata=listings[
                [
                    "vin",
                    "make",
                    "model",
                    "style",
                    "mpg",
                    "dealer_name",
                    "distance",
                    "year",
                    "price",
                    "drivetrain",
                    "engine",
                ]
            ],
            hovertemplate=(
                '<b style="color: green">$%{customdata[8]}</i><br>'
                "<i>%{customdata[0]}</i><br>"
                '<b style="font-size:16">%{customdata[7]}</b><br>'
                "<i>%{customdata[9]} - %{customdata[10]}</b><br>"
                "<b>%{customdata[1]} %{customdata[2]} %{customdata[3]}</b><br>"
                "Dealer: %{customdata[5]}<br>"
                "<b>About %{customdata[6]:.0f} miles from you.</b>"
            ),
            marker=dict(
                color=listings["color_hex"].apply(lambda x: x or "#000000"),
                opacity=1
                - 0.75
                * listings["color_hex"].apply(lambda x: x is None).astype(int),
                size=10,
                line=dict(width=0),
            ),
            mode="markers+text",
        ),
        layout=dict(
            title="Available Cars",
            clickmode="event",
            xaxis=dict(title="Mileage, mi", ticks="inside"),
            yaxis=dict(title="Price, $", ticks="inside"),
            margin=dict(b=0, t=0, l=0, r=0, pad=0),
        ),
    )

    fig.update_yaxes(automargin=True)

    graph = Graph(
        id="scatter-price-mileage",
        config=dict(displayModeBar=False),
        figure=fig,
    )

    return graph


def start_app(debug: bool = True) -> NoReturn:

    etl.refresh_universe()

    skeleton = create_div_skeleton()
    app = setup_dash_layout(skeleton)
    setup_data_callbacks(app)

    app.run_server(debug=debug, host="0.0.0.0")
    assert 0


if __name__ == "__main__":
    start_app()
