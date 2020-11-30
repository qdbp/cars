from typing import Dict, Iterable, List, Set, Tuple

import dash
import dash_bootstrap_components as dbc
import dash_html_components as html
from dash import Dash
from dash import dependencies as dd
from dash.dependencies import ALL, MATCH, Input, Output, State
from dash_core_components import (
    Dropdown,
    Graph,
    Interval,
    RangeSlider,
    Slider,
    Store,
)
from dash_html_components import Div

from cars import LOG
from cars import scrapers as scr
from cars.analysis import etl as etl

from . import PERSIST_ARGS, opts_from_vals
from .callbacks import deferred_clientside_callback
from .skeleton import (
    SK_CACHE,
    SK_CAR_OPTS_BOX,
    SK_INFO_A,
    SK_LL_INFO,
    SK_MM_PICKER,
    SK_MMT_MATRIX,
    SK_SCATTER,
    SK_SLIDER_MILEAGE,
    SK_SLIDER_MPG,
    SK_SLIDER_PRICE,
    SK_SLIDER_YEAR,
    SK_TOP_SELECTORS,
    DivSkeleton,
)

INPID_YEAR = "input-year"
INPID_MILEAGE = "input-mileage"
INPID_PRICE = "input-price"
INPID_MPG = "input-mpg"

INPID_ZIPCODE = "input-zipcode"
INPID_MAX_DIST = "input-max-distance"
INPID_STATE = "input-state-picker"

INPID_OPTS_TRANS = "input-car-opts-transmission"
INPID_OPTS_DRIVETRAIN = "input-car-opts-drivetrain"
INPID_OPTS_FUEL = "input-car-opts-fuel-type"
INPID_OPTS_BODY = "input-car-opts-body"

ALERT_MM_PICKER = "alert-mm-picker"
INPID_MM_PICKER = "input-mm-picker"

SLIDER_INPUTS = {
    "year": dd.Input(INPID_YEAR, "value"),
    "mileage": dd.Input(INPID_MILEAGE, "value"),
    "price": dd.Input(INPID_PRICE, "value"),
    "mpg": dd.Input(INPID_MPG, "value"),
}
SLIDER_STATES = {
    "year": dd.State(INPID_YEAR, "value"),
    "mileage": dd.State(INPID_MILEAGE, "value"),
    "price": dd.State(INPID_PRICE, "value"),
    "mpg": dd.State(INPID_MPG, "value"),
}

CAR_OPTS = "car-opts"
TOGGLE_BUTTON = "toggle-btn"
TOGGLE_BUTTON_DUMMY = "toggle-btn-dummy"
TOGGLE_BUTTON_STATE = "toggle-btn-state"
TOGGLE_BUTTON_DEFAULT_STATE = "toggle-btn-default-state"
TOGGLE_BUTTON_STATE_IX = "toggle-btn-state-ix"
TOGGLE_BUTTON_BOX = "toggle-button-box"

PLOT_BUTTON = "input-matrix-button"
PLOT_ALERT_BOX = "plot-alert-box"
PLOT_ALERT = "plot-alert"

STORE_ALL_CARS = "store-all-cars"
STORE_FILTERED_CARS = "store-filtered-cars"

IVAL_TRIGGER_LOAD = "ival-trigger-load"


# let's get abstract
class ToggleButtonGroup:
    """
    Encapsulates the the creation and operation of a grouping of toggle
    buttons.

    Allows these groupings to be parameterized by an arbitrary selection
    of keys, automatically generating the necessary client side callback
    boilerplate when necessary.
    """

    TYPE_SELECTOR = "_btn_type"
    KEY_SELECTOR = "key"

    HAVE_CALLBACKS: Set[Tuple[str, ...]] = set()

    @classmethod
    def selector(cls, **matches: str) -> Dict[str, str]:
        """
        Returns a dictionary input/output selector for a bg, with key=ALL.

        Accepts as keyword argument any selectors for non-managed id
        keys.
        """
        return {
            ToggleButtonGroup.TYPE_SELECTOR: TOGGLE_BUTTON,
            "key": ALL,
            **{sel: val for sel, val in matches.items()},
        }

    @classmethod
    def make_buttons(
        cls,
        *,
        label: str,
        values: Iterable[str],
        selectors: Dict[str, str],
        defaults: Iterable[bool] = None,
    ) -> List[dbc.Button]:
        values = list(values)
        defaults = list(defaults) if defaults else ([True] * len(values))

        return [
            dbc.Button(
                label,
                id=dict(_btn_type=TOGGLE_BUTTON_DUMMY, **selectors),
                className=f"{TOGGLE_BUTTON} {TOGGLE_BUTTON_DUMMY}",
                color="primary",
                disabled=True,
            ),
            *[
                dbc.Button(
                    key,
                    className=TOGGLE_BUTTON,
                    id=dict(_btn_type=TOGGLE_BUTTON, key=key, **selectors),
                    outline=True,
                    active=default,
                    color="info",
                )
                for key, default in zip(values, defaults)
            ],
            Store(
                id=dict(_btn_type=TOGGLE_BUTTON_STATE, **selectors),
                storage_type="session",
            ),
            Store(
                id=dict(_btn_type=TOGGLE_BUTTON_DEFAULT_STATE, **selectors),
                storage_type="session",
                data=defaults,
            ),
            Store(
                id=dict(_btn_type=TOGGLE_BUTTON_STATE_IX, **selectors),
                data={value: ix for ix, value in enumerate(values)},
            ),
            html.Br(),
        ]

    @classmethod
    def stage_deferred_callbacks(cls, selector_keys: Tuple[str, ...]) -> None:
        """
        Enables the appropriate client side callbacks.

        Only needs to be executed once per unique set of selector keys.

        Args:
            selector_keys: the set of keys uniquely identifying the button.
        """

        if selector_keys in cls.HAVE_CALLBACKS:
            return

        LOG.info(f"Installing button group callbacks for {selector_keys=}")
        cls.HAVE_CALLBACKS.add(selector_keys)

        no_key = {sel: MATCH for sel in selector_keys}
        key_match = dict(**no_key, key=MATCH)
        key_all = dict(**no_key, key=ALL)

        deferred_clientside_callback(
            f"write_button_states-{selector_keys}",
            # language=js
            """
            function(_clicked, actives, indexer) {
                const triggered_id = (
                    dash_clientside.callback_context.triggered.map(
                        t => t['prop_id']
                    )[0]
                )
                const key = JSON.parse(
                    triggered_id.split('.').slice(0, -1).join('.')
                )['key'];
                const ix = indexer[key];
                actives[ix] = !actives[ix];
                return actives;
            }
            """,
            Output(dict(_btn_type=TOGGLE_BUTTON_STATE, **no_key), "data"),
            Input(dict(_btn_type=TOGGLE_BUTTON, **key_all), "n_clicks"),
            State(dict(_btn_type=TOGGLE_BUTTON, **key_all), "active"),
            State(dict(_btn_type=TOGGLE_BUTTON_STATE_IX, **no_key), "data"),
            prevent_initial_call=True,
        )

        deferred_clientside_callback(
            f"read_button_states-{selector_keys}",
            # language=js
            """
            function(_timestamp, states, default_states) {
                
                // read defaults on first instantiation
                if (!states) {
                    states = default_states;
                }
            
                let colors = [];
                let actives = [];
                
                for (const state of states) {
                    actives.push(state);
                    if (state)  {
                        colors.push("info");
                    } else {
                        colors.push("secondary");
                    }
                }
                
                return [colors, actives];
            }
            """,
            Output(dict(_btn_type=TOGGLE_BUTTON, **key_all), "color"),
            Output(dict(_btn_type=TOGGLE_BUTTON, **key_all), "active"),
            Input(
                sid := dict(_btn_type=TOGGLE_BUTTON_STATE, **no_key),
                "modified_timestamp",
            ),
            State(sid, "data"),
            State(
                dict(_btn_type=TOGGLE_BUTTON_DEFAULT_STATE, **no_key), "data"
            ),
        )

        deferred_clientside_callback(
            f"manage_button_warning-{selector_keys}",
            # language=js
            """
            function(_timestamp, actives, default_actives) {
                if (!actives) {
                    actives = default_actives;
                }
                for (const active of actives) {
                    if (active === undefined || active === true) {
                        return "primary";
                    }
                }
                return "danger"
            }
            """,
            Output(dict(_btn_type=TOGGLE_BUTTON_DUMMY, **no_key), "color"),
            Input(
                sid := dict(_btn_type=TOGGLE_BUTTON_STATE, **no_key),
                "modified_timestamp",
            ),
            State(sid, "data"),
            State(
                dict(_btn_type=TOGGLE_BUTTON_DEFAULT_STATE, **no_key), "data"
            ),
            prevent_initial_call=True,
        )


# pre-register deferred callbacks for known button groups
CAR_OPTS_SELECTORS = ("input",)
ToggleButtonGroup.stage_deferred_callbacks(CAR_OPTS_SELECTORS)


def setup_dash_layout(app: Dash, sk: DivSkeleton) -> dash.Dash:
    def create_sliders() -> Tuple[
        RangeSlider, RangeSlider, RangeSlider, RangeSlider
    ]:
        slider_height = 460
        year_slider = RangeSlider(
            INPID_YEAR,
            min=(mn := etl.ATTRS.index.get_level_values("year").min()),
            max=(mx := etl.ATTRS.index.get_level_values("year").max()),
            value=[2012, 2018],
            marks={y: str(y) for y in range(mn, mx + 1)},
            vertical=True,
            verticalHeight=slider_height,
            updatemode="mouseup",
            **PERSIST_ARGS,
        )
        mileage_slider = RangeSlider(
            INPID_MILEAGE,
            min=0,
            max=etl.MAX_MILEAGE,
            value=[10000, 70000],
            marks={
                y: f"{y // 1000}k" for y in range(0, etl.MAX_MILEAGE, 25_000)
            },
            step=1,
            vertical=True,
            updatemode="mouseup",
            verticalHeight=slider_height,
            **PERSIST_ARGS,
        )
        price_slider = RangeSlider(
            INPID_PRICE,
            min=0,
            max=etl.MAX_PRICE,
            value=[10000, 35000],
            marks={
                int(y): f"{y // 1000}k" for y in range(0, etl.MAX_PRICE, 10_000)
            },
            step=1,
            vertical=True,
            updatemode="mouseup",
            verticalHeight=slider_height,
            **PERSIST_ARGS,
        )
        mpg_slider = RangeSlider(
            INPID_MPG,
            min=etl.ATTRS["mpg"].min(),
            max=(mx := etl.ATTRS["mpg"].max()),
            value=[20, mx],
            marks={int(y): f"{y:.0f}" for y in range(10, int(mx) + 1, 10)},
            step=1,
            vertical=True,
            updatemode="mouseup",
            verticalHeight=slider_height,
            **PERSIST_ARGS,
        )

        return year_slider, mileage_slider, price_slider, mpg_slider

    for name, slider, div_id in zip(
        ["Year", "Mileage", "Price", "MPG"],
        create_sliders(),
        [SK_SLIDER_MILEAGE, SK_SLIDER_PRICE, SK_SLIDER_YEAR, SK_SLIDER_MPG],
    ):
        sk.fill(
            div_id,
            [
                dbc.Badge(name, color="primary", className="slider"),
                slider,
            ],
        )
    top_selectors = [
        dbc.Alert(
            "Select your location.",
            id="alert-loc-picker",
            color="primary",
        ),
        Dropdown(
            id=INPID_ZIPCODE,
            placeholder="Zipcode",
            clearable=False,
            options=opts_from_vals(etl.LATLONG_BY_ZIP.keys()),
            **PERSIST_ARGS,
        ),
        Slider(
            id=INPID_MAX_DIST,
            className="form-control",
            min=10,
            max=250,
            marks={
                mark: dict(label=str(mark) + ("mi." if mark == 10 else ""))
                for mark in [10, 50, 100, 150, 200, 250]
            },
            value=50,
            **PERSIST_ARGS,
        ),
        dbc.Alert(
            "Limit dealership states.",
            id="alert-state-picker",
            color="primary",
        ),
        Dropdown(
            id=INPID_STATE,
            # options by callback
            multi=True,
            **PERSIST_ARGS,
        ),
        Div(
            id="plot-info-flex",
            children=[
                dbc.Button(
                    children="Plot Cars Now!",
                    id=PLOT_BUTTON,
                    color="success",
                ),
                Div(
                    id=PLOT_ALERT_BOX,
                    children=dbc.Alert(
                        id=PLOT_ALERT,
                        children="",
                        color="danger",
                    ),
                    hidden=True,
                ),
                dbc.Alert(
                    "Plot does not refresh automatically.",
                    color="light",
                ),
            ],
            style={"display": "flex", "flex-direction": "column-reverse"},
        ),
    ]
    ## car options pickers
    top_selectors.extend([])

    ###
    ##
    # === BOTTOM ROW ===
    ##
    ###

    sk.fill(SK_TOP_SELECTORS, top_selectors)
    mm_picker_menu = [
        dbc.Alert(
            "Select makes and models you are interested in. "
            "Filtered by sliders.",
            id=ALERT_MM_PICKER,
            color="primary",
        ),
        Dropdown(
            id=INPID_MM_PICKER,
            # options by callback
            multi=True,
            placeholder="Select makes",
            clearable=False,
            **PERSIST_ARGS,
        ),
    ]

    sk.fill(SK_MM_PICKER, mm_picker_menu)
    sk.fill(
        SK_LL_INFO,
        dbc.Alert(
            id="alert-site-info",
            children=(
                "Used car picker by Evgeny Naumov.",
                html.Br(),
                "Built on top of Truecar data with Plotly + Dash.",
            ),
            color="light",
        ),
    )
    # car type options

    button_layout = {
        ("Transmission", INPID_OPTS_TRANS): scr.TRANSMISSIONS,
        ("Fuel Type", INPID_OPTS_FUEL): scr.KNOWN_FUEL_TYPES,
        ("Drivetrain", INPID_OPTS_DRIVETRAIN): scr.KNOWN_DRIVETRAINS,
        ("Body", INPID_OPTS_BODY): scr.KNOWN_BODIES,
    }

    car_opt_picker = [
        dbc.Alert("Select car attributes.", color="primary"),
        Div(
            id="car-opts-box",
            className=TOGGLE_BUTTON_BOX,
            children=[
                button
                for (inp_name, inp_id), inp_opts in button_layout.items()
                for button in ToggleButtonGroup.make_buttons(
                    label=inp_name,
                    values=inp_opts,
                    selectors=dict(input=inp_id),
                )
            ],
        ),
    ]
    sk.fill(SK_CAR_OPTS_BOX, car_opt_picker)

    mmt_refine_menu = [
        dbc.Alert(
            "Select models to refine trims.", id="mmt-alert", color="secondary"
        ),
        Div(id="mmt-card-group"),
    ]
    sk.fill(SK_MMT_MATRIX, mmt_refine_menu)
    sk.fill(
        SK_CACHE,
        [
            Interval(IVAL_TRIGGER_LOAD, max_intervals=1, interval=1),
            Store(
                id=STORE_ALL_CARS,
                storage_type="memory",
                data=etl.RAW_CLIENT_DATA,
            ),
            Store(
                id=STORE_FILTERED_CARS,
                storage_type="session",
            ),
            Div(id="devnull"),
        ],
    )

    ## GRAPH
    scatter_graph = html.Div(
        id="scatter-box",
        children=Graph(id="scatter-price-mileage"),
        hidden=True,
    )
    sk.fill(SK_SCATTER, scatter_graph)

    ### ALERTS

    alert_link = dbc.Alert(
        id="output-link",
        children="A plot of listings will appear above when executed.",
        color="secondary",
    )
    sk.fill(SK_INFO_A, alert_link)

    app.layout = sk["root"]
    return app
