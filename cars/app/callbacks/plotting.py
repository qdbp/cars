from __future__ import annotations

from typing import Any, Tuple, Union

import dash.html as html
import pandas as pd
from dash import dependencies as dd
from dash.dcc import Graph
from dash.dependencies import ALL, Input, Output
from dash.development.base_component import Component
from numpy import uint64
from pandas import DataFrame
from plotly import graph_objects as go

from cars.analysis import etl as etl
from cars.analysis.etl import LISTING_LIMIT, get_dealers_in_range

from ..layout import (
    INPID_MAX_DIST,
    INPID_MM_PICKER,
    INPID_STATE,
    INPID_ZIPCODE,
    PLOT_ALERT,
    PLOT_ALERT_BOX,
    PLOT_BUTTON,
    SLIDER_STATES,
    STORE_FILTERED_CARS,
    ToggleButtonGroup,
)
from . import deferred_callback, deferred_clientside_callback
from .mmt_refine import INPID_MMT_REFINE_TRIM, INPID_MMT_REFINE_YEAR

INPID_GRAPH = "scatter-price-mileage"

deferred_clientside_callback(
    "plot-button-manager",
    # language=js
    """
    function(mm_opts, mm_values, zip_value, sel_trims, sel_years) {
        if (zip_value === undefined) {
            return ["Please select your location", "info", true]
        }
        
        const mm_vals = new Set(mm_values)
        let have_any_valid_mm = false;
        
        for (opt of mm_opts) {
            if (mm_vals.has(opt['value'])) {
                have_any_valid_mm = true;
                break;
            }
        }
        
        if (!have_any_valid_mm) {
            return ["Please select your makes and models.", "info", true]
        }
        
        if (sel_trims.every(it => !it)) {
            return ["You have excluded all years.", "warning", true];
        }
        
        if (sel_years.every(it => !it)) {
            return ["You have excluded all years.", "warning", true]
        }
        
        return ["Plot Cars Now!", "success", false]
    }
    """,
    Output(PLOT_BUTTON, "children"),
    Output(PLOT_BUTTON, "color"),
    Output(PLOT_BUTTON, "disabled"),
    Input(INPID_MM_PICKER, "options"),
    Input(INPID_MM_PICKER, "value"),
    Input(INPID_ZIPCODE, "value"),
    dd.Input(
        ToggleButtonGroup.selector(
            input=INPID_MMT_REFINE_TRIM, make=ALL, model=ALL
        ),
        "active",
    ),
    dd.Input(
        ToggleButtonGroup.selector(
            input=INPID_MMT_REFINE_YEAR, make=ALL, model=ALL
        ),
        "active",
    ),
    prevent_initial_call=True,
)


def plot_listings(listings: DataFrame) -> Graph:
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
                ]
            ],
            hoverlabel=dict(bgcolor="#F8F5F0"),
            hovertemplate=(
                '<b style="color: green;">$%{customdata[8]}</i><br>'
                "<i>%{customdata[0]}</i><br>"
                '<b style="font-size:16">'
                "%{customdata[7]} %{customdata[1]} "
                "%{customdata[2]} %{customdata[3]}"
                "</b><br>"
                "<i>%{customdata[9]}</b><br>"
                "Dealer: %{customdata[5]}<br>"
                "<b>About %{customdata[6]:.0f} miles from you.</b>"
                "<extra></extra>"
            ),
            marker=dict(
                color=listings["color_rgb_ext"].fillna("#000000"),
                opacity=1
                - 0.75
                * listings["color_rgb_ext"].transform(pd.isna).astype(int),
                size=10,
                line=dict(width=0),
            ),
            mode="markers+text",
        ),
        layout=dict(
            clickmode="event",
            xaxis=dict(title="Mileage, mi", ticks="inside"),
            yaxis=dict(title="Price, $", ticks="inside"),
            margin=dict(b=0, t=0, l=0, r=0, pad=0),
        ),
    )

    fig.update_yaxes(automargin=True)
    fig.update_xaxes(automargin=True)

    graph = Graph(id=INPID_GRAPH, config=dict(displayModeBar=False), figure=fig)

    return graph


@deferred_callback(
    dd.Output("scatter-box", "children"),
    dd.Output("scatter-box", "hidden"),
    dd.Output(PLOT_ALERT, "children"),
    dd.Output(PLOT_ALERT, "color"),
    dd.Output(PLOT_ALERT_BOX, "hidden"),
    [dd.Input("input-matrix-button", "n_clicks")],
    [
        dd.State(INPID_ZIPCODE, "value"),
        dd.State(INPID_MAX_DIST, "value"),
        dd.State(INPID_STATE, "value"),
        dd.State(INPID_STATE, "options"),
        SLIDER_STATES["price"],
        SLIDER_STATES["mileage"],
        dd.State(
            ToggleButtonGroup.selector(
                input=INPID_MMT_REFINE_TRIM, make=ALL, model=ALL
            ),
            "active",
        ),
        dd.State(
            ToggleButtonGroup.selector(
                input=INPID_MMT_REFINE_TRIM, make=ALL, model=ALL
            ),
            "id",
        ),
        dd.State(
            ToggleButtonGroup.selector(
                input=INPID_MMT_REFINE_YEAR, make=ALL, model=ALL
            ),
            "active",
        ),
        dd.State(
            ToggleButtonGroup.selector(
                input=INPID_MMT_REFINE_YEAR, make=ALL, model=ALL
            ),
            "id",
        ),
        dd.State(STORE_FILTERED_CARS, "data"),
    ],
    prevent_inital_call=True,
)
def generate_filtered_graph(
    n_clicks: int | None,
    zipcode: str | None,
    max_miles: int,
    picked_states: list[str | None],
    picked_state_opts: list[dict[str, str]],
    lim_price: Tuple[int, int],
    lim_mileage: Tuple[int, int],
    refine_trim: list[bool],
    refine_trim_id: list[dict[str, str]],
    refine_year: list[bool],
    refine_year_id: list[dict[str, str]],
    filtered_attrs: list[dict[str, Any]],
) -> Tuple[Any, bool, str, str, bool]:
    """
    Generates the scatter plot based on selected car and listing params.
    """
    if n_clicks is None:
        return [], True, "", "danger", True

    assert zipcode is not None
    assert max_miles is not None

    assert refine_year
    assert refine_trim

    # filter by dealerships and states
    # TODO can be offloaded to client
    dealers = get_dealers_in_range(zipcode, max_miles)
    # if not None or empty
    if picked_states is not None and (
        valid_picked_states := (
            set(picked_states) & set(opt["value"] for opt in picked_state_opts)
        )
    ):
        # noinspection PyUnboundLocalVariable
        dealers = dealers[dealers["state"].isin(valid_picked_states)]

    sel_trims = [
        trim_id for trim_id, sel in zip(refine_trim_id, refine_trim) if sel
    ]
    want_trims = DataFrame(sel_trims).rename({"key": "trim_slug"}, axis=1)

    sel_years = [
        year_id for year_id, sel in zip(refine_year_id, refine_year) if sel
    ]
    want_years = (
        DataFrame(sel_years)
        .rename({"key": "year"}, axis=1)
        .astype({"year": uint64})
    )

    # this prevents the initial auto plot -- should not be the first return
    # since we want other error conditions to be checked on load.

    cross = pd.merge(
        want_years[["make", "model", "year"]],
        want_trims[["make", "model", "trim_slug"]],
        on=["make", "model"],
    )

    ymmt = ["year", "make", "model", "trim_slug"]
    ymms = ["year", "make", "model", "style"]
    attrs = pd.merge(cross, DataFrame(filtered_attrs), on=ymmt)

    lst = etl.query_listings(
        attrs[["ymms_id"]],
        dealers.reset_index()[["dealer_id"]],
        *lim_mileage,
        *lim_price,
    )

    if len(lst) == 0:
        return (
            [],
            True,
            "No listings within price, mileage and location constraints.",
            "danger",
            False,
        )
    elif len(lst) < LISTING_LIMIT:
        msg = f"{len(lst)} listing{'s' * bool(len(lst) - 1)} found."
        color = "info"
        hidden = False
    else:
        msg = (
            f"Listing load limit of {LISTING_LIMIT} reached -- "
            f"some valid listings will not be shown. Refine your options."
        )
        color = "warning"
        hidden = False

    lst = pd.merge(lst, attrs, on=["ymms_id"])
    lst = pd.merge(lst, dealers, on=["dealer_id"])
    lst.drop(["ymms_id", "dealer_id"], axis=1, inplace=True)
    lst["color_rgb_ext"] = "#" + lst["color_rgb_ext"]

    plot: Graph = plot_listings(lst.reset_index())

    return plot, False, msg, color, hidden


@deferred_callback(
    dd.Output("output-link", "children"),
    dd.Output("output-link", "color"),
    [dd.Input(INPID_GRAPH, "clickData")],
    prevent_initial_call=True,
)
def fill_in_link(
    click_data: dict[str, Any | None]
) -> Tuple[Union[str, list[Component]], str]:
    """
    Generates extra information when a scatter plot point is clicked.
    """

    if click_data is None:
        return "Click on a plot point to see details.", "primary"

    data = click_data["points"][0]["customdata"]
    vin, make, model, trim, mpg, dealer, distance, *_ = data

    return [
        html.A(
            f"{make} {model} [{vin}] on Truecar",
            href=f"https://www.truecar.com/used-cars-for-sale/listing/{vin}/",
        ),
        html.Br(),
        html.I(f"Around {round(distance, -1):.0f} miles away."),
    ], "success"


__all__ = ["generate_filtered_graph", "fill_in_link"]
