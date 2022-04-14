import operator
from collections import defaultdict
from functools import reduce
from typing import Any

import dash.html as html
import dash_bootstrap_components as dbc
from dash import dependencies as dd
from dash.development.base_component import Component
from dash.html import Div

from cars.app import PERSIST_ARGS
from cars.app.layout import (
    INPID_MM_PICKER,
    SLIDER_INPUTS,
    STORE_FILTERED_CARS,
    TOGGLE_BUTTON_BOX,
    ToggleButtonGroup,
)

from . import deferred_callback

MMT_TRIM_WIDTH = 120
MMT_TD_WIDTH = 20

INPID_MMT_CHECK = "mmt-check"
INPID_MMT_DISABLE = "mmt-disable-trim"

INPID_MMT_REFINE_TRIM = "mmt-refine-trim"
INPID_MMT_REFINE_YEAR = "mmt-refine-year"


def mk_trim_row_header(make: str, model: str, trim: str) -> Div:
    return Div(
        className="mmt-trim mmt-trim-box",
        children=[
            Div(
                className="mmt-trim mmt-trim-left",
                children=[
                    dbc.Checklist(
                        className=INPID_MMT_DISABLE,
                        id=dict(
                            id=INPID_MMT_DISABLE,
                            make=make,
                            model=model,
                            trim=trim,
                        ),
                        value=[True],
                        options=[dict(label="", value=True)],
                        switch=True,
                        **PERSIST_ARGS,
                    )
                ],
            ),
            Div(
                className="mmt-trim mmt-trim-right",
                children=html.Span(trim.upper(), className="mmt-trim"),
            ),
        ],
    )


# pre-register button group deferred callbacks
MMT_REFINE_SELECTORS = ("input", "make", "model")
ToggleButtonGroup.stage_deferred_callbacks(MMT_REFINE_SELECTORS)


@deferred_callback(
    [
        dd.Output("mmt-alert", "children"),
        dd.Output("mmt-alert", "color"),
        dd.Output("mmt-card-group", "children"),
    ],
    [
        SLIDER_INPUTS["year"],
        dd.Input(INPID_MM_PICKER, "value"),
        dd.Input(INPID_MM_PICKER, "options"),
        dd.State(STORE_FILTERED_CARS, "data"),
    ],
    prevent_initial_call=True,
)
def generate_mmt_refinement_cards(
    year_range: tuple[int, int],
    selected_mms: list[str],
    mm_opts: list[dict[str, str]],
    cars: str | list[dict[str, Any]],
) -> tuple[str, str, list[Component]]:
    """
    Generates year/trim refinement menus for each selected make/model.
    """

    visible_opts = {opt["value"] for opt in (mm_opts or [])}
    ymin, ymax = year_range
    need_mms = visible_opts & set(selected_mms or [])

    if len(need_mms) == 0:
        return "Select models to refine trims", "secondary", []
    if isinstance(cars, str):
        return "Invalid make and model selection.", "warning", []

    valid_mms = {
        (make, model)
        for selected in sorted(set(selected_mms) & visible_opts)
        for make, model in [selected.split(";;;")]
    }

    tys_by_mm: dict[tuple[str, str], dict[str, set[int]]] = {}
    for car in cars:
        make = car["make"]
        model = car["model"]
        if (mm := (make, model)) not in valid_mms:
            continue

        year = car["year"]
        if not (ymin <= year <= ymax):
            continue

        if (trim_dict := tys_by_mm.get((make, model))) is None:
            trim_dict = tys_by_mm[mm] = defaultdict(set)

        trim_dict[car["trim_slug"]].add(year)

    assert not (
        valid_mms - tys_by_mm.keys()
    ), f"{valid_mms=}, {tys_by_mm.keys()=}"

    cards = []
    for mm in valid_mms:
        make, model = mm
        trim_dict = tys_by_mm[mm]
        trims = sorted(trim_dict.keys())

        years = sorted(
            reduce(operator.or_, (trim_dict[trim] for trim in trims))
            & set(range(year_range[0], year_range[1] + 1))
        )

        buttons = Div(
            id=f"trim-opts-box-{make}-{model}",
            className=TOGGLE_BUTTON_BOX,
            children=ToggleButtonGroup.make_buttons(
                label="Trims",
                values=trims,
                selectors=dict(
                    input=INPID_MMT_REFINE_TRIM, make=make, model=model
                ),
            )
            + ToggleButtonGroup.make_buttons(
                label="Years",
                values=map(str, years),
                selectors=dict(
                    input=INPID_MMT_REFINE_YEAR, make=make, model=model
                ),
            ),
        )

        card = dbc.Card(
            id=dict(id="yt_refine", make=make, model=model),
            color="info",
            outline=True,
            children=[dbc.CardHeader(f"{make} {model}:"), buttons],
        )
        cards.append(card)

    return (
        "Refine years and trims by model. "
        "Hover over trims for more options.",
        "primary",
        cards,
    )


__all__ = [
    "generate_mmt_refinement_cards",
    "INPID_MMT_CHECK",
    "INPID_MMT_DISABLE",
    "INPID_MMT_REFINE_TRIM",
    "INPID_MMT_REFINE_YEAR",
]
