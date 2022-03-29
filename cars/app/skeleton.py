from typing import Any

import dash_html_components as html
from dash.development.base_component import Component
from dash_html_components import Div

SK_DIV_ID_PREFIX = "_sk_div"
CLASSNAME_SK_DIV = "sk_div"

SK_ROOT = "root"

SK_SLIDER_BOX = "slider-box"

SK_SLIDER_MPG = "slider-mpg"
SK_SLIDER_MILEAGE = "slider-mileage"
SK_SLIDER_PRICE = "slider-price"
SK_SLIDER_YEAR = "slider-year"

SK_LOWER_LEFT = "ll_input_box"
SK_MM_PICKER = "mm_picker"
SK_LL_INFO = "ll_info"
SK_CAR_OPTS_BOX = "car-opts-box"

SK_MMT_MATRIX = "mmt"
SK_SCATTER = "scatter-box"
SK_TOP_SELECTORS = "top_selectors_box"

SK_INFO_BOX = "info-box"
SK_INFO_A = "cars-info-a"
SK_INFO_B = "cars-info-b"
SK_INFO_C = "cars-info-c"
SK_INFO_D = "cars-info-d"

SK_CACHE = "cached-data"


class DivSkeleton(dict[str, Div]):
    """
    A dict of named ids to Divs.

    These divs should be "holes" expecting a single interactive input or
    output component.
    """

    def fill(self, key: str, children: Any) -> None:
        if len(self[key].children) == 0:
            self[key].children = children
        else:
            raise ValueError(
                f"Container {key} already filled with {self[key].children}"
            )


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
            kwargs["className"] += " " + CLASSNAME_SK_DIV
        else:
            kwargs["className"] = CLASSNAME_SK_DIV
        out = html.Div(
            id=f"{SK_DIV_ID_PREFIX}_{div_id}", children=children, **kwargs
        )
        all_divs[div_id] = out
        return out

    D(
        SK_ROOT,
        D(
            SK_SLIDER_BOX,
            D(SK_SLIDER_MPG, className=SK_SLIDER_BOX),
            D(SK_SLIDER_MILEAGE, className=SK_SLIDER_BOX),
            D(SK_SLIDER_PRICE, className=SK_SLIDER_BOX),
            D(SK_SLIDER_YEAR, className=SK_SLIDER_BOX),
        ),
        D(SK_TOP_SELECTORS),
        D(SK_LOWER_LEFT, D(SK_CAR_OPTS_BOX), D(SK_MM_PICKER), D(SK_LL_INFO)),
        D(SK_MMT_MATRIX),
        D(SK_SCATTER),
        D(
            SK_INFO_BOX,
            D(SK_INFO_A, className=SK_INFO_BOX),
            D(SK_INFO_B, className=SK_INFO_BOX),
            D(SK_INFO_C, className=SK_INFO_BOX),
            D(SK_INFO_D, className=SK_INFO_BOX),
        ),
        D(SK_CACHE, className="hidden"),
    )

    return DivSkeleton(all_divs)
