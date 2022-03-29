from dash import dependencies as dd
from dash.exceptions import PreventUpdate

from cars.analysis import etl as etl

from .. import DashOptions, opts_from_vals
from ..layout import INPID_MAX_DIST, INPID_STATE, INPID_ZIPCODE
from . import deferred_callback


@deferred_callback(
    dd.Output(INPID_STATE, "options"),
    [dd.Input(INPID_ZIPCODE, "value"), dd.Input(INPID_MAX_DIST, "value")],
)
def populate_state_options(zipcode: str, max_miles: int) -> list[DashOptions]:
    """
    Generates the state refinement menu.
    """
    if zipcode not in etl.LATLONG_BY_ZIP:
        raise PreventUpdate
    return opts_from_vals(
        state.upper() for state in etl.get_states_in_range(zipcode, max_miles)
    )


__all__ = ["populate_state_options"]
ERR_BAD_ZIP = "bad_zip"
ERR_NO_ZIP = "err_no_zip"
ERR_NO_DEALERS = "err_no_dealers"
