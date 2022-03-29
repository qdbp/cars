from logging import INFO
from typing import Any, Iterable, TypedDict

import plotly

from cars import LOG

plotly.io.renderers.default = "chromium"

PERSIST_ARGS = dict(persistence=True, persistence_type="session")

LOG.setLevel(INFO)


class DashOptions(TypedDict):
    label: str
    value: Any


def opts_from_vals(vals: Iterable[int | str]) -> list[DashOptions]:
    """
    Makes a dict of {'value': x, 'label': x} for simple cases.
    """
    return [dict(label=str(v), value=v) for v in vals]
