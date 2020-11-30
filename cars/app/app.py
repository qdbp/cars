from pathlib import Path
from typing import NoReturn, Tuple

import dash_bootstrap_components as dbc
from dash import Dash
from flask import Flask

import cars.app.callbacks as cb
from cars.analysis import etl as etl
from cars.app.layout import setup_dash_layout
from cars.app.skeleton import create_div_skeleton


def prepare_app() -> Tuple[Flask, Dash]:

    app = Flask(__name__)
    dash = Dash(
        __name__,
        server=app,
        external_stylesheets=[dbc.themes.SANDSTONE],
    )

    # import callback files to get deferred callbacks
    for file in Path(__file__).parent.joinpath("callbacks").glob("*.py"):
        if "__init__" in str(file):
            continue
        __import__(f"cars.app.callbacks.{file.name[:-3]}")

    etl.refresh_universe()
    skeleton = create_div_skeleton()
    dash = setup_dash_layout(dash, skeleton)
    cb.deferred_registry.apply(dash)

    return app, dash


def debug_app() -> NoReturn:  # type: ignore

    _, dash = prepare_app()
    dash.run_server(debug=True, host="0.0.0.0")


if __name__ == "__main__":
    debug_app()
