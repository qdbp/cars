import pandas as pd

import cars.analysis.etl as etl

etl.DEALERS = None
from cars.analysis.etl import get_dealers_in_range

FAKE_DEALERS = pd.read_csv("./tests/truecar_dealerships.csv").set_index(
    "dealer_id"
)


def test_dealers_in_range(monkeypatch):

    monkeypatch.setattr(etl, "DEALERS", FAKE_DEALERS)

    out = get_dealers_in_range("08525", 50)
    assert len(out) > 0
    assert out["state"].unique()[0] == "NJ"
