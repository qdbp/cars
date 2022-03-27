from hypothesis import given
from hypothesis.strategies import from_type

from cars.scrapers import VehicleHistory


@given(from_type(VehicleHistory).filter(lambda hist: 0 <= hist.n_owners < 16))
def test_hist_rtt(hist: VehicleHistory) -> None:
    assert VehicleHistory.from_int(hist.as_int) == hist
    assert hist.as_int == VehicleHistory.from_int(hist.as_int).as_int
