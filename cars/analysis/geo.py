from functools import cache
from sqlite3 import connect
from typing import Tuple

import numpy as np
import zipcodes as zp
from geopy import Nominatim
from numba import jit
from requests import Session

from cars.util import CAR_DB

R_MEAN_EARTH_MI = 3_958.7613

LATLONG_BY_ZIP: dict[str, Tuple[float, float]] = {
    z["zip_code"]: (float(z["lat"]), float(z["long"]))
    for z in zp.list_all()
    if z["zip_code_type"] == "STANDARD"
}


@jit(nopython=True)  # type: ignore
def great_circle_miles(p0: np.ndarray, lon1: float, lat1: float) -> np.ndarray:
    """
    Vectorized great-circle distance calculation.

    Args:
        p0: array, shape [n, 2]: lon/lat of first point
        lon1: lon of second point, scalar
        lat1: lat of second point, scalar

    Returns:
        great distances, same shape as first point array
    """

    lon0 = p0[:, 0] * np.pi / 180
    lon1 = lon1 * np.pi / 180
    lat0 = p0[:, 1] * np.pi / 180
    lat1 = lat1 * np.pi / 180

    return (
        R_MEAN_EARTH_MI
        * 2
        * np.arcsin(
            np.sqrt(
                np.sin(np.abs(lat1 - lat0) / 2) ** 2
                + (
                    np.cos(lat1)
                    * np.cos(lat0)
                    * np.sin(np.abs(lon1 - lon0) / 2) ** 2
                )
            )
        )
    )


CENSUS_PATH = "http://geocoding.geo.census.gov/geocoder/locations/address"


@cache
def geocode_census_zip(
    session: Session, addr: str, zipcode: str
) -> tuple[float, float]:
    """
    Args:
        session: requests session to use for http calls
        addr: the US address to look up
        zipcode: the zipcode within which to look

    Returns:
        lat, lon of address as known by census.gov

    """
    raw = session.get(
        f"{CENSUS_PATH}?street={addr}&zip={zipcode}&benchmark=2020&format=json",
        timeout=30,
    ).json()
    try:
        coords = raw["result"]["addressMatches"][0]["coordinates"]
    except IndexError as e:
        raise ValueError(f"Address {addr} not found at {zipcode}") from e
    return coords["y"], coords["x"]


@cache
def geocode_census_city_state(
    session: Session, addr: str, city: str, state: str
) -> tuple[float, float]:
    """
    Args:
        session: requests session to use for http calls
        addr: the US address to look up
        city: the city
        state: the state
    Returns:
        lat, lon of address as known by census.gov

    """
    raw = session.get(
        f"{CENSUS_PATH}?street={addr}&city={city}&state={state}"
        f"&benchmark=2020&format=json",
        timeout=30,
    ).json()
    try:
        coords = raw["result"]["addressMatches"][0]["coordinates"]
    except IndexError as e:
        raise ValueError(f"Address {addr} not found at {city}, {state}") from e
    return coords["y"], coords["x"]


def _get_local_nominatim() -> Nominatim:
    key = "__nominatim"
    if key not in globals():
        globals()[key] = Nominatim(
            domain="127.0.0.1:8080",
            user_agent="en-car-shopping-tool",
            scheme="http",
        )
    return globals()[key]


@cache
def tryhard_geocode(
    session: Session, addr: str, zipcode: str, city: str, state: str
) -> tuple[float, float]:

    with connect(CAR_DB) as conn:
        rows = conn.execute(
            f""" SELECT lat, lon FROM dealerships
                 WHERE address = ? AND zip = ?
            """,
            [addr, zipcode],
        ).fetchall()
    if rows:
        return rows[0][0], rows[0][1]

    try:
        return geocode_census_zip(session, addr, zipcode)
    except ValueError:
        try:
            return geocode_census_city_state(session, addr, city, state)
        except ValueError:
            geo = _get_local_nominatim().geocode(
                query=dict(street=addr, postalcode=zipcode)
            )
            if geo is None:
                geo = _get_local_nominatim().geocode(
                    query=dict(street=addr, city=city, state=state)
                )
            if geo is not None:
                return geo.latitude, geo.longitude
            raise ValueError("Could not geocode despite best efforts.")
