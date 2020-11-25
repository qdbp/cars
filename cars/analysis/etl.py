import sqlite3 as sql
from dataclasses import dataclass
from functools import lru_cache
from gc import collect
from types import MappingProxyType
from typing import Dict, Iterable, List, Mapping, Set, Tuple, Union

import pandas as pd
from pandas import DataFrame
from pandas import IndexSlice as X
from pandas import Series

from cars import LOG
from cars.analysis.geo import LATLONG_BY_ZIP, great_circle_miles
from cars.util import CAR_DB

YMMS_KEY = ["year", "make", "model", "style"]
YMMT_KEY = ["year", "make", "model", "trim_slug"]


@dataclass(frozen=True, eq=True)
class ListingSettings:
    min_price: float = 1.0
    max_price: float = 100_000  # this is a shopping site for the sensible
    min_mileage: int = 0
    max_mileage: int = 200_000
    min_mpg: float = 10
    max_mpg: float = 1e3
    min_year: int = 1990
    min_timestamp: int = 0


LOAD_SETTINGS = ListingSettings()


def load_listings(s: ListingSettings) -> DataFrame:
    with sql.connect(CAR_DB) as conn:
        listings = pd.read_sql_query(
            f"""
            SELECT
                tl.vin, mileage, tl.price,
                tl.year, tl.make, tl.model, tl.style, 
                tl.dealer_id,
                ('#' || tl.color_rgb) as color_hex
            FROM truecar_listings tl
            JOIN truecar_ymms_attrs ta ON
                ta.year = tl.year
                and ta.make = tl.make
                and ta.model = tl.model
                and ta.style = tl.style
            WHERE (0.45 * ta.mpg_hwy + 0.55 * ta.mpg_city) >= {s.min_mpg}
            AND (0.45 * ta.mpg_hwy + 0.55 * ta.mpg_city) <= {s.max_mpg}
            AND ta.year >= {max(s.min_year, 1900)}
            AND mileage >= {s.min_mileage}
            AND mileage <= {s.max_mileage}
            AND price >= {s.min_price}
            AND price <= {s.max_price}
            AND timestamp >= {s.min_timestamp}
            """,
            conn,
            index_col=["year", "make", "model", "style"],
        )

    return listings


def load_attrs(s: ListingSettings) -> DataFrame:
    with sql.connect(CAR_DB) as conn:
        out = pd.read_sql(
            f"""
            SELECT 
                year, make, model, style, trim_slug,
                (0.45 * mpg_hwy + 0.55 * mpg_city) as mpg,
                fuel_type, body, drivetrain, is_auto, engine
            FROM truecar_ymms_attrs
            WHERE year >= {s.min_year}
            AND mpg >= {s.min_mpg}
            AND mpg <= {s.max_mpg}
            """,
            conn,
            index_col=["year", "make", "model", "trim_slug"],
        )
        out.sort_index(inplace=True)
        return out


def load_all_dealers() -> DataFrame:
    with sql.connect(CAR_DB) as conn:
        out = pd.read_sql(
            """
            SELECT * FROM truecar_dealerships
            """,
            conn,
            index_col="dealer_id",
        )
        out.rename(dict(name="dealer_name"), axis=1, inplace=True)
        out.sort_index(inplace=True)
        return out


LISTINGS: DataFrame
DEALERS: DataFrame
ATTRS: DataFrame

# caches
ZIP_DEALER_DISTANCE: Dict[str, Series] = {}
TRIMS_BY_YEAR: Mapping[int, Set[str]]
TRIMS_BY_MM: Mapping[Tuple[str, str], Set[str]]


def refresh_universe() -> None:
    LOG.info("Refreshing global data frames...")

    global LOAD_SETTINGS

    global ATTRS
    global DEALERS
    global LISTINGS

    global ZIP_DEALER_DISTANCE
    global TRIMS_BY_YEAR
    global TRIMS_BY_MM

    # memory is the constraint here so with pandas we do a full drop and reload
    # to avoid having to make any copies.
    try:
        del DEALERS
        del LISTINGS
        del ATTRS
        del TRIMS_BY_YEAR
        del TRIMS_BY_MM
        collect()
    except NameError:
        pass

    ZIP_DEALER_DISTANCE.clear()

    # need this form to prevent autoflake from misbehaving
    globals()["DEALERS"] = load_all_dealers()
    ATTRS = load_attrs(LOAD_SETTINGS)
    globals()["LISTINGS"] = load_listings(LOAD_SETTINGS)
    # low effort write protection -- just to catch stupid mistakes
    TRIMS_BY_YEAR = MappingProxyType(
        ATTRS.reset_index().groupby("year")["trim_slug"].agg(set).to_dict()
    )
    TRIMS_BY_MM = MappingProxyType(
        ATTRS.reset_index()
        .groupby(["make", "model"])["trim_slug"]
        .agg(set)
        .to_dict()
    )


@lru_cache(maxsize=32)
def get_dealers_in_range(zipcode: str, max_miles: int) -> DataFrame:
    global DEALERS
    if (distance := ZIP_DEALER_DISTANCE.get(zipcode)) is None:
        q_lat, q_lon = LATLONG_BY_ZIP[zipcode]
        distance = great_circle_miles(
            DEALERS.loc[:, ["lon", "lat"]].values,
            q_lon,
            q_lat,
        )
        distance = ZIP_DEALER_DISTANCE[zipcode] = Series(
            distance, index=DEALERS.index
        )
        distance.name = "distance"

    out = DEALERS.join(distance)
    out.query(f"distance <= {max_miles}", inplace=True)
    return out


@lru_cache(maxsize=32)
def get_states_in_range(zipcode: str, max_miles: int) -> List[str]:
    return [*get_dealers_in_range(zipcode, max_miles)["state"].unique()]


@lru_cache(maxsize=32)
def filter_cars_by_year_mpg(
    year_min: int,
    year_max: int,
    mpg_min: int,
    mpg_max: int,
) -> DataFrame:
    """
    Selects available cars within mpg and year bounds.

    Returns:
        a filtered copy of ATTRS
    """

    return ATTRS.loc[X[year_min:year_max, :, :, :]].query(
        f"({mpg_min} <= mpg <= {mpg_max})"
    )


def filter_given_cars_by_mm(
    cars: DataFrame, make: str, model: str
) -> DataFrame:
    return cars.query(f"make == '{make}' & model == '{model}'")


def get_specific_cars(ymmts: Iterable[Tuple[int, str, str, str]]) -> DataFrame:
    key = ["year", "make", "model", "trim_slug"]
    selector = pd.DataFrame(sorted(ymmts), columns=key).set_index(key)
    return selector.join(ATTRS, how="inner")


CONN = sql.connect(CAR_DB)


def select_listings(
    want_cars: DataFrame,
    want_dealers: DataFrame,
    min_miles: int,
    max_miles: int,
    min_price: int,
    max_price: int,
) -> Union[str, DataFrame]:

    out = LISTINGS.join(
        want_cars.reset_index().set_index(YMMS_KEY), how="inner"
    )
    # out = pd.merge(LISTINGS, want_cars, on=YMMS_KEY, how="inner")
    if len(out) == 0:
        return "No listings that meet your criteria found."

    out = out.join(want_dealers, on=["dealer_id"], how="inner")
    if len(out) == 0:
        return "No listings in the chosen area."

    out.query(
        f"{min_miles} <= mileage <= {max_miles} &"
        f"{min_price} <= price <= {max_price}",
        inplace=True,
    )
    if len(out) == 0:
        return "No cars in your price and mileage range!"

    return out
