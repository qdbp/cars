import sqlite3 as sql
from functools import lru_cache
from gc import collect
from typing import Dict, Iterable, List, Mapping
from typing import Optional as Opt
from typing import Set, Tuple, TypedDict, TypeVar, Union

import pandas as pd
from numpy import int32, int64, uint32, uint64
from pandas import DataFrame, Series
from py9lib.util import timed

import cars.scrapers as scr
from cars.analysis.geo import LATLONG_BY_ZIP, great_circle_miles
from cars.util import CAR_DB

from .. import LOG

T = TypeVar("T")

YMMS_KEY = ["year", "make", "model", "style"]
YMMT_KEY = ["year", "make", "model", "trim_slug"]

MAX_PRICE = 100_000
MAX_MILEAGE = 200_000

LISTING_LIMIT = 250

sql.register_adapter(int64, int)
sql.register_adapter(uint64, int)
sql.register_adapter(int32, int)
sql.register_adapter(uint32, int)


@timed(LOG.info)  # type: ignore
def load_listings_preindexer() -> DataFrame:
    with sql.connect(CAR_DB) as conn:
        listings = pd.read_sql_query(
            f"""
            SELECT vin, dealer_id, price, mileage
            FROM truecar_listings tl
            """,
            conn,
        )

    return listings


def load_attrs() -> DataFrame:
    with sql.connect(CAR_DB) as conn:
        out = pd.read_sql(
            f"""
            SELECT 
                year, make, model, style, trim_slug, style,
                (0.45 * mpg_hwy + 0.55 * mpg_city) as mpg,
                fuel_type, body, drivetrain, is_auto, engine
            FROM truecar_ymms_attrs
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
        out.index.name = "dealer_id"
        return out


LISTINGS_PREINDEXER: DataFrame
DEALERS: DataFrame
ATTRS: DataFrame

# caches
ZIP_DEALER_DISTANCE: Dict[str, Series] = {}
TRIMS_BY_YEAR: Mapping[int, Set[str]]
TRIM_YEARS_BY_MM: Mapping[str, Dict[str, Dict[str, List[int]]]]
MMS: List[Tuple[str, str]]


class RawClientData(TypedDict):
    attrs: List[Tuple[int, str, str, str, bool, str, str, str]]
    prop_to_ix: Dict[str, Dict[Union[str, int], int]]


RAW_CLIENT_DATA: RawClientData


def reverse_index(vals: Iterable[T]) -> Dict[T, int]:
    return {v: ix for ix, v in enumerate(vals)}


@timed(LOG.info)
def refresh_universe() -> None:

    global ATTRS
    global DEALERS

    global ZIP_DEALER_DISTANCE
    global TRIMS_BY_YEAR
    global TRIM_YEARS_BY_MM
    global MMS
    global RAW_CLIENT_DATA

    # memory is the constraint here so with pandas we do a full drop and reload
    # to avoid having to make any copies.
    try:
        del DEALERS
        del ATTRS
        del LISTINGS_PREINDEXER
        collect()
    except NameError:
        pass

    ZIP_DEALER_DISTANCE.clear()

    # need this form to prevent autoflake from misbehaving
    # globals()["LISTINGS_PREINDEXER"] = load_listings_preindexer()
    globals()["DEALERS"] = load_all_dealers()
    ATTRS = load_attrs()
    # low effort write protection -- just to catch stupid mistakes

    RAW_CLIENT_DATA = {
        "attrs": list(
            ATTRS.reset_index()
            .loc[
                :,
                [
                    "year",
                    "make",
                    "model",
                    "trim_slug",
                    "style",
                    "mpg",
                    "is_auto",
                    "drivetrain",
                    "fuel_type",
                    "body",
                    "engine",
                ],
            ]
            .agg(lambda s: s.to_dict(), axis=1)
        ),
        "prop_to_ix": {
            "is_auto": reverse_index(scr.TRANSMISSION_VALS),
            "drivetrain": reverse_index(scr.KNOWN_DRIVETRAINS),
            "fuel_type": reverse_index(scr.KNOWN_FUEL_TYPES),
            "body": reverse_index(scr.KNOWN_BODIES),
        },
    }


@lru_cache(maxsize=32)
@timed(LOG.info)  # type: ignore
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


@lru_cache(maxsize=200)
def quote(s: str) -> str:
    return f"'{s}'"


@lru_cache(maxsize=32)
@timed(LOG.info)  # type: ignore
def filter_cars_by_attr_selectors(
    year_min: int,
    year_max: int,
    mpg_min: int,
    mpg_max: int,
    transmissions: Tuple[str, ...],
    drivetrains: Tuple[str, ...],
    fuel_types: Tuple[str, ...],
    bodies: Tuple[str, ...],
    ymmt_only: bool = True,
) -> Opt[DataFrame]:

    """
    Selects available cars within mpg and year bounds.

    Returns:
        a filtered copy of ATTRS
    """

    query = (
        f"({mpg_min} <= mpg <= {mpg_max})"
        f"&({year_min} <= year <= {year_max})"
    )

    assert 0 < len(transmissions) <= 2
    if len(transmissions) == 1:
        query += f"&(is_auto == {int(transmissions[0] == 'auto')})"

    for col, selectors in zip(
        ["drivetrain", "fuel_type", "body"],
        [drivetrains, fuel_types, bodies],
    ):
        assert len(selectors) > 0
        query += (
            f"&({'|'.join(f'({col} == {quote(sel)})' for sel in selectors)})"
        )

    try:
        if not ymmt_only:
            return ATTRS.query(query)
        else:
            return ATTRS.query(query).reset_index()[YMMT_KEY]
    # this means the query is empty... probably...
    except ValueError as e:
        LOG.error(e)
        return None


def filter_given_cars_by_mm(
    cars: DataFrame, make: str, model: str
) -> DataFrame:
    return cars.query(f"make == '{make}' & model == '{model}'")


def get_specific_cars(ymmts: Iterable[Tuple[int, str, str, str]]) -> DataFrame:
    key = ["year", "make", "model", "trim_slug"]
    selector = pd.DataFrame(sorted(ymmts), columns=key).set_index(key)
    return selector.join(ATTRS, how="inner")


def query_listings(
    ymms_selector: DataFrame,
    dealer_ids: DataFrame,
    min_miles: int,
    max_miles: int,
    min_price: float,
    max_price: float,
) -> DataFrame:

    assert ymms_selector.shape[1] == 4
    assert dealer_ids.shape[1] == 1

    assert isinstance(min_miles, (float, int))
    assert isinstance(max_miles, (float, int))
    assert isinstance(min_price, (int, float))
    assert isinstance(max_price, (int, float))

    with sql.connect(CAR_DB) as conn:
        conn.executescript(
            # language=sql
            """
            CREATE TEMP TABLE q_dealer (
                dealer_id INTEGER PRIMARY KEY 
            );
            CREATE TEMP TABLE q_ymms (
                year INTEGER,
                make TEXT,
                model TEXT,
                style TEXT,
                PRIMARY KEY (year, make, model, style)
            ) WITHOUT ROWID ;
            """
        )
        conn.executemany(
            """
            INSERT INTO q_dealer VALUES  (?)
            """,
            dealer_ids.values,
        )
        conn.executemany(
            """
            INSERT INTO q_ymms VALUES  (?, ?, ?, ?)
            """,
            ymms_selector.values,
        )

        sel_listings = pd.read_sql(
            f"""
                SELECT * FROM (
                    truecar_listings
                    NATURAL JOIN q_ymms
                    NATURAL JOIN q_dealer
                )
                WHERE 
                    mileage >= :min_miles
                AND mileage <= :max_miles
                AND price >= :min_price
                AND price <= :max_price
                LIMIT :limit
                """,
            conn,
            params=(
                dict(
                    max_price=max_price,
                    min_price=min_price,
                    max_miles=max_miles,
                    min_miles=min_miles,
                    limit=LISTING_LIMIT,
                )
            ),
        )
        conn.executescript(
            """
            DROP TABLE q_dealer;
            DROP TABLE q_ymms;
            """
        )

    sel_listings.query(
        "(@min_price <= price <= @max_price)"
        "&(@min_miles <= mileage <= @max_miles)",
        inplace=True,
    )

    return sel_listings
