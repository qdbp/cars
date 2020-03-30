from __future__ import annotations

import json
import sqlite3 as sql
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional
from urllib.parse import urlencode
from urllib.request import urlopen

from tqdm import tqdm
from webcolors import name_to_hex

from src.util import CAR_DB

URL_AGG = "..."
URL_LISTING = "https://www.truecar.com/abp/api/vehicles/used/listings/{}/"
URL_TEMPLATE_LOC = "https://www.truecar.com/abp/api/geographic/locations/{}"

TRUECAR_SCRAPER_STATE = ".scraper_truecar"


@dataclass()
class TruecarState:
    scrape_started_unix: Optional[int]
    scrape_finished_unix: Optional[int]
    start_mileage: Optional[int]

    @classmethod
    def load(cls, fn=TRUECAR_SCRAPER_STATE) -> Optional[TruecarState]:
        try:
            with open(fn, "r") as f:
                return cls(**json.load(f))
        except Exception:
            return None

    def dump(self, fn=TRUECAR_SCRAPER_STATE):
        with open(fn, "w") as f:
            json.dump(vars(self), f)


def truecar_get_rgb_color(vehicle_dict: Dict[str, Any]) -> str:

    if (rgb_str := vehicle_dict.get("exterior_color_rgb")) is not None:
        return rgb_str

    try:
        return name_to_hex(vehicle_dict.get("exterior_color"))[1:]
    except Exception:
        try:
            return name_to_hex(vehicle_dict.get("exterior_color_generic"))[1:]
        except Exception:
            return None


def get_listings_shard_sqlite(
    conn: sql.Connection,
    electric=True,
    hybrid=True,
    gas=True,
    location="bryn-mawr",
    radius=5000,
    min_mileage=0,
    max_mileage=500000,
    min_price=0,
    max_price=2500000,
    year_low=1900,
    yearh_high=2030,
    progress=None,
    do_historical=True,
) -> int:

    """
    Workhorse function to download car data from Truecar based on query limits.
    """

    base_url = "https://www.truecar.com/abp/api/vehicles/used/listings"

    params = [
        ("collapse", "false"),
        ("fallback", "true"),
        ("city", location),
        ("search_radius", radius),
        ("mileage_low", min_mileage),
        ("mileage_high", max_mileage),
        ("list_price_high", max_price),
        ("list_price_low", min_price),
        ("year_high", yearh_high),
        ("year_low", year_low),
        ("transmission[]", "Automatic"),
        ("new_or_used", "u"),
        ("per_page", "30"),
        ("state", "pa"),
    ]

    if hybrid:
        params.append(("fuel_type[]", "Hybrid"))
    if gas:
        params.append(("fuel_type[]", "Gas"))
    if electric:
        params.append(("fuel_type[]", "Electric"))

    page = 1
    seen = 0
    total = None

    params.append(("page", page))

    while True:

        url = f"{base_url}?{urlencode(params)}"

        with urlopen(url) as raw:
            j = json.loads(raw.read())

        dealerships = {}
        attributes = {}
        listings = {}

        for listing in j["listings"]:

            vehicle = listing["vehicle"]
            dealership = listing["dealership"]
            dealership_id = dealership["id"]
            pricing = listing["pricing"]

            vin = vehicle["vin"]

            if vehicle["mpg_highway"] is None or vehicle["mpg_city"] is None:
                continue

            dealerships[dealership_id] = [
                dealership_id,
                dealership["name"],
                dealership["location"]["lat"],
                dealership["location"]["lng"],
                dealership["location"]["city"],
                dealership["location"]["state"],
            ]

            attributes[vin] = [
                vin,
                vehicle["year"],
                vehicle["make"],
                vehicle["model"],
                vehicle["trim"],
                vehicle["style"],
                truecar_get_rgb_color(vehicle),
                vehicle["interior_color"].lower(),
                vehicle["mpg_city"],
                vehicle["mpg_highway"],
                vehicle["fuel_type"].lower(),
            ]

            listings[vin] = [
                vin,
                round(time.time()),
                vehicle["mileage"],
                pricing["list_price"],
                pricing["total_fees"],
                vehicle["days_in_inventory"],
                dealership_id,  # this is the id
            ]

        with conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO truecar_dealerships
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                dealerships.values(),
            )
            conn.executemany(
                """
                INSERT OR REPLACE INTO truecar_attrs
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                attributes.values(),
            )
            if do_historical:
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO truecar_listings
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    listings.values(),
                )
            conn.executemany(
                """
                INSERT OR REPLACE INTO truecar_listings_snapshot
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                listings.values(),
            )

        if progress is None:
            total = j["total"]
            progress = tqdm(total=total, unit="cars", desc="shard")
        elif total is None:
            total = j["total"]
            progress.reset(total=total)

        page += 1
        params[-1] = ("page", page)

        n_new_cars = len(j["listings"])
        seen += n_new_cars
        progress.update(n_new_cars)

        if seen >= progress.total or n_new_cars == 0:
            break

    return total


def run_scraper(**filter_kwargs):

    if (state := TruecarState.load()) is None:
        state = TruecarState(-1, -1, 1)

    nodelete = len(filter_kwargs) > 0

    if "min_mileage" in filter_kwargs:
        start_mileage = filter_kwargs.pop("min_mileage")
    else:
        if state.scrape_finished_unix < state.scrape_started_unix:
            start_mileage = state.start_mileage
        else:
            start_mileage = 1

    mileage_delta = 10
    target_total = 300

    mileage_cap = filter_kwargs.pop("max_mileage", None) or 500000

    conn = sql.Connection(CAR_DB)

    progress = tqdm(
        total=mileage_cap,
        unit="miles",
        desc="outer scrape",
        initial=start_mileage,
        leave=True,
        position=0,
    )

    inner_progress = tqdm(
        total=0, unit="cars", desc="shard", leave=True, position=1
    )

    state.scrape_started_unix = int(time.time())

    while True:

        max_mileage = min(start_mileage + mileage_delta, mileage_cap)

        total = get_listings_shard_sqlite(
            conn,
            min_mileage=start_mileage,
            max_mileage=max_mileage,
            **filter_kwargs,
            progress=inner_progress,
        )

        progress.update(mileage_delta)

        # mileage_delta < 5 is bugged on the server side
        start_mileage += mileage_delta
        mileage_delta = max(5, int(mileage_delta * target_total / total))

        state.start_mileage = start_mileage
        state.dump()

        if max_mileage == mileage_cap:
            break

    state.scrape_finished_unix = int(time.time())
    state.dump()

    if not nodelete:
        with conn:
            conn.execute(
                f"""DELETE FROM truecar_listings_snapshot
                WHERE timestamp < {state.scrape_started_unix - 1}"""
            )
