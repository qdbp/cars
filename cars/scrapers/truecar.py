from __future__ import annotations

import json
import sqlite3 as sql
import time
from asyncio import run
from collections import namedtuple
from dataclasses import dataclass
from typing import AsyncIterable, Dict, Iterable, Literal
from typing import Optional as Opt
from urllib.parse import urlencode

from aiohttp import ClientError, ClientSession
from py9lib.async_ import aenumerate, aratelimit, retry
from py9lib.db_ import mk_column_spec
from tqdm import tqdm
from webcolors import name_to_hex

from cars import LOG
from cars.scrapers import Dealership, TruecarListing, YMMSAttr
from cars.util import CAR_DB

URL_AGG = "..."
URL_LISTING = "https://www.truecar.com/abp/api/vehicles/used/listings/{}/"
URL_TEMPLATE_LOC = "https://www.truecar.com/abp/api/geographic/locations/{}"


@dataclass()
class TruecarState:
    scrape_started_unix: int
    scrape_finished_unix: int
    start_mileage: int
    fn: str = (default_fn := ".scraper_truecar")

    @classmethod
    def load(cls, fn: str = default_fn) -> Opt[TruecarState]:
        try:
            with open(fn, "r") as f:
                return cls(**json.load(f))
        except Exception:
            return None

    def dump(self) -> None:
        with open(self.fn, "w") as f:
            json.dump(vars(self), f)


def truecar_get_rgb_color(
    vehicle_dict: Dict[str, str], which: Literal["interior", "exterior"]
) -> Opt[str]:
    out: Opt[str] = None
    if (rgb_str := vehicle_dict.get(f"{which}_color_rgb")) is not None:
        return rgb_str
    try:
        out = name_to_hex(vehicle_dict.get(f"{which}_color"))[1:]
    except ValueError:
        try:
            out = name_to_hex(vehicle_dict.get(f"{which}_color_generic"))[1:]
        except ValueError:
            return None
    return out


TruecarDetails = namedtuple(
    "TruecarDetails", ["dealership", "ymms_attr", "listing"]
)


async def get_listings_shard_sqlite(
    sess: ClientSession,
    limiter: aratelimit,
    progress: tqdm,
    *,
    electric: bool = True,
    hybrid: bool = True,
    gas: bool = True,
    location: str = "bryn-mawr",
    radius: int = 5000,
    min_mileage: int = 0,
    max_mileage: int = 500000,
    min_price: int = 0,
    max_price: int = 2500000,
    min_year: int = 1900,
    max_year: int = 2030,
) -> AsyncIterable[TruecarDetails]:

    """
    Workhorse function to download car data from Truecar based on query limits.
    """

    base_url = "https://www.truecar.com/abp/api/vehicles/used/listings"

    params = [
        ("collapse", "false"),
        ("fallback", "false"),
        ("city", location),
        ("search_radius", radius),
        ("mileage_low", min_mileage),
        ("mileage_high", max_mileage),
        ("list_price_high", max_price),
        ("list_price_low", min_price),
        ("year_high", max_year),
        ("year_low", min_year),
        ("new_or_used", "u"),
        ("per_page", "30"),
    ]

    if hybrid:
        params.append(("fuel_type[]", "Hybrid"))
    if gas:
        params.append(("fuel_type[]", "Gas"))
    if electric:
        params.append(("fuel_type[]", "Electric"))

    page = 1
    seen = 0
    params.append(("page", page))

    limited_get = limiter(sess.get)
    global trims

    while True:
        resp = await limited_get(f"{base_url}?{urlencode(params)}")
        j = await resp.json()

        for listing in j["listings"]:

            vehicle = listing["vehicle"]
            dealer_dict = listing["dealership"]

            if vehicle["mpg_highway"] is None or vehicle["mpg_city"] is None:
                continue

            dealership = Dealership(
                dealer_id=(dealer_id := dealer_dict["id"]),
                dealer_name=dealer_dict["name"],
                lat=(loc := dealer_dict["location"])["lat"],
                lon=loc["lng"],
                city=loc["city"],
                state=loc["state"],
            )
            ymms_attr = YMMSAttr(
                year=(year := vehicle["year"]),
                make=(make := vehicle["make"]),
                model=(model := vehicle["model"]),
                style=(style := vehicle["style"]),
                trim_slug=vehicle["trim_slug"],
                mpg_city=vehicle["mpg_city"],
                mpg_hwy=vehicle["mpg_highway"],
                fuel_type=vehicle["fuel_type"].lower(),
                body=vehicle["body_style"],
                drivetrain=vehicle["drive_train"],
                engine=vehicle["engine"],
                is_auto=vehicle["transmission"] == "Automatic",
            )

            listing = TruecarListing(
                vin=(vehicle["vin"]),
                timestamp=round(time.time()),
                dealer_id=dealer_id,
                year=year,
                make=make,
                model=model,
                style=style,
                mileage=vehicle["mileage"],
                price=listing["pricing"]["total_price"],
                color_rgb=truecar_get_rgb_color(vehicle, "exterior"),
                # TODO parse interior to hex as well.
                color_interior=vehicle["interior_color"],
            )

            yield TruecarDetails(dealership, ymms_attr, listing)

        total = j["total"]
        page += 1
        params[-1] = ("page", page)
        n_new_cars = len(j["listings"])

        seen += n_new_cars
        progress.update(n_new_cars)

        if seen >= total or n_new_cars == 0:
            break


def insert_listings(details: Iterable[TruecarDetails]) -> None:

    details = list(details)
    if not details:
        return

    dealers = list(
        {l.dealership["dealer_id"]: l.dealership for l in details}.values()
    )
    ymmss = list(
        {
            (
                l.ymms_attr["year"],
                l.ymms_attr["make"],
                l.ymms_attr["model"],
                l.ymms_attr["style"],
            ): l.ymms_attr
            for l in details
        }.values()
    )

    with sql.connect(CAR_DB) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executemany(
            # language=sql
            f"""
            INSERT OR IGNORE INTO truecar_dealerships
            {mk_column_spec(dealers[0])}
            """,
            dealers,
        )
        conn.executemany(
            # language=sql
            f"""
            INSERT OR IGNORE INTO truecar_ymms_attrs
            {mk_column_spec(ymmss[0])}
            """,
            ymmss,
        )
        conn.executemany(
            # language=sql
            f"""
            INSERT OR IGNORE INTO truecar_listings
            {mk_column_spec(details[0].listing)}
            """,
            (d.listing for d in details),
        )


@retry(
    [ClientError],
    backoff_start=10,
    backoff_rate=10,
    log_fun=LOG.error,
)  # type: ignore
async def run_scraper(**filter_kwargs) -> None:

    LOG.info("Starting scrape.")

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
    target_total = 1000

    mileage_cap = filter_kwargs.pop("max_mileage", None) or 500000

    conn = sql.Connection(CAR_DB)

    state.scrape_started_unix = int(time.time())

    limiter = aratelimit(3, 0.5)
    session = ClientSession()

    listings = []
    progress = tqdm(unit=" cars")

    async with session:
        while True:
            max_mileage = min(start_mileage + mileage_delta, mileage_cap)
            count = 1

            progress.set_description_str(
                f"Scraping mileage {start_mileage} -> {max_mileage}"
            )

            async for count, listing in aenumerate(
                get_listings_shard_sqlite(
                    session,
                    progress=progress,
                    limiter=limiter,
                    min_mileage=start_mileage,
                    max_mileage=max_mileage,
                    **filter_kwargs,
                )
            ):
                listings.append(listing)

            insert_listings(listings)
            listings.clear()

            # mileage_delta < 5 is bugged on the server side
            start_mileage += mileage_delta
            mileage_delta = max(5, int(mileage_delta * target_total / count))

            state.start_mileage = start_mileage
            state.dump()

            if max_mileage == mileage_cap:
                break

    state.scrape_finished_unix = int(time.time())
    state.dump()

    if not nodelete:
        with conn:
            conn.execute(
                f"""DELETE FROM truecar_listings
                WHERE timestamp < {state.scrape_started_unix - 1}"""
            )


if __name__ == "__main__":
    run(run_scraper())
