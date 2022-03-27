from __future__ import annotations

import asyncio
import json
import sqlite3 as sql
import time
from argparse import ArgumentParser, Namespace
from asyncio import run
from atexit import register
from dataclasses import dataclass
from datetime import datetime
from typing import AsyncIterable, Literal
from urllib.parse import urlencode

from aiohttp import ClientError, ClientSession
from py9lib.async_ import aenumerate, aratelimit, retry
from tqdm import tqdm
from webcolors import CSS2, CSS3, CSS21, HTML4, name_to_hex

from cars import LOG
from cars.scrapers import (
    Dealership,
    Listing,
    ListingWithContext,
    VehicleHistory,
    YMMSAttr,
    insert_listings,
)
from cars.util import CAR_DB

SOURCE_NAME = "truecar"
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
    def load(cls, fn: str = default_fn) -> TruecarState | None:
        try:
            with open(fn, "r") as f:
                return cls(**json.load(f))
        except Exception:
            return None

    def dump(self) -> None:
        with open(self.fn, "w") as f:
            json.dump(vars(self), f, indent=4)


def tryhard_name_to_hex(name: str) -> str | None:
    for spec in [CSS3, CSS21, CSS2, HTML4]:
        try:
            return name_to_hex(name.lower(), spec)  # type: ignore
        except ValueError:
            continue
    return None


def truecar_get_rgb_color(
    vehicle_dict: dict[str, str], which: Literal["interior", "exterior"]
) -> str | None:
    out: str | None
    if (rgb_str := vehicle_dict.get(f"{which}_color_rgb")) is not None:
        return rgb_str.lstrip("#").upper()

    for key in [f"{which}_color", f"{which}_color_generic"]:
        cstr = vehicle_dict.get(key)
        if cstr is not None:
            out = tryhard_name_to_hex(cstr)
            if out is not None:
                return out.lstrip("#").upper()
    return None


async def get_listings_shard_sqlite(
    sess: ClientSession,
    limiter: aratelimit,
    progress: tqdm,
    *,
    electric: bool = True,
    hybrid: bool = True,
    gas: bool = True,
    min_mileage: int = 0,
    max_mileage: int = 500000,
    min_price: int = 0,
    max_price: int = 2500000,
    min_year: int = 1900,
    max_year: int = 2030,
) -> AsyncIterable[ListingWithContext]:

    """
    Workhorse function to download car data from Truecar based on query limits.
    """

    base_url = "https://www.truecar.com/abp/api/vehicles/used/listings"

    params = [
        ("collapse", "false"),
        ("fallback", "false"),
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

    while True:
        resp = await limited_get(f"{base_url}?{urlencode(params)}")
        j = await resp.json()

        for listing in j["listings"]:

            vehicle = listing["vehicle"]
            hist_dict = vehicle["condition_history"]
            ti = hist_dict["titleInfo"]

            history = VehicleHistory(
                is_accident=hist_dict["accidentCount"] == 0,
                is_framedamage=ti["isFrameDamaged"],
                is_salvage=ti["isSalvage"],
                is_lemon=ti["isLemon"],
                is_theft=ti["isTheftRecovered"],
                n_owners=hist_dict["ownerCount"] or 0,
                is_rental=hist_dict["isRentalCar"],
                is_fleet=hist_dict["isFleetCar"],
            )

            dealer_dict = listing["dealership"]
            loc = dealer_dict["location"]
            dealer_addr = loc["address1"] + (
                "" if loc["address2"] is None else " " + loc["address2"]
            )

            if vehicle["mpg_highway"] is None or vehicle["mpg_city"] is None:
                continue

            dealership = Dealership(
                address=dealer_addr,
                zip=(dealer_zip := loc["postal_code"]),
                dealer_name=dealer_dict["name"],
                lat=loc["lat"],
                lon=loc["lng"],
                city=loc["city"],
                state=loc["state"],
                phone=None,  # TODO check if we're just missing this
                website=dealer_dict["links"]["website_link"],
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
                # engine=vehicle["engine"],
                is_auto=vehicle["transmission"] == "Automatic",
            )

            listing = Listing(
                source=SOURCE_NAME,
                vin=vehicle["vin"],
                first_seen=round(
                    datetime.fromisoformat(listing["listed_at"]).timestamp()
                ),
                last_seen=round(time.time()),
                dealer_address=dealer_addr,
                dealer_zip=dealer_zip,
                year=year,
                make=make,
                model=model,
                style=style,
                mileage=vehicle["mileage"],
                price=listing["pricing"]["total_price"],
                color_rgb_int=truecar_get_rgb_color(vehicle, "exterior"),
                # TODO parse interior to hex as well.
                color_rgb_ext=truecar_get_rgb_color(vehicle, "interior"),
                history_flags=history,
            )

            yield ListingWithContext(dealership, ymms_attr, listing)

        total = j["total"]
        page += 1
        params[-1] = ("page", page)
        n_new_cars = len(j["listings"])

        seen += n_new_cars
        progress.update(n_new_cars)

        if seen >= total or n_new_cars == 0:
            break


@retry(
    [ClientError, asyncio.TimeoutError],
    backoff_start=10,
    backoff_rate=10,
    log_fun=LOG.error,
)  # type: ignore
async def run_scraper(args: Namespace) -> None:

    LOG.info("Starting Truecar scrape.")

    if (state := TruecarState.load()) is None or args.force_restart:
        state = TruecarState(-1, -1, 1)
    else:
        LOG.info(f"Restarting scrape with state {state}.")

    if state.scrape_finished_unix < state.scrape_started_unix:
        start_mileage = state.start_mileage
    else:
        start_mileage = 1

    mileage_delta = 10
    target_total = 1000
    mileage_cap = 500_000

    conn = sql.Connection(CAR_DB)
    register(conn.close)

    state.scrape_started_unix = int(time.time())

    limiter = aratelimit(3, args.ratelimit)
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

            if max_mileage >= mileage_cap:
                break

    state.scrape_finished_unix = int(time.time())
    state.dump()


def init_parser(parser: ArgumentParser) -> None:
    parser.add_argument(
        "--ratelimit",
        default=0.5,
        type=float,
        help="rate limit, seconds between requests",
    )
    parser.add_argument(
        "--force-restart",
        action="store_true",
        help="trash state and restart from scratch",
    )
    parser.set_defaults(exe=lambda args: run(run_scraper(args)))
