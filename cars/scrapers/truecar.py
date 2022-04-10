from __future__ import annotations

import sqlite3 as sql
import time
from argparse import ArgumentParser, Namespace
from atexit import register
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import ClassVar, Iterable, Literal
from urllib.parse import urlencode

from py9lib.io_ import ratelimit
from requests import ConnectTimeout, ReadTimeout, Session

from cars import LOG
from cars.scrapers import (
    Dealership,
    Listing,
    ListingWithContext,
    ScraperState,
    VehicleHistory,
    YMMSAttr,
    inject_state,
    insert_listings,
    normalize_address,
    tryhard_name_to_hex,
)
from cars.util import CAR_DB

SOURCE_NAME = "truecar"
TRUECAR_LL_QUAL = 200


@dataclass
class TruecarState(ScraperState):
    name: ClassVar[str] = SOURCE_NAME
    scrape_started_unix: int
    scrape_finished_unix: int
    start_mileage: int

    @classmethod
    def new(cls) -> TruecarState:
        return TruecarState(int(time.time()), -1, 0)


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
                return out
    return None


def get_listings_shard_sqlite(
    sess: Session,
    limiter: ratelimit,
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
) -> Iterable[ListingWithContext]:

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

    limited_get = limiter(partial(sess.get, timeout=30))

    while True:
        resp = limited_get(f"{base_url}?{urlencode(params)}")
        j = resp.json()

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
            dealer_addr = normalize_address(loc["address1"])

            if vehicle["mpg_highway"] is None or vehicle["mpg_city"] is None:
                continue

            dealership = Dealership(
                address=dealer_addr,
                zip=loc["postal_code"],
                name=dealer_dict["name"],
                lat=loc["lat"],
                lon=loc["lng"],
                city=loc["city"],
                state=loc["state"],
                phone=None,  # TODO check if we're just missing this
                website=dealer_dict["links"]["website_link"],
                ll_qual=TRUECAR_LL_QUAL,
            )
            ymms_attr = YMMSAttr(
                year=vehicle["year"],
                make=vehicle["make"],
                model=vehicle["model"],
                style=vehicle["style"],
                trim_slug=vehicle["trim_slug"],
                mpg_city=vehicle["mpg_city"],
                mpg_hwy=vehicle["mpg_highway"],
                fuel_type=vehicle["fuel_type"].lower(),
                body=vehicle["body_style"],
                drivetrain=vehicle["drive_train"],
                is_auto=vehicle["transmission"] == "Automatic",
                source=SOURCE_NAME,
            )

            listing = Listing(
                source=SOURCE_NAME,
                vin=vehicle["vin"],
                first_seen=round(
                    datetime.fromisoformat(listing["listed_at"]).timestamp()
                ),
                last_seen=round(time.time()),
                mileage=vehicle["mileage"],
                price=listing["pricing"]["total_price"],
                color_rgb_int=truecar_get_rgb_color(vehicle, "interior"),
                # TODO parse interior to hex as well.
                color_rgb_ext=truecar_get_rgb_color(vehicle, "exterior"),
                history_flags=history,
            )

            yield ListingWithContext(dealership, ymms_attr, listing)

        total = j["total"]
        page += 1
        params[-1] = ("page", page)
        n_new_cars = len(j["listings"])

        seen += n_new_cars
        # progress.update(n_new_cars)

        if seen >= total or n_new_cars == 0:
            break


@inject_state(
    TruecarState,
    catch=[ConnectTimeout, TimeoutError, ReadTimeout],
    backoff_start=10,
    backoff_rate=10,
    log_fun=LOG.error,
)
def run_scraper(state: TruecarState, args: Namespace) -> None:

    LOG.info(f"Starting scrape with state {state}")

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

    limiter = ratelimit(3, args.ratelimit)
    session = Session()
    insert_executor = ThreadPoolExecutor(max_workers=1)

    with session:
        while True:
            max_mileage = min(start_mileage + mileage_delta, mileage_cap)
            listings = list(
                get_listings_shard_sqlite(
                    session,
                    limiter,
                    min_mileage=start_mileage,
                    max_mileage=max_mileage,
                )
            )
            if len(listings) == 0 and state.start_mileage >= 500_000:
                yield state.new()
                return

            insert_executor.submit(insert_listings, listings)

            # mileage_delta < 5 is bugged on the server side
            start_mileage += mileage_delta
            mileage_delta = max(
                5, int(mileage_delta * target_total / len(listings))
            )

            LOG.info(
                f"Inserted {len(listings)} listings: "
                f"mileage {start_mileage}->{start_mileage + mileage_delta}"
            )

            state.start_mileage = start_mileage
            yield state
            listings.clear()

            if max_mileage >= mileage_cap:
                break

    state.scrape_finished_unix = int(time.time())
    yield state


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
    parser.set_defaults(exe=lambda args: run_scraper(args))


if __name__ == "__main__":
    parser = ArgumentParser()
    init_parser(parser)
    run_scraper(parser.parse_args())
