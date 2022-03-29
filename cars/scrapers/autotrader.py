from __future__ import annotations

import re
import time
from argparse import ArgumentParser, Namespace
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from functools import partial
from sqlite3 import connect
from typing import Any, ClassVar, Generator

from py9lib.io_ import ratelimit
from py9lib.util import suppress
from requests import ReadTimeout, Session

from cars import LOG
from cars.analysis.geo import tryhard_geocode
from cars.scrapers import (
    Dealership,
    Listing,
    ListingWithContext,
    ScraperState,
    YMMSAttr,
    inject_state,
    insert_listings,
    normalize_address,
    tryhard_name_to_hex,
)
from cars.util import CAR_DB

SOURCE_NAME = "autotrader"
BASE_URL = "http://www.autotrader.com/rest/searchresults/base"


def prepare_listing_dict(nd: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        raw_listings = nd["listings"]
        filters = nd["filters"]
    except KeyError:
        return []
    owners_by_id = {it["id"]: it for it in nd["owners"]}
    bodies_by_code = {
        it["value"]: it["label"] for it in filters["bodyStyleCode"]["options"]
    }

    filtered_listings = []
    for ld in raw_listings:
        try:
            ld["owner"] = owners_by_id[ld["owner"]]
            ld["bodyStyleCodes"] = [
                bodies_by_code[code] for code in ld["bodyStyleCodes"]
            ]
        except KeyError:
            continue
        filtered_listings.append(ld)
    return filtered_listings


@suppress(KeyError, logger=LOG.error)
def handle_listing(
    session: Session, ld: dict[str, Any]
) -> ListingWithContext | None:
    od = ld["owner"]

    # do some pre-filtering:
    # not dealing with these for now, wany only dealerships for greater
    # price consistency
    if od["privateSeller"]:
        return None

    # if we're missing critical keys, skip
    spec = ld["specifications"]
    for need_key in ["transmission", "driveType", "mpg", "mileage"]:
        if not spec.get(need_key):
            return None

    # this is a pretty solid geocoding routine, gets most addresses.
    # if it fails we're probably dealing with something wonky and can skip it
    dld = od["location"]["address"]
    dld["address1"] = normalize_address(dld["address1"])
    try:
        lat, lon = tryhard_geocode(
            session, dld["address1"], dld["zip"], dld["city"], dld["state"]
        )
    except ValueError:
        return None

    dealer = Dealership(
        address=dld["address1"],
        zip=dld["zip"],
        name=od["name"],
        city=dld["city"].title(),
        state=dld["state"],
        lat=round(lat, 6),
        lon=round(lon, 6),
        phone=od.get("phone", {}).get("value"),
        website=None,
    )

    mpgs = re.findall(r"([0-9]+) .*?([0-9]+)", spec["mpg"]["value"])[0]
    ymms_attr = YMMSAttr(
        year=ld["year"],
        make=ld["make"],
        model=ld["model"],
        # yes, this is a fucking mess. ¯\_(ツ)_/¯
        # some duplication in ymms_attr is acceptable, it only leads to slowdown
        # not unsoundness
        trim_slug=(trim := ld["style"][0]).lower(),
        style=ld.get("trim", trim),
        is_auto=spec["transmission"]["value"] == "Automatic",
        drivetrain=spec["driveType"]["value"],
        mpg_city=int(mpgs[0]),
        mpg_hwy=int(mpgs[1]),
        body=ld["bodyStyleCodes"][0].title(),
        fuel_type=ld["fuelType"],
        source=SOURCE_NAME,
    )

    with connect(CAR_DB) as conn:
        fs_rows = conn.execute(
            f"SELECT first_seen FROM autotrader_listings WHERE listing_id = ?",
            (ld["id"],),
        ).fetchall()
    if fs_rows:
        first_seen = fs_rows[0][0]
    else:
        first_seen = int(time.time())
        with connect(CAR_DB) as conn:
            conn.execute(
                "INSERT INTO autotrader_listings (listing_id, first_seen)"
                "VALUES (?, ?)",
                (ld["id"], first_seen),
            )

    listing = Listing(
        source=SOURCE_NAME,
        vin=ld["vin"],
        mileage=int(spec["mileage"]["value"].replace(",", "")),
        # NB this relies CRITICALLY on only processing any given listing ID once.
        first_seen=first_seen,
        last_seen=int(time.time()),
        price=ld["pricingDetail"]["salePrice"],
        history_flags=None,
        color_rgb_int=spec.get("interiorColor")
        and tryhard_name_to_hex(spec["interiorColor"]["value"]),
        color_rgb_ext=spec.get("color")
        and tryhard_name_to_hex(spec["color"]["value"]),
    )

    return ListingWithContext(
        ymms_attr=ymms_attr, listing=listing, dealership=dealer
    )


# fmt: off
AT_BODIES = [
    "CONVERT", "COUPE", "HATCH", "SEDAN",
    "SUV", "TRUCKS", "VANS", "WAGON",
]
# fmt: on


@dataclass
class AutotraderState(ScraperState):
    name: ClassVar[str] = "autotrader"
    cur_min_price: int
    cur_shards: list[tuple[str, int]] = field(default_factory=list)

    @classmethod
    def new(cls) -> AutotraderState:
        return AutotraderState(0)

    def next_sector(self, price_delta: int):
        self.cur_min_price += price_delta + 1


@inject_state(
    AutotraderState,
    catch=[TimeoutError, ReadTimeout],
    backoff_start=10,
    backoff_rate=10,
    log_fun=LOG.error,
)
def scrape(
    st: AutotraderState, args: Namespace = None
) -> Generator[AutotraderState, None, None]:
    args = args or Namespace

    if args.force_restart or st.cur_min_price > 1_000_000:
        st = st.new()
        yield st

    sess = Session()
    sess.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:98.0) "
            "Gecko/20100101 Firefox/98.0"
        }
    )

    insert_pool = ThreadPoolExecutor(max_workers=1)
    limiter = ratelimit(3, 1)

    http_get = limiter(partial(sess.get, timeout=30))

    delta = 256
    inserted = processed = 0
    while True:
        yield st

        # determine a suitable price range that will keep us under offset 1000,
        # which is the backend allowed limit
        while True:
            params = dict(
                allLisingType="USED",
                sellerTypes="d",
                searchRadius=0,
                numRecords=25,
                minPrice=st.cur_min_price,
                maxPrice=st.cur_min_price + delta,
            )

            if st.cur_shards:
                # TODO formalize/generalize parameter sector/sharding logic
                body, ofs = st.cur_shards[-1]
                params |= dict(firstRecord=ofs)
                if body:
                    params |= dict(vehicleStyleCodes=body)

            # without shards this tries to get the whole sector, which lets us know
            # how to shard
            nd = http_get(BASE_URL, params=params).json()
            tot_results = nd["totalResultCount"]
            LOG.debug(
                f"Got page: firstRecord={params.get('firstRecord', 0)}, "
                f"{tot_results=}; n_shards={len(st.cur_shards)}"
            )

            if st.cur_shards:
                break

            elif 0 < tot_results < 1000:
                st.cur_shards = [
                    (None, ofs) for ofs in range(0, tot_results, 25)
                ]
            elif tot_results == 0:
                st.next_sector(delta)
                yield st
            elif delta > 0:
                delta //= 2
            else:
                LOG.info(f"Still too many results, sharding by body type.")
                shards = {}
                for bt in AT_BODIES:
                    params["vehicleStyleCodes"] = bt
                    shards[bt] = http_get(BASE_URL, params=params).json()[
                        "totalResultCount"
                    ]
                st.cur_shards = [
                    (key, ofs)
                    for key, val in shards.items()
                    for ofs in range(0, val, 25)
                ]

        raw_listings = prepare_listing_dict(nd)

        listings = [handle_listing(sess, ld) for ld in raw_listings]
        inserted += len([it for it in listings if it is not None])
        processed += len(listings)

        insert_pool.submit(insert_listings, listings)
        st.cur_shards.pop()

        if not st.cur_shards:
            LOG.info(
                f"Inserted {inserted} listings: "
                f"price {st.cur_min_price}->{st.cur_min_price + delta}; "
                f"{inserted/processed if processed > 0 else 1:.3f} accept rate."
            )
            st.next_sector(delta)
            inserted = processed = 0
            # aim for 750 results
            delta = int(delta * 750 / (100 + tot_results))

        if st.cur_min_price > 1_000_000:
            return


def init_parser(parser: ArgumentParser) -> None:
    parser.add_argument(
        "--force-restart",
        action="store_true",
        help="trash state and restart from scratch",
    )
    parser.set_defaults(exe=lambda args: scrape(args))


if __name__ == "__main__":
    # noinspection PyTypeChecker
    scrape(Namespace(force_restart=False))
