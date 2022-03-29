import gzip
import json
import sys
import time
from argparse import ArgumentParser
from typing import Any

from requests import Session
from selenium.webdriver.common.by import By
from seleniumwire.request import Request, Response
from seleniumwire.webdriver import Firefox, FirefoxOptions

from cars.analysis.geo import tryhard_geocode
from cars.scrapers import (
    Dealership,
    Listing,
    ListingWithContext,
    VehicleHistory,
    YMMSAttr,
    insert_listings,
    normalize_address,
)

SOURCE_NAME = "edmunds"
BASE_URL = "https://www.edmunds.com"
SESSION: Session


def edmunds_resolve_latlon(da_dict: dict[str, Any]) -> tuple[float, float]:
    return tryhard_geocode(
        SESSION,
        da_dict["street"],
        da_dict["zip"],
        state=da_dict["state"],
        city=da_dict["city"],
    )


def parse_edmunds_listing(
    listing_dict: dict[str, Any]
) -> ListingWithContext | None:
    dd = listing_dict["dealerInfo"]
    da_dict = dd["address"]

    pd = dd["phoneNumbers"].get("basic") or dd["phoneNumbers"].get("trackable")
    phone_number = pd and (pd["areaCode"] + pd["prefix"] + pd["postfix"])
    website = (
        listing_dict["listingUrl"]
        if listing_dict.get("listingUrl", "").startswith("http://")
        else None
    )

    da_dict["street"] = normalize_address(da_dict["street"])

    try:
        lat, lon = edmunds_resolve_latlon(da_dict)
    except ValueError:
        return None

    dealership = Dealership(
        address=da_dict["street"],
        zip=da_dict["zip"],
        name=dd["name"],
        city=da_dict["city"],
        state=da_dict["stateCode"],
        lat=lat,
        lon=lon,
        phone=phone_number,
        website=website,
    )

    veh = listing_dict["vehicleInfo"]
    si = veh["styleInfo"]
    pi = veh["partsInfo"]
    ymms_attr = YMMSAttr(
        year=si["year"],
        make=si["make"],
        model=si["model"],
        style=si["style"].split("(")[0].strip(),
        trim_slug=si["trim"].lower(),
        mpg_city=si["fuel"]["epaCityMPG"],
        mpg_hwy=si["fuel"]["epaHighwayMPG"],
        fuel_type=pi["engineType"].lower(),
        body=si["bodyType"],
        drivetrain=pi["driveTrain"],
        is_auto=pi["transmission"] == "Automatic",
        source=SOURCE_NAME,
    )

    hi = listing_dict["historyInfo"]
    hist = VehicleHistory(
        is_accident=not hi["noAccidents"],
        is_lemon=hi["lemonHistory"],
        is_framedamage=hi["frameDamage"],
        is_salvage=hi["salvageHistory"],
        is_theft=hi["theftHistory"],
        n_owners=int(hi["ownerText"] or "1"),
        # TODO double check this captures the semantics correctly
        is_fleet=hi["usageType"] != "Personal Use",
        is_rental=hi["usageType"] in ["Taxi", "Lease"],
    )

    vc = veh["vehicleColors"]
    vci = vc["interior"]
    vce = vc["exterior"]
    listing = Listing(
        source=SOURCE_NAME,
        vin=listing_dict["vin"],
        first_seen=listing_dict["firstPublishedDate"] // 1000,
        last_seen=int(time.time()),
        color_rgb_int=None
        if "r" not in vci
        else f"{vci['r']:02X}{vci['g']:02X}{vci['b']:02X}",
        color_rgb_ext=None
        if "r" not in vce
        else f"{vce['r']:02X}{vce['g']:02X}{vce['b']:02X}",
        mileage=veh["mileage"],
        price=listing_dict["prices"]["displayPrice"],
        history_flags=hist,
    )

    return ListingWithContext(
        ymms_attr=ymms_attr, listing=listing, dealership=dealership
    )


def handle_json_payload(ed: dict[str, Any]) -> None:
    listings = []
    for ld in ed["inventories"]["results"]:
        try:
            listings.append(parse_edmunds_listing(ld))
        except KeyError as e:
            si = ld["vehicleInfo"]["styleInfo"]
            print(
                f"bad key parsing listing for a {si['make']} {si['model']}: {e}",
                file=sys.stderr,
            )
    insert_listings(listings)


def interceptor(request: Request) -> None:
    if request.path.endswith((".png", ".jpg", ".gif")):
        request.abort()
        return

    if (
        "edmunds.com" not in request.host
        and "edmunds-media.com" not in request.host
    ):
        request.abort()
        return

    if "activate" in request.path or "certified-program" in request.path:
        request.abort()
        return

    request.headers["User-Agent"] = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/99.0.4844.82 Safari/537.36 Vivaldi/4.3"
    )

    if (ffkey := "fetchSuggestedFacets") in request.params.keys():
        new_params = request.params
        new_params[ffkey] = "false"
        request.params = new_params


def resp_interceptor(request: Request, response: Response) -> None:
    if "inventory" in request.path and "api" in request.path:
        handle_json_payload(
            json.loads(gzip.decompress(response.body).decode("utf-8"))
        )


# TODO scrape state
def scrape_edmunds() -> None:

    global SESSION
    SESSION = Session()

    opts = FirefoxOptions()
    opts.add_argument("--headless")

    drv = Firefox(options=opts)
    drv.request_interceptor = interceptor
    drv.response_interceptor = resp_interceptor

    next_xpath = ".//a[@aria-label='Pagination left']"
    for px in range(29, 10000):
        drv.get(
            f"https://www.edmunds.com/inventory/srp.html"
            f"?inventorytype=used,cpo&pagenumber={px}"
            f"&sort=mileage:asc&radius=500"
        )
        while True:
            try:
                btn = drv.find_element(By.XPATH, next_xpath)
                btn.click()
                print(drv.current_url)
                break
            # TODO detect last page properly
            except Exception:
                time.sleep(1)

    drv.close()
    SESSION.close()


def init_parser(parser: ArgumentParser) -> None:
    parser.set_defaults(exe=lambda args: scrape_edmunds())


if __name__ == "__main__":
    scrape_edmunds()
