import gzip
import json
import re
import sys
import time
from argparse import ArgumentParser
from sqlite3 import connect
from typing import Any

from geopy.geocoders import Nominatim
from requests import Session
from selenium.webdriver.common.by import By
from seleniumwire.request import Request, Response
from seleniumwire.webdriver import Firefox, FirefoxOptions

from cars.scrapers import (
    Dealership,
    Listing,
    ListingWithContext,
    VehicleHistory,
    YMMSAttr,
    insert_listings,
)
from cars.util import CAR_DB

BASE_URL = "https://www.edmunds.com"
NOMINATIM = Nominatim(
    domain="127.0.0.1:8080", user_agent="en-car-shopping-tool", scheme="http"
)
geocode = NOMINATIM.geocode


def patch_up_addr(addr: str) -> str:
    addr = addr.replace("US Hwy", "US")
    addr = re.sub(
        r"(?:rt|Rt|Rte|rte) ([0-9]+) (?:East|West|North|South|E|W|N|S)",
        r"Rte \1",
        addr,
    )
    addr = addr.replace("Tpke", "Turnpike")
    return addr


CENSUS_PATH = "https://geocoding.geo.census.gov/geocoder/locations/address"
SESSION: Session


def geocode_census_zip(addr: str, zipcode: str) -> tuple[float, float]:
    """
    Args:
        addr: the US address to look up
        zipcode: the zipcode within which to look

    Returns:
        lat, lon of address as known by census.gov

    """
    raw = SESSION.get(
        f"{CENSUS_PATH}?street={addr}&zip={zipcode}&benchmark=2020&format=json"
    ).json()
    try:
        coords = raw["result"]["addressMatches"][0]["coordinates"]
    except IndexError as e:
        raise ValueError(f"Address {addr} not found at {zipcode}") from e
    return coords["y"], coords["x"]


def geocode_census_city_state(
    addr: str, city: str, state: str
) -> tuple[float, float]:
    """
    Args:
        addr: the US address to look up
        city: the city
        state: the state
    Returns:
        lat, lon of address as known by census.gov

    """
    raw = SESSION.get(
        f"{CENSUS_PATH}?street={addr}&city={city}&state={state}&benchmark=2020&format=json"
    ).json()
    try:
        coords = raw["result"]["addressMatches"][0]["coordinates"]
    except IndexError as e:
        raise ValueError(f"Address {addr} not found at {city}, {state}") from e
    return coords["y"], coords["x"]


def edmunds_resolve_latlon(da_dict: dict[str, Any]) -> tuple[float, float]:
    addr = da_dict["street"]
    zipcode = da_dict["zip"]
    with connect(CAR_DB) as conn:
        rows = conn.execute(
            f"""
            SELECT lat, lon from dealerships
            WHERE address like '{addr}' AND zip like '{zipcode}'
            """
        ).fetchall()
    if rows:
        return rows[0][0], rows[0][1]

    try:
        lat, lon = geocode_census_zip(addr, zipcode)
    except ValueError:
        state = da_dict["stateCode"]
        city = da_dict["city"]
        try:
            lat, lon = geocode_census_city_state(addr, city, state)
        except ValueError:
            gc_info = geocode(
                query=dict(street=addr, city=city, state=state, country="U.S."),
            )
            if gc_info is None:
                raise KeyError(f"No valid lat/lon for {addr}/{zipcode}")
            lat = round(gc_info.latitude, 6)
            lon = round(gc_info.longitude, 6)
    return lat, lon


def parse_edmunds_listing(listing_dict: dict[str, Any]) -> ListingWithContext:
    dd = listing_dict["dealerInfo"]
    da_dict = dd["address"]

    pd = dd["phoneNumbers"].get("basic") or dd["phoneNumbers"].get("trackable")
    phone_number = pd and (pd["areaCode"] + pd["prefix"] + pd["postfix"])
    website = (
        listing_dict["listingUrl"]
        if listing_dict.get("listingUrl", "").startswith("http://")
        else None
    )

    da_dict["street"] = patch_up_addr(da_dict["street"])
    lat, lon = edmunds_resolve_latlon(da_dict)

    dealership = Dealership(
        address=da_dict["street"],
        zip=da_dict["zip"],
        dealer_name=dd["name"],
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
    dt_map = {
        "all wheel drive": "AWD",
        "front wheel drive": "FWD",
        "rear wheel drive": "RWD",
        "four wheel drive": "4WD",
    }
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
        drivetrain=dt_map[pi["driveTrain"]],
        is_auto=pi["transmission"] == "Automatic",
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
        source="edmunds",
        vin=listing_dict["vin"],
        first_seen=listing_dict["firstPublishedDate"] // 1000,
        last_seen=int(time.time()),
        color_rgb_int=None
        if "r" not in vci
        else f"{vci['r']:02X}{vci['g']:02X}{vci['b']:02X}",
        color_rgb_ext=None
        if "r" not in vce
        else f"{vce['r']:02X}{vce['g']:02X}{vce['b']:02X}",
        dealer_address=dealership.address,
        dealer_zip=dealership.zip,
        year=ymms_attr.year,
        make=ymms_attr.make,
        model=ymms_attr.model,
        style=ymms_attr.style,
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
