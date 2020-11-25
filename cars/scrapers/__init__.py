from typing import Optional, TypedDict


class YMMSAttr(TypedDict):
    year: int
    make: str
    model: str
    style: str

    trim_slug: str

    mpg_city: float
    mpg_hwy: float
    fuel_type: str
    is_auto: bool
    drivetrain: str
    body: str
    engine: str


class Dealership(TypedDict):
    dealer_id: int
    dealer_name: str
    lat: float
    lon: float
    city: str
    state: str


class TruecarListing(TypedDict):
    vin: str
    timestamp: int
    dealer_id: int

    year: int
    make: str
    model: str
    style: str

    mileage: int
    price: float
    color_rgb: Optional[str]
    color_interior: Optional[str]
