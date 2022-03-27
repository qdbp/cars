from __future__ import annotations

import sqlite3 as sql
from dataclasses import asdict, dataclass, fields
from typing import ClassVar, Iterable, Optional

from bitstruct import pack as bitpack
from bitstruct import unpack as bitunpack
from py9lib.db_ import mk_column_spec

from cars.util import CAR_DB

KNOWN_DRIVETRAINS = ("4WD", "AWD", "FWD", "RWD")
KNOWN_BODIES = (
    "Cargo Van",
    "Chassis Cab Truck",
    "Convertible",
    "Coupe",
    "Hatchback",
    "Minivan",
    "Passenger Van",
    "Pickup Truck",
    "SUV",
    "Sedan",
    "Wagon",
)
KNOWN_FUEL_TYPES = ("gas", "diesel", "hybrid", "electric", "flex")
TRANSMISSIONS = ("auto", "manual")
TRANSMISSION_VALS = (1, 0)


def normalize_body(body: str) -> str:
    return {"Pickup": "Pickup Truck", "Station Wagon": "Wagon"}.get(body, body)


def normalize_fuel(fuel: str) -> str:
    if "flex" in fuel.lower():
        return "flex"
    if fuel == "mild hybrid":
        return "hybrid"
    return fuel


@dataclass
class VehicleHistory:
    FMT: ClassVar[str] = "u1u1u1u1u1u4u1u1"

    is_accident: bool
    is_framedamage: bool
    is_salvage: bool
    is_lemon: bool
    is_theft: bool

    n_owners: int
    is_fleet: bool
    is_rental: bool

    @property
    def as_int(self) -> int:
        bs = bitpack(self.FMT, *[getattr(self, f.name) for f in fields(self)])
        return int.from_bytes(bs, byteorder="big")

    @classmethod
    def from_int(cls, pack: int) -> VehicleHistory:
        return cls(
            *[
                eval(f.type)(it)  # type: ignore
                for f, it in zip(
                    fields(cls),
                    bitunpack(
                        cls.FMT, pack.to_bytes(byteorder="big", length=2)
                    ),
                )
            ]
        )


@dataclass
class YMMSAttr:
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

    def __post_init__(self) -> None:
        self.fuel_type = normalize_fuel(self.fuel_type)
        assert self.fuel_type in KNOWN_FUEL_TYPES

        assert self.drivetrain in KNOWN_DRIVETRAINS

        self.body = normalize_body(self.body)
        assert self.body in KNOWN_BODIES


@dataclass
class Dealership:
    address: str
    zip: str
    dealer_name: str
    city: str
    state: str
    lat: float
    lon: float
    phone: str | None
    website: str | None


@dataclass
class Listing:
    source: str
    vin: str
    first_seen: int
    last_seen: int
    dealer_address: str
    dealer_zip: str
    year: int
    make: str
    model: str
    style: str
    mileage: int
    price: float
    color_rgb_int: Optional[str]
    color_rgb_ext: Optional[str]
    history_flags: VehicleHistory


@dataclass
class ListingWithContext:
    dealership: Dealership
    ymms_attr: YMMSAttr
    listing: Listing


def insert_listings(details: Iterable[ListingWithContext]) -> None:

    details = list(details)
    if not details:
        return

    dealers = list(
        {
            (dt.dealership.address, dt.dealership.zip): asdict(dt.dealership)
            for dt in details
        }.values()
    )
    ymmss = list(
        {
            (
                dt.ymms_attr.year,
                dt.ymms_attr.make,
                dt.ymms_attr.model,
                dt.ymms_attr.style,
            ): asdict(dt.ymms_attr)
            for dt in details
        }.values()
    )

    with sql.connect(CAR_DB) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executemany(
            # language=sql
            f"""
            INSERT OR IGNORE INTO dealerships
            {mk_column_spec(dealers[0])}
            """,
            dealers,
        )
        conn.executemany(
            # language=sql
            f"""
            INSERT OR IGNORE INTO ymms_attrs
            {mk_column_spec(ymmss[0])}
            """,
            ymmss,
        )
        conn.executemany(
            # language=sql
            f"""
            INSERT OR REPLACE INTO listings
            {mk_column_spec(asdict(details[0].listing))}
            """,
            (
                asdict(d.listing)
                | {"history_flags": d.listing.history_flags.as_int}
                for d in details
            ),
        )
