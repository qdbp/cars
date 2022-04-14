from __future__ import annotations

import json
import sqlite3 as sql
from abc import abstractmethod
from dataclasses import asdict, dataclass, fields
from functools import wraps
from pathlib import Path
from sqlite3 import Connection
from typing import (
    Any,
    Callable,
    ClassVar,
    Concatenate,
    Generator,
    Iterable,
    ParamSpec,
    Type,
    TypeVar,
)

from bitstruct import pack as bitpack
from bitstruct import unpack as bitunpack
from py9lib.db_ import mk_column_spec
from py9lib.io_ import retry
from webcolors import CSS2, CSS3, CSS21, HTML4, name_to_hex
from xdg import xdg_cache_home

from cars.util import CAR_DB

P = ParamSpec("P")
T = TypeVar("T")

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


def normalize_address(addr: str) -> str:
    addr = addr.title().rstrip(".")
    words = addr.split(" ")
    for ix in [-1, -2]:
        try:
            words[ix] = {
                "Ave": "Avenue",
                "Blvd": "Boulevard",
                "Dr": "Drive",
                "Hwy": "Highway",
                "Ln": "Lane",
                "Rd": "Road",
                "St": "Street",
                "Tpke": "Turnpike",
            }.get(words[ix], words[ix])
        except IndexError:
            continue
    return " ".join(words)


def normalize_body(body: str) -> str:
    body = body.title() if body.lower() != "suv" else "SUV"
    return {
        "Convert": "Convertible",  # autotrader
        "Hatch": "Hatchback",  # autotrader
        "Pickup": "Pickup Truck",  # edmunds
        "Station Wagon": "Wagon",  # edmunds
        "Sport Utility": "SUV",  # autotrader
        "Van": "Passenger Van",  # autotrader
        "Truck": "Pickup Truck",
    }.get(body, body)


def normalize_fuel(fuel: str) -> str:
    fuel = fuel.lower()
    if fuel == "gasoline":
        return "gas"
    if "flex" in fuel:
        return "flex"
    if "hybrid" in fuel:
        return "hybrid"
    return fuel


def normalize_drivetrain(drivetrain: str) -> str:
    return {
        "all wheel drive": "AWD",
        "front wheel drive": "FWD",
        "rear wheel drive": "RWD",
        "four wheel drive": "4WD",
        "2 wheel drive - front": "FWD",
        "4 wheel drive - rear wheel default": "4WD",
        "4 wheel drive - front wheel default": "4WD",
        "4 wheel drive": "4WD",
        "2 wheel drive - rear": "RWD",
    }.get(drivetrain.lower(), drivetrain)


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
    source: str

    def __post_init__(self) -> None:
        self.fuel_type = normalize_fuel(self.fuel_type)
        assert self.fuel_type in KNOWN_FUEL_TYPES

        self.drivetrain = normalize_drivetrain(self.drivetrain)
        assert self.drivetrain in KNOWN_DRIVETRAINS

        self.body = normalize_body(self.body)
        assert self.body in KNOWN_BODIES

    def insert(self, conn: Connection) -> int:
        yd = asdict(self)
        rows = conn.execute(
            """ SELECT id, source FROM ymms_attrs
                WHERE year = :year
                  AND make = :make
                  AND model = :model
                  AND style = :style
            """,
            yd,
        ).fetchall()

        if row := rows[0]:
            if row[1] is None:
                conn.execute(
                    # language=sql
                    """ UPDATE ymms_attrs
                        SET source = :source
                        WHERE id = :id
                    """,
                    dict(id=row[0], source=self.source),
                )
            return row[0]
        else:
            # language=sql
            cur = conn.execute(
                f"INSERT OR REPLACE INTO ymms_attrs {mk_column_spec(yd)}", yd
            )
            return cur.lastrowid


@dataclass
class Dealership:
    address: str
    zip: str
    name: str
    city: str
    state: str
    lat: float
    lon: float
    phone: str | None
    website: str | None
    ll_qual: int = 0

    def insert(self, conn: Connection) -> int:
        dd = asdict(self)
        row = conn.execute(
            """ SELECT id, ll_qual FROM dealerships
                WHERE address = :address
                  AND zip = :zip
                  AND name = :name""",
            dd,
        ).fetchall()
        if row:
            conn.execute(
                """ UPDATE dealerships
                    SET
                        website = coalesce(:website, website),
                        phone = coalesce(:phone, phone),
                    -- if we have improved our geocoding quality, we update the latlon
                        lat = iif(ll_qual < :ll_qual, :lat, lat),
                        lon = iif(ll_qual < :ll_qual, :lon, lon),
                        ll_qual = iif(ll_qual < :ll_qual, :ll_qual, ll_qual)
                    WHERE
                        address = :address AND zip = :zip AND name = :name""",
                dd,
            )
            return row[0][0]
        else:
            cur = conn.execute(
                # language=sql
                f"INSERT INTO dealerships {mk_column_spec(dd)}",
                dd,
            )
            return cur.lastrowid


@dataclass
class Listing:
    source: str
    vin: str
    first_seen: int
    last_seen: int
    mileage: int
    price: float
    color_rgb_int: str | None
    color_rgb_ext: str | None
    history_flags: VehicleHistory | None


@dataclass
class ListingWithContext:
    dealership: Dealership
    ymms_attr: YMMSAttr
    listing: Listing

    def insert(self, conn: Connection):
        ld = asdict(self.listing)
        ld["dealer_id"] = self.dealership.insert(conn)
        ld["ymms_id"] = self.ymms_attr.insert(conn)
        ld["history_flags"] = (
            ld["history_flags"] and self.listing.history_flags.as_int
        )

        conn.execute(
            # language=sql
            f"""
            INSERT OR REPLACE INTO listings
            {mk_column_spec(ld)}""",
            ld,
        )


def insert_listings(details: Iterable[ListingWithContext | None]) -> None:
    details = [it for it in details if it is not None]
    with sql.connect(CAR_DB) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        for lwx in details:
            lwx.insert(conn)


SC = TypeVar("SC", bound="ScraperState", covariant=True)


def inject_state(
    state_cls: Type[SC], /, *retry_args: Any, **retry_kwargs: Any
) -> Callable[
    Callable[Concatenate[SC, P], Generator[SC, None, None]], Callable[P, None]
]:
    def _wrapper(f: Callable[Concatenate[SC, P], Generator[SC, None, None]]):
        @wraps(f)
        @retry(*retry_args, **retry_kwargs)
        def _wrapped(*args: P.args, **kwargs: P.kwargs):
            state = state_cls.load()
            for out_state in f(state, *args, **kwargs):
                out_state.dump()

        return _wrapped

    return _wrapper


@dataclass
class ScraperState:
    name: ClassVar[str]

    @classmethod
    def state_path(cls) -> Path:
        return xdg_cache_home().joinpath(f"cars.{cls.name}.state")

    def dump(self) -> None:
        with self.state_path().open("w") as f:
            json.dump(asdict(self), f, indent=4)

    @classmethod
    def load(cls: Type[SC]) -> SC:
        try:
            with cls.state_path().open() as f:
                # noinspection PyArgumentList
                return cls(**json.load(f))
        except IOError:
            return cls.new()

    @classmethod
    @abstractmethod
    def new(cls: Type[SC]) -> SC:
        ...


def tryhard_name_to_hex(name: str) -> str | None:
    for spec in [CSS3, CSS21, CSS2, HTML4]:
        try:
            return name_to_hex(name.lower(), spec).lstrip("#").upper()  # type: ignore
        except ValueError:
            continue
    return None
