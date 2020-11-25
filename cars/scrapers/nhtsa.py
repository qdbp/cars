from __future__ import annotations

import json
import sqlite3 as sql
from asyncio import FIRST_COMPLETED, Future, Semaphore, ensure_future
from asyncio import run as aiorun
from asyncio import wait
from hashlib import sha1
from typing import Any, Dict, Iterable, List, Set, Union
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from aiohttp import ClientSession
from tqdm import tqdm

from cars.util import CAR_DB, get_sql_type, try_convert_to_num
from cars.vin import TEST_VIN, ShortVin, Vin

NHTSA_BATCH_DECODE_URL = (
    "https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/"
)
NHTSA_VIN_DECODE_URL = (
    "https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVinValues/{}?format={}"
)

# these have non-null ratios of over 10% in the dataset aggregated by models
# and are not otherwise redundant or useless
NHTSA_KEEP_COLS = [
    "make",
    "model",
    "year",
    "ABS",
    "ESC",
    "GVWR",
    "TPMS",
    "air_bag_loc_curtain",
    "air_bag_loc_knee",
    "air_bag_loc_side",
    "blind_spot_mon",
    "body_cab_type",
    "body_class",
    "daytime_running_light",
    "displacement_L",
    "doors",
    "drive_type",
    "dynamic_brake_support",
    "engine_KW",
    "engine_configuration",
    "engine_cylinders",
    "engine_manufacturer",
    "engine_model",
    "error_code",
    "forward_collision_warning",
    "fuel_injection_type",
    "fuel_type_primary",
    "fuel_type_secondary",
    "keyless_ignition",
    "lane_departure_warning",
    "manufacturer",
    "other_engine_info",
    "plant_city",
    "plant_company_name",
    "plant_country",
    "plant_state",
    "rear_visibility_system",
    "seat_belts_all",
    "seat_rows",
    "seats",
    "semiautomatic_headlamp_beam_switching",
    "series",
    "steering_location",
    "top_speed_MPH",
    "traction_control",
    "transmission_speeds",
    "transmission_style",
    "trim",
    "turbo",
    "valve_train_design",
    "vehicle_type",
    "wheel_base_short",
    "wheel_size_front",
    "wheel_size_rear",
    "wheels",
]

# renaming map, after snake_case conversion
NHTSA_RENAME_COLS = {
    "model_year": "year",
}


def to_snake_case(s: str) -> str:

    out = []
    for prev, c, nxt in zip("x" + s, s, s[1:] + "X"):
        if c.isupper():
            if nxt.isalnum() and nxt.islower():
                out.append("_" + c.lower())
            elif prev.islower():
                out.append("_" + c)
            else:
                out.append(c)
        else:
            out.append(c)

    return "".join(out).strip("_").strip()


def make_nhtsa_table() -> None:

    reference_vin = TEST_VIN
    result = download_vins_batch([reference_vin])[0]

    need_cols = ["make", "model", "year"]

    cmd_head = [
        """
        CREATE TABLE IF NOT EXISTS nhtsa_attributes_2 (
        nhtsa_id TEXT PRIMARY KEY
        """
    ]
    cmd_foot = [") WITHOUT ROWID;"]
    cmd_rows = []

    with sql.connect(CAR_DB) as conn:
        for col in NHTSA_KEEP_COLS:
            v = result[col]
            typ = get_sql_type(v)
            not_null = "NOT NULL" if col in need_cols else ""
            cmd_rows.append(f", {col} {typ} {not_null}")

        cmd = "".join(cmd_head + cmd_rows + cmd_foot)
        conn.execute(cmd)
        with open("schema_nhtsa.sql", "w") as f:
            f.write(cmd)


def download_vins_batch(
    truncated_vins: Iterable[Union[Vin, ShortVin]]
) -> List[Dict[str, Any]]:

    vin_str = ";".join(
        [tv + ("*" if len(tv) < 17 else "") for tv in truncated_vins]
    )
    req = Request(
        NHTSA_BATCH_DECODE_URL,
        data=urlencode({"DATA": vin_str, "format": "json"}).encode("ascii"),
        method="POST",
    )

    results = json.loads(urlopen(req).read())["Results"]
    results = [clean_nhtsa_dict(res) for res in results]
    return results


def clean_nhtsa_dict(result: Dict[str, str]) -> Dict[str, Any]:

    out = {
        new_k: try_convert_to_num(v)
        for k, v in sorted(result.items())
        if (new_k := NHTSA_RENAME_COLS.get((s_k := to_snake_case(k)), s_k))
        in NHTSA_KEEP_COLS
    }

    out["nhtsa_id"] = (
        "nh_"
        + sha1("".join(map(str, out.values())).encode("ascii"))
        .hexdigest()[:16]
        .upper()
    )

    return out


async def nhtsa_scraper() -> None:

    semaphore_limit = 256
    futures_target = 4 * semaphore_limit
    flush_every = 100

    with sql.connect(CAR_DB) as conn:
        need_vins = {
            row[0]
            for row in conn.execute("SELECT vin FROM truecar_attrs").fetchall()
        }

        have_vins = {
            row[0]
            for row in conn.execute(
                """
                SELECT vin FROM nhtsa_attributes_2
                    INNER JOIN map_vin_nhtsa mvn
                    ON nhtsa_attributes_2.nhtsa_id = mvn.nhtsa_id
                """
            ).fetchall()
        }

    vins_to_dl = sorted(need_vins - have_vins)
    progress = tqdm(total=len(vins_to_dl), unit="vins", smoothing=0)

    semaphore = Semaphore(semaphore_limit)
    session = ClientSession()

    results = []

    async def get_vin(vin: Vin, sess: ClientSession) -> None:
        url = NHTSA_VIN_DECODE_URL.format(vin, "json")
        async with semaphore:
            async with sess.get(url) as resp:
                raw = await resp.text()
        result = clean_nhtsa_dict(json.loads(raw)["Results"][0])
        results.append((vin, result))

    cur_vin_ix = futures_target
    futures: Set[Future[None]] = {
        ensure_future(get_vin(vin, session))
        for vin in vins_to_dl[:futures_target]
    }

    while len(futures) > 0:
        done, futures = await wait(futures, return_when=FIRST_COMPLETED)
        progress.update(len(done))

        while len(futures) < futures_target and cur_vin_ix < len(vins_to_dl):
            futures.add(ensure_future(get_vin(vins_to_dl[cur_vin_ix], session)))
            cur_vin_ix += 1

        if len(results) < flush_every or len(futures) == 0:
            continue

        cmd_nhtsa = (
            "INSERT OR REPLACE INTO nhtsa_attributes_2 ("
            + ",".join(k for k in results[0][1].keys())
            + ") VALUES ("
            + ",".join(":" + k for k in results[0][1].keys())
            + ")"
        )
        with sql.connect(CAR_DB) as conn:
            conn.executemany(cmd_nhtsa, [res[1] for res in results])
            conn.executemany(
                """
                INSERT OR REPLACE INTO map_vin_nhtsa
                VALUES (?, ?)
                """,
                [(res[0], res[1]["nhtsa_id"]) for res in results],
            )

        results.clear()


def scrape_nhtsa() -> None:
    aiorun(nhtsa_scraper())
