from __future__ import annotations

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
