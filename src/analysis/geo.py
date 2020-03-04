import numpy as np
from numba import jit
import zipcodes as zp

R_MEAN_EARTH_MI = 3_958.7613

LATLONG_BY_ZIP = {
    z["zip_code"]: (float(z["lat"]), float(z["long"]))
    for z in zp.list_all()
    if z["zip_code_type"] == "STANDARD"
}


@jit
def great_circle_miles(
    lon0: float, lat0: float, lon1: float, lat1: float
) -> float:

    lon0 = lon0 * np.pi / 180
    lon1 = lon1 * np.pi / 180
    lat0 = lat0 * np.pi / 180
    lat1 = lat1 * np.pi / 180

    return (
        R_MEAN_EARTH_MI
        * 2
        * np.arcsin(
            np.sqrt(
                np.sin(np.abs(lat1 - lat0) / 2) ** 2
                + (
                    np.cos(lat1)
                    * np.cos(lat0)
                    * np.sin(np.abs(lon1 - lon0) / 2) ** 2
                )
            )
        )
    )
