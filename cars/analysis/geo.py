from typing import Dict, Tuple

import numpy as np
import zipcodes as zp
from numba import jit

R_MEAN_EARTH_MI = 3_958.7613

LATLONG_BY_ZIP: Dict[str, Tuple[float, float]] = {
    z["zip_code"]: (float(z["lat"]), float(z["long"]))
    for z in zp.list_all()
    if z["zip_code_type"] == "STANDARD"
}


@jit(nopython=True)  # type: ignore
def great_circle_miles(p0: np.ndarray, lon1: float, lat1: float) -> np.ndarray:
    """
    Vectorized great-circle distance calculation.

    Args:
        p0: array, shape [n, 2]: lon/lat of first point
        lon1: lon of second point, scalar
        lat1: lat of second point, scalar

    Returns:
        great distances, same shape as first point array
    """

    lon0 = p0[:, 0] * np.pi / 180
    lon1 = lon1 * np.pi / 180
    lat0 = p0[:, 1] * np.pi / 180
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
