import numba
import numpy as np
from scipy.interpolate import InterpolatedUnivariateSpline


class CostModel:
    @staticmethod
    def cost_75_by_make(make: str) -> int:
        make_dict = {
            "hyundai": 4000,
            "kia": 4000,
            "toyota": 4300,
            "nissan": 4600,
            "subaru": 4700,
            "scion": 4800,
            "mazda": 4900,
            "honda": 4900,
            "volkswagen": 5600,
            "acura": 5700,
            "lexus": 5800,
            "infiniti": 5800,
            "jeep": 6500,
            "mini": 6500,
            "gmc": 6600,
            "dodge": 6700,
            "mitsubishi": 7000,
            "chevrolet": 7100,
            "ford": 7900,
            "buick": 8100,
            "chrysler": 8400,
            "volvo": 8700,
            "audi": 8800,
            "lincoln": 10300,
            "saturn": 11000,
            "cadillac": 11000,
            "mercedes": 11000,
            "pontiac": 11300,
            "bmw": 13300,
        }
        default = np.median(list(make_dict.values()))
        return make_dict.get(make, default)

    @staticmethod
    def cost_curve() -> InterpolatedUnivariateSpline:
        try:
            interp = getattr(CostModel, "__cost_curve")
        except AttributeError:

            points = np.array(
                [
                    [0, 1000 / 25000],
                    [12500, 1400 / 25000],
                    [37500, 2200 / 25000],
                    [62500, 3000 / 25000],
                    [87500, 3900 / 25000],
                    [112500, 4100 / 25000],
                    [137500, 4400 / 25000],
                    [162500, 4800 / 25000],
                    [187500, 5000 / 25000],
                    # fake numbers below roughly extrapolating
                    [300000, 6000 / 25000],
                    [400000, 7000 / 25000],
                    [500000, 8000 / 25000],
                ]
            )
            base_interp = InterpolatedUnivariateSpline(
                points[:, 0], points[:, 1]
            )
            norm = base_interp.integral(0, 75000)

            points[:, 1] /= norm
            interp = InterpolatedUnivariateSpline(points[:, 0], points[:, 1])

            setattr(CostModel, "__cost_curve", interp)

        return interp

    @staticmethod
    def cost_from_to(make: str, fm: int, to: int) -> int:
        interp = CostModel.cost_curve()
        base = interp.integral(fm, to)
        return int(base * CostModel.cost_75_by_make(make))

    @staticmethod
    @numba.jit  # type: ignore
    def gas_cost(
        mpg_city: float,
        mpg_highway: float,
        city_ratio: float = 0.55,
        distance: int = 50000,
        gas_price: float = 3.0,
    ) -> float:
        return (
            gas_price
            * distance
            * (city_ratio / mpg_city + (1 - city_ratio) / mpg_highway)
        )
