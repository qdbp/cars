import numpy as np
import pandas as pd
import plotly.io
import scipy.spatial as sss
from scipy.spatial.qhull import QhullError

plotly.io.renderers.default = "chromium"


def find_pareto_points(
    points: np.ndarray, n_peel: int = 3, eliminate_dominated=True
):
    """
    Uses a multi-peel pareto front approximation algorithm. The pareto points
    are approximated by the vertices of [n_peel] successive steps of
    convex hull onion peeling, restricted to vertices pointing toward the ideal
    point.

    By convention, the optimality point is ∞ * (-1, -1, ..., -1); i.e. the
    individual objectives are minimization problems.

    Args:
        points: (n, k) array of n points of k dimensions.
            The convention is that the
        n_peel: number of onion peeling passes to perform
        eliminate_dominated: if true, dominated points will be deterministically
            eliminated from the heuristic set returned by the convex hull.
            Worst case running time is O(#(points in c. hull set)^2)

    Returns:
        indices into [points] of the approximate pareto set

    """

    n, k = points.shape

    # specialcase some stupid inputs
    if n == 0:
        return np.array([], dtype=np.uint64)

    # specialcased for normalization (see below) to work
    elif n == 1:
        return np.array([0], dtype=np.uint64)

    elif k == 0:
        raise ValueError("No pareto-optimal set of points with no features")

    # we normalize our point sets
    points = (points - points.mean(axis=0)) / points.std(axis=0)

    # for finding qhull faces pointing toward optimality
    test_vector = np.zeros(k + 1)
    test_vector[:-1] = -1

    # vertices in the candidate pareto set
    vertex_mask = np.zeros(len(points), dtype=bool)

    for layer in range(n_peel):

        # construct hull of all vertices NOT already in the set
        try:
            qhull = sss.ConvexHull(points[~vertex_mask])
        except (QhullError, ValueError):
            break

        pareto_side = np.where((qhull.equations @ test_vector) > 0)
        pareto_vertices = np.unique(qhull.simplices[pareto_side].ravel())
        vertex_mask[pareto_vertices] = True

    # list of indices into points in the candidate set of dominator points
    pareto_vertices = np.where(vertex_mask)[0]

    if eliminate_dominated:
        # estimate goodness as the total score
        # NB. this is where it helps to be normalized
        goodness_order = np.argsort(points[pareto_vertices].sum(axis=1))

        # from the most promising dominator
        for dominator_ix in goodness_order:
            # and the most promising dominated
            for dominated_ix in goodness_order[::-1]:
                if np.all(points[dominator_ix] > points[dominated_ix]):
                    # mark that vertex as dominated
                    pareto_vertices[dominated_ix] = -1
                    break

    return pareto_vertices[pareto_vertices >= 0]


def calculate_listing_pareto_front(
    listings: pd.DataFrame, max_miles: int = 150, **pareto_kwargs,
):

    # otherwise the convex hull is degenerate -> qhull explodes
    if len(listings) <= 4:
        return listings

    # invert "good" attributes to conform to minimization problem
    listings["inv_mpg"] = 1 / (listings["mpg"] + 1)
    listings["inv_year"] = 1 / listings["year"]

    points = (
        listings[["mileage", "price", "inv_year", "inv_mpg"]].dropna().values
    )

    pareto_vertices = find_pareto_points(points, **pareto_kwargs)

    listings.drop(["inv_mpg", "inv_year"], axis=1, inplace=True)

    return listings.iloc[pareto_vertices]
