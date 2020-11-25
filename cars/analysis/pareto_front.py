from dataclasses import dataclass

import numpy as np
import scipy.spatial as sss
from pandas import DataFrame
from scipy.spatial.qhull import QhullError


@dataclass(frozen=True)
class ParetoFinder:
    # noinspection PyUnresolvedReferences
    """
    Uses a multi-peel pareto front approximation algorithm. The pareto points
    are approximated by the vertices of [n_peel] successive steps of convex
    hull onion peeling, restricted to vertices pointing toward the ideal point.

    By convention, the optimality point is ∞ * (-1, -1, ..., -1); i.e. the
    individual objectives are minimization problems.

    Args:
        n_peel: number of onion peeling passes to perform. If 0, then no
            qhull peeling is performed and all points are included in the
            elimіnate_dominated pass. If eliminate_dominated is False,
            n_peel = 0 makes this function a noop.
        eliminate_dominated: if true, dominated points will be deterministically
            eliminated from the heuristic set returned by the convex hull.
            Worst case running time is O(#(points in c. hull set)^2)
    """

    n_peel: int = 3
    eliminate_dominated: bool = True

    def _find_pareto_points(self, points: np.ndarray) -> np.ndarray:
        """
        Runs the finder on the given points.

        Args:
            points: (n, k) array of n points of k dimensions.
                The convention is that the

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
        if self.n_peel == 0:
            vertex_mask[:] = True
        else:
            for layer in range(self.n_peel):

                # construct hull of all vertices NOT already in the set
                try:
                    qhull = sss.ConvexHull(points[~vertex_mask])
                except (QhullError, ValueError):
                    break

                # noinspection PyUnresolvedReferences
                pareto_side = np.where((qhull.equations @ test_vector) > 0)
                # noinspection PyUnresolvedReferences
                pareto_vertices = np.unique(
                    qhull.simplices[pareto_side].ravel()
                )
                vertex_mask[pareto_vertices] = True

        # list of indices into points in the candidate set of dominator points
        pareto_vertices = np.where(vertex_mask)[0]

        if self.eliminate_dominated:
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

    def calculate_listing_pareto_front(self, listings: DataFrame) -> DataFrame:

        # otherwise the convex hull is degenerate -> qhull explodes
        if len(listings) <= 4:
            return listings

        # invert "good" attributes to conform to minimization problem
        listings["inv_mpg"] = 1 / (listings["mpg"] + 1)
        listings["inv_year"] = 1 / listings["year"]

        points = (
            listings[["mileage", "price", "inv_year", "inv_mpg"]]
            .dropna()
            .values
        )

        pareto_vertices = self._find_pareto_points(points)

        listings.drop(["inv_mpg", "inv_year"], axis=1, inplace=True)

        return listings.iloc[pareto_vertices]
