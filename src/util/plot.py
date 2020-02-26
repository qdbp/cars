from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.gridspec import GridSpec
from pandas import DataFrame

import matplotlib.pyplot as plt
from functools import reduce
from operator import iand
import numpy as np


def hexbin_pairplot(
    df: DataFrame, bins: int = 100, hexbins: int = 25, limit_quantile=0.975
) -> Figure:

    plt.set_cmap('magma')

    fig = plt.figure()
    ncols = len(df.columns)

    gs = GridSpec(ncols, ncols, figure=fig, wspace=0.025, hspace=0.025)

    lbs = df.quantile(1 - limit_quantile, axis=0)
    ubs = df.quantile(limit_quantile, axis=0)

    df = df[
        reduce(
            iand,
            [
                # (df.iloc[:, ix] >= lbs[ix]) &
                (df.iloc[:, ix] <= ubs[ix])
                for ix in range(ncols)
            ],
        )
    ]

    print(df.shape)

    max_bins = [len(df.iloc[:, col].value_counts()) for col in range(ncols)]

    for diag in range(ncols):
        sp = fig.add_subplot(gs[diag, diag])
        sp.hist(
            df.iloc[:, diag].values, bins=min(bins, max_bins[diag]),
        )
        sp.set_xlim(lbs.iloc[diag], ubs.iloc[diag])
        if diag == 0:
            sp.set_ylabel(df.columns[0])
        elif diag == ncols - 1:
            sp.set_xlabel(df.columns[-1])

    for row in range(ncols):
        for col in range(ncols):
            if row == col:
                continue
            sp: Axes = fig.add_subplot(gs[row, col])
            sp.hexbin(
                df.iloc[:, col],
                df.iloc[:, row],
                gridsize=min([hexbins, max_bins[col], max_bins[row]]),
                bins=np.linspace(3, 1000, num=100) ** 2
            )
            sp.set_xlim(df.iloc[:, col].min(), ubs.iloc[col])
            sp.set_ylim(df.iloc[:, row].min(), ubs.iloc[row])

            if row == ncols - 1:
                sp.set_xlabel(df.columns[col])
            else:
                sp.set_xticklabels([])

            if col == 0:
                sp.set_ylabel(df.columns[row])
            else:
                sp.set_yticklabels([])

    fig.tight_layout()
    fig.set_size_inches((12, 8))
    fig.set_dpi(200)
    return fig
