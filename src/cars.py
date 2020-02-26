from __future__ import annotations

from typing import List

import click
import matplotlib.pyplot as plt
import numpy as np
import numpy.random as npr
import pandas as pd
from keras.models import load_model
from pandas import DataFrame as DF
from sklearn.model_selection import train_test_split

from .costmodel import CostModel
from src.scrapers.truecar import get_cars
from .util import plotfile, weightsfile


def augment_cars(cars: DF, next_miles=50000):

    cars.dropna(
        inplace=True,
        subset=[
            "mpg_city",
            "mpg_highway",
            "price",
            "mileage",
            "e_volume",
            "color_l",
        ],
    )

    # NEXT_MILES = 50000

    # cars['total_cost'] = (
    #     CostModel.gas_cost(
    #         cars['mpg_city'],
    #         cars['mpg_highway']
    #     )
    #     + np.vectorize(CostModel.cost_from_to)(
    #         cars['model'],
    #         cars['mileage'],
    #         cars['mileage'] + NEXT_MILES
    #     )
    #     + cars['price']
    # )

    # cars['mileage'] = cars['mileage'] / 10000
    # cars['price'] = cars['price'] / 1000
    # cars['total_cost'] = cars['total_cost'] / 1000

    # cars['mm'] = cars['make'].astype('str') + cars['model']
    # cars['vymm'] = cars['year'].astype('str') + '__' +\
    #     cars['e_volume'].astype('str') + '__' + cars['mm']

    # vymm_cols = ['year', 'e_volume', 'make']
    # vymm_medians = cars.groupby(vymm_cols)['price'].median()
    # vymm_medians.name = 'med_price'

    # cars = cars.join(vymm_medians, on=vymm_cols, how='left')
    # cars['normed_price'] = cars['price'] / cars['med_price']
    # cars['normed_cost'] = cars['total_cost'] / cars['med_price']

    return cars


@click.group()
def main():
    pass


def getcols(df: DF, prefix: str) -> List[str]:
    return [c for c in df.columns if c.startswith(prefix)]


def plot_kdes(cars: DF) -> None:

    maxprice = cars.price.max()
    from sklearn.neighbors import KernelDensity

    for style in cars.body_style.unique():
        y = cars.price[cars.body_style == style]
        if len(y) < 10:
            continue
        model = KernelDensity(bandwidth=1000, kernel="linear").fit(
            y.values.reshape(-1, 1)
        )
        l = np.linspace(0, maxprice, num=1000)
        plt.plot(l, np.exp(model.score_samples(l.reshape(-1, 1))), label=style)

    plt.legend()
    plt.tight_layout()
    plt.gcf().set_size_inches(12, 8)
    plt.savefig(plotfile("price-hist"))


def r2_loss(y_true, y_pred):
    import keras.backend as K

    SSr = K.sum(K.square(y_true - y_pred))
    SSt = K.sum(K.square(y_true - K.mean(y_true)))
    return 1 - SSr / (SSt + K.epsilon())


def mk_car_model(*, w_l0=128, w_l1=128, w_l2=64):

    from keras.layers import Dense, Dropout
    from keras.models import Sequential

    m = Sequential()

    m.add(Dense(w_l0, activation="relu"))
    m.add(Dropout(0.5))
    m.add(Dense(w_l0, activation="relu"))
    m.add(Dropout(0.5))
    m.add(Dense(w_l1, activation="relu"))
    m.add(Dropout(0.5))
    m.add(Dense(w_l1, activation="relu"))
    m.add(Dropout(0.5))
    m.add(Dense(w_l2, activation="relu"))
    m.add(Dropout(0.5))
    m.add(Dense(w_l2, activation="relu"))
    m.add(Dropout(0.5))
    m.add(Dense(1, activation="linear"))

    # optim = SGD(lr=0.001, momentum=0.99)
    m.compile(loss="mse", optimizer="adam", metrics=[r2_loss])

    return m


def get_callbacks(name: str):
    import keras.callbacks as kcb

    return [
        kcb.ReduceLROnPlateau(
            verbose=1,
            monitor="val_loss",
            factor=0.2,
            patience=5,
            min_lr=0.00001,
        ),
        kcb.ModelCheckpoint(weightsfile(name), save_best_only=True),
        kcb.EarlyStopping(patience=10, verbose=1),
    ]


def keras_grid_search(model_func, param_grid, random_order=True):
    """
    Expands a parameter grid and yield model with parameters from that grid.
    """

    from itertools import product as iproduct

    paramspace = iproduct(*param_grid.values())
    denormed_grid = [
        {k: v for k, v in zip(param_grid.keys(), tup)} for tup in paramspace
    ]

    if random_order:
        npr.shuffle(denormed_grid)

    for params in denormed_grid:
        yield model_func(**params)


def get_training_data(cars):

    X = pd.get_dummies(
        cars,
        columns=[
            "body_style",
            "make",
            "drive_train",
            # 'model'
        ],
    )

    y = X["price"]
    y -= y.mean()
    y /= y.std()

    X = X[
        ["mileage", "year", "e_volume", "hybrid", "lat", "lng"]
        + getcols(X, "drive_train_")
        + getcols(X, "body_style_")
        + getcols(X, "make_")
        # + getcols(X, 'model_')
        + getcols(X, "color_")
    ].astype(np.float32)

    X.iloc[:, :6] -= X.iloc[:, :6].mean(axis=0)
    X.iloc[:, :6] /= X.iloc[:, :6].std(axis=0)

    return X.values, y.values


def train_car_nn(X: np.ndarray, y: np.ndarray) -> None:

    Xt, Xv, yt, yv = train_test_split(X, y)

    # param_grid = {
    #     'w_l0': [192, 256, 320],
    #     'w_l1': [96, 128, 160],
    #     'w_l2': [48, 64, 80],
    #     'w_l3': [24, 32, 40],
    # }
    # for model in keras_grid_search(mk_car_model, param_grid):

    model = mk_car_model()
    model.fit(
        Xt,
        yt,
        epochs=1000,
        batch_size=128,
        validation_data=(Xv, yv),
        callbacks=get_callbacks("cars_all_styles"),
    )


@main.command()
@click.option("--refresh", is_flag=True, default=False)
def train(refresh=False):

    cars = augment_cars(get_cars(refresh=refresh))
    plot_kdes(cars)

    X, y = get_training_data(cars)
    train_car_nn(X, y)


@main.command()
@click.argument("weights")
def evaluate(weights):

    cars = augment_cars(get_cars())
    X, y = get_training_data(cars)

    model = load_model(weights, custom_objects={"r2_loss": r2_loss})
    pred = model.predict(X).squeeze()

    plt.figure(0)
    plt.scatter(y, y - pred, marker="+", s=1, color="k")
    plt.title(f"{weights} - residuals")
    plt.savefig(plotfile("residuals"))

    plt.figure(1)
    plt.scatter(y, pred, marker="+", s=1, color="k")
    plt.title(f"{weights} - predictions")
    plt.savefig(plotfile("true-v-pred"))


@main.command()
def test():
    print(CostModel.cost_from_to("honda", 0, 75000))
    print(CostModel.cost_from_to("honda", 50000, 150000))
    print(CostModel.cost_from_to("honda", 150000, 250000))
    print(CostModel.cost_from_to("honda", 350000, 450000))
