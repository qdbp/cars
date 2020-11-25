import sqlite3 as sql

import pandas as pd

from ..util import CAR_DB, DATA_DIR, get_sql_type

DROP_COLS = ["id"]


def make_fueleconomy_table() -> None:

    with open(DATA_DIR + "/fueleconomy.csv", "r") as f:

        lines = f.readlines()
        header = lines[0].strip().split(",")
        row = lines[1].strip().split(",")

        for col in DROP_COLS:
            del_ix = header.index(col)
            del header[del_ix]
            del row[del_ix]

    with sql.connect(CAR_DB) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS fueleconomy "
            + "(vin TEXT PRIMARY KEY, "
            + ",".join([f"{k} {get_sql_type(v)}" for k, v in zip(header, row)])
            + ",FOREIGN KEY (vin) REFERENCES truecar_attributes (vin)"
            + ") WITHOUT ROWID;"
        )


pd.set_option("display.max_columns", None)
pd.set_option("display.width", 1000)


def get_fueleco_for_vin(nhtsa_row: pd.Series, fueleco: pd.DataFrame) -> None:

    candidate_vins = fueleco[
        (fueleco["year"] == nhtsa_row["year"])
        & (fueleco["make"].str.lower() == nhtsa_row["make"].lower())
    ]

    print(f"{nhtsa_row =}")
    print(f"{len(candidate_vins) =}")
    print(candidate_vins["model"])
    # print(f"{candidate_vins = }")

    input()
    import code

    code.interact(local=locals())


def populate_fueleconomy_table() -> None:

    fueleco = pd.read_csv(DATA_DIR + "/fueleconomy.csv").sort_values(
        ["year"], ascending=False
    )
    fueleco.drop(DROP_COLS, axis=1)

    with sql.connect(CAR_DB) as conn:
        nhtsa_table = pd.read_sql_query(
            """
            SELECT
                nhtsa_id, make, model, year,
                drive_type, engine_cylinders, displacement_L
            FROM nhtsa_attributes_2
            """,
            conn,
            index_col="nhtsa_id",
        )

    print(nhtsa_table.shape)

    for _, row in nhtsa_table.iterrows():
        get_fueleco_for_vin(row, fueleco)
