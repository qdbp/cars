from pathlib import Path

DATA_DIR = Path(__file__).parent.parent.parent.joinpath('data').absolute().__str__()


def plotfile(s: str) -> str:
    return f"./plots/{s}.png"


def tsvfile(s: str) -> str:
    return f"{DATA_DIR}/{s}.tsv"


def csvfile(s: str) -> str:
    return f"{DATA_DIR}/{s}.csv"


def jsonfile(s: str) -> str:
    return f"{DATA_DIR}/{s}.json"


def weightsfile(s: str) -> str:
    return f"./weights/{s}.hdf5"


def sqlfile(s: str) -> str:
    return f"{DATA_DIR}/{s}.db"


CAR_DB = sqlfile("truecar")


def try_convert(v):

    try:
        return int(v)
    except ValueError:
        pass

    try:
        return float(v)
    except ValueError:
        pass

    return v


def get_sql_type(v: str) -> str:
    if isinstance(v, int):
        typ = "INTEGER"
    elif isinstance(v, float):
        typ = "REAL"
    else:
        typ = "TEXT"

    return typ
