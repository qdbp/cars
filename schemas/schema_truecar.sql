CREATE TABLE IF NOT EXISTS
    truecar_listings
(

    vin            TEXT PRIMARY KEY,
    timestamp      INTEGER NOT NULL,
    dealer_id      INTEGER NOT NULL,
    year           INTEGER NOT NULL,
    make           TEXT    NOT NULL,
    model          TEXT    NOT NULL,
    style          TEXT    NOT NULL,
    mileage        INTEGER NOT NULL,
    price          REAL    NOT NULL,
    color_rgb      TEXT,
    color_interior TEXT,

    FOREIGN KEY (dealer_id)
        REFERENCES truecar_dealerships (dealer_id),
    FOREIGN KEY (year, make, model, style)
        REFERENCES truecar_ymms_attrs (year, make, model, style)

) WITHOUT ROWID;

CREATE INDEX 'ix_attrs_trim' ON truecar_ymms_attrs(trim_slug);
CREATE INDEX 'ix_listings_ymms' ON truecar_listings(year, make, model, style);
CREATE INDEX 'ix_listings_mileage' ON truecar_listings(mileage);
CREATE INDEX 'ix_listings_price' ON truecar_listings(price);

CREATE TABLE IF NOT EXISTS
    truecar_ymms_attrs
(
    year       INTEGER NOT NULL,
    make       TEXT    NOT NULL,
    model      TEXT    NOT NULL,
    style      TEXT    NOT NULL,
    trim_slug  TEXT    NOT NULL,
    mpg_city   INTEGER NOT NULL,
    mpg_hwy    INTEGER NOT NULL,
    fuel_type  TEXT,
    is_auto    INTEGER,
    drivetrain TEXT,
    body       TEXT,
    engine     TEXT,
    PRIMARY KEY (year, make, model, style)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS
    truecar_dealerships
(
    dealer_id   INTEGER PRIMARY KEY,
    dealer_name TEXT NOT NULL,
    lat         REAL NOT NULL,
    lon         REAL NOT NULL,
    city        TEXT NOT NULL,
    state       TEXT NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS
    edmunds_car_perks
(
    id   INTEGER PRIMARY KEY,
    perk TEXT UNIQUE NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS
    car_perks_by_vin
(
    vin     TEXT PRIMARY KEY,
    perk_id INTEGER NOT NULL,
    FOREIGN KEY (perk_id) REFERENCES edmunds_car_perks
        ON DELETE CASCADE ON UPDATE RESTRICT
) WITHOUT ROWID;

