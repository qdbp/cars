CREATE TABLE IF NOT EXISTS
    listings
(
    source         TEXT    NOT NULL,
    vin            TEXT    NOT NULL,
    first_seen     INTEGER NOT NULL,
    last_seen      INTEGER NOT NULL,
    dealer_address TEXT    NOT NULL,
    dealer_zip     TEXT    NOT NULL,
    year           INTEGER NOT NULL,
    make           TEXT    NOT NULL,
    model          TEXT    NOT NULL,
    style          TEXT    NOT NULL,
    mileage        INTEGER NOT NULL,
    price          REAL    NOT NULL,
    color_rgb_ext  TEXT,
    color_rgb_int  TEXT,
    history_flags  INTEGER NOT NULL,

    PRIMARY KEY (source, vin),
    FOREIGN KEY (dealer_address, dealer_zip)
        REFERENCES dealerships (address, zip),
    FOREIGN KEY (year, make, model, style)
        REFERENCES ymms_attrs (year, make, model, style)
) WITHOUT ROWID;

CREATE INDEX 'ix_listings_ymms' ON listings (year, make, model, style);
CREATE INDEX 'ix_listings_mileage' ON listings (mileage);
CREATE INDEX 'ix_listings_price' ON listings (price);


CREATE TABLE IF NOT EXISTS
    ymms_attrs
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
    PRIMARY KEY (year, make, model, style)
) WITHOUT ROWID;

CREATE INDEX 'ix_attrs_trim' ON ymms_attrs (trim_slug);

CREATE TABLE IF NOT EXISTS
    dealerships
(
    address     TEXT NOT NULL,
    zip         TEXT NOT NULL,
    dealer_name TEXT NOT NULL,
    city        TEXT,
    state       TEXT,
    lat         REAL,
    lon         REAL,
    phone       TEXT,
    website     TEXT,
    PRIMARY KEY (address, zip)
) WITHOUT ROWID;


CREATE TABLE IF NOT EXISTS
    autotrader_listings
(
    listing_id INTEGER PRIMARY KEY,
    first_seen INTEGER
) WITHOUT ROWID;
