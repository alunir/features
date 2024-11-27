-- +goose Up
-- Already defined in sql/schema/004_ffd.sql
-- CREATE TYPE RESOLUTION AS ENUM ('1Min', '5Min', '15Min', '30Min', '1H', '4H', '1D');

CREATE TABLE IF NOT EXISTS emd (
    Instrument BIGINT NOT NULL,
    Resolution RESOLUTION NOT NULL,
    Fdim NUMERIC NOT NULL,
    Epoch TIMESTAMP NOT NULL,
    If_0 NUMERIC,
    If_1 NUMERIC,
    If_2 NUMERIC,
    If_3 NUMERIC,
    If_4 NUMERIC,
    If_5 NUMERIC,
    If_6 NUMERIC,
    If_7 NUMERIC,
    If_8 NUMERIC,
    If_9 NUMERIC,
    If_10 NUMERIC,
    If_11 NUMERIC,
    If_12 NUMERIC,
    If_13 NUMERIC,
    If_14 NUMERIC,
    If_15 NUMERIC,
    Ia_0 NUMERIC,
    Ia_1 NUMERIC,
    Ia_2 NUMERIC,
    Ia_3 NUMERIC,
    Ia_4 NUMERIC,
    Ia_5 NUMERIC,
    Ia_6 NUMERIC,
    Ia_7 NUMERIC,
    Ia_8 NUMERIC,
    Ia_9 NUMERIC,
    Ia_10 NUMERIC,
    Ia_11 NUMERIC,
    Ia_12 NUMERIC,
    Ia_13 NUMERIC,
    Ia_14 NUMERIC,
    Ia_15 NUMERIC,
    Ip_0 NUMERIC,
    Ip_1 NUMERIC,
    Ip_2 NUMERIC,
    Ip_3 NUMERIC,
    Ip_4 NUMERIC,
    Ip_5 NUMERIC,
    Ip_6 NUMERIC,
    Ip_7 NUMERIC,
    Ip_8 NUMERIC,
    Ip_9 NUMERIC,
    Ip_10 NUMERIC,
    Ip_11 NUMERIC,
    Ip_12 NUMERIC,
    Ip_13 NUMERIC,
    Ip_14 NUMERIC,
    Ip_15 NUMERIC,
    PRIMARY KEY (Instrument, Resolution, Fdim, Epoch),
    FOREIGN KEY (Instrument) REFERENCES instrument(id)
);

CREATE INDEX IF NOT EXISTS idx_instrument_resolution_fdim_epoch_desc ON emd (Instrument, Resolution, Fdim, Epoch DESC);


-- +goose Down
DROP TABLE IF EXISTS emd;
