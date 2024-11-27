-- +goose Up
CREATE TYPE RESOLUTION AS ENUM ('1Min', '5Min', '15Min', '30Min', '1H', '4H', '1D');

CREATE TABLE IF NOT EXISTS ffd (
    Instrument BIGINT NOT NULL,
    Resolution RESOLUTION NOT NULL,
    Fdim NUMERIC NOT NULL,
    Epoch TIMESTAMP NOT NULL,
    Open NUMERIC,
    High NUMERIC,
    Low NUMERIC,
    Close NUMERIC,
    Volume NUMERIC,
    Trades NUMERIC,
    PRIMARY KEY (Instrument, Resolution, Fdim, Epoch),
    FOREIGN KEY (Instrument) REFERENCES instrument(id)
);
CREATE INDEX IF NOT EXISTS idx_instrument_resolution_fdim_epoch_desc ON ffd (Instrument, Resolution, Fdim, Epoch DESC);


-- +goose Down
DROP TABLE IF EXISTS feature_ffd;
