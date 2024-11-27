-- +goose Up
CREATE TABLE IF NOT EXISTS ohlcvt (
    Instrument bigint NOT NULL,
    Epoch TIMESTAMP NOT NULL,
    Open NUMERIC NOT NULL,
    High NUMERIC NOT NULL,
    Low NUMERIC NOT NULL,
    Close NUMERIC NOT NULL,
    Volume NUMERIC NOT NULL,
    Trades bigint NOT NULL,
    PRIMARY KEY (Instrument, Epoch),
    FOREIGN KEY (Instrument) REFERENCES instrument(id)
);
CREATE INDEX idx_instrument_epoch_desc ON ohlcvt (Instrument, Epoch DESC);

-- +goose Down
DROP TABLE IF EXISTS ohlcvt;