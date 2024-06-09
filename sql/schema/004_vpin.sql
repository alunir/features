-- +goose Up
CREATE TABLE IF NOT EXISTS vpin (
    id BIGSERIAL PRIMARY KEY,
    Threshold NUMERIC NOT NULL
);

CREATE TABLE IF NOT EXISTS vpin_ohlcv (
    Instrument bigint NOT NULL,
    VPIN bigint NOT NULL,
    Epoch TIMESTAMP NOT NULL,
    Open NUMERIC,
    High NUMERIC,
    Low NUMERIC,
    Close NUMERIC,
    Volume NUMERIC,
    BuyVolume NUMERIC,
    SellVolume NUMERIC,
    Number bigint,
    PRIMARY KEY (Instrument, VPIN, Epoch),
    FOREIGN KEY (Instrument) REFERENCES instrument(id),
    FOREIGN KEY (VPIN) REFERENCES vpin(id)
);

-- +goose Down
DROP TABLE IF EXISTS vpin_ohlcv;
DROP TABLE IF EXISTS vpin;
