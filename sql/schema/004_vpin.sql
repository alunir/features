-- +goose Up
CREATE TABLE IF NOT EXISTS vpin (
    id BIGSERIAL PRIMARY KEY,
    Threshold NUMERIC NOT NULL
);

CREATE TABLE IF NOT EXISTS vpin_ohlcv (
    id BIGSERIAL PRIMARY KEY,
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
    FOREIGN KEY (Instrument) REFERENCES instrument(id),
    FOREIGN KEY (VPIN) REFERENCES vpin(id)
);

-- +goose Down
DROP TABLE IF EXISTS vpin_ohlcv;
DROP TABLE IF EXISTS vpin;
