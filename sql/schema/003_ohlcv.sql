-- +goose Up
CREATE TABLE IF NOT EXISTS ohlcv (
    id BIGSERIAL PRIMARY KEY,
    Instrument bigint NOT NULL,
    Epoch TIMESTAMP NOT NULL,
    Open NUMERIC NOT NULL,
    High NUMERIC NOT NULL,
    Low NUMERIC NOT NULL,
    Close NUMERIC NOT NULL,
    Volume NUMERIC NOT NULL,
    Number bigint NOT NULL,
    UNIQUE (Instrument, Epoch),
    FOREIGN KEY (Instrument) REFERENCES instrument(id)
);

-- +goose Down
DROP TABLE IF EXISTS ohlcv;