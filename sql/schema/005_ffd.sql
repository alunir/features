-- +goose Up
CREATE TABLE IF NOT EXISTS ffd (
    Instrument bigint NOT NULL,
    Resolution bigint NOT NULL,
    Epoch TIMESTAMP NOT NULL,
    Open NUMERIC,
    High NUMERIC,
    Low NUMERIC,
    Close NUMERIC,
    Volume NUMERIC,
    Number NUMERIC,
    PRIMARY KEY (Instrument, Resolution, Epoch),
    FOREIGN KEY (Instrument) REFERENCES instrument(id),
    FOREIGN KEY (Resolution) REFERENCES resolution(id)
);

-- +goose Down
DROP TABLE IF EXISTS ffd;
