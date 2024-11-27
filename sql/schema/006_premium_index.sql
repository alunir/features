-- +goose Up
CREATE TABLE IF NOT EXISTS premium_index (
    SpotInstrument BIGINT NOT NULL,
    FuturesInstrument BIGINT NOT NULL,
    Resolution RESOLUTION NOT NULL,
    Epoch TIMESTAMP NOT NULL,
    PremiumIndex NUMERIC NOT NULL,
    PRIMARY KEY (SpotInstrument, FuturesInstrument, Resolution, Epoch),
    FOREIGN KEY (SpotInstrument) REFERENCES instrument(id),
    FOREIGN KEY (FuturesInstrument) REFERENCES instrument(id)
);
CREATE INDEX IF NOT EXISTS idx_spot_futures_epoch_desc ON premium_index (SpotInstrument, FuturesInstrument, Resolution, Epoch DESC);

-- +goose Down
DROP TABLE IF EXISTS premium_index;
