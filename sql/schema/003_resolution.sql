-- +goose Up
CREATE TABLE IF NOT EXISTS features (
    id BIGSERIAL PRIMARY KEY,
    Features VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS resolution (
    id BIGSERIAL PRIMARY KEY,
    Features bigint NOT NULL,
    Resolution NUMERIC NOT NULL,
    FOREIGN KEY (Features) REFERENCES features(id)
);

-- +goose Down
DROP TABLE IF EXISTS resolution;
DROP TABLE IF EXISTS features;