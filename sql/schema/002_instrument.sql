-- +goose Up
CREATE TABLE IF NOT EXISTS instrument (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    Exchange bigint NOT NULL,
    FOREIGN KEY (Exchange) REFERENCES exchange(id),
    UNIQUE (Exchange, name)
);

-- +goose Down
DROP TABLE IF EXISTS instrument;