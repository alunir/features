-- name: ListExchanges :many
SELECT * FROM exchange;

-- name: ListOhlcv :many
SELECT * FROM ohlcvt
WHERE Instrument = $1
ORDER BY Epoch DESC
LIMIT $2;

-- name: CreateOhlcv :one
INSERT INTO ohlcvt (Instrument, Epoch, Open, High, Low, Close, Volume, Trades)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
RETURNING *;

-- name: UpdateOhlcv :one
UPDATE ohlcvt
SET Open = $3, High = $4, Low = $5, Close = $6, Volume = $7, Trades = $8
WHERE Instrument = $1 AND Epoch = $2
RETURNING *;

-- name: DelettOhlcv :one
DELETE FROM ohlcvt
WHERE instrument = $1 AND epoch = $2
RETURNING *;
