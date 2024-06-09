# Postgres Connector
# This file contains the functions to connect to the Postgres database
#

import os
import logging
import asyncpg
from typing import List
from dataclasses import astuple

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())


UPSERT_OHLCV_QUERY = """
INSERT INTO ohlcv (Instrument, Epoch, Open, High, Low, Close, Volume, Number)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (Instrument, Epoch)
DO UPDATE SET
Open = EXCLUDED.Open,
High = EXCLUDED.High,
Low = EXCLUDED.Low,
Close = EXCLUDED.Close,
Volume = EXCLUDED.Volume,
Number = EXCLUDED.Number;
"""

UPSERT_VPIN_OHLCV_QUERY = """
INSERT INTO vpin_ohlcv (Instrument, VPIN, Epoch, Open, High, Low, Close, Volume, BuyVolume, SellVolume, Number)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (Instrument, VPIN, Epoch)
DO UPDATE SET
Open = EXCLUDED.Open,
High = EXCLUDED.High,
Low = EXCLUDED.Low,
Close = EXCLUDED.Close,
Volume = EXCLUDED.Volume,
BuyVolume = EXCLUDED.BuyVolume,
SellVolume = EXCLUDED.SellVolume,
Number = EXCLUDED.Number;
"""

UPSERT_FEATURES_202406_QUERY = """
INSERT INTO features_202406 (Instrument, VPIN, Epoch, Volume, Number, imf_imf_0, imf_imf_1, imf_imf_2, imf_imf_3, imf_imf_4, imf_imf_5, imf_imf_6, imf_imf_7, imf_imf_8, imf_ia_0, imf_ia_1, imf_ia_2, imf_ia_3, imf_ia_4, imf_ia_5, imf_ia_6, imf_ia_7, imf_ia_8, imf_ip_0, imf_ip_1, imf_ip_2, imf_ip_3, imf_ip_4, imf_ip_5, imf_ip_6, imf_ip_7, imf_ip_8, Open, High, Low, Close, Ask, Bid, Last)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39)
ON CONFLICT (Instrument, VPIN, Epoch)
DO UPDATE SET
Volume = EXCLUDED.Volume,
Number = EXCLUDED.Number,
imf_imf_0 = EXCLUDED.imf_imf_0,
imf_imf_1 = EXCLUDED.imf_imf_1,
imf_imf_2 = EXCLUDED.imf_imf_2,
imf_imf_3 = EXCLUDED.imf_imf_3,
imf_imf_4 = EXCLUDED.imf_imf_4,
imf_imf_5 = EXCLUDED.imf_imf_5,
imf_imf_6 = EXCLUDED.imf_imf_6,
imf_imf_7 = EXCLUDED.imf_imf_7,
imf_imf_8 = EXCLUDED.imf_imf_8,
imf_ia_0 = EXCLUDED.imf_ia_0,
imf_ia_1 = EXCLUDED.imf_ia_1,
imf_ia_2 = EXCLUDED.imf_ia_2,
imf_ia_3 = EXCLUDED.imf_ia_3,
imf_ia_4 = EXCLUDED.imf_ia_4,
imf_ia_5 = EXCLUDED.imf_ia_5,
imf_ia_6 = EXCLUDED.imf_ia_6,
imf_ia_7 = EXCLUDED.imf_ia_7,
imf_ia_8 = EXCLUDED.imf_ia_8,
imf_ip_0 = EXCLUDED.imf_ip_0,
imf_ip_1 = EXCLUDED.imf_ip_1,
imf_ip_2 = EXCLUDED.imf_ip_2,
imf_ip_3 = EXCLUDED.imf_ip_3,
imf_ip_4 = EXCLUDED.imf_ip_4,
imf_ip_5 = EXCLUDED.imf_ip_5,
imf_ip_6 = EXCLUDED.imf_ip_6,
imf_ip_7 = EXCLUDED.imf_ip_7,
imf_ip_8 = EXCLUDED.imf_ip_8,
Open = EXCLUDED.Open,
High = EXCLUDED.High,
Low = EXCLUDED.Low,
Close = EXCLUDED.Close,
Ask = EXCLUDED.Ask,
Bid = EXCLUDED.Bid,
Last = EXCLUDED.Last;
"""


class Connection:
    """
    Connection class to connect to the Postgres database
    """

    def __init__(self):
        self.conn = None

    async def connect_to_db(self) -> asyncpg.Connection:
        """
        Connects to the Postgres database
        """
        return await asyncpg.connect(
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=os.getenv("POSTGRES_DB"),
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
        )

    async def connection_test(self):
        """
        Tests the connection to the Postgres database
        """
        try:
            self.conn = await self.connect_to_db()
        except Exception as e:
            logging.info(f"Failed to connect to Postgres: {e}")
            raise e

        logging.info("Connected to Postgres")

    async def send(self, data: List, table: str) -> None:
        """
        Upserts OHLCV data into the Postgres database
        """
        logging.debug(f"Inserting {len(data)} rows into Postgres")
        match table:
            case "ohlcv":
                query = UPSERT_OHLCV_QUERY
            case "vpin_ohlcv":
                query = UPSERT_VPIN_OHLCV_QUERY
            case "features_202406":
                query = UPSERT_FEATURES_202406_QUERY
            case _:
                raise ValueError(f"Table {table} not supported")
        async with self.conn.transaction():
            await self.conn.executemany(query, [astuple(d) for d in data])

    async def fetch(self, target: str, source: str) -> List:
        """
        Fetches data from the Postgres database
        """
        async with self.conn.transaction():
            return await self.conn.fetch(
                f"SELECT * FROM {target} WHERE Epoch > (SELECT Epoch FROM {source} ORDER BY Epoch DESC LIMIT 1) ORDER BY Epoch ASC"
            )

    async def fetch_all(self, target: str) -> List:
        """
        Fetches data from the Postgres database
        """
        async with self.conn.transaction():
            return await self.conn.fetch(f"SELECT * FROM {target} ORDER BY Epoch ASC")
