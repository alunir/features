# Postgres Connector
# This file contains the functions to connect to the Postgres database
#

import os
import logging
import asyncpg
from datetime import datetime
from typing import List
from .types import Data
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

    async def get_last_epoch(self, table: str) -> List:
        async with self.conn.transaction():
            return await self.conn.fetch(
                f"SELECT * FROM {table} ORDER BY Epoch DESC LIMIT 1"
            )

    async def send(
        self,
        data: List[Data],
    ) -> None:
        """
        Upserts OHLCV data into the Postgres database
        """
        logging.debug(f"Inserting {len(data)} rows into Postgres")
        async with self.conn.transaction():
            await self.conn.executemany(UPSERT_OHLCV_QUERY, [astuple(d) for d in data])

    async def fetch(self, table: str, last_epoch: datetime) -> List:
        """
        Fetches data from the Postgres database
        """
        async with self.conn.transaction():
            return await self.conn.fetch(
                f"SELECT * FROM {table} WHERE Epoch > $1 ORDER BY Epoch ASC", last_epoch
            )
