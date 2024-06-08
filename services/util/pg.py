# Postgres Connector
# This file contains the functions to connect to the Postgres database
#

import os
import logging
import asyncpg
from typing import List
from .types import OHLCV
from dataclasses import astuple

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())


class Connection:
    """
    Connection class to connect to the Postgres database
    """

    def __init__(self):
        self.conn = self.connect_to_db()

    async def connect_to_db(self) -> asyncpg.Connection:
        """
        Connects to the Postgres database
        """
        return await asyncpg.connect(
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=os.getenv("POSTGRES_DB"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
        )

    async def send(
        self,
        data: List[OHLCV],
    ) -> None:
        """
        Upserts OHLCV data into the Postgres database
        """
        logging.info(f"Inserting {len(data)} rows into Postgres")
        await self.conn.executemany(
            """
            INSERT INTO ohlcv (instrument, epoch, open, high, low, close, volume, number)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (epoch) DO UPDATE
            SET instrument = EXCLUDED.instrument,
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                number = EXCLUDED.number
            """,
            [astuple(d) for d in data],
        )
        logging.info("Inserted all records")
