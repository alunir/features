# Postgres Connector
# This file contains the functions to connect to the Postgres database
#

import os
import logging
import psycopg2
from psycopg2  import extras
from typing import List
from dataclasses import astuple

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())


UPSERT_OHLCV_QUERY = """
INSERT INTO ohlcvt (Instrument, Epoch, Open, High, Low, Close, Volume, Trades)
VALUES %s
ON CONFLICT (Instrument, Epoch)
DO UPDATE SET
Open = EXCLUDED.Open,
High = EXCLUDED.High,
Low = EXCLUDED.Low,
Close = EXCLUDED.Close,
Volume = EXCLUDED.Volume,
Trades = EXCLUDED.Trades;
"""

UPSERT_FFD_QUERY = """
INSERT INTO ffd (Instrument, Resolution, Epoch, Open, High, Low, Close, Volume, Trades)
VALUES %s
ON CONFLICT (Instrument, Resolution, Epoch)
DO UPDATE SET
Open = EXCLUDED.Open,
High = EXCLUDED.High,
Low = EXCLUDED.Low,
Close = EXCLUDED.Close,
Volume = EXCLUDED.Volume,
Trades = EXCLUDED.Trades;
"""

UPSERT_FEATURES_202406_QUERY = """
INSERT INTO features_202406 (Instrument, VPIN, Epoch, Volume, Trades, imf_imf_0, imf_imf_1, imf_imf_2, imf_imf_3, imf_imf_4, imf_imf_5, imf_imf_6, imf_imf_7, imf_imf_8, imf_ia_0, imf_ia_1, imf_ia_2, imf_ia_3, imf_ia_4, imf_ia_5, imf_ia_6, imf_ia_7, imf_ia_8, imf_ip_0, imf_ip_1, imf_ip_2, imf_ip_3, imf_ip_4, imf_ip_5, imf_ip_6, imf_ip_7, imf_ip_8, Open, High, Low, Close, Ask, Bid, Last)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (Instrument, VPIN, Epoch)
DO UPDATE SET
Volume = EXCLUDED.Volume,
Trades = EXCLUDED.Trades,
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

    def connect_to_db(self):
        """
        Connects to the Postgres database
        """
        return psycopg2.connect(
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=os.getenv("POSTGRES_DB"),
            host=os.getenv("POSTGRES_HOSTNAME"),
            port=os.getenv("POSTGRES_PORT"),
        )

    def connection_test(self):
        """
        Tests the connection to the Postgres database
        """
        self.conn = self.connect_to_db
        try:
            with self.conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
        except Exception as e:
            logging.info(f"Failed to connect to Postgres: {e}")
            raise e

        logging.info("Connected to Postgres")

    def send(self, data: List, table: str) -> None:
        """
        Upserts OHLCV data into the Postgres database
        """
        logging.debug(f"Inserting {len(data)} rows into Postgres")
        match table:
            case "ohlcvn":
                query = UPSERT_OHLCV_QUERY
            case "ffd":
                query = UPSERT_FFD_QUERY
            # case "features_202406":
            #     query = UPSERT_FEATURES_202406_QUERY
            case _:
                raise ValueError(f"Table {table} not supported")
        with self.conn() as conn:
            with conn.cursor() as cursor:
                extras.execute_values(cursor, query, [astuple(d) for d in data])

    def fetch(
        self, target: str, source: str, instrument_id: str, vpin_id: str
    ) -> List:
        """
        Fetches data from the Postgres database
        """
        with self.conn() as conn:
            with conn.cursor() as cursor:
                return cursor.fetch(
                    f"SELECT * FROM {target} WHERE Epoch > (SELECT Epoch FROM {source} WHERE instrument = {instrument_id} and vpin = {vpin_id} ORDER BY Epoch DESC LIMIT 1) ORDER BY Epoch ASC"
                )

    def fetch_all(self, target: str) -> List:
        """
        Fetches data from the Postgres database
        """
        with self.conn() as conn:
            with conn.cursor() as cursor:
                return cursor.fetch(f"SELECT * FROM {target} ORDER BY Epoch ASC")

    def fetch_all_with_limit(self, target: str, limit: int = 10000) -> List:
        """
        Fetches data from the Postgres database
        """
        with self.conn() as conn:
            with conn.cursor() as cursor:
                return cursor.fetch(
                    f"SELECT * FROM {target} ORDER BY Epoch ASC LIMIT {limit}"
                )

    def fetch_all_by_inst_vpin(
        self, target: str, instrument_id: str, vpin_id: str
    ) -> List:
        """
        Fetches data from the Postgres database
        """
        with self.conn() as conn:
            with conn.cursor() as cursor:
                return cursor.fetch(
                    f"SELECT * FROM {target} WHERE instrument = {instrument_id} and vpin = {vpin_id} ORDER BY Epoch ASC"
                )
