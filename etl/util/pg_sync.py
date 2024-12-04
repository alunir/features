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


UPSERT_OHLCVT_QUERY = """
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
INSERT INTO ffd (Instrument, Resolution, Fdim, Epoch, Open, High, Low, Close, Volume, Trades)
VALUES %s
ON CONFLICT (Instrument, Resolution, Fdim, Epoch)
DO UPDATE SET
Open = EXCLUDED.Open,
High = EXCLUDED.High,
Low = EXCLUDED.Low,
Close = EXCLUDED.Close,
Volume = EXCLUDED.Volume,
Trades = EXCLUDED.Trades;
"""

UPSERT_EMD_QUERY = """
INSERT INTO emd (Instrument, Resolution, Fdim, Epoch, Imf_0, Imf_1, Imf_2, Imf_3, Imf_4, Imf_5, Imf_6, Imf_7, Imf_8, Imf_9, Imf_10, Imf_11, Imf_12, Imf_13, Imf_14, Imf_15, If_0, If_1, If_2, If_3, If_4, If_5, If_6, If_7, If_8, If_9, If_10, If_11, If_12, If_13, If_14, If_15, Ia_0, Ia_1, Ia_2, Ia_3, Ia_4, Ia_5, Ia_6, Ia_7, Ia_8, Ia_9, Ia_10, Ia_11, Ia_12, Ia_13, Ia_14, Ia_15, Ip_0, Ip_1, Ip_2, Ip_3, Ip_4, Ip_5, Ip_6, Ip_7, Ip_8, Ip_9, Ip_10, Ip_11, Ip_12, Ip_13, Ip_14, Ip_15)
VALUES %s
ON CONFLICT (Instrument, Resolution, Fdim, Epoch)
DO UPDATE SET
Imf_0 = EXCLUDED.Imf_0,
Imf_1 = EXCLUDED.Imf_1,
Imf_2 = EXCLUDED.Imf_2,
Imf_3 = EXCLUDED.Imf_3,
Imf_4 = EXCLUDED.Imf_4,
Imf_5 = EXCLUDED.Imf_5,
Imf_6 = EXCLUDED.Imf_6,
Imf_7 = EXCLUDED.Imf_7,
Imf_8 = EXCLUDED.Imf_8,
Imf_9 = EXCLUDED.Imf_9,
Imf_10 = EXCLUDED.Imf_10,
Imf_11 = EXCLUDED.Imf_11,
Imf_12 = EXCLUDED.Imf_12,
Imf_13 = EXCLUDED.Imf_13,
Imf_14 = EXCLUDED.Imf_14,
Imf_15 = EXCLUDED.Imf_15,
If_0 = EXCLUDED.If_0,
If_1 = EXCLUDED.If_1,
If_2 = EXCLUDED.If_2,
If_3 = EXCLUDED.If_3,
If_4 = EXCLUDED.If_4,
If_5 = EXCLUDED.If_5,
If_6 = EXCLUDED.If_6,
If_7 = EXCLUDED.If_7,
If_8 = EXCLUDED.If_8,
If_9 = EXCLUDED.If_9,
If_10 = EXCLUDED.If_10,
If_11 = EXCLUDED.If_11,
If_12 = EXCLUDED.If_12,
If_13 = EXCLUDED.If_13,
If_14 = EXCLUDED.If_14,
If_15 = EXCLUDED.If_15,
Ia_0 = EXCLUDED.Ia_0,
Ia_1 = EXCLUDED.Ia_1,
Ia_2 = EXCLUDED.Ia_2,
Ia_3 = EXCLUDED.Ia_3,
Ia_4 = EXCLUDED.Ia_4,
Ia_5 = EXCLUDED.Ia_5,
Ia_6 = EXCLUDED.Ia_6,
Ia_7 = EXCLUDED.Ia_7,
Ia_8 = EXCLUDED.Ia_8,
Ia_9 = EXCLUDED.Ia_9,
Ia_10 = EXCLUDED.Ia_10,
Ia_11 = EXCLUDED.Ia_11,
Ia_12 = EXCLUDED.Ia_12,
Ia_13 = EXCLUDED.Ia_13,
Ia_14 = EXCLUDED.Ia_14,
Ia_15 = EXCLUDED.Ia_15,
Ip_0 = EXCLUDED.Ip_0,
Ip_1 = EXCLUDED.Ip_1,
Ip_2 = EXCLUDED.Ip_2,
Ip_3 = EXCLUDED.Ip_3,
Ip_4 = EXCLUDED.Ip_4,
Ip_5 = EXCLUDED.Ip_5,
Ip_6 = EXCLUDED.Ip_6,
Ip_7 = EXCLUDED.Ip_7,
Ip_8 = EXCLUDED.Ip_8,
Ip_9 = EXCLUDED.Ip_9,
Ip_10 = EXCLUDED.Ip_10,
Ip_11 = EXCLUDED.Ip_11,
Ip_12 = EXCLUDED.Ip_12,
Ip_13 = EXCLUDED.Ip_13,
Ip_14 = EXCLUDED.Ip_14,
Ip_15 = EXCLUDED.Ip_15;
"""

UPSERT_PREMIUM_INDEX_QUERY = """
INSERT INTO premium_index (SpotInstrument, FuturesInstrument, Resolution, Epoch, PremiumIndex)
VALUES %s
ON CONFLICT (SpotInstrument, FuturesInstrument, Resolution, Epoch)
DO UPDATE SET
PremiumIndex = EXCLUDED.PremiumIndex;
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
            case "ohlcvt":
                query = UPSERT_OHLCVT_QUERY
            case "ffd":
                query = UPSERT_FFD_QUERY
            case "emd":
                query = UPSERT_EMD_QUERY
            case "premium_index":
                query = UPSERT_PREMIUM_INDEX_QUERY
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

    def fetch_all_by_inst(
        self, target: str, instrument_id: str
    ) -> List:
        """
        Fetches data from the Postgres database
        """
        with self.conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT * FROM {target} WHERE instrument = {instrument_id} ORDER BY Epoch ASC;")
                return cursor.fetchall()
