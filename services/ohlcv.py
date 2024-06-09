import os
import re
import traceback
import logging
import msgpack
import asyncio
import websockets
import pandas as pd
from typing import List
from util.types import OHLCV
from util.pg import Connection
from util.rd import RedisStore
import pymarketstore as pymkts


logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())


def OHLCV_from_df(df: pd.DataFrame) -> List[OHLCV]:
    if "Epoch" not in df.columns:
        df = df.reset_index().rename(columns={"index": "Epoch"})
    v: List[OHLCV] = []
    for row in df.itertuples():
        d = OHLCV(
            1,  # Instrument ID 1 : Binance_ETH-USDT
            row.Epoch.tz_localize(None),
            row.Open,
            row.High,
            row.Low,
            row.Close,
            row.Volume,
            row.Number,
        )
        v += [d]
    return v


def msg_to_df(msg: dict) -> pd.DataFrame:
    df = pd.DataFrame(
        [msg], columns=["Epoch", "Open", "High", "Low", "Close", "Volume", "Number"]
    )
    df["Epoch"] = pd.to_datetime(df["Epoch"], unit="s")
    df["Epoch"] = df["Epoch"].dt.tz_localize(None)
    df["Open"] = df["Open"].astype(float)
    df["High"] = df["High"].astype(float)
    df["Low"] = df["Low"].astype(float)
    df["Close"] = df["Close"].astype(float)
    df["Volume"] = df["Volume"].astype(float)
    df["Number"] = df["Number"].astype(int)
    return df


async def send(df: pd.DataFrame, rd: RedisStore, pg: Connection):
    data = OHLCV_from_df(df)
    await rd.send(data, "ohlcv")
    # await asyncio.gather(rd.send(data, "ohlcv"), pg.send(data))
    logging.info("Sent all records to Postgres and SurrealDB")


async def main():
    # Wait for surrealdb start
    rd = RedisStore()
    await rd.connection_test()

    pg = Connection()
    await pg.connection_test()

    marketstore_url = os.environ.get("MARKETSTORE_URL", None)

    client = pymkts.Client(endpoint=f"http://{marketstore_url}:5993/rpc")

    backoff_days = int(os.environ.get("BACKOFF_DAYS", 1))
    ping_interval = os.environ.get("PING_INTERVAL", 120)
    ping_timeout = os.environ.get("PING_TIMEOUT", None)

    logging.info("Connected to marketstore")

    while True:
        try:
            param = pymkts.Params(
                "Binance_ETH-USDT", "1Min", "OHLCV", limit=60 * 24 * backoff_days
            )
            reply = client.query(param)
            df = reply.first().df()
            logging.info(f"Got {len(df)} rows")

            await send(df, rd, pg)

            pat = re.compile(r"^Binance_ETH-USDT/*")
            while True:
                connection = websockets.connect(
                    f"ws://{marketstore_url}:5993/ws",
                    ping_interval=ping_interval,
                    ping_timeout=ping_timeout,
                )
                async with connection as ws:
                    msg = msgpack.dumps(
                        {
                            "streams": ["*/*/*"],
                        }
                    )

                    await ws.send(msg)

                    logging.info("Subscribed to Binance_ETH-USDT")

                    async for message in ws:
                        msg = msgpack.loads(message)
                        key = msg.get("key")
                        if key is not None and pat.match(key):
                            data = msg["data"]
                            logging.debug(f"Received message: {data}")
                            df = msg_to_df(data)
                            await send(df, rd, pg)

        except KeyboardInterrupt:
            return
        except Exception as e:
            logging.warning(traceback.format_exc())
            logging.warning(e)
        await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
