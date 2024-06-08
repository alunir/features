import os
import re
import logging
import msgpack
import asyncio
import websockets
import pandas as pd
from typing import List
from .util.types import OHLCV
from .util.pg import Connection
from .util.sd import SurrealDBStore
import pymarketstore as pymkts


logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())


def OHLCV_from_df(df: pd.DataFrame) -> List[OHLCV]:
    if "Epoch" not in df.columns:
        df = df.reset_index().rename(columns={"index": "Epoch"})
    v: List[OHLCV] = []
    for row in df.itertuples():
        print(row)
        d = OHLCV(
            1,
            row.Epoch.strftime("%Y-%m-%dT%H:%M:%SZ"),
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
    df["Epoch"] = df["Epoch"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df["Open"] = df["Open"].astype(float)
    df["High"] = df["High"].astype(float)
    df["Low"] = df["Low"].astype(float)
    df["Close"] = df["Close"].astype(float)
    df["Volume"] = df["Volume"].astype(float)
    df["Number"] = df["Number"].astype(int)
    return df


async def main():
    store = SurrealDBStore()
    pg = Connection()

    marketstore_url = os.environ.get("MARKETSTORE_URL", None)

    client = pymkts.Client(endpoint=f"http://{marketstore_url}:5993/rpc")

    days = int(os.environ.get("DAYS", 1))
    ping_interval = os.environ.get("PING_INTERVAL", 120)
    ping_timeout = os.environ.get("PING_TIMEOUT", None)

    logging.info("Connected to marketstore")
    while True:
        try:
            param = pymkts.Params(
                "Binance_ETH-USDT", "1Min", "OHLCV", limit=60 * 24 * days
            )
            reply = client.query(param)
            df = reply.first().df()
            logging.info(f"Got {len(df)} rows")

            data = OHLCV_from_df(df)

            await store.send(data)
            await pg.send(data)

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
                            data = OHLCV_from_df(df)
                            await store.send(data)
                            await pg.send(data)

        except KeyboardInterrupt:
            return
        except Exception as e:
            logging.warning(e)
        await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
