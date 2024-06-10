import os
import json
import logging
import asyncio
import numpy as np
import pandas as pd
from typing import List
import redis.asyncio as redis
from datetime import datetime, timedelta
from util.types import VpinOHLCV, OHLCV
from util.pg import Connection
from util.rd import RedisStore
from mlfinlab.bars.dollar_imbalance_bars import compute_imbalance_bars

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())


def VpinOHLCV_from_df(df: pd.DataFrame) -> List[VpinOHLCV]:
    if "Epoch" not in df.columns:
        df = df.reset_index().rename(columns={"index": "Epoch"})
    v: List[VpinOHLCV] = []
    for row in df.itertuples():
        d = VpinOHLCV(
            1,  # Instrument ID 1 : Binance_ETH-USDT
            1,  # VPIN ID 1 : VPIN = 1000
            row.Epoch.to_pydatetime(),
            row.Open,
            row.High,
            row.Low,
            row.Close,
            row.Volume,
            row.BuyVolume,
            row.SellVolume,
            row.Number,
        )
        v += [d]
    return v


async def fetch_ohlcv(pg: Connection) -> List[OHLCV]:
    ohlcvs = await pg.fetch("ohlcv", "vpin_ohlcv")
    return [
        OHLCV(
            ohlcv["instrument"],
            ohlcv["epoch"],
            ohlcv["open"],
            ohlcv["high"],
            ohlcv["low"],
            ohlcv["close"],
            ohlcv["volume"],
            ohlcv["number"],
        )
        for ohlcv in ohlcvs
    ]


async def main():
    rd = RedisStore()
    await rd.connection_test()

    pg = Connection()
    await pg.connection_test()

    ohlcvs = await fetch_ohlcv(pg)

    logging.info(f"ohlcvs: {len(ohlcvs)} rows")
    logging.info("Start subscribing to Redis PubSub channel...")

    channel = "ohlcv"

    async def reader(channel: redis.client.PubSub):
        while True:
            message = await channel.get_message(ignore_subscribe_messages=True)
            if message is None:
                continue
            logging.debug(f"Message: {message}")

            ohlcv = json.loads(message["data"])
            epoch = datetime.strptime(ohlcv["Epoch"], "%Y-%m-%dT%H:%M:%SZ")
            now = datetime.now().replace(second=0, microsecond=0)
            two_min_ago = now - timedelta(minutes=2)
            one_min_ago = now - timedelta(minutes=1)
            if two_min_ago <= epoch and epoch < one_min_ago:
                # Just use as trigger
                logging.debug(f"Received OHLCV: {ohlcv}")

                # fetch ohlcv from postgres again
                ohlcvs = await fetch_ohlcv(pg)

                df = pd.DataFrame(ohlcvs)
                df.index = df["Epoch"]

                log1p_df = np.log1p(df[df.columns[df.columns != "Epoch"]].astype(float))

                if log1p_df.empty:
                    continue

                imb_df = compute_imbalance_bars(
                    log1p_df,
                    bucket_size=1000,
                )

                logging.debug(f"imb_df: {len(imb_df)} rows")

                if len(imb_df) < 2:
                    continue

                data = VpinOHLCV_from_df(imb_df)

                # write postgres
                await asyncio.gather(
                    rd.send(data, "vpin_ohlcv"), pg.send(data, "vpin_ohlcv")
                )

                logging.info("Sent all records to Postgres and Redis")

    while True:
        try:
            async with rd.r.pubsub() as pubsub:
                await pubsub.subscribe(channel)
                await reader(pubsub)
        except redis.ConnectionError as e:
            logging.error(
                f"Failed to subscribe to Redis PubSub channel {channel}. Error: {e}"
            )
            asyncio.sleep(30)
            continue


if __name__ == "__main__":
    asyncio.run(main())
