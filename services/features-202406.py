import os
import emd
import json
import logging
import asyncio
import numpy as np
import pandas as pd
from typing import List
import redis.asyncio as redis
from datetime import datetime, timedelta
from util.types import VpinOHLCV, Features202406
from util.pg import Connection
from util.rd import RedisStore

from mlfinlab.features.fracdiff import frac_diff_ffd


logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())


def make_features(data: pd.DataFrame, WINDOW: int = 16, max_imf=8):
    prices = np.expm1(data.Close.rename("Last"))
    ask = np.expm1(data.High.rename("Ask"))
    bid = np.expm1(data.Low.rename("Bid"))

    features = pd.DataFrame(
        data[["Volume", "Number"]],
        columns=["Volume", "Number"],
        index=data.index,
    )

    ###########
    ## IMFs
    ###########

    imfs = emd.sift.sift(data.Close.values, max_imfs=max_imf)
    imfnum = imfs.shape[1]

    sample_rate = 1024
    IP, IF, IA = emd.spectra.frequency_transform(imfs, sample_rate, "nht")

    for i in range(imfnum):
        features[f"imf_imf_{i}"] = imfs[:, i]
        features[f"imf_ia_{i}"] = IA[:, i, None]
        features[f"imf_ip_{i}"] = IP[:, i, None]

    fdim = 0.4
    # frac_diff_ffd_df = np.array([frac_diff_ffd(pd.DataFrame(data[col]), fdim, thresh=1e-4) for col in ["open", "high", "low", "close"]])
    frac_diff_ffd_df = frac_diff_ffd(
        data[["Open", "High", "Low", "Close"]], fdim, thresh=1e-4
    )
    ohlcv = pd.DataFrame(
        frac_diff_ffd_df.values.reshape(-1, 4),
        columns=["open", "high", "low", "close"],
        index=data.index,
    )
    return features.join([ohlcv, ask, bid, prices])  # .dropna()


def Features202406_from_df(df: pd.DataFrame) -> List[Features202406]:
    if "Epoch" not in df.columns:
        df = df.reset_index().rename(columns={"index": "Epoch"})
    v: List[Features202406] = []
    for row in df.itertuples():
        d = Features202406(
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


async def fetch_vpin_ohlcv(pg: Connection) -> List[VpinOHLCV]:
    ohlcvs = await pg.fetch_all("vpin_ohlcv")
    return [
        VpinOHLCV(
            ohlcv["instrument"],
            ohlcv["vpin"],
            ohlcv["epoch"],
            ohlcv["open"],
            ohlcv["high"],
            ohlcv["low"],
            ohlcv["close"],
            ohlcv["volume"],
            ohlcv["buyvolume"],
            ohlcv["sellvolume"],
            ohlcv["number"],
        )
        for ohlcv in ohlcvs
    ]


async def main():
    rd = RedisStore()
    await rd.connection_test()

    pg = Connection()
    await pg.connection_test()

    ohlcvs = await fetch_vpin_ohlcv(pg)

    logging.info(f"vpin_ohlcvs: {len(ohlcvs)} rows")
    logging.info("Start subscribing to Redis PubSub channel...")

    channel = "vpin_ohlcv"

    async def reader(channel: redis.client.PubSub):
        while True:
            message = await channel.get_message(ignore_subscribe_messages=True)
            if message is None:
                continue
            # Just use as trigger
            logging.debug(f"Message: {message}")

            # fetch vpin_ohlcv from postgres again
            ohlcvs = await fetch_vpin_ohlcv(pg)
            df = pd.DataFrame(ohlcvs)
            df.index = df["Epoch"]

            features_df = make_features(
                df.drop(columns=["Volume", "SellVolume"]).rename(
                    columns={"BuyVolume": "Volume"}
                )
            )

            logging.debug(f"features_df: {len(features_df)} rows")

            if len(features_df) < 2:
                continue

            data = Features202406_from_df(features_df)

            # write postgres
            await asyncio.gather(
                rd.send(data, "features_202406"), pg.send(data, "features_202406")
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
