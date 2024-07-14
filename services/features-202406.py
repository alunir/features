import os
import emd
import asyncio
import numpy as np
import pandas as pd
from typing import List
import redis.asyncio as redis
from util.types import VpinOHLCV, Features202406
from util.pg import Connection
from util.rd import RedisStore

from mlfinlab.features.fracdiff import frac_diff_ffd
from logging import getLogger, StreamHandler, Formatter


logger = getLogger(__name__)
stream_handler = StreamHandler()
formatter = Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())


np.random.seed(42)


instrument_id = int(os.environ.get("INSTRUMENT_ID"))
assert instrument_id, "INSTRUMENT_ID is not set"
vpin_id = int(os.environ.get("VPIN_ID"))
assert vpin_id, "VPIN_ID is not set"


def make_features(data: pd.DataFrame, max_imf=8):
    prices = np.expm1(data.Close.rename("Last"))
    ask = np.expm1(data.High.rename("Ask"))
    bid = np.expm1(data.Low.rename("Bid"))

    features = pd.DataFrame(
        data[["Volume", "Number"]],
        columns=["Volume", "Number"],
        index=data.index,
    )

    ###########
    # IMFs
    ###########

    imfs = emd.sift.sift(
        data.Close.values,
        max_imfs=max_imf,
    )
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
            instrument_id,  # Instrument ID 1 : Binance_ETH-USDT
            vpin_id,  # VPIN ID 1 : VPIN = 1000
            row.Epoch.to_pydatetime(),
            row.Volume,
            row.Number,
            row.imf_imf_0,
            row.imf_imf_1,
            row.imf_imf_2,
            row.imf_imf_3,
            row.imf_imf_4,
            row.imf_imf_5,
            row.imf_imf_6,
            row.imf_imf_7,
            row.imf_imf_8,
            row.imf_ia_0,
            row.imf_ia_1,
            row.imf_ia_2,
            row.imf_ia_3,
            row.imf_ia_4,
            row.imf_ia_5,
            row.imf_ia_6,
            row.imf_ia_7,
            row.imf_ia_8,
            row.imf_ip_0,
            row.imf_ip_1,
            row.imf_ip_2,
            row.imf_ip_3,
            row.imf_ip_4,
            row.imf_ip_5,
            row.imf_ip_6,
            row.imf_ip_7,
            row.imf_ip_8,
            row.open,
            row.high,
            row.low,
            row.close,
            row.Ask,
            row.Bid,
            row.Last,
        )
        v += [d]
    return v


async def fetch_vpin_ohlcv(pg: Connection, source: str) -> List[VpinOHLCV]:
    # ohlcvs = await pg.fetch_all_with_limit("vpin_ohlcv", limit=10000)
    ohlcvs = await pg.fetch_all(source, instrument_id, vpin_id)
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

    source = os.environ.get("SOURCE")
    assert source, "SOURCE is not set"

    output = os.environ.get("OUTPUT")
    assert output, "OUTPUT is not set"

    lag = int(os.environ.get("LAG"))
    assert lag, "LAG is not set"

    ohlcvs = await fetch_vpin_ohlcv(pg, source)

    logger.info(f"vpin_ohlcvs: {len(ohlcvs)} rows")
    logger.info("Start subscribing to Redis PubSub channel...")

    async def reader(channel: redis.client.PubSub):
        while True:
            message = await channel.get_message(ignore_subscribe_messages=True)
            if message is None:
                continue
            # Just use as trigger
            logger.debug(f"Message: {message}")

            # fetch vpin_ohlcv from postgres again
            ohlcvs = await fetch_vpin_ohlcv(pg, source)
            if len(ohlcvs) == 0:
                continue

            df = pd.DataFrame(ohlcvs)
            df = df.set_index("Epoch").astype(float)
            df.sort_index(inplace=True)
            df = df.drop(columns=["Volume", "SellVolume"]).rename(
                columns={"BuyVolume": "Volume"}
            )

            features_df = make_features(df)

            logger.info(f"features_df: {len(features_df)} rows")

            # if len(features_df) < 2:
            #     continue

            data = Features202406_from_df(features_df)
            latest_features_for_algo = data[-lag:]

            # write postgres
            await asyncio.gather(
                rd.send_bulk(latest_features_for_algo, output),
                pg.send(data, output),
            )

            logger.info(f"Sent all records to Postgres and lag {lag} records to Redis")

            del df, data, ohlcvs

    while True:
        try:
            async with rd.r.pubsub() as pubsub:
                await pubsub.subscribe(f"{source}_{instrument_id}_{vpin_id}")
                await reader(pubsub)
        except redis.ConnectionError as e:
            logger.error(
                f"Failed to subscribe to Redis PubSub channel {source}. Error: {e}"
            )
            asyncio.sleep(30)
            continue


if __name__ == "__main__":
    asyncio.run(main())
