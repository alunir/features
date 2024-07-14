import os
import ast
import json
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
from logging import getLogger, StreamHandler, Formatter


logger = getLogger(__name__)
stream_handler = StreamHandler()
formatter = Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())


instrument_id = int(os.environ.get("INSTRUMENT_ID"))
assert instrument_id, "INSTRUMENT_ID is not set"
vpin_id = int(os.environ.get("VPIN_ID"))
assert vpin_id, "VPIN_ID is not set"

redis_suffix = f"_{instrument_id}_{vpin_id}"


def VpinOHLCV_from_df(df: pd.DataFrame) -> List[VpinOHLCV]:
    if "Epoch" not in df.columns:
        df = df.reset_index().rename(columns={"index": "Epoch"})
    v: List[VpinOHLCV] = []
    for row in df.itertuples():
        d = VpinOHLCV(
            instrument_id,  # Instrument ID 1 : Binance_ETH-USDT
            vpin_id,  # VPIN ID 1 : VPIN = 1000
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


async def fetch_ohlcv(pg: Connection, fetch_all_data: bool = False) -> List[OHLCV]:
    if fetch_all_data:
        ohlcvs = await pg.fetch_all("ohlcv")
    else:
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
    bucket_size = int(os.environ.get("BUCKET_SIZE"))
    assert bucket_size, "BUCKET_SIZE is not set"

    output = os.environ.get("OUTPUT")
    assert output, "OUTPUT is not set"

    fetch_all_data = ast.literal_eval(os.environ.get("FETCH_ALL_DATA", "False"))
    logger.info(f"FETCH_ALL_DATA: {fetch_all_data}")

    rd = RedisStore()
    await rd.connection_test()

    pg = Connection()
    await pg.connection_test()

    ohlcvs = await fetch_ohlcv(pg, fetch_all_data=fetch_all_data)

    logger.info(f"ohlcvs: {len(ohlcvs)} rows")
    logger.info("Start subscribing to Redis PubSub channel...")

    source = "ohlcv"

    async def reader(channel: redis.client.PubSub):
        while True:
            message = await channel.get_message(ignore_subscribe_messages=True)
            if message is None:
                continue
            logger.debug(f"Message: {message}")

            ohlcv = json.loads(message["data"])
            epoch = datetime.strptime(ohlcv["Epoch"], "%Y-%m-%dT%H:%M:%SZ").replace(
                second=0, microsecond=0
            )
            now = datetime.now().replace(second=0, microsecond=0)
            # two_min_ago = now - timedelta(minutes=2)
            one_min_ago = now - timedelta(minutes=1)
            if epoch == one_min_ago:
                # Just use as trigger
                logger.debug(f"Received OHLCV: {ohlcv}")

                # fetch ohlcv from postgres again
                ohlcvs = await fetch_ohlcv(pg)

                if len(ohlcvs) == 0:
                    logger.warning("No OHLCV data in Postgres")
                    continue

                df = pd.DataFrame(ohlcvs)
                df.index = df["Epoch"]

                log1p_df = np.log1p(df[df.columns[df.columns != "Epoch"]].astype(float))

                imb_df = compute_imbalance_bars(
                    log1p_df,
                    bucket_size=bucket_size,
                )

                # compute_imbalance_bars returns an empty DataFrame if the input DataFrame is small
                if imb_df.empty:
                    continue

                logger.debug(f"imb_df: {len(imb_df)} rows")

                data = VpinOHLCV_from_df(imb_df)

                # write postgres
                await asyncio.gather(
                    rd.send(data, output + redis_suffix),
                    pg.send(data, output),
                )

                logger.info("Sent all records to Postgres and Redis")

    while True:
        try:
            async with rd.r.pubsub() as pubsub:
                await pubsub.subscribe(source)
                await reader(pubsub)
        except redis.ConnectionError as e:
            logger.error(
                f"Failed to subscribe to Redis PubSub channel {source}. Error: {e}"
            )
            asyncio.sleep(30)
            continue


if __name__ == "__main__":
    asyncio.run(main())
