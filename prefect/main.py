import os
import traceback
import asyncio
import pandas as pd
from typing import List
from util.types import OHLCV
from util.pg import Connection
import pymarketstore as pymkts
from logging import getLogger, StreamHandler, Formatter

from prefect import flow, task
from mlfinlab.features.fracdiff import frac_diff_ffd


logger = getLogger(__name__)
stream_handler = StreamHandler()
formatter = Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

marketstore_url = os.environ.get("MARKETSTORE_URL", None)

columns = ["Open", "High", "Low", "Close", "Volume", "Number"]


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
        [msg], columns=["Epoch"]+columns
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


async def send(df: pd.DataFrame, pg: Connection):
    data = OHLCV_from_df(df)
    await pg.send(data, "ohlcvn")
    logger.info("Sent all records to Postgres")


@task(log_prints=True)
async def ohlcvn_stream_task(pg, instrument: str):
    client = pymkts.Client(endpoint=f"http://{marketstore_url}:5993/rpc")

    backoff_days = int(os.environ.get("BACKOFF_DAYS", 1))

    logger.info("Connected to marketstore")

    try:
        param = pymkts.Params(
            instrument, "1Min", "OHLCV", limit=60 * 24 * backoff_days
        )
        reply = client.query(param)
        df = reply.first().df()
        logger.debug(f"Got {len(df)} rows")

        await send(df, pg)
        
        return df

    except KeyboardInterrupt:
        return
    except Exception as e:
        logger.warning(traceback.format_exc())
        logger.warning(e)


@task(log_prints=True)
async def ffd_stream_task(pg, df: pd.DataFrame, fdim: float, thresh: float):
    ffd_df = frac_diff_ffd(df[columns], fdim, thresh)
    ffd_df = pd.concat([df.Epoch, ffd_df], axis=1)
    
    await send(ffd_df, pg)
    
    return ffd_df

@task(log_prints=True)
async def stream_task(pg, instrument: str, fdim: float, thresh: float):
    df = await ohlcvn_stream_task(pg, instrument)
    await ffd_stream_task(pg, df, fdim, thresh)


@flow(log_prints=True)
async def etl_flow(instruments: List[str], fdim: float, thresh: float):
    pg = Connection()
    await pg.connection_test()

    asyncio.gather(*[stream_task(pg, instrument, fdim, thresh) for instrument in instruments])


if __name__ == "__main__":
    asyncio.run(
        etl_flow.serve(
            name="features",
            tags=["features"],
            parameters={
                "instruments": ["Binance_ETH-USDT", "Binance_ETH-USDT.P"],
                "fdim": 0.1,
                "thresh": 1e-4
            },
            interval=60
        )
    )
