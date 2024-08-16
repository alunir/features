import os
import traceback
import pandas as pd
from typing import List, Tuple
from util.types import OHLCV, FFD
from util.pg_sync import Connection
import pymarketstore as pymkts

from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from mlfinlab.features.fracdiff import frac_diff_ffd


marketstore_url = os.environ.get("MARKETSTORE_URL", None)

columns = ["Open", "High", "Low", "Close", "Volume", "Trades"]


def OHLCV_from_df(instrument_id: int, df: pd.DataFrame) -> List[OHLCV]:
    if "Epoch" not in df.columns:
        df = df.reset_index().rename(columns={"index": "Epoch"})
    v: List[OHLCV] = []
    for row in df.itertuples():
        d = OHLCV(
            instrument_id,
            row.Epoch.tz_localize(None),
            row.Open,
            row.High,
            row.Low,
            row.Close,
            row.Volume,
            row.Trades,
        )
        v += [d]
    return v


def FFD_from_df(instrument_id: int, resolution_id: int, df: pd.DataFrame) -> List[FFD]:
    if "Epoch" not in df.columns:
        df = df.reset_index().rename(columns={"index": "Epoch"})
    v: List[FFD] = []
    for row in df.itertuples():
        d = FFD(
            instrument_id,
            resolution_id,
            row.Epoch.to_pydatetime(),
            row.Open,
            row.High,
            row.Low,
            row.Close,
            row.Volume,
            row.Trades,
        )
        v += [d]
    return v


@task(log_prints=True)
def ohlcvn_stream_task(pg, instrument_id: int, instrument: str, backoff_days: int):
    logger = get_run_logger()
    client = pymkts.Client(endpoint=f"http://{marketstore_url}:5993/rpc")

    logger.info("Connected to marketstore")

    try:
        param = pymkts.Params(
            instrument, "1Min", "OHLCV", limit=60 * 24 * backoff_days
        )
        reply = client.query(param)
        df = reply.first().df()
        
        assert len(df) > 0, "No data retrieved"
        
        logger.debug(f"Got {len(df)} rows")
        data = OHLCV_from_df(instrument_id, df)
        pg.send(data, "ohlcvn")
        logger.info(f"Sent all records of {instrument} to Postgres")

        return df

    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.warning(traceback.format_exc())
        logger.warning(e)
    return


@task(log_prints=True)
def ffd_stream_task(pg, instrument_id: int, resolution: Tuple[int, int, float], df: pd.DataFrame, thresh: float):
    resolution_id, resolution_seconds, fdim = resolution
    
    df = df.resample(f"{resolution_seconds}s").agg(
        {
            "Open": "first",
            "High": "max",
            "Low": "min",
            "Close": "last",
            "Volume": "sum",
            "Trades": "sum",
        }
    )
    
    ffd_df = frac_diff_ffd(df[columns], fdim, thresh)
    data = FFD_from_df(instrument_id, resolution_id, df)
    pg.send(data, "ffd")
    return ffd_df


@flow(log_prints=True, task_runner=ConcurrentTaskRunner())
def stream_flow(pg, instrument_id: int, instrument: str, resolutions: List[Tuple[int, int, float]], backoff_days: int, thresh: float):
    df = ohlcvn_stream_task(pg, instrument_id, instrument, backoff_days)
    for resolution in resolutions:
        ffd_stream_task.submit(pg, instrument_id, resolution, df, thresh)


@flow(log_prints=True)
async def etl_flow(instruments: List[Tuple[int, str]], resolutions: List[Tuple[int, int, float]], backoff_days: int, thresh: float):
    logger = get_run_logger()
    pg = Connection()
    pg.connection_test()
    for instrument in instruments:
        stream_flow(pg, instrument[0], instrument[1], resolutions, backoff_days, thresh)
        logger.info(f"Updated for {instrument}")


if __name__ == "__main__":
    etl_flow.serve(
        name="features",
        tags=["features"],
        parameters={
            "instruments": [
                (1, "BINANCE_ETHUSDT"), (1, "BINANCE_ETHUSDT.P")
            ],
            "resolutions": [
                (1, 60, 0.05), (2, 300, 0.1), (3, 900, 0.15), (4, 3600, 0.2), (5, 14400, 0.25), (6, 86400, 0.3)
            ],
            "backoff_days": 1,
            "thresh": 1e-4
        },
        cron="*/1 * * * *"
    )
