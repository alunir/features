import os
import emd
import time
import traceback
import pandas as pd
from enum import Enum

import uvicorn
from fastapi import FastAPI
from typing import List, Dict, Optional
from pydantic import BaseModel
from util.types import OHLCV, FFD, EMD, PremiumIndex
from util.pg_sync import Connection
import pymarketstore as pymkts

from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from prefect.futures import wait
from prefect.utilities.annotations import quote
from mlfinlab.features.fracdiff import frac_diff_ffd


marketstore_url = os.environ.get("MARKETSTORE_URL", None)

# TODO: remove Volume, Trades from columns
columns = ["Open", "High", "Low", "Close", "Volume", "Trades"]

app = FastAPI()


class OhlcvtSource(Enum):
    MarketStore = "market_store"
    Postgres = "postgres"


# 解像度を表すEnumの定義
class Resolution(Enum):
    # 秒数で解像度を定義
    OneMin = 60
    FiveMin = 300
    FifteenMin = 900
    ThirtyMin = 1800
    OneHour = 3600
    FourHour = 14400
    OneDay = 86400

    @classmethod
    def from_string(cls, name: str) -> "Resolution":
        """文字列から対応するEnumを取得"""
        name_mapping = {
            "1Min": cls.OneMin,
            "5Min": cls.FiveMin,
            "15Min": cls.FifteenMin,
            "30Min": cls.ThirtyMin,
            "1H": cls.OneHour,
            "4H": cls.FourHour,
            "1D": cls.OneDay,
        }
        return name_mapping[name]

    def to_string(self) -> str:
        """Enumを対応する文字列表現に変換"""
        string_mapping = {
            Resolution.OneMin: "1Min",
            Resolution.FiveMin: "5Min",
            Resolution.FifteenMin: "15Min",
            Resolution.ThirtyMin: "30Min",
            Resolution.OneHour: "1H",
            Resolution.FourHour: "4H",
            Resolution.OneDay: "1D",
        }
        return string_mapping[self]


class Instrument(BaseModel):
    ID: int
    Name: str


# premium_indexを計算するためのペア
class InstrumentPair(BaseModel):
    Primary: Instrument
    Secondary: Instrument


InstrumentUnion = Instrument | InstrumentPair


class Request(BaseModel):
    resolution: str
    inst: List[InstrumentUnion]
    source: Optional[OhlcvtSource] = OhlcvtSource.MarketStore
    thresh: Optional[float] = None
    fdim: Optional[float] = None
    max_imfs: Optional[int] = None
    backoff_ticks: Optional[int] = None


class Parameter(BaseModel):
    ohlcvt_source: OhlcvtSource
    thresh: float
    fdim: float
    max_imfs: int
    backoff_ticks: int


# サンプルデータ
resolutions: List[Resolution] = [
    Resolution.OneMin,
    Resolution.FiveMin,
    Resolution.FifteenMin,
    Resolution.ThirtyMin,
    Resolution.OneHour,
    Resolution.FourHour,
    Resolution.OneDay,
]

# デフォルトのパラメータ
DEFAULT_PARAMETERS = {
    "thresh": 1e-4,
    "fdim": 0.3,
    "max_imfs": 16,
    "backoff_ticks": 24 * 60 * 7,  # 1 week
}


# パラメータの例
Parameters = Dict[Resolution, Parameter]  # パラメータのリスト


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


def FFD_from_df(instrument_id: int, resolution: Resolution, fdim: float, df: pd.DataFrame) -> List[FFD]:
    if "Epoch" not in df.columns:
        df = df.reset_index().rename(columns={"index": "Epoch"})
    v: List[FFD] = []
    for row in df.itertuples():
        d = FFD(
            instrument_id,
            resolution.to_string(),
            fdim,
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


def EMD_from_df(instrument_id: int, resolution: Resolution, fdim: float, df: pd.DataFrame):
    if "Epoch" not in df.columns:
        df = df.reset_index().rename(columns={"index": "Epoch"})
    v: List[EMD] = []
    for row in df.itertuples():
        d = EMD(
            instrument_id,
            resolution.to_string(),
            fdim,
            row.Epoch.to_pydatetime(),
            row.imf_0,
            row.imf_1,
            row.imf_2,
            row.imf_3,
            row.imf_4,
            row.imf_5,
            row.imf_6,
            row.imf_7,
            row.imf_8,
            row.imf_9,
            row.imf_10,
            row.imf_11,
            row.imf_12,
            row.imf_13,
            row.imf_14,
            row.imf_15,
            row.if_0,
            row.if_1,
            row.if_2,
            row.if_3,
            row.if_4,
            row.if_5,
            row.if_6,
            row.if_7,
            row.if_8,
            row.if_9,
            row.if_10,
            row.if_11,
            row.if_12,
            row.if_13,
            row.if_14,
            row.if_15,
            row.ia_0,
            row.ia_1,
            row.ia_2,
            row.ia_3,
            row.ia_4,
            row.ia_5,
            row.ia_6,
            row.ia_7,
            row.ia_8,
            row.ia_9,
            row.ia_10,
            row.ia_11,
            row.ia_12,
            row.ia_13,
            row.ia_14,
            row.ia_15,
            row.ip_0,
            row.ip_1,
            row.ip_2,
            row.ip_3,
            row.ip_4,
            row.ip_5,
            row.ip_6,
            row.ip_7,
            row.ip_8,
            row.ip_9,
            row.ip_10,
            row.ip_11,
            row.ip_12,
            row.ip_13,
            row.ip_14,
            row.ip_15,
        )
        v += [d]
    return v


def PremiumIndex_from_df(instrument_id1: int, instrument_id2: int, resolution: Resolution, df: pd.DataFrame):
    if "Epoch" not in df.columns:
        df = df.reset_index().rename(columns={"index": "Epoch"})
    v: List[PremiumIndex] = []
    for row in df.itertuples():
        d = PremiumIndex(
            instrument_id1,
            instrument_id2,
            resolution.to_string(),
            row.Epoch.to_pydatetime(),
            row.premium_index,
        )
        v += [d]
    return v


@task(log_prints=True)
def ohlcvt_stream_task(pg, resolution: Resolution, inst: Instrument, ohlcvt_source: OhlcvtSource, backoff_ticks: int):
    logger = get_run_logger()
    
    try:
        if ohlcvt_source == OhlcvtSource.MarketStore:
            client = pymkts.Client(endpoint=f"http://{marketstore_url}:5993/rpc")

            logger.info(f"Connected to marketstore for {inst} at {resolution}")

            param = pymkts.Params(
                inst.Name, "1Min", "OHLCV", limit=backoff_ticks * resolution.value / 60
            )
            reply = client.query(param)
            df = reply.first().df().resample(f"{resolution.value}s").agg({
                "Open": "first",
                "High": "max",
                "Low": "min",
                "Close": "last",
                "Volume": "sum",
                "Trades": "sum",
            })
            
            assert len(df) > 0, "No data retrieved"
            
            logger.info(f"Got {len(df)} rows")
            data = OHLCV_from_df(inst.ID, df)
            pg.send(data, "ohlcvt")
            logger.info(f"Sent all records of {inst.Name} to Postgres")
        
        elif ohlcvt_source == OhlcvtSource.Postgres:
            data = pg.fetch_all_by_inst("ohlcvt", inst.ID)
            assert len(data) > 0, "No data retrieved"
            
            columns = ['instrument_id', 'Epoch', 'Open', 'High', 'Low', 'Close', 'Volume', 'Trades']
            # Convert to DataFrame
            df = pd.DataFrame(data, columns=columns)
            # Convert Decimal columns to float after creation
            decimal_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            for col in decimal_columns:
                df[col] = df[col].apply(float)
            # Set 'Epoch' as the index
            df.set_index('Epoch', inplace=True)
            
            df = pd.DataFrame(df).resample(f"{resolution.value}s").agg({
                "Open": "first",
                "High": "max",
                "Low": "min",
                "Close": "last",
                "Volume": "sum",
                "Trades": "sum",
            })
            logger.info(f"Got {len(df)} rows")

        else:
            logger.warning(f"Invalid source: {ohlcvt_source}")
            raise ValueError(f"Invalid source: {ohlcvt_source}")

        return df

    except Exception as e:
        print(e)
        logger.warning(traceback.format_exc())
        logger.warning(e)
        raise e


@task(log_prints=True)
def ffd_stream_task(pg, resolution: Resolution, instrument_id: int, p: Parameter, df: pd.DataFrame):
    df = df.resample(f"{resolution.value}s").agg(
        {
            "Open": "first",
            "High": "max",
            "Low": "min",
            "Close": "last",
            "Volume": "sum",
            "Trades": "sum",
        }
    )
    
    ffd_df = frac_diff_ffd(df[columns], p.fdim, p.thresh).dropna()
    data = FFD_from_df(instrument_id, resolution, p.fdim, ffd_df)
    pg.send(data, "ffd")
    return ffd_df


@task(log_prints=True)
def emd_stream_task(pg, resolution: Resolution, instrument_id: int, p: Parameter, ffd_df: pd.DataFrame):
    l: int = len(ffd_df)
    emd_df = pd.DataFrame({}, index=ffd_df.index)

    imfs = emd.sift.sift(ffd_df["Close"].values, max_imfs=p.max_imfs)
    # imfnum = min(imfs.shape[1], max_imfs)

    sample_rate = len(ffd_df.index)/((ffd_df.index[-1] - ffd_df.index[0]).seconds)
    IP, IF, IA = emd.spectra.frequency_transform(imfs, sample_rate, 'nht')

    for i in range(0, p.max_imfs):
        emd_df[f"imf_{i}"] = imfs[:, i] if imfs.shape[1] > i else [None] * l  # Intrinsic Mode Function
        emd_df[f"ip_{i}"] = IP[:, i] if IP.shape[1] > i else [None] * l  # Instantaneous Power
        emd_df[f"if_{i}"] = IF[:, i] if IF.shape[1] > i else [None] * l  # Instantaneous Frequency
        emd_df[f"ia_{i}"] = IA[:, i] if IA.shape[1] > i else [None] * l  # Instantaneous Amplitude

    data = EMD_from_df(instrument_id, resolution, p.fdim, emd_df)
    pg.send(data, "emd")
    return emd_df


@task(log_prints=True)
def premium_index_stream_task(pg, inst_pair: InstrumentPair, resolution: Resolution, primary_df: pd.DataFrame, secondary_df: pd.DataFrame):
    logger = get_run_logger()
    try:
        if not inst_pair.Secondary.Name.startswith(inst_pair.Primary.Name):
            logger.warning(f"Invalid pair: {inst_pair.Primary.Name} and {inst_pair.Secondary.Name}")
            return
        if not inst_pair.Secondary.Name.endswith(".P"):
            logger.warning(f"Invalid pair: {inst_pair.Secondary.Name} doesn't ends with '.P'")
            return

        if primary_df.empty or secondary_df.empty:
            logger.warning("Primary or secondary DataFrame is empty")
            return

        primary_df = primary_df.resample(f"{resolution.value}s").agg(
            {
                "Open": "first",
                "High": "max",
                "Low": "min",
                "Close": "last",
                "Volume": "sum",
                "Trades": "sum",
            }
        )
        secondary_df = secondary_df.resample(f"{resolution.value}s").agg(
            {
                "Open": "first",
                "High": "max",
                "Low": "min",
                "Close": "last",
                "Volume": "sum",
                "Trades": "sum",
            }
        )
        
        premium_index = (secondary_df["Close"] - primary_df["Close"]) / primary_df["Close"] * 100
        df = premium_index.dropna().to_frame(name='premium_index')
        data = PremiumIndex_from_df(inst_pair.Primary.ID, inst_pair.Secondary.ID, resolution, df)
        pg.send(data, "premium_index")
        return
    except Exception as e:
        logger.error(traceback.format_exc())
        raise e


@task(log_prints=True)
def calc_features_task(pg, p: Parameter, resolution: Resolution, df: pd.DataFrame, instrument_id: int):
    ffd_df = ffd_stream_task.submit(pg, resolution, instrument_id, p, df)
    emd_df = emd_stream_task.submit(pg, resolution, instrument_id, p, ffd_df.result())
    return emd_df


@flow(log_prints=True, task_runner=ConcurrentTaskRunner())
def etl_flow(resolution: Resolution, inst: InstrumentUnion, p: Parameter):
    logger = get_run_logger()
    
    pg = Connection()
    pg.connection_test()
    logger.info("Connected to Postgres")
    
    tasks = []
    if isinstance(inst, Instrument):
        df_task = ohlcvt_stream_task.submit(pg, resolution, inst, p.ohlcvt_source, p.backoff_ticks)
        feature_task = calc_features_task.submit(pg, p, resolution, df_task.result(), inst.ID)
        tasks.append(feature_task)
    elif isinstance(inst, InstrumentPair):
        primary_task = ohlcvt_stream_task.submit(pg, resolution, inst.Primary, p.ohlcvt_source, p.backoff_ticks)
        secondary_task = ohlcvt_stream_task.submit(pg, resolution, inst.Secondary, p.ohlcvt_source, p.backoff_ticks)
        feature_task_primary = calc_features_task.submit(pg, p, resolution, primary_task.result(), inst.Primary.ID)
        feature_task_secondary = calc_features_task.submit(pg, p, resolution, secondary_task.result(), inst.Secondary.ID)
        premium_task = premium_index_stream_task.submit(pg, inst, resolution, primary_task.result(), secondary_task.result())
        tasks.extend([feature_task_primary, feature_task_secondary, premium_task])
    else:
        logger.warning(f"Invalid type: {type(inst)}")
    wait(tasks)
    logger.info("Updated for all instruments")


@app.post("/")
def root(req: Request):
    start = time.time()
    
    resolution = Resolution.from_string(req.resolution)
    
    if req.inst is None:
        return {"message": "No 'inst' in request"}
    if len(req.inst) == 0:
        return {"message": "No instruments"}
    if len(req.inst) > 2:
        return {"message": "Too many instruments"}
    if len(req.inst) == 1:
        inst = req.inst[0]
    if len(req.inst) == 2:
        inst = InstrumentPair(
            Primary=req.inst[0],
            Secondary=req.inst[1]
        )

    thresh = DEFAULT_PARAMETERS["thresh"] if req.thresh is None else req.thresh
    max_imfs = DEFAULT_PARAMETERS["max_imfs"] if req.max_imfs is None else req.max_imfs
    fdim = DEFAULT_PARAMETERS["fdim"] if req.fdim is None else req.fdim
    backoff_ticks = DEFAULT_PARAMETERS["backoff_ticks"] if req.backoff_ticks is None else req.backoff_ticks

    params: Parameters = {resol: Parameter(
        ohlcvt_source=req.source, fdim=fdim, max_imfs=max_imfs, thresh=thresh, backoff_ticks=backoff_ticks
    ) for resol in resolutions}

    etl_flow(
        resolution,
        inst,
        params[resolution],
    )
    
    end_time = time.time()

    return {
        "message": "Success",
        "resolution": resolution,
        "inst": inst,
        "elapsed_time": end_time - start,
    }


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
