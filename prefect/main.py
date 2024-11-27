import os
import emd
import traceback
import pandas as pd
from enum import Enum
from typing import List, Tuple
from util.types import OHLCV, FFD, EMD, PremiumIndex
from util.pg_sync import Connection
import pymarketstore as pymkts

from prefect import flow, task, get_run_logger
from prefect.futures import wait
# from prefect_multiprocess.task_runners import MultiprocessTaskRunner
from mlfinlab.features.fracdiff import frac_diff_ffd


marketstore_url = os.environ.get("MARKETSTORE_URL", None)

columns = ["Open", "High", "Low", "Close", "Volume", "Trades"]


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


class Instrument:
    ID: int
    Name: str


# premium_indexを計算するためのペア
class InstrumentPair:
    Primary: Instrument
    Secondary: Instrument


InstrumentUnion = Instrument | InstrumentPair


# 型注釈
Parameter = Tuple[Resolution, float]  # 各解像度とfdimのペア
Parameters = List[Parameter]  # パラメータのリスト

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

fdim = 0.3

# パラメータの例
parameters: Parameters = [(res, fdim) for res in resolutions]


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
def ohlcvt_stream_task(pg, inst: Instrument, backoff_ticks: int):
    logger = get_run_logger()
    client = pymkts.Client(endpoint=f"http://{marketstore_url}:5993/rpc")

    logger.info("Connected to marketstore")

    try:
        param = pymkts.Params(
            inst.Name, "1Min", "OHLCV", limit=backoff_ticks
        )
        reply = client.query(param)
        df = reply.first().df()
        
        assert len(df) > 0, "No data retrieved"
        
        logger.debug(f"Got {len(df)} rows")
        data = OHLCV_from_df(inst.ID, df)
        pg.send(data, "ohlcvt")
        logger.info(f"Sent all records of {inst.Name} to Postgres")

        return df

    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.warning(traceback.format_exc())
        logger.warning(e)
    return


@task(log_prints=True)
def ffd_stream_task(pg, instrument_id: int, parameter: Parameter, df: pd.DataFrame, thresh: float):
    resolution, fdim = parameter
    
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
    
    ffd_df = frac_diff_ffd(df[columns], fdim, thresh)
    data = FFD_from_df(instrument_id, resolution, fdim, ffd_df)
    pg.send(data, "ffd")
    return ffd_df


@task(log_prints=True)
def emd_stream_task(pg, instrument_id: int, parameter: Parameter, ffd_df: pd.DataFrame, max_imfs: int):
    resolution, fdim = parameter
    emd_df = pd.DataFrame({}, index=ffd_df.index)

    imfs = emd.sift.sift(ffd_df["Close"].values, max_imfs=max_imfs)
    imfnum = min(imfs.shape[1], max_imfs)

    sample_rate = len(ffd_df.index)/((ffd_df.index[-1] - ffd_df.index[0]).seconds)
    IP, IF, IA = emd.spectra.frequency_transform(imfs, sample_rate, 'nht')

    for i in range(0, imfnum):
        emd_df[f"ip_{i}"] = IP[:, i, None]  # Instantaneous Power
        emd_df[f"if_{i}"] = IF[:, i, None]  # Instantaneous Frequency
        emd_df[f"ia_{i}"] = IA[:, i, None]  # Instantaneous Amplitude

    data = EMD_from_df(instrument_id, resolution, fdim, emd_df)
    pg.send(data, "emd")
    return emd_df


@task(log_prints=True)
def premium_index_stream_task(pg, inst_pair: InstrumentPair, parameters: Parameters, primary_df: pd.DataFrame, secondary_df: pd.DataFrame):
    if not inst_pair.Secondary.Name.startswith(inst_pair.Primary.Name):
        return
    if not inst_pair.Secondary.Name.endswith(".P"):
        return
    
    # primary_df = "BINANCE_ETHUSDT", secondary_df = "BINANCE_ETHUSDT.P" を想定
    premium_index = (secondary_df["Close"] - primary_df["Close"]) / primary_df["Close"] * 100
    
    for parameter in parameters:
        resolution, _ = parameter
        
        df = premium_index.resample(f"{resolution.value}s").agg(
            {
                "Open": "first",
                "High": "max",
                "Low": "min",
                "Close": "last",
                "Volume": "sum",
                "Trades": "sum",
            }
        )
        
        data = PremiumIndex_from_df(inst_pair.Primary.ID, inst_pair.Secondary.ID, resolution, df)
        pg.send(data, "premium_index")
    return


@task(log_prints=True)
def calc_features_task(parameter: Parameter, df: pd.DataFrame, pg, instrument_id: int, thresh: float, max_imfs: int):
    ffd_df = ffd_stream_task.submit(pg, instrument_id, parameter, df, thresh)
    emd_df = emd_stream_task.submit(pg, instrument_id, parameter, ffd_df, max_imfs)
    return emd_df


@flow(log_prints=True)
def etl_flow_inst_pair(pg, inst_pair: InstrumentPair, parameter: Parameter, backoff_ticks: int, thresh: float, max_imfs: int):
    primary_df = ohlcvt_stream_task.submit(pg, inst_pair.Primary, backoff_ticks)
    secondary_df = ohlcvt_stream_task.submit(pg, inst_pair.Secondary, backoff_ticks)

    tasks = []
    tasks += [premium_index_stream_task.submit(pg, inst_pair, parameter, primary_df, secondary_df)]
    tasks += [calc_features_task.submit(parameter, primary_df, pg, inst_pair.Primary.ID, thresh, max_imfs)]
    tasks += [calc_features_task.submit(parameter, secondary_df, pg, inst_pair.Secondary.ID, thresh, max_imfs)]
    wait(tasks)


@flow(log_prints=True)
async def etl_flow(instruments: List[InstrumentUnion], params: Parameters, backoff_ticks: int, thresh: float, max_imfs: int):
    logger = get_run_logger()
    pg = Connection()
    pg.connection_test()
    
    tasks = []
    for inst in instruments:
        if isinstance(inst, Instrument):
            df = ohlcvt_stream_task.submit(pg, inst, backoff_ticks)
            tasks += [calc_features_task.submit(parameter, df, pg, inst.ID, thresh, max_imfs) for parameter in params]
        elif isinstance(inst, InstrumentPair):
            tasks += [etl_flow_inst_pair.submit(pg, inst, parameter, backoff_ticks, thresh, max_imfs) for parameter in params]
        else:
            logger.warning(f"Invalid type: {type(inst)}")
        logger.info(f"Updated for {inst}")
    wait(tasks)


if __name__ == "__main__":
    etl_flow.serve(
        name="features",
        tags=["features"],
        parameters={
            "instruments": [
                InstrumentPair(
                    Instrument(1, "BINANCE_BTCUSDT"),
                    Instrument(2, "BINANCE_BTCUSDT.P")
                ),
                InstrumentPair(
                    Instrument(3, "BINANCE_ETHUSDT"),
                    Instrument(4, "BINANCE_ETHUSDT.P")
                ),
                InstrumentPair(
                    Instrument(5, "BYBIT_BTCUSDT"),
                    Instrument(6, "BYBIT_BTCUSDT.P")
                ),
                InstrumentPair(
                    Instrument(7, "BYBIT_ETHUSDT"),
                    Instrument(8, "BYBIT_ETHUSDT.P")
                ),
                Instrument(9, "TVC_US02Y"),
                Instrument(10, "TVC_US10Y"),
                Instrument(11, "TVC_USOIL"),
                Instrument(12, "TVC_VIX"),
                Instrument(13, "TVC_GOLD"),
                Instrument(14, "FXCM_SPX500"),
                Instrument(15, "FXCM_US30"),
                Instrument(16, "FXCM_USDJPY"),
                Instrument(17, "FXCM_EURUSD"),
            ],
            "params": parameters,
            "backoff_ticks": 24 * 60 * 1,
            "thresh": 1e-4,
            "max_imfs": 16
        },
        cron="*/1 * * * *"
    )
