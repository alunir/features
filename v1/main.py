from io import BytesIO
import pickle

import pandas as pd
import emd

from mlfinlab.features.fracdiff import frac_diff_ffd

from fastapi import Form, File, UploadFile, APIRouter
from fastapi.responses import Response


router = APIRouter()

"""
This function calculates the instantaneous modulation frequency (IMF) of a given time series.

Args:
    parquet (bytes): The parquet file containing the time series data.
    column (str): The name of the column containing the time series data.
    max_imfs (int): The maximum number of IMFs to calculate.

Returns:
    imfs (bytes): The parquet file containing the IMFs.

Raises:
    Exception: If an error occurs during calculation.
"""


@router.post("/")
def root(
    file: UploadFile = File(...),
    preread: str = None,
    column: str = Form(...),
    max_imf: int = Form(...),
):
    b = file.file.read()
    df = pd.read_parquet(BytesIO(b), engine="pyarrow")

    if preread is not None:
        preread_df = pickle.load(open(f"./history/{preread}", "rb"))
        concat_df = pd.concat([preread_df, df])
        df = concat_df[~concat_df.index.duplicated(keep="first")]

    features = pd.DataFrame(
        df[["Volume", "Number"]],
        columns=["Volume", "Number"],
        index=df.index,
    )

    imfs = emd.sift.sift(df[column].values, max_imfs=max_imf)
    imfnum = imfs.shape[1]

    sample_rate = 1024
    IP, IF, IA = emd.spectra.frequency_transform(imfs, sample_rate, "nht")

    for i in range(imfnum):
        features[f"imf_imf_{i}"] = imfs[:, i]
        features[f"imf_ia_{i}"] = IA[:, i, None]
        features[f"imf_ip_{i}"] = IP[:, i, None]

    fdim = 0.4
    frac_diff_ffd_df = frac_diff_ffd(
        df[["Open", "High", "Low", "Close"]], fdim, thresh=1e-4
    )
    ohlcv = pd.DataFrame(
        frac_diff_ffd_df.values.reshape(-1, 4),
        columns=["open", "high", "low", "close"],
        index=df.index,
    )

    features = features.join(ohlcv)

    b = features.to_parquet(engine="pyarrow")
    return Response(
        b,
        headers={"Content-Disposition": f"attachment; filename={column}_imfs.parquet"},
    )
