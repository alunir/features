from io import BytesIO

import pandas as pd
import emd

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
    file: UploadFile = File(...), column: str = Form(...), max_imf: int = Form(...)
):
    b = file.file.read()
    df = pd.read_parquet(BytesIO(b), engine="pyarrow")

    imfs = emd.sift.sift(df[column].values, max_imfs=max_imf)
    imfnum = imfs.shape[1]
    columns = [f"{column}_{i}" for i in range(imfnum)]
    imfdf = pd.DataFrame(imfs, columns=columns, index=df.index)

    b = imfdf.to_parquet(engine="pyarrow")
    return Response(
        b,
        headers={"Content-Disposition": f"attachment; filename={column}_imfs.parquet"},
    )
