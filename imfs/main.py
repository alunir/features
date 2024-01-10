from logging import getLogger, StreamHandler
from io import BytesIO

import pandas as pd
from PyEMD import EMD

from fastapi import FastAPI, Form, File, UploadFile
from fastapi.responses import Response

app = FastAPI()

logger = getLogger(__name__)
logger.addHandler(StreamHandler())
logger.setLevel("INFO")


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


@app.post("/")
def root(
    file: UploadFile = File(...), column: str = Form(...), max_imfs: int = Form(...)
):
    b = file.file.read()
    df = pd.read_parquet(BytesIO(b), engine="pyarrow")

    series = df.to_numpy().reshape(len(df)).astype(float)

    emd = EMD()
    arr = emd(series, max_imf=max_imfs)
    columns = [f"{column}_{i}" for i in range(arr.shape[0])]
    logger.info(f"IMFs of {column} is calculated")
    imfs = pd.DataFrame(arr.T, columns=columns, index=df.index)

    b = imfs.to_parquet(engine="pyarrow")
    return Response(
        b,
        headers={"Content-Disposition": f"attachment; filename={column}_imfs.parquet"},
    )
