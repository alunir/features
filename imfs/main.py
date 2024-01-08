import logging
from io import BytesIO

import pandas as pd
from PyEMD import EMD
from pydantic import BaseModel

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError

app = FastAPI()


@app.exception_handler(RequestValidationError)
async def handler(request: Request, exc: RequestValidationError):
    logging.warning(exc)
    return JSONResponse(content={}, status_code=status.HTTP_422_UNPROCESSABLE_ENTITY)


class ImfsRequest(BaseModel):
    parquet: bytes
    column: str
    max_imfs: int


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
def root(req: ImfsRequest):
    try:
        df = pd.read_parquet(BytesIO(req.parquet), engine="pyarrow")

        emd = EMD()
        arr = emd(df.to_numpy(), max_imf=req.max_imf)
        columns = [f"{req.column}_{i}" for i in range(arr.shape[0])]
        logging.info(f"IMFs of {req.column} is calculated")
        imfs = pd.DataFrame(arr.T, columns=columns, index=df.index)

        return {"data": imfs.to_parquet(engine="pyarrow")}
    except Exception as e:
        logging.error(e)
        return {"error": str(e)}
