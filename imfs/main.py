import logging
from io import BytesIO

import pandas as pd
from PyEMD import EMD
from pydantic import BaseModel

from fastapi import FastAPI

app = FastAPI()


class ImfsRequest(BaseModel):
    parquet: bytes
    column: str
    max_imfs: int


@app.post("/")
def root(req: ImfsRequest):
    df = pd.read_parquet(BytesIO(req.parquet), engine="pyarrow")

    emd = EMD()
    arr = emd(df.to_numpy(), max_imf=req.max_imf)
    columns = [f"{req.column}_{i}" for i in range(arr.shape[0])]
    logging.debug(f"imfs of {req.column} is calculated")
    imfs = pd.DataFrame(arr.T, columns=columns, index=df.index)

    return imfs.to_parquet(engine="pyarrow")
