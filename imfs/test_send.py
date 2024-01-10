import httpx
import pandas as pd
from io import BytesIO

with open("test_data_close.parquet", "rb") as f:
    b = f.read()

result = httpx.post(
    # "https://features-kfdmb7eyrq-uc.a.run.app",
    "http://localhost:8765",
    data={
        "column": "Close",
        "max_imfs": 10,
    },
    files={"file": b},
    timeout=600,
)


df = pd.read_parquet(BytesIO(result.content), engine="pyarrow")

print(df)
