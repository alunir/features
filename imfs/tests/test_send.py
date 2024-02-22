import httpx
import pandas as pd
from io import BytesIO

with open("test_data_close.parquet", "rb") as f:
    b = f.read()

result = httpx.post(
    # "https://features-kfdmb7eyrq-uc.a.run.app",
    "http://localhost:8765/imfs/",
    data={
        "column": "Close",
        "max_imf": 10,
    },
    files={"file": b},
    timeout=600,
)

# print(result)

df = pd.read_parquet(BytesIO(result.content), engine="pyarrow")

print(df)
