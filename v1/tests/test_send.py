import httpx
import pandas as pd
from io import BytesIO

with open("test_data.parquet", "rb") as f:
    b = f.read()

result = httpx.post(
    # "https://features-kfdmb7eyrq-uc.a.run.app",
    "http://localhost:8765/v1/",
    data={
        "preread": "binance/ethusdt/202309-202401.pkl",
        "column": "Close",
        "max_imf": 8,
    },
    files={"file": b},
    timeout=600,
)

# print(result)

# 列の省略を防ぐ
pd.set_option("display.max_columns", None)

df = pd.read_parquet(BytesIO(result.content), engine="pyarrow")

print(df)
