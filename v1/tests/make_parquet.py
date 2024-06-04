import pandas as pd

pd.read_pickle("./history/binance/ethusdt/202309-202401.pkl").to_parquet(
    "test_data.parquet", engine="pyarrow"
)
