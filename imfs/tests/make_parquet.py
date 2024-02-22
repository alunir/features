import pandas as pd

pd.read_csv("./test_data.csv").to_parquet("test_data_close.parquet", engine="pyarrow")
