import sys
import pandas as pd

"""
data.csv should be like

1,2017-08-17 04:00:00,301.13,301.13,301.13,301.13,128.4108659,2
1,2017-08-17 04:01:00,301.13,301.13,301.13,301.13,830.4773931,4
1,2017-08-17 04:02:00,300.0,300.0,300.0,300.0,29.79,2
1,2017-08-17 04:03:00,300.0,300.0,300.0,300.0,94.167,3

"""


if len(sys.argv) != 4:
    print(f"Usage: python new_converter.py ohlcvn all_df.pkl/csv data.csv but the number of arguments was {len(sys.argv)}")
    sys.exit(1)

table, input_file, output_file = sys.argv[1:]

assert table in ["ohlcvn"], f"Invalid table: {table}"


if input_file.endswith(".pkl"):
    df = pd.read_pickle(input_file)
elif input_file.endswith(".csv"):
    df = pd.read_csv(input_file)
    df = df.rename(columns={
        "instrument": "Instrument",
        "epoch": "Epoch",
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume",
        "number": "Number"
    })
    df = df[["Instrument", "Epoch", "Open", "High", "Low", "Close", "Volume", "Number"]]

# fillna with 0
df.fillna(0, inplace=True)

# find non-finite values in Number
print(df["Number"].isna().sum())
# show the rows with non-finite values in Number
print(df[df["Number"].isna()])

# convert Number to int
if table == "ohlcvn":
    df.Number = df.Number.astype(int)
else:
    df.Number = df.Number.astype(float)

# match table:
#     case "ohlcvn":
#     case _:

# sort by Epoch
df.sort_values(by="Epoch", inplace=True)

# if table == "ohlcvn":
#     df = df.iloc[1:]

# convert Epoch to no timezone
df["Epoch"] = pd.to_datetime(df["Epoch"]).dt.tz_localize(None)

# if mode != "ohlcv":
#     df["VPIN"] = 1

# dummy = [0, 0, "2017-08-17 04:00:00", 301.13, 301.13, 301.13, 301.13, 128.4108659, 2]
# df = pd.concat([pd.DataFrame([dummy], columns=df.columns), df], axis=0)
# df.index += 1
# df.reset_index(inplace=True)
# df.drop("index", axis=1, inplace=True)
# df = df.iloc[1:]
df.to_csv(output_file, index=False, header=False)
