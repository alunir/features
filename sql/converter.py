import sys
import pandas as pd

"""
data.csv

1,1,2017-08-17 04:00:00,301.13,301.13,301.13,301.13,128.4108659,2
2,1,2017-08-17 04:01:00,301.13,301.13,301.13,301.13,830.4773931,4
3,1,2017-08-17 04:02:00,300.0,300.0,300.0,300.0,29.79,2
4,1,2017-08-17 04:03:00,300.0,300.0,300.0,300.0,94.167,3

"""

if len(sys.argv) != 4:
    print("Usage: python converter.py ohlcv all_df.pkl data.csv")
    sys.exit(1)


mode = sys.argv[1]
input = sys.argv[2]
output = sys.argv[3]

if mode == "ohlcv":
    df = pd.DataFrame([1], columns=["Instrument"])
else:
    df = pd.DataFrame([[1, 1]], columns=["Instrument", "VPIN"])

ddf = pd.read_pickle(input)
if "Epoch" not in ddf.columns:
    ddf = ddf.reset_index().rename(columns={"index": "Epoch"})
df = pd.concat([df, ddf], axis=1)
print(df.columns)

# fillna with 0
df.fillna(0, inplace=True)

# find non-finite values in Number
print(df["Number"].isna().sum())
# show the rows with non-finite values in Number
print(df[df["Number"].isna()])
# convert Number to int
if mode == "ohlcv":
    df.Number = df.Number.astype(int)
else:
    df.Number = df.Number.astype(float)

# sort by Epoch
df.sort_values(by="Epoch", inplace=True)
if mode == "ohlcv":
    df = df.iloc[1:]

# convert Epoch to no timezone
df["Epoch"] = df["Epoch"].dt.tz_localize(None)
df["Instrument"] = 1
if mode != "ohlcv":
    df["VPIN"] = 1

df.to_csv(output, index=False, header=False)
