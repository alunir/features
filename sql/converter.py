"""
data.csv

1,1,2017-08-17 04:00:00,301.13,301.13,301.13,301.13,128.4108659,2
2,1,2017-08-17 04:01:00,301.13,301.13,301.13,301.13,830.4773931,4
3,1,2017-08-17 04:02:00,300.0,300.0,300.0,300.0,29.79,2
4,1,2017-08-17 04:03:00,300.0,300.0,300.0,300.0,94.167,3

"""

import pandas as pd

df = pd.DataFrame([1], columns=["Instrument"])
df = pd.concat([df, pd.read_pickle("all_df.pkl")], axis=1)
print(df.columns)

# fillna with 0
df.fillna(0, inplace=True)

# find non-finite values in Number
print(df["Number"].isna().sum())
# show the rows with non-finite values in Number
print(df[df["Number"].isna()])
# convert Number to int
df.Number = df.Number.astype(int)

# sort by Epoch
df.sort_values(by="Epoch", inplace=True)
df = df.iloc[1:]
df.index = range(1, len(df) + 1)

# convert Epoch to no timezone
df["Epoch"] = df["Epoch"].dt.tz_localize(None)
df["Instrument"] = 1


df.to_csv("data.csv", index=True, header=False)
