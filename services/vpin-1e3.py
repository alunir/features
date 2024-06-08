import os
import logging
import asyncio
import numpy as np
from mlfinlab.bars.dollar_imbalance_bars import compute_imbalance_bars

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())


async def main():
    while True:
        imb_df = compute_imbalance_bars(np.log1p(diff_ohlcv_df), bucket_size=1000)

        if len(imb_df) < 2:
            asyncio.sleep(60)
            continue

        imb_df = (
            imb_df.drop(columns=["Volume", "SellVolume"])
            .rename(columns={"BuyVolume": "Volume"})
            .reset_index()
            .rename(columns={"index": "Epoch"})
        )

        vpin_last_epoch = imb_df.Epoch.sorted_values().tail(1)

        asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
