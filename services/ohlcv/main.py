import os
import re
import msgpack
import asyncio
import websockets
import pandas_gbq
import pandas as pd
import logging
import pymarketstore as pymkts

from google.oauth2 import service_account
from google.cloud import bigquery


logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())


cred = service_account.Credentials.from_service_account_file(
    "serviceaccount.json",
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)


def msg_to_df(msg: dict) -> pd.DataFrame:
    df = pd.DataFrame(
        [msg], columns=["Epoch", "Open", "High", "Low", "Close", "Volume", "Number"]
    )
    df["Epoch"] = pd.to_datetime(df["Epoch"], unit="s")
    df["Epoch"] = df["Epoch"].dt.tz_localize(None)
    df["Epoch"] = df["Epoch"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df["Open"] = df["Open"].astype(float)
    df["High"] = df["High"].astype(float)
    df["Low"] = df["Low"].astype(float)
    df["Close"] = df["Close"].astype(float)
    df["Volume"] = df["Volume"].astype(float)
    df["Number"] = df["Number"].astype(int)
    return df


def push_bigquery(df: pd.DataFrame) -> None:
    project_id = os.getenv("PROJECT_ID")
    assert project_id is not None, "PROJECT_ID is None"
    dataset_id = os.getenv("DATASET_ID")
    assert dataset_id is not None, "DATASET_ID is None"
    table_id = os.getenv("TABLE_ID")
    assert table_id is not None, "TABLE_ID is None"

    logging.debug(f"Pushing to BigQuery: {len(df)} rows")

    # Define the table ID
    destination_table = f"{project_id}.{dataset_id}.{table_id}"
    source_table = f"{project_id}.tmp.{dataset_id}-{table_id}"

    # Insert the DataFrame into the BigQuery table
    pandas_gbq.to_gbq(
        df, source_table, project_id=project_id, if_exists="replace", credentials=cred
    )

    # push to bigquery
    client = bigquery.Client(project=project_id, credentials=cred)

    # UPSERT 操作のための MERGE ステートメント
    merge_query = f"""
        MERGE INTO {destination_table} AS target
        USING {source_table} AS source
        ON target.Epoch = source.Epoch
        WHEN MATCHED THEN
            UPDATE SET target.Open = source.Open, target.High = source.High, target.Low = source.Low, target.Close = source.Close, target.Volume = source.Volume, target.Number = source.Number
        WHEN NOT MATCHED THEN
            INSERT (Epoch, Open, High, Low, Close, Volume, Number) VALUES (source.Epoch, source.Open, source.High, source.Low, source.Close, source.Volume, source.Number)
    """.format(
        destination_table=destination_table,
        source_table=source_table,
    )

    # MERGE ステートメントの実行
    client.query(merge_query)


async def main():
    marketstore_url = os.environ.get("MARKETSTORE_URL", None)

    client = pymkts.Client(endpoint=f"http://{marketstore_url}:5993/rpc")

    days = int(os.environ.get("DAYS", 1))
    ping_interval = os.environ.get("PING_INTERVAL", 120)
    ping_timeout = os.environ.get("PING_TIMEOUT", None)

    logging.info("Connected to marketstore")
    while True:
        try:
            param = pymkts.Params(
                "Binance_ETH-USDT", "1Min", "OHLCV", limit=60 * 24 * days
            )
            reply = client.query(param)
            df = reply.first().df()
            logging.info(f"Got {len(df)} rows")

            push_bigquery(df)

            pat = re.compile(r"^Binance_ETH-USDT/*")
            while True:
                connection = websockets.connect(
                    f"ws://{marketstore_url}:5993/ws",
                    ping_interval=ping_interval,
                    ping_timeout=ping_timeout,
                )
                async with connection as ws:
                    msg = msgpack.dumps(
                        {
                            "streams": ["*/*/*"],
                        }
                    )

                    await ws.send(msg)

                    logging.info("Subscribed to Binance_ETH-USDT")

                    async for message in ws:
                        msg = msgpack.loads(message)
                        key = msg.get("key")
                        if key is not None and pat.match(key):
                            data = msg["data"]
                            logging.debug(f"Received message: {data}")
                            df = msg_to_df(data)
                            push_bigquery(df)
        except KeyboardInterrupt:
            return
        except Exception as e:
            logging.warning(e)
        await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
