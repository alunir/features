import os
import asyncio
import logging
from typing import List
from datetime import datetime
from .types import Data, OHLCV, VpinOHLCV, Features202406
from surrealdb import Surreal
from dataclasses import asdict


def convert_data(data: List[Data]) -> List[dict]:
    v = []
    for ohlcv in data:
        d = asdict(ohlcv)
        d["Epoch"] = ohlcv.Epoch.strftime("%Y-%m-%dT%H:%M:%SZ")
        v += [d]
    return v


class SurrealDBStore:
    def __init__(self):
        self.surrealdb_host = os.environ.get("SURREALDB_HOST", "surrealdb")
        self.surrealdb_port = os.environ.get("SURREALDB_PORT", "8000")
        self.surrealdb_ns = os.environ.get("SURREALDB_NS", "ns")
        self.surrealdb_db = os.environ.get("SURREALDB_DB", "db")
        self.surrealdb_user = os.environ.get("SURREALDB_USER", "root")
        self.surrealdb_pass = os.environ.get("SURREALDB_PASS", "root")

    async def connection_test(self):
        while True:
            try:
                async with Surreal(
                    f"ws://{self.surrealdb_host}:{self.surrealdb_port}/rpc"
                ) as db:
                    await db.signin(
                        {"user": self.surrealdb_user, "pass": self.surrealdb_pass}
                    )
                    await db.use(self.surrealdb_ns, self.surrealdb_db)
            except Exception as e:
                logging.info(f"Failed to connect to SurrealDB: {e}")
                await asyncio.sleep(5)
                continue

            logging.info("Connected to SurrealDB")
            break

    async def send(self, data: List[Data], table: str) -> None:
        logging.debug(f"Inserting {len(data)} rows into SurrealDB")
        async with Surreal(
            f"ws://{self.surrealdb_host}:{self.surrealdb_port}/rpc"
        ) as db:
            await db.signin({"user": self.surrealdb_user, "pass": self.surrealdb_pass})
            await db.use(self.surrealdb_ns, self.surrealdb_db)
            await db.query(f"insert into {table} {convert_data(data)};")

    async def get_last_epoch(self, table: str) -> List:
        async with Surreal(
            f"ws://{self.surrealdb_host}:{self.surrealdb_port}/rpc"
        ) as db:
            await db.signin({"user": self.surrealdb_user, "pass": self.surrealdb_pass})
            await db.use(self.surrealdb_ns, self.surrealdb_db)
            resp = await db.query(
                f"select Epoch from {table} order by Epoch desc limit 1;"
            )
            return resp[0]["result"]

    # async def subscribe(self, table: str, last_epoch: str):
    #     async with Surreal(
    #         f"ws://{self.surrealdb_host}:{self.surrealdb_port}/rpc"
    #     ) as db:
    #         await db.signin({"user": self.surrealdb_user, "pass": self.surrealdb_pass})
    #         await db.use(self.surrealdb_ns, self.surrealdb_db)
    #         return await db.live(table)
    # await db.query(f"LIVE SELECT * FROM {table} WHERE epoch > '{last_epoch}';")

    # async def get_target_from_last_source(
    #     self, target: str, source: str
    # ) -> List[OHLCV]:
    #     async with Surreal(
    #         f"ws://{self.surrealdb_host}:{self.surrealdb_port}/rpc"
    #     ) as db:
    #         await db.signin({"user": self.surrealdb_user, "pass": self.surrealdb_pass})
    #         await db.use(self.surrealdb_ns, self.surrealdb_db)
    #         return await db.query(
    #             f"select * from {target} where epoch > (select epoch from {source} order by epoch desc limit 1) order by epoch asc;"
    #         )
