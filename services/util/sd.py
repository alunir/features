import os
import logging
from .types import OHLCV
from typing import List
from surrealdb import Surreal
from dataclasses import asdict


class SurrealDBStore:
    def __init__(self):
        self.surrealdb_host = os.environ.get("SURREALDB_HOST", "surrealdb")
        self.surrealdb_port = os.environ.get("SURREALDB_PORT", "8000")
        self.surrealdb_ns = os.environ.get("SURREALDB_NS", "ns")
        self.surrealdb_db = os.environ.get("SURREALDB_DB", "db")
        self.surrealdb_user = os.environ.get("SURREALDB_USER", "root")
        self.surrealdb_pass = os.environ.get("SURREALDB_PASS", "root")

    async def send(self, data: List[OHLCV], table: str = "ohlcv") -> None:
        async with Surreal(
            f"ws://{self.surrealdb_host}:{self.surrealdb_port}/rpc"
        ) as db:
            await db.signin({"user": self.surrealdb_user, "pass": self.surrealdb_pass})
            await db.use(self.surrealdb_ns, self.surrealdb_db)
            await db.query(f"insert into {table} {[asdict(d) for d in data]};")

        logging.info("Sent all records")
