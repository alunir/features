import os
import json
import logging
from typing import List
import redis.asyncio as redis
from dataclasses import asdict


def convert_data(data: List) -> List[dict]:
    v = []
    for ohlcv in data:
        d = asdict(ohlcv)
        d["Epoch"] = ohlcv.Epoch.strftime("%Y-%m-%dT%H:%M:%SZ")
        v += [d]
    return v


class RedisStore:
    def __init__(self):
        self.redis_host = os.environ.get("REDIS_HOST", "redis")
        self.redis_port = os.environ.get("REDIS_PORT", "6379")
        self.redis_db = os.environ.get("REDIS_DB", "0")
        self.redis_pass = os.environ.get("REDIS_PASSWORD", None)
        assert self.redis_pass, "REDIS_PASSWORD is not set"

        self.r = redis.from_url(
            f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}",
            password=self.redis_pass,
        )
        logging.info(
            f"Redis connection created: redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"
        )

    async def connection_test(self):
        try:
            async with self.r as conn:
                await conn.ping()
            logging.info(
                f"Connected to Redis at {self.redis_host}:{self.redis_port} successfully."
            )
            return True
        except redis.ConnectionError as e:
            logging.error(
                f"Failed to connect to Redis at {self.redis_host}:{self.redis_port}. Error: {e}"
            )
            return False

    async def send_bulk(self, data: List, channel: str) -> None:
        """Publish data as bulk to Redis PubSub channel

        Args:
            data (List): data
            db (int): Redis database number
        """
        try:
            bulk = convert_data(data)
            await self.r.publish(channel, json.dumps(bulk))
            logging.info(
                f"Data published to Redis PubSub channel {channel} successfully."
            )
        except redis.ConnectionError as e:
            logging.error(
                f"Failed to publish data to Redis PubSub channel {channel}. Error: {e}"
            )

    async def send(self, data: List, channel: str) -> None:
        """Publish data to Redis PubSub channel

        Args:
            data (List): data
            db (int): Redis database number
        """
        try:
            for item in convert_data(data):
                await self.r.publish(channel, json.dumps(item))
            logging.info(
                f"Data published to Redis PubSub channel {channel} successfully."
            )
        except redis.ConnectionError as e:
            logging.error(
                f"Failed to publish data to Redis PubSub channel {channel}. Error: {e}"
            )
