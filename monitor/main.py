import os
import asyncio
from json import dumps
from aiodocker import Docker
import aiohttp

webhook_url = os.environ.get("SLACK_URL")


async def send_slack_message(message):
    async with aiohttp.ClientSession() as session:
        payload = {"text": message}
        async with session.post(webhook_url, json=payload) as resp:
            print("Message sent to Slack:", await resp.text())


async def async_loop(loop, docker):
    loop.create_task(
        docker.events.run(
            filters=dumps(
                {
                    "type": ["container"],
                    "event": ["die"],  # 監視するイベントを`start`から`die`に変更
                }
            )
        )
    )
    s = docker.events.subscribe(create_task=False)
    while True:
        event = await s.get()
        container_id = event["Actor"]["ID"]
        container_name = event["Actor"]["Attributes"].get("name", "unknown")
        message = f"Container {container_name} ({container_id}) has failed."
        await send_slack_message(message)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    docker = Docker()
    loop.run_until_complete(async_loop(loop, docker))
