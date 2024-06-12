import os
import asyncio
from json import dumps
from aiodocker import Docker
import slackweb


slack = slackweb.Slack(url=os.environ.get("SLACK_URL"))


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
        slack.notify(text=message)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    docker = Docker()
    loop.run_until_complete(async_loop(loop, docker))
