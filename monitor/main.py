import os
import asyncio
import aiodocker
import aiohttp

webhook_url = os.environ.get("SLACK_URL")


async def send_slack_message(message):
    async with aiohttp.ClientSession() as session:
        payload = {"text": message}
        async with session.post(webhook_url, json=payload) as resp:
            print("Message sent to Slack:", await resp.text())


async def monitor_docker_events():
    docker = aiodocker.Docker()
    events = docker.events.subscribe()

    try:
        async with events as stream:
            async for event in stream:
                if event["Action"] == "die" and event["Type"] == "container":
                    container_id = event["Actor"]["ID"]
                    container = await docker.containers.get(container_id)
                    container_name = (
                        container._container["Names"][0]
                        if container._container.get("Names")
                        else "unknown"
                    )
                    message = f"Container {container_name} ({container_id}) has failed."
                    await send_slack_message(message)
    finally:
        await docker.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(monitor_docker_events())
