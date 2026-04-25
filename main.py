import argparse
import asyncio
import json
from typing import Any

from network import connection
from plugin.extension_bridge import ExtensionBridge, Message
try:
    import conf
except ImportError:
    raise ImportError("Copy conf.py.example to conf.py and configure it")


def _encode_for_network(message: Message) -> str:
    if isinstance(message, str):
        return message
    return json.dumps(message, ensure_ascii=False)


async def run_bridge(
    ws_host: str,
    ws_port: int,
    connect_peer: str | None,
    poll_interval: float,
) -> None:
    connection.setup("HTTP")
    connection.create(connect_peer)

    bridge = ExtensionBridge(host=ws_host, port=ws_port)

    async def on_extension_message(message: Message) -> None:
        if isinstance(message, dict) and "value" in message:
            payload: Any = message["value"]
        else:
            payload = message

        connection.send(_encode_for_network(payload))

    bridge.on_message(on_extension_message)
    await bridge.start()

    print(f"Extension bridge listening on ws://{ws_host}:{ws_port}")
    if connect_peer:
        print(f"Connected via seed peer: {connect_peer}")

    try:
        while True:
            incoming = connection.nrecv()
            if incoming is not None:
                preview = incoming if len(incoming) <= 140 else (incoming[:137] + "...")
                print(f"[http] inbound data={preview!r}")
                await bridge.send({"type": "network_recv", "value": incoming})
            await asyncio.sleep(poll_interval)
    finally:
        await bridge.stop()

def main() -> None:
    try:
        asyncio.run(
            run_bridge(
                ws_host=conf.WS_HOST,
                ws_port=conf.WS_PORT,
                connect_peer=conf.HTTP_CONNECT_PEER,
                poll_interval=conf.POLL_INTERVAL,
            )
        )
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
