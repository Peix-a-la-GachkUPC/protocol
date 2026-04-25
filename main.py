import argparse
import asyncio
import json
from typing import Any

from network.HTTP import connection as http_connection
from plugin.extension_bridge import ExtensionBridge, Message


def _configure_http_connection(host: str, port: int) -> None:
    http_connection.HOST_NAME = host
    http_connection.SERVER_PORT = port
    http_connection.URL = f"http://{host}:{port}"


def _encode_for_network(message: Message) -> str:
    if isinstance(message, str):
        return message
    return json.dumps(message, ensure_ascii=False)


async def run_bridge(
    ws_host: str,
    ws_port: int,
    http_host: str,
    http_port: int,
    connect_peer: str | None,
    poll_interval: float,
) -> None:
    _configure_http_connection(http_host, http_port)
    http_connection.start_server(connect_peer=connect_peer, host=http_host, port=http_port)

    bridge = ExtensionBridge(host=ws_host, port=ws_port)

    async def on_extension_message(message: Message) -> None:
        if isinstance(message, dict) and "value" in message:
            payload: Any = message["value"]
        else:
            payload = message

        http_connection.send(_encode_for_network(payload))

    bridge.on_message(on_extension_message)
    await bridge.start()

    print(f"Extension bridge listening on ws://{ws_host}:{ws_port}")
    print(f"HTTP peer server listening on http://{http_host}:{http_port}")
    if connect_peer:
        print(f"Connected via seed peer: {connect_peer}")

    try:
        while True:
            incoming = http_connection.nrecv()
            if incoming is not None:
                await bridge.send({"type": "network_recv", "value": incoming})
            await asyncio.sleep(poll_interval)
    finally:
        await bridge.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run extension<->python<->HTTP network bridge",
    )
    parser.add_argument("--ws-host", default="127.0.0.1", help="WebSocket host")
    parser.add_argument("--ws-port", type=int, default=42069, help="WebSocket port")
    parser.add_argument("--http-host", default="127.0.0.1", help="HTTP peer host")
    parser.add_argument("--http-port", type=int, default=8080, help="HTTP peer port")
    parser.add_argument(
        "--connect-peer",
        default=None,
        help="Existing peer URL (example: http://127.0.0.1:8080)",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=0.05,
        help="Polling interval in seconds for incoming network messages",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        asyncio.run(
            run_bridge(
                ws_host=args.ws_host,
                ws_port=args.ws_port,
                http_host=args.http_host,
                http_port=args.http_port,
                connect_peer=args.connect_peer,
                poll_interval=args.poll_interval,
            )
        )
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
