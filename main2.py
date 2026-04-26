import argparse
import asyncio
import importlib.util
import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from typing import Any, Callable

from network import connection
from plugin.extension_bridge import ExtensionBridge, Message


@dataclass
class Proposal:
    tstamp: int
    origin: str
    data: Any


@dataclass
class Consensus:
    tstamp: int
    voter: str
    accepted: bool


def _encode_for_network(message: Message) -> str:
    if isinstance(message, str):
        return message
    return json.dumps(message, ensure_ascii=False)


def _payload_preview(payload: Any, limit: int = 300) -> str:
    text = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False)
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _incoming_sort_key(payload: str, arrival_index: int) -> tuple[int, int, int]:
    try:
        envelope = json.loads(payload)
    except json.JSONDecodeError:
        return (1, 0, arrival_index)

    if not isinstance(envelope, dict):
        return (1, 0, arrival_index)

    tstamp = envelope.get("tstamp")
    if not isinstance(tstamp, int):
        return (1, 0, arrival_index)

    return (0, tstamp, arrival_index)


def _sort_incoming_payloads(payloads: list[str]) -> list[str]:
    indexed = list(enumerate(payloads))
    indexed.sort(key=lambda entry: _incoming_sort_key(entry[1], entry[0]))
    return [payload for _, payload in indexed]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run extension bridge with external config.")
    parser.add_argument(
        "--config",
        default="conf.py",
        help="Path to python config file (default: conf.py)",
    )
    return parser.parse_args()


def _build_logger(debug_protocol: bool) -> logging.Logger:
    level = logging.DEBUG if debug_protocol else logging.INFO
    logger = logging.getLogger("protocol")
    logger.setLevel(level)
    handler = logging.StreamHandler()
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def _load_config(config_path: str) -> Any:
    resolved = Path(config_path).expanduser().resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"Config file not found: {resolved}")

    spec = importlib.util.spec_from_file_location("runtime_conf", resolved)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load config from: {resolved}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _require_config(config: Any, key: str) -> Any:
    if not hasattr(config, key):
        raise ValueError(f"Missing config key: {key}")
    return getattr(config, key)


def _resolve_node_id(config: Any) -> str:
    network_node_id = _require_config(config, "NETWORK_NODE_ID")
    if network_node_id:
        return str(network_node_id)
    ws_host = _require_config(config, "WS_HOST")
    ws_port = _require_config(config, "WS_PORT")
    return f"ws://{ws_host}:{ws_port}"


async def run_bridge(
    ws_host: str,
    ws_port: int,
    connect_peer: str | None,
    poll_interval: float,
    node_id: str,
    logger: logging.Logger,
    http_host: str|None = None,
    http_port: int|None = None,
    protocol: str = "HTTP",
) -> None:
    connection.setup(protocol, host=http_host, port=http_port)
    connection.create(connect_peer)

    bridge = ExtensionBridge(host=ws_host, port=ws_port)

    async def on_extension_message(message: Message) -> None:
        logger.debug("Bridge IN payload=%s", message)
        connection.send(json.dumps({
            "tstamp": int(time.time() * 1000),
            "data": message,
        }))


    bridge.on_message(on_extension_message)
    await bridge.start()

    logger.info("Extension bridge listening on ws://%s:%s", ws_host, ws_port)
    if connect_peer:
        logger.info("Connected via seed peer: %s", connect_peer)

    try:
        history: list[dict] = []
        while True:
            while history and history[0]["tstamp"] < (time.time() - 1.0) * 1000:
                elem = history.pop(0)
                logger.debug("Bridge OUT payload=%s", _payload_preview(elem["data"]))
                await bridge.send(json.dumps(elem["data"]))

            incoming = connection.nrecv()
            if incoming is None:
                await asyncio.sleep(poll_interval)
                continue

            data = json.loads(incoming)
            if "tstamp" not in data or "data" not in data:
                logger.debug("Network IN skipped invalid payload")
                continue

            if data["tstamp"] < (time.time() - 1.0) * 1000:
                continue

            tstamp = data["tstamp"]
            inserted = False
            for i in range(len(history) - 1, -1, -1):
                if history[i]["tstamp"] <= tstamp:
                    history.insert(i + 1, data)
                    inserted = True
                    break
            if not inserted:
                history.insert(0, data)

    finally:
        await bridge.stop()


def main() -> None:
    args = _parse_args()
    config = _load_config(args.config)
    logger = _build_logger(bool(getattr(config, "PROTOCOL_DEBUG", False)))

    try:
        asyncio.run(
            run_bridge(
                ws_host=_require_config(config, "WS_HOST"),
                ws_port=_require_config(config, "WS_PORT"),
                connect_peer=_require_config(config, "NETWORK_CONNECT_PEER"),
                poll_interval=_require_config(config, "POLL_INTERVAL"),
                node_id=_resolve_node_id(config),
                logger=logger,
                http_host=_require_config(config, "HTTP_HOST"),
                http_port=_require_config(config, "HTTP_PORT"),
                protocol=_require_config(config, "PROTOCOL"),
            )
        )
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
