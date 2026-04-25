import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any

network_dir = Path(__file__).resolve().parent / "network"
network_dir_s = str(network_dir)
if network_dir_s not in sys.path:
    sys.path.insert(0, network_dir_s)

import HTTP.connection as http_connection
import network.connection as network_connection
from TextOS.main import prepare_and_acknowledge
from TextOS.paxos_json import decode_envelope
from plugin.extension_bridge import ExtensionBridge, Message


def _ensure_network_connection_http_protocol() -> None:
    network_connection.PROTOCOL = "HTTP"


def _configure_http_connection(host: str, port: int, public_url: str | None) -> None:
    http_connection.HOST_NAME = host
    http_connection.SERVER_PORT = port
    if public_url:
        http_connection.URL = public_url.rstrip("/")
    elif host in ("0.0.0.0", "::"):
        http_connection.URL = f"http://127.0.0.1:{port}"
    else:
        http_connection.URL = f"http://{host}:{port}"


def _normalize_url(raw: str) -> str:
    return raw.rstrip("/")


def _cluster_urls() -> tuple[str, list[str]]:
    self_url = _normalize_url(http_connection.URL)
    peers = {_normalize_url(url) for url in http_connection.peer_list if isinstance(url, str)}
    peers.discard("")
    peers.add(self_url)
    ordered = sorted(peers)
    return self_url, ordered


def _derive_paxos_membership() -> dict[str, Any]:
    self_url, urls = _cluster_urls()
    proposer_id = f"p:{self_url}"
    acceptor_ids = [f"a:{url}" for url in urls]
    learner_ids = [f"l:{url}" for url in urls]
    return {
        "proposer_id": proposer_id,
        "acceptor_ids": acceptor_ids,
        "learner_ids": learner_ids,
        "local_acceptor_ids": [f"a:{self_url}"],
        "local_learner_ids": [f"l:{self_url}"],
    }


def _extension_value_payload(changes: list[dict[str, Any]]) -> dict[str, str]:
    return {"value": json.dumps(changes, ensure_ascii=False)}


def _is_paxos_wire(payload: str) -> bool:
    try:
        decode_envelope(payload)
        return True
    except Exception:
        return False


def _decode_commit_payload(payload: str) -> tuple[str, list[dict[str, Any]]] | None:
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return None
    if not isinstance(data, dict):
        return None
    if data.get("kind") != "textos_commit":
        return None
    logical_file = data.get("file")
    changes = data.get("changes")
    if not isinstance(logical_file, str):
        return None
    if not isinstance(changes, list) or not all(isinstance(c, dict) for c in changes):
        return None
    return logical_file, changes


def _normalize_paxos_payload(
    message: Message,
    default_file: str,
) -> tuple[str, list[dict[str, Any]]]:
    payload: Any
    if isinstance(message, dict) and "value" in message:
        payload = message["value"]
    else:
        payload = message

    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ValueError("payload string must be valid JSON") from exc

    if isinstance(payload, list):
        changes = payload
        if not changes or not all(isinstance(change, dict) for change in changes):
            raise ValueError("payload list must contain one or more command objects")
        return default_file, changes

    if not isinstance(payload, dict):
        raise ValueError("payload must be a command list or object")

    logical_file = payload.get("file", default_file)
    if not isinstance(logical_file, str) or logical_file.strip() == "":
        raise ValueError("'file' must be a non-empty string")

    changes_raw = payload.get("changes", payload)
    if isinstance(changes_raw, dict):
        changes = [changes_raw]
    elif isinstance(changes_raw, list):
        changes = changes_raw
    else:
        raise ValueError("'changes' must be a list of objects or a single object")

    if not changes:
        raise ValueError("'changes' must not be empty")
    if not all(isinstance(change, dict) for change in changes):
        raise ValueError("'changes' must only contain objects")

    return logical_file, changes


async def run_bridge(
    ws_host: str,
    ws_port: int,
    http_host: str,
    http_port: int,
    http_public_url: str | None,
    connect_peer: str | None,
    default_file: str,
    poll_interval: float,
    network_idle_loops: int,
    network_idle_sleep: float,
) -> None:
    _configure_http_connection(http_host, http_port, http_public_url)
    http_connection.start_server(
        connect_peer=connect_peer,
        host=http_host,
        port=http_port,
        public_url=http_public_url,
    )

    _ensure_network_connection_http_protocol()
    proposer_idle_sleep = network_idle_sleep if network_idle_sleep > 0 else 0.001

    bridge = ExtensionBridge(host=ws_host, port=ws_port)
    paxos_round_lock = asyncio.Lock()

    async def _run_passive_acceptor_once() -> None:
        membership = _derive_paxos_membership()
        await asyncio.to_thread(
            prepare_and_acknowledge,
            ".__paxos_passive__.py",
            [],
            proposer_id=membership["proposer_id"],
            acceptor_ids=membership["acceptor_ids"],
            learner_ids=membership["learner_ids"],
            local_acceptor_ids=membership["local_acceptor_ids"],
            local_learner_ids=[],
            start_proposer=False,
            use_network=True,
            network_idle_loops=2,
            network_idle_sleep_s=0.0,
        )

    async def on_extension_message(message: Message) -> None:
        try:
            logical_file, changes = _normalize_paxos_payload(message, default_file)
        except ValueError as exc:
            await bridge.send({"error": str(exc), "value": "[]"})
            return

        try:
            async with paxos_round_lock:
                membership = _derive_paxos_membership()
                row = await asyncio.to_thread(
                    prepare_and_acknowledge,
                    logical_file,
                    changes,
                    proposer_id=membership["proposer_id"],
                    acceptor_ids=membership["acceptor_ids"],
                    learner_ids=membership["learner_ids"],
                    local_acceptor_ids=membership["local_acceptor_ids"],
                    local_learner_ids=membership["local_learner_ids"],
                    use_network=True,
                    network_idle_loops=network_idle_loops,
                    network_idle_sleep_s=proposer_idle_sleep,
                )
            if row is not None:
                commit = json.dumps(
                    {
                        "kind": "textos_commit",
                        "file": logical_file,
                        "changes": row["changes"],
                    },
                    ensure_ascii=False,
                )
                http_connection.send(commit)
        except Exception as exc:
            await bridge.send({"error": str(exc), "file": logical_file, "value": "[]"})

    bridge.on_message(on_extension_message)
    await bridge.start()

    print(f"Extension bridge listening on ws://{ws_host}:{ws_port}")
    print(f"HTTP peer server listening on http://{http_host}:{http_port}")
    if connect_peer:
        print(f"Connected via seed peer: {connect_peer}")

    try:
        while True:
            if paxos_round_lock.locked():
                await asyncio.sleep(poll_interval)
                continue
            incoming = http_connection.nrecv()
            if incoming is not None:
                preview = incoming if len(incoming) <= 140 else (incoming[:137] + "...")
                print(f"[http] inbound data={preview!r}")
                commit = _decode_commit_payload(incoming)
                if commit is not None:
                    _, changes = commit
                    await bridge.send(_extension_value_payload(changes))
                elif _is_paxos_wire(incoming):
                    http_connection.recv_list.append(incoming)
                    await _run_passive_acceptor_once()
            await asyncio.sleep(poll_interval)
    finally:
        await bridge.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run extension<->python<->HTTP network bridge",
    )
    parser.add_argument("--ws-host", default="127.0.0.1", help="WebSocket host")
    parser.add_argument("--ws-port", type=int, default=42069, help="WebSocket port")
    parser.add_argument("--http-host", default="0.0.0.0", help="HTTP peer host")
    parser.add_argument("--http-port", type=int, default=8080, help="HTTP peer port")
    parser.add_argument(
        "--http-public-url",
        default=None,
        help="Reachable URL advertised to peers (example: http://192.168.1.5:8080)",
    )
    parser.add_argument(
        "--connect-peer",
        default=None,
        help="Existing peer URL (example: http://127.0.0.1:8080)",
    )
    parser.add_argument(
        "--default-file",
        default="shared_buffer.py",
        help="Logical file used when extension payload omits file",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=0.05,
        help="Polling interval in seconds for incoming network messages",
    )
    parser.add_argument(
        "--network-idle-loops",
        type=int,
        default=50_000,
        help="Max consecutive empty network polls for a Paxos round",
    )
    parser.add_argument(
        "--network-idle-sleep",
        type=float,
        default=0.001,
        help="Sleep in seconds per empty poll while waiting for Paxos traffic",
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
                http_public_url=args.http_public_url,
                connect_peer=args.connect_peer,
                default_file=args.default_file,
                poll_interval=args.poll_interval,
                network_idle_loops=args.network_idle_loops,
                network_idle_sleep=args.network_idle_sleep,
            )
        )
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
