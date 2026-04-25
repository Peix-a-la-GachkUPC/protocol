import argparse
import asyncio
import hashlib
import json
from typing import Any

from network import connection
from TextOS.main import prepare_and_acknowledge
from TextOS.paxos_json import decode_envelope
from plugin.extension_bridge import ExtensionBridge, Message

try:
    import conf
except ImportError as exc:
    raise ImportError("Copy conf.py.example to conf.py and configure it") from exc


def _normalize_endpoint(raw: str) -> str:
    return raw.rstrip("/")


def _node_key(endpoint: str) -> str:
    normalized = _normalize_endpoint(endpoint)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


def _cluster_endpoints() -> tuple[str, list[str]]:
    self_endpoint = _normalize_endpoint(connection.self_url())
    peers = {
        _normalize_endpoint(endpoint)
        for endpoint in connection.peers()
        if isinstance(endpoint, str)
    }
    peers.discard("")
    peers.add(self_endpoint)
    ordered = sorted(peers)
    return self_endpoint, ordered


def _derive_paxos_membership() -> dict[str, Any]:
    self_endpoint, endpoints = _cluster_endpoints()
    self_key = _node_key(self_endpoint)
    endpoint_keys = [_node_key(endpoint) for endpoint in endpoints]
    proposer_id = f"p:{self_key}"
    acceptor_ids = [f"a:{key}" for key in endpoint_keys]
    learner_ids = [f"l:{key}" for key in endpoint_keys]
    return {
        "proposer_id": proposer_id,
        "acceptor_ids": acceptor_ids,
        "learner_ids": learner_ids,
        "local_acceptor_ids": [f"a:{self_key}"],
        "local_learner_ids": [f"l:{self_key}"],
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
    network_protocol: str,
    connect_peer: str | None,
    default_file: str,
    poll_interval: float,
    network_idle_loops: int,
    network_idle_sleep: float,
) -> None:
    connection.setup(network_protocol)
    connection.create(connect_peer)

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
                connection.send(commit)
        except Exception as exc:
            await bridge.send({"error": str(exc), "file": logical_file, "value": "[]"})

    bridge.on_message(on_extension_message)
    await bridge.start()

    print(f"Extension bridge listening on ws://{ws_host}:{ws_port}")
    if connect_peer:
        print(f"Connected via seed peer: {connect_peer}")

    try:
        while True:
            if paxos_round_lock.locked():
                await asyncio.sleep(poll_interval)
                continue

            incoming = connection.nrecv()
            if incoming is not None:
                preview = incoming if len(incoming) <= 140 else (incoming[:137] + "...")
                print(f"[connection] inbound data={preview!r}")
                commit = _decode_commit_payload(incoming)
                if commit is not None:
                    _, changes = commit
                    await bridge.send(_extension_value_payload(changes))
                elif _is_paxos_wire(incoming):
                    connection.requeue(incoming)
                    await _run_passive_acceptor_once()
            await asyncio.sleep(poll_interval)
    finally:
        await bridge.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run extension<->python<->network bridge",
    )
    parser.add_argument("--ws-host", default=conf.WS_HOST, help="WebSocket host")
    parser.add_argument("--ws-port", type=int, default=conf.WS_PORT, help="WebSocket port")
    parser.add_argument(
        "--network-protocol",
        default=getattr(conf, "NETWORK_PROTOCOL", "HTTP"),
        help="Network transport protocol (HTTP, hyperswarm, ...)",
    )
    parser.add_argument(
        "--connect-peer",
        default=getattr(conf, "NETWORK_CONNECT_PEER", None),
        help="Seed peer/topic for the selected network protocol",
    )
    parser.add_argument(
        "--default-file",
        default=getattr(conf, "DEFAULT_FILE", "shared_buffer.py"),
        help="Logical file used when extension payload omits file",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=conf.POLL_INTERVAL,
        help="Polling interval in seconds for incoming network messages",
    )
    parser.add_argument(
        "--network-idle-loops",
        type=int,
        default=getattr(conf, "NETWORK_IDLE_LOOPS", 50_000),
        help="Max consecutive empty network polls for a Paxos round",
    )
    parser.add_argument(
        "--network-idle-sleep",
        type=float,
        default=getattr(conf, "NETWORK_IDLE_SLEEP", 0.001),
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
                network_protocol=args.network_protocol,
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
