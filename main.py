import argparse
import asyncio
from collections import deque
import hashlib
import json
import logging
from urllib.parse import urlparse
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
    proposer_ids = [f"p:{key}" for key in endpoint_keys]
    acceptor_ids = [f"a:{key}" for key in endpoint_keys]
    learner_ids = [f"l:{key}" for key in endpoint_keys]
    return {
        "proposer_id": proposer_id,
        "proposer_ids": proposer_ids,
        "acceptor_ids": acceptor_ids,
        "learner_ids": learner_ids,
        "local_acceptor_ids": [f"a:{self_key}"],
        "local_learner_ids": [f"l:{self_key}"],
    }


def _membership_contains_local_roles(membership: dict[str, Any]) -> bool:
    local_acceptors = membership.get("local_acceptor_ids", [])
    local_learners = membership.get("local_learner_ids", [])
    acceptors = set(membership.get("acceptor_ids", []))
    learners = set(membership.get("learner_ids", []))
    return all(a in acceptors for a in local_acceptors) and all(
        l in learners for l in local_learners
    )


def _is_loopback_endpoint(endpoint: str) -> bool:
    host = urlparse(endpoint).hostname
    return host in {"127.0.0.1", "localhost", "::1"}


def _validate_membership(membership: dict[str, Any], *, self_endpoint: str) -> None:
    proposer_ids = list(membership.get("proposer_ids", []))
    acceptor_ids = list(membership.get("acceptor_ids", []))
    learner_ids = list(membership.get("learner_ids", []))

    if len(set(proposer_ids)) != len(proposer_ids):
        raise RuntimeError(
            "Duplicate proposer IDs derived from cluster endpoints. "
            "Set a stable unique conf.NETWORK_NODE_ID per node."
        )
    if len(set(acceptor_ids)) != len(acceptor_ids):
        raise RuntimeError(
            "Duplicate acceptor IDs derived from cluster endpoints. "
            "Set a stable unique conf.NETWORK_NODE_ID per node."
        )
    if len(set(learner_ids)) != len(learner_ids):
        raise RuntimeError(
            "Duplicate learner IDs derived from cluster endpoints. "
            "Set a stable unique conf.NETWORK_NODE_ID per node."
        )
    if not _membership_contains_local_roles(membership):
        raise RuntimeError(
            "Local Paxos role IDs do not belong to cluster membership. "
            "Set conf.NETWORK_NODE_ID to a stable public endpoint (for example "
            "http://10.0.0.12:8080) so every node derives consistent IDs."
        )
    if len(acceptor_ids) > 1 and _is_loopback_endpoint(self_endpoint):
        raise RuntimeError(
            "Cluster has remote peers but local identity is loopback "
            f"({self_endpoint}). Set conf.NETWORK_NODE_ID to your LAN/public URL."
        )


def _ballot_params(membership: dict[str, Any]) -> tuple[int, int]:
    proposer_ids = list(membership.get("proposer_ids", []))
    proposer_id = membership.get("proposer_id")
    if not proposer_ids:
        return 1, 0
    if proposer_id not in proposer_ids:
        raise RuntimeError(
            "Local proposer ID is not present in cluster proposer membership."
        )
    stride = len(proposer_ids)
    offset = proposer_ids.index(proposer_id)
    return stride, offset


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


def _map_pos_through_insert(pos: int, at: int, length: int) -> int:
    if pos >= at:
        return pos + length
    return pos


def _map_pos_through_delete(pos: int, at: int, length: int) -> int:
    end = at + length
    if pos < at:
        return pos
    if pos >= end:
        return pos - length
    return at


def _rebase_change_against_remote(change: dict[str, Any], remote: dict[str, Any]) -> dict[str, Any]:
    rebased = dict(change)

    if (
        isinstance(remote.get("insert"), str)
        and isinstance(remote.get("index"), int)
        and remote["index"] >= 0
    ):
        insert_at = remote["index"]
        insert_len = len(remote["insert"])
        if insert_len == 0:
            return rebased

        if isinstance(rebased.get("index"), int):
            rebased["index"] = _map_pos_through_insert(rebased["index"], insert_at, insert_len)

        delete_obj = rebased.get("delete")
        if isinstance(delete_obj, dict):
            start = delete_obj.get("index")
            length = delete_obj.get("delete")
            if isinstance(start, int) and isinstance(length, int) and length >= 0:
                end = start + length
                new_start = _map_pos_through_insert(start, insert_at, insert_len)
                new_end = _map_pos_through_insert(end, insert_at, insert_len)
                new_delete = dict(delete_obj)
                new_delete["index"] = new_start
                new_delete["delete"] = max(0, new_end - new_start)
                rebased["delete"] = new_delete

        return rebased

    delete_obj = remote.get("delete")
    if isinstance(delete_obj, dict):
        remote_start = delete_obj.get("index")
        remote_len = delete_obj.get("delete")
        if (
            isinstance(remote_start, int)
            and isinstance(remote_len, int)
            and remote_start >= 0
            and remote_len > 0
        ):
            if isinstance(rebased.get("index"), int):
                rebased["index"] = _map_pos_through_delete(
                    rebased["index"], remote_start, remote_len
                )

            local_delete_obj = rebased.get("delete")
            if isinstance(local_delete_obj, dict):
                start = local_delete_obj.get("index")
                length = local_delete_obj.get("delete")
                if isinstance(start, int) and isinstance(length, int) and length >= 0:
                    end = start + length
                    new_start = _map_pos_through_delete(start, remote_start, remote_len)
                    new_end = _map_pos_through_delete(end, remote_start, remote_len)
                    new_delete = dict(local_delete_obj)
                    new_delete["index"] = new_start
                    new_delete["delete"] = max(0, new_end - new_start)
                    rebased["delete"] = new_delete

    return rebased


def _rebase_changes_against_remote(
    local_changes: list[dict[str, Any]],
    remote_changes: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rebased = [dict(ch) for ch in local_changes]
    for remote in remote_changes:
        if not isinstance(remote, dict):
            continue
        rebased = [_rebase_change_against_remote(ch, remote) for ch in rebased]
    return rebased


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
    configured_node_id = getattr(conf, "NETWORK_NODE_ID", None)
    if not isinstance(configured_node_id, str) or configured_node_id.strip() == "":
        raise RuntimeError(
            "conf.NETWORK_NODE_ID is required for distributed Paxos. "
            "Set it to this node's stable endpoint, e.g. http://10.0.0.12:8080"
        )

    connection.setup(network_protocol)
    connection.create(connect_peer)

    proposer_idle_sleep = network_idle_sleep if network_idle_sleep > 0 else 0.001
    bridge = ExtensionBridge(host=ws_host, port=ws_port)
    paxos_round_lock = asyncio.Lock()
    pending_lock = asyncio.Lock()
    pending_event = asyncio.Event()
    pending_edits: deque[dict[str, Any]] = deque()
    file_revisions: dict[str, int] = {}
    self_endpoint = _normalize_endpoint(connection.self_url())
    membership = _derive_paxos_membership()
    _validate_membership(membership, self_endpoint=self_endpoint)

    async def _record_remote_commit(
        logical_file: str,
        remote_changes: list[dict[str, Any]],
    ) -> None:
        async with pending_lock:
            new_rev = file_revisions.get(logical_file, 0) + 1
            file_revisions[logical_file] = new_rev
            for item in pending_edits:
                if item["file"] != logical_file:
                    continue
                if item["base_rev"] >= new_rev:
                    continue
                item["changes"] = _rebase_changes_against_remote(
                    item["changes"],
                    remote_changes,
                )
                item["base_rev"] = new_rev

    async def _process_pending_edits() -> None:
        while True:
            await pending_event.wait()
            while True:
                async with pending_lock:
                    if not pending_edits:
                        pending_event.clear()
                        break
                    item = pending_edits.popleft()

                logical_file = item["file"]
                changes = item["changes"]

                try:
                    async with paxos_round_lock:
                        nonlocal membership
                        membership = _derive_paxos_membership()
                        _validate_membership(membership, self_endpoint=self_endpoint)
                        ballot_stride, ballot_offset = _ballot_params(membership)
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
                            ballot_stride=ballot_stride,
                            ballot_offset=ballot_offset,
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
                    await bridge.send(
                        {"error": str(exc), "file": logical_file, "value": "[]"}
                    )

    worker_task = asyncio.create_task(_process_pending_edits())

    async def _run_passive_acceptor_once() -> None:
        nonlocal membership
        membership = _derive_paxos_membership()
        _validate_membership(membership, self_endpoint=self_endpoint)
        await asyncio.to_thread(
            prepare_and_acknowledge,
            ".__paxos_passive__.py",
            [],
            proposer_id=membership["proposer_id"],
            acceptor_ids=membership["acceptor_ids"],
            learner_ids=membership["learner_ids"],
            local_acceptor_ids=membership["local_acceptor_ids"],
            local_learner_ids=membership["local_learner_ids"],
            start_proposer=False,
            use_network=True,
            network_idle_loops=max(50, network_idle_loops // 20),
            network_idle_sleep_s=0.0,
        )

    async def on_extension_message(message: Message) -> None:
        try:
            logical_file, changes = _normalize_paxos_payload(message, default_file)
        except ValueError as exc:
            await bridge.send({"error": str(exc), "value": "[]"})
            return

        async with pending_lock:
            base_rev = file_revisions.get(logical_file, 0)
            pending_edits.append(
                {
                    "file": logical_file,
                    "changes": [dict(change) for change in changes],
                    "base_rev": base_rev,
                }
            )
            pending_event.set()

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
                print(f"[connection] inbound data={preview!r}")
                membership = _derive_paxos_membership()
                _validate_membership(membership, self_endpoint=self_endpoint)
                commit = _decode_commit_payload(incoming)
                if commit is not None:
                    logical_file, changes = commit
                    await _record_remote_commit(logical_file, changes)
                    await bridge.send(_extension_value_payload(changes))
                elif _is_paxos_wire(incoming):
                    connection.requeue(incoming)
                    await _run_passive_acceptor_once()
            await asyncio.sleep(poll_interval)
    finally:
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
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
    parser.add_argument(
        "--paxos-log-level",
        choices=["none", "info", "debug"],
        default="info",
        help="Verbosity for TextOS.paxos (none silences paxos logs)",
    )
    return parser.parse_args()


def _setup_paxos_logging(level: str) -> None:
    plog = logging.getLogger("TextOS.paxos")
    if level == "none":
        plog.handlers.clear()
        plog.setLevel(logging.CRITICAL + 1)
        return
    if level == "debug":
        plog.setLevel(logging.DEBUG)
    else:
        plog.setLevel(logging.INFO)
    if not plog.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("%(levelname)s [paxos] %(message)s"))
        plog.addHandler(h)
    plog.propagate = False


def main() -> None:
    args = parse_args()
    _setup_paxos_logging(args.paxos_log_level)

    print(args.network_protocol)

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
