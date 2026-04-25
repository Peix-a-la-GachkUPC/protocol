import argparse
import asyncio
import importlib.util
import json
import logging
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
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


class EditConsensus:
    def __init__(
        self,
        node_id: str,
        logger: logging.Logger,
        emit_to_bridge: Callable[[Any], None],
    ) -> None:
        self.node_id = node_id
        self.logger = logger
        self.emit_to_bridge = emit_to_bridge
        self.last_timestamp = 0
        self.best_proposal: Proposal | None = None
        self.active_proposal = False
        self.active_proposal_votes: set[str] = set()
        self.pending_local_edits: deque[Any] = deque()
        self.active_local_edit: Any | None = None

    def on_plugin_edit(self, data: Any) -> None:
        self.pending_local_edits.append(data)
        self.logger.debug(
            "Queued local edit pending=%d active=%s",
            len(self.pending_local_edits),
            self.active_proposal,
        )
        self._try_start_next_local_proposal()

    def on_network_raw(self, raw: str) -> None:
        try:
            envelope = json.loads(raw)
        except json.JSONDecodeError:
            self.emit_to_bridge(raw)
            return

        if not isinstance(envelope, dict):
            self.emit_to_bridge(raw)
            return

        message_type = envelope.get("type")
        if message_type == "proposal":
            proposal = self._proposal_from_dict(envelope)
            if proposal is None:
                return
            self._handle_proposal(proposal)
            return

        if message_type == "consensus":
            consensus = self._consensus_from_dict(envelope)
            if consensus is None:
                return
            self._handle_consensus(consensus)
            return

        self.emit_to_bridge(raw)

    def _proposal_from_dict(self, data: dict[str, Any]) -> Proposal | None:
        try:
            return Proposal(
                tstamp=int(data["tstamp"]),
                origin=str(data["origin"]),
                data=data["data"],
            )
        except (KeyError, TypeError, ValueError):
            return None

    def _consensus_from_dict(self, data: dict[str, Any]) -> Consensus | None:
        try:
            return Consensus(
                tstamp=int(data["tstamp"]),
                voter=str(data["voter"]),
                accepted=bool(data["accepted"]),
            )
        except (KeyError, TypeError, ValueError):
            return None

    def _handle_proposal(self, proposal: Proposal) -> None:
        self.last_timestamp = max(self.last_timestamp, proposal.tstamp)
        best_tstamp = self.best_proposal.tstamp if self.best_proposal is not None else 0
        self.logger.debug(
            "Received proposal tstamp=%d origin=%s active=%s best_tstamp=%d",
            proposal.tstamp,
            proposal.origin,
            self.active_proposal,
            best_tstamp,
        )

        if not self.active_proposal or self.best_proposal is None:
            self._accept_proposal(proposal)
            self._try_commit_active_proposal()
        elif self._is_better(proposal, self.best_proposal):
            self._on_local_proposal_superseded()
            self._send_consensus(self.best_proposal, accepted=False)
            self._accept_proposal(proposal)
            self._try_commit_active_proposal()
        else:
            self._send_consensus(proposal, accepted=False)

    def _handle_consensus(self, consensus: Consensus) -> None:
        best_tstamp = self.best_proposal.tstamp if self.best_proposal is not None else 0
        self.logger.debug(
            "Received consensus tstamp=%d voter=%s accepted=%s active=%s best_tstamp=%d",
            consensus.tstamp,
            consensus.voter,
            consensus.accepted,
            self.active_proposal,
            best_tstamp,
        )

        if not self.active_proposal or self.best_proposal is None:
            self.logger.debug("Ignoring consensus because no active proposal")
            return

        if consensus.tstamp != self.best_proposal.tstamp:
            self.logger.debug(
                "Ignoring consensus for different proposal tstamp=%d expected=%d",
                consensus.tstamp,
                self.best_proposal.tstamp,
            )
            return

        if consensus.accepted:
            self.active_proposal_votes.add(consensus.voter)
            self.logger.debug(
                "Accepted vote count=%d required=%d tstamp=%d",
                len(self.active_proposal_votes),
                self._required_votes(),
                self.best_proposal.tstamp,
            )
            self._try_commit_active_proposal()
        else:
            self._reject_best_proposal()

    def _accept_proposal(self, proposal: Proposal) -> None:
        self._send_consensus(proposal, accepted=True)
        self.best_proposal = proposal
        self.active_proposal = True
        self.active_proposal_votes = {proposal.origin, self.node_id}

    def _on_local_proposal_superseded(self) -> None:
        if self.best_proposal is None:
            return
        if self.best_proposal.origin != self.node_id:
            return
        if self.active_local_edit is None:
            return
        self.pending_local_edits.appendleft(self.active_local_edit)
        self.active_local_edit = None

    def _is_better(self, incoming: Proposal, current: Proposal) -> bool:
        return incoming.tstamp < current.tstamp

    def _required_votes(self) -> int:
        return max(1, connection.number_of_peers() + 1)

    def _send_consensus(self, proposal: Proposal, accepted: bool) -> None:
        message = {
            "type": "consensus",
            "tstamp": proposal.tstamp,
            "voter": self.node_id,
            "accepted": accepted,
        }
        self.logger.debug(
            "Broadcast consensus tstamp=%d accepted=%s",
            proposal.tstamp,
            accepted,
        )
        connection.send(json.dumps(message, ensure_ascii=False))

    def _send_proposal(self, proposal: Proposal) -> None:
        message = {
            "type": "proposal",
            "tstamp": proposal.tstamp,
            "origin": proposal.origin,
            "data": proposal.data,
        }
        self.logger.debug("Broadcast proposal tstamp=%d origin=%s", proposal.tstamp, proposal.origin)
        connection.send(json.dumps(message, ensure_ascii=False))

    def _try_commit_active_proposal(self) -> None:
        if not self.active_proposal or self.best_proposal is None:
            return

        required_votes = self._required_votes()
        if len(self.active_proposal_votes) >= required_votes:
            self.logger.debug(
                "Committing proposal tstamp=%d votes=%d required=%d",
                self.best_proposal.tstamp,
                len(self.active_proposal_votes),
                required_votes,
            )
            self._apply_proposal(self.best_proposal)

    def _next_timestamp(self) -> int:
        now_ns = time.time_ns()
        self.last_timestamp = max(self.last_timestamp + 1, now_ns)
        return self.last_timestamp

    def _apply_proposal(self, proposal: Proposal) -> None:
        self.logger.info("Proposal committed tstamp=%d origin=%s", proposal.tstamp, proposal.origin)
        self.active_proposal = False
        self.best_proposal = None
        self.active_proposal_votes = set()

        if proposal.origin == self.node_id:
            self.active_local_edit = None
        else:
            self._rebase_local_edits_against_remote_commit(proposal)
            self.emit_to_bridge(proposal.data)
        self._try_start_next_local_proposal()

    def _reject_best_proposal(self) -> None:
        if self.best_proposal is None:
            return

        rejected = self.best_proposal
        self.logger.info("Proposal rejected tstamp=%d origin=%s", rejected.tstamp, rejected.origin)
        self.active_proposal = False
        self.best_proposal = None
        self.active_proposal_votes = set()

        if rejected.origin == self.node_id:
            self._rollback_rebase_reapply_uncommitted_locals()

        self._try_start_next_local_proposal()

    def _try_start_next_local_proposal(self) -> None:
        if self.active_proposal or not self.pending_local_edits:
            return

        payload = self.pending_local_edits.popleft()
        proposal = Proposal(
            tstamp=self._next_timestamp(),
            origin=self.node_id,
            data=payload,
        )

        self.best_proposal = proposal
        self.active_proposal = True
        self.active_proposal_votes = {self.node_id}
        self.active_local_edit = payload
        self.logger.info("Starting local proposal tstamp=%d", proposal.tstamp)
        self._send_proposal(proposal)
        self._try_commit_active_proposal()

    def _rebase_local_edits_against_remote_commit(self, proposal: Proposal) -> None:
        remote_ops = self._as_ops(proposal.data)
        if remote_ops is None:
            return

        if self.active_local_edit is not None:
            self.active_local_edit = self._rebase_payload(remote_ops, self.active_local_edit, proposal.origin)
            if self.best_proposal is not None and self.best_proposal.origin == self.node_id:
                self.best_proposal.data = self.active_local_edit

        rebased_pending: deque[Any] = deque()
        while self.pending_local_edits:
            payload = self.pending_local_edits.popleft()
            rebased_pending.append(self._rebase_payload(remote_ops, payload, proposal.origin))
        self.pending_local_edits = rebased_pending

    def _as_ops(self, payload: Any) -> list[dict[str, Any]] | None:
        if not isinstance(payload, list):
            return None

        ops: list[dict[str, Any]] = []
        for entry in payload:
            if not isinstance(entry, dict):
                return None
            if "index" not in entry:
                return None
            ops.append(dict(entry))
        return ops

    def _rebase_payload(self, remote_ops: list[dict[str, Any]], payload: Any, remote_origin: str) -> Any:
        local_ops = self._as_ops(payload)
        if local_ops is None:
            return payload

        rebased = [dict(op) for op in local_ops]
        for remote_op in remote_ops:
            for local_op in rebased:
                self._shift_local_index(remote_op, local_op, remote_origin)
        return rebased

    def _shift_local_index(self, remote_op: dict[str, Any], local_op: dict[str, Any], remote_origin: str) -> None:
        if "index" not in local_op:
            return
        if not isinstance(local_op["index"], int):
            return

        if "index" not in remote_op:
            return
        if not isinstance(remote_op["index"], int):
            return

        local_index = local_op["index"]
        remote_index = remote_op["index"]

        if "add" in remote_op and isinstance(remote_op["add"], str):
            remote_len = len(remote_op["add"])
            if remote_len <= 0:
                return

            same_index_remote_first = remote_index == local_index and remote_origin < self.node_id
            if remote_index < local_index or same_index_remote_first:
                local_op["index"] = local_index + remote_len
            return

        if "del" in remote_op and isinstance(remote_op["del"], int):
            remote_len = remote_op["del"]
            if remote_len <= 0:
                return
            if remote_index >= local_index:
                return

            shift = min(remote_len, local_index - remote_index)
            local_op["index"] = local_index - shift

    def _rollback_rebase_reapply_uncommitted_locals(self) -> None:
        uncommitted_edits = self._snapshot_uncommitted_local_edits()
        if not uncommitted_edits:
            self.active_local_edit = None
            return

        rollback_ops = self._inverse_edits(uncommitted_edits)
        if rollback_ops:
            self.logger.info("Rolling back %d uncommitted local ops", len(rollback_ops))
            self.emit_to_bridge(rollback_ops)

        self.active_local_edit = None
        self.pending_local_edits = deque(uncommitted_edits)

        reapply_ops = self._flatten_ops(uncommitted_edits)
        if reapply_ops:
            self.logger.info("Reapplying %d uncommitted local ops", len(reapply_ops))
            self.emit_to_bridge(reapply_ops)

    def _snapshot_uncommitted_local_edits(self) -> list[Any]:
        edits: list[Any] = []
        if self.active_local_edit is not None:
            edits.append(self.active_local_edit)
        edits.extend(self.pending_local_edits)
        return edits

    def _flatten_ops(self, edits: list[Any]) -> list[dict[str, Any]]:
        flattened: list[dict[str, Any]] = []
        for payload in edits:
            ops = self._as_ops(payload)
            if ops is None:
                continue
            flattened.extend(ops)
        return flattened

    def _inverse_edits(self, edits: list[Any]) -> list[dict[str, Any]]:
        inverse_ops: list[dict[str, Any]] = []
        for payload in reversed(edits):
            ops = self._as_ops(payload)
            if ops is None:
                continue

            for op in reversed(ops):
                inverse = self._inverse_op(op)
                if inverse is None:
                    self.logger.warning("Could not invert local op: %s", op)
                    continue
                inverse_ops.append(inverse)
        return inverse_ops

    def _inverse_op(self, op: dict[str, Any]) -> dict[str, Any] | None:
        if "index" not in op or not isinstance(op["index"], int):
            return None

        index = op["index"]

        if "add" in op and isinstance(op["add"], str):
            return {"index": index, "del": len(op["add"])}

        if "del" in op and isinstance(op["del"], int):
            deleted_text = op.get("deleted_text")
            if isinstance(deleted_text, str):
                return {"index": index, "add": deleted_text}
            return None

        return None


def _encode_for_network(message: Message) -> str:
    if isinstance(message, str):
        return message
    return json.dumps(message, ensure_ascii=False)


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
    http_host: str,
    http_port: int,
    logger: logging.Logger,
) -> None:
    connection.setup("HTTP", host=http_host, port=http_port)
    connection.create(connect_peer)

    bridge = ExtensionBridge(host=ws_host, port=ws_port)

    async def send_network_recv(payload: Any) -> None:
        logger.info("Applying remote edit to extension")
        await bridge.send({"type": "network_recv", "value": _encode_for_network(payload)})

    def emit_to_bridge(payload: Any) -> None:
        asyncio.create_task(send_network_recv(payload))

    consensus = EditConsensus(node_id=node_id, logger=logger, emit_to_bridge=emit_to_bridge)

    async def on_extension_message(message: Message) -> None:
        if isinstance(message, dict) and "value" in message:
            payload: Any = message["value"]
        else:
            payload = message

        consensus.on_plugin_edit(payload)

    bridge.on_message(on_extension_message)
    await bridge.start()

    logger.info("Extension bridge listening on ws://%s:%s", ws_host, ws_port)
    if connect_peer:
        logger.info("Connected via seed peer: %s", connect_peer)

    try:
        while True:
            incoming = connection.nrecv()
            if incoming is not None:
                consensus.on_network_raw(incoming)
            await asyncio.sleep(poll_interval)
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
                http_host=_require_config(config, "HTTP_HOST"),
                http_port=_require_config(config, "HTTP_PORT"),
                logger=logger,
            )
        )
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
