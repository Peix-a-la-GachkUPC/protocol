import argparse
import asyncio
import importlib.util
import json
import logging
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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
    def __init__(self, node_id: str, logger: logging.Logger) -> None:
        self.node_id = node_id
        self.logger = logger
        self.last_timestamp = 0
        self.best_proposal = Proposal(tstamp=0, origin="", data={})
        self.active_proposal = False
        self.active_proposal_votes: set[str] = set()
        self.pending_local_edits: deque[Any] = deque()

    def on_plugin_edit(self, data: Any) -> None:
        self.pending_local_edits.append(data)
        self.logger.debug(
            "Queued local edit pending=%d active=%s",
            len(self.pending_local_edits),
            self.active_proposal,
        )
        self._try_start_next_local_proposal()

    def on_network_raw(self, raw: str) -> Any | None:
        try:
            envelope = json.loads(raw)
        except json.JSONDecodeError:
            return raw

        if not isinstance(envelope, dict):
            return raw

        message_type = envelope.get("type")
        if message_type == "proposal":
            proposal = self._proposal_from_dict(envelope)
            if proposal is None:
                return None
            return self._on_proposal(proposal)

        if message_type == "consensus":
            consensus = self._consensus_from_dict(envelope)
            if consensus is None:
                return None
            return self._on_consensus(consensus)

        return raw

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

    def _on_proposal(self, proposal: Proposal) -> Any | None:
        self.last_timestamp = max(self.last_timestamp, proposal.tstamp)
        self.logger.debug(
            "Received proposal tstamp=%d origin=%s active=%s best_tstamp=%d",
            proposal.tstamp,
            proposal.origin,
            self.active_proposal,
            self.best_proposal.tstamp,
        )

        if self.active_proposal:
            if self._is_better(proposal, self.best_proposal):
                if self.best_proposal.tstamp > 0:
                    self._send_consensus(self.best_proposal, accepted=False)
                self._send_consensus(proposal, accepted=True)
                self.best_proposal = proposal
                self.active_proposal_votes = {proposal.origin, self.node_id}
                return self._maybe_commit_active_proposal()
            else:
                self._send_consensus(proposal, accepted=False)
            return None

        self._send_consensus(proposal, accepted=True)
        self.best_proposal = proposal
        self.active_proposal = True
        self.active_proposal_votes = {proposal.origin, self.node_id}
        return self._maybe_commit_active_proposal()

    def _on_consensus(self, consensus: Consensus) -> Any | None:
        self.logger.debug(
            "Received consensus tstamp=%d voter=%s accepted=%s active=%s best_tstamp=%d",
            consensus.tstamp,
            consensus.voter,
            consensus.accepted,
            self.active_proposal,
            self.best_proposal.tstamp,
        )
        if not self.active_proposal:
            self.logger.debug("Ignoring consensus because no active proposal")
            return None

        if consensus.tstamp != self.best_proposal.tstamp:
            self.logger.debug(
                "Ignoring consensus for different proposal tstamp=%d expected=%d",
                consensus.tstamp,
                self.best_proposal.tstamp,
            )
            return None

        if consensus.accepted:
            self.active_proposal_votes.add(consensus.voter)
            self.logger.debug(
                "Accepted vote count=%d required=%d tstamp=%d",
                len(self.active_proposal_votes),
                self._required_votes(),
                self.best_proposal.tstamp,
            )
            return self._maybe_commit_active_proposal()

        return self._reject_best_proposal()

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

    def _maybe_commit_active_proposal(self) -> Any | None:
        required_votes = self._required_votes()
        if len(self.active_proposal_votes) >= required_votes:
            self.logger.debug(
                "Committing proposal tstamp=%d votes=%d required=%d",
                self.best_proposal.tstamp,
                len(self.active_proposal_votes),
                required_votes,
            )
            return self._apply_proposal(self.best_proposal)
        return None

    def _next_timestamp(self) -> int:
        now_ns = time.time_ns()
        self.last_timestamp = max(self.last_timestamp + 1, now_ns)
        return self.last_timestamp

    def _apply_proposal(self, proposal: Proposal) -> Any | None:
        self.logger.info("Proposal committed tstamp=%d origin=%s", proposal.tstamp, proposal.origin)
        self.active_proposal = False
        self.active_proposal_votes = set()

        committed = proposal.data if proposal.origin != self.node_id else None
        self._try_start_next_local_proposal()
        return committed

    def _reject_best_proposal(self) -> Any | None:
        rejected = self.best_proposal
        self.logger.info("Proposal rejected tstamp=%d origin=%s", rejected.tstamp, rejected.origin)
        self.active_proposal = False
        self.active_proposal_votes = set()

        if rejected.origin == self.node_id:
            self.pending_local_edits.appendleft(rejected.data)

        self._try_start_next_local_proposal()
        return None

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
        self.logger.info("Starting local proposal tstamp=%d", proposal.tstamp)
        self._send_proposal(proposal)
        self._maybe_commit_active_proposal()


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
    consensus = EditConsensus(node_id=node_id, logger=logger)

    bridge = ExtensionBridge(host=ws_host, port=ws_port)

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
                applied = consensus.on_network_raw(incoming)
                if applied is not None:
                    logger.info("Applying remote edit to extension")
                    await bridge.send({"type": "network_recv", "value": _encode_for_network(applied)})
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
