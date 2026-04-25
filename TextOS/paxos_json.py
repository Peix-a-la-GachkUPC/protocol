"""
JSON wire codec for TextOS Paxos messages (envelope: v, to, d) for use with
broadcast transports where each frame names the logical recipient.
"""

from __future__ import annotations

import json
from typing import Any

from TextOS.messages import (
    AcceptMsg,
    AcceptResponseMsg,
    AdjustWeightsMsg,
    PrepareMsg,
    PrepareResponseMsg,
    Proposal,
)

WIRE_VERSION = 1


def proposal_to_dict(p: Proposal) -> dict[str, Any]:
    return {"number": p.number, "value": p.value}


def proposal_from_dict(d: dict[str, Any]) -> Proposal:
    return Proposal(int(d["number"]), d["value"])


def _msg_kind_to_dict(msg) -> dict[str, Any]:
    if isinstance(msg, PrepareMsg):
        return {
            "kind": "PrepareMsg",
            "source": msg.source,
            "proposal": proposal_to_dict(msg.proposal),
        }
    if isinstance(msg, PrepareResponseMsg):
        return {
            "kind": "PrepareResponseMsg",
            "source": msg.source,
            "proposal": proposal_to_dict(msg.proposal),
            "highest_proposal": proposal_to_dict(msg.highest_proposal),
        }
    if isinstance(msg, AcceptMsg):
        return {
            "kind": "AcceptMsg",
            "source": msg.source,
            "proposal": proposal_to_dict(msg.proposal),
        }
    if isinstance(msg, AcceptResponseMsg):
        return {
            "kind": "AcceptResponseMsg",
            "source": msg.source,
            "proposal": proposal_to_dict(msg.proposal),
        }
    if isinstance(msg, AdjustWeightsMsg):
        return {
            "kind": "AdjustWeightsMsg",
            "source": msg.source,
            "weights": dict(msg.weights),
        }
    raise TypeError(f"unknown message type: {type(msg)}")


def _dict_to_msg(d: dict[str, Any]):
    kind = d["kind"]
    if kind == "PrepareMsg":
        return PrepareMsg(
            d["source"],
            proposal_from_dict(d["proposal"]),
        )
    if kind == "PrepareResponseMsg":
        return PrepareResponseMsg(
            d["source"],
            proposal_from_dict(d["proposal"]),
            proposal_from_dict(d["highest_proposal"]),
        )
    if kind == "AcceptMsg":
        return AcceptMsg(
            d["source"],
            proposal_from_dict(d["proposal"]),
        )
    if kind == "AcceptResponseMsg":
        return AcceptResponseMsg(
            d["source"],
            proposal_from_dict(d["proposal"]),
        )
    if kind == "AdjustWeightsMsg":
        return AdjustWeightsMsg(d["source"], d["weights"])
    raise ValueError(f"unknown message kind: {kind}")


def encode_envelope(to: str, msg) -> str:
    """Build a single JSON string for one logical target (envelope: v, to, d)."""
    body = _msg_kind_to_dict(msg)
    return json.dumps(
        {"v": WIRE_VERSION, "to": to, "d": body},
        separators=(",", ":"),
        sort_keys=True,
    )


def decode_envelope(s: str) -> tuple[str, object]:
    """Parse a wire string; return (to, message object). May raise on bad input."""
    data = json.loads(s)
    if not isinstance(data, dict):
        raise ValueError("envelope must be a JSON object")
    v = data.get("v")
    if v != WIRE_VERSION:
        raise ValueError(f"unsupported wire v: {v!r}")
    to = data.get("to")
    if not isinstance(to, str):
        raise ValueError("envelope to must be a str")
    d = data.get("d")
    if not isinstance(d, dict):
        raise ValueError("envelope d must be a JSON object")
    return to, _dict_to_msg(d)
