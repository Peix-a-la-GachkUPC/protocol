"""
Multi-host harness for the TextOS Paxos wire + ``network.connection`` (HTTP) transport.

Run from the repository root (``protocol/``) so that ``import network`` works::

  # Terminal 1 (bootstrap node; add your machine IP:port)
  python -m TextOS.network_multihost_test --public-url http://192.168.0.10:9000 --port 9000

  # Terminal 2
  python -m TextOS.network_multihost_test --public-url http://192.168.0.11:9001 --port 9001 \\
      --discover http://192.168.0.10:9000

``--public-url`` is what other peers will call; use ``0.0.0.0`` for bind (default) so the
server listens on all interfaces. For one node to run a full in-process ``run_synod`` with
``use_network=True``, that node's ``peer_list`` must include *this* node's ``public-url`` so
that ``send`` can loop back into this process's ``recv_list``; use ``--include-self`` (default).

**Limitation:** the bundled HTTP layer in ``network/HTTP/connection.py`` is minimal (GET /data
only). Full consensus split across *only one role per host* is not implemented in TextOS; this
script runs the *whole* proposer+acceptors+learner in the **current** process, while still using
the real HTTP path so you can test multi-node connectivity and the JSON wire. For a true
one-role-per-node deployment you would add a process-level router that calls
``dispatch_paxos_wire`` and drive state across hosts manually.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import urllib.parse
from pathlib import Path

# ``HTTP`` lives under network/; ensure it is importable (same as network.connection).
_root = Path(__file__).resolve().parents[1]
_net_root = _root / "network"
if str(_net_root) not in sys.path:
    sys.path.insert(0, str(_net_root))


def _patch_http_identity(
    public_base: str, bind_host: str, port: int
) -> None:
    import HTTP.connection as h  # type: ignore[import-not-found, import-untyped]

    h.URL = public_base.rstrip("/")
    h.SERVER_PORT = port
    h.HOST_NAME = bind_host


def _include_self_in_peers(public_base: str) -> None:
    import HTTP.connection as h  # type: ignore[import-not-found, import-untyped]

    b = public_base.rstrip("/")
    if b not in h.peer_list:
        h.peer_list.append(b)


def _patch_network_send() -> None:
    """
    ``network.connection.send`` (library bug) points at ``HTTP.connection.recv``; rewire
    to real ``HTTP.connection.send`` so this harness can run without editing ``network/``.
    """
    import HTTP.connection as h  # type: ignore[import-not-found, import-untyped]
    import network.connection as nc  # type: ignore[import-not-found, import-untyped]

    def _send(data: str) -> None:
        h.send(data)

    nc.send = _send  # type: ignore[method-assign]


def _start_stack(discover: str | None) -> None:
    import network.connection as nc  # type: ignore[import-not-found, import-untyped]

    if nc.PROTOCOL not in ("", "HTTP"):
        raise SystemExit("network.connection.PROTOCOL must be empty or 'HTTP' before start.")
    _patch_network_send()
    nc.PROTOCOL = "HTTP"
    if discover is not None:
        nc.create(discover)
    else:
        import HTTP.connection as h  # type: ignore[import-not-found, import-untyped]

        h.start_server(connect_peer=None, host=h.HOST_NAME, port=h.SERVER_PORT)
    time.sleep(0.4)


def _listen_loop() -> None:
    import network.connection as nc  # type: ignore[import-not-found, import-untyped]

    from TextOS.paxos_json import decode_envelope

    print("Listen mode: printing decoded TextOS wire frames (Ctrl+C to stop).", flush=True)
    while True:
        s = nc.nrecv()
        if s is None:
            time.sleep(0.02)
            continue
        try:
            to, msg = decode_envelope(s)
            print("to =", to, "msg =", type(msg).__name__, flush=True)
        except Exception as e:  # noqa: BLE001
            print("raw / decode error:", repr(s)[:200], e, flush=True)


def _run_paxos_demo(logical_file: str) -> None:
    from TextOS.main import prepare_and_acknowledge

    row = prepare_and_acknowledge(
        logical_file,
        [{"insert": " multihost", "index": 0}],
        use_network=True,
        network_idle_loops=50_000,
        network_idle_sleep_s=0.001,
    )
    print("prepare_and_acknowledge result:", row, flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Start HTTP network layer and optionally run a TextOS Paxos round over it.",
    )
    ap.add_argument(
        "--public-url",
        default=os.environ.get("TEXTOS_PUBLIC_URL", "http://127.0.0.1:9000"),
        help="Reachable base URL of this process (other peers and /connect?url=).",
    )
    ap.add_argument(
        "--bind",
        default="0.0.0.0",
        help="Address to bind the HTTP server (default all interfaces).",
    )
    ap.add_argument(
        "--port",
        type=int,
        default=None,
        help="Port if not present in --public-url (default 80 for http, 443 for https).",
    )
    ap.add_argument(
        "--discover",
        default=None,
        help="A peer base URL to join (GET /get_peers, then /connect for mesh).",
    )
    ap.add_argument(
        "--include-self",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Append --public-url to HTTP peer_list (needed for in-process loopback on send).",
    )
    ap.add_argument(
        "--mode",
        choices=["paxos", "listen"],
        default="paxos",
        help="paxos: one full in-process run_synod with use_network. listen: only print inbound wire.",
    )
    ap.add_argument(
        "--file",
        default=".__multihost_test__.py",
        help="Logical path for the TextOS log (mapped under .textos like other documents).",
    )
    args = ap.parse_args()

    pub = args.public_url.strip()
    if not pub.lower().startswith(("http://", "https://")):
        pub = "http://" + pub

    parsed = urllib.parse.urlparse(pub)
    port = args.port
    if port is None:
        if parsed.port is not None:
            port = parsed.port
        else:
            port = 443 if (parsed.scheme or "http") == "https" else 80
    if parsed.port is not None and args.port is not None and int(parsed.port) != int(args.port):
        ap.error("--port and port inside --public-url must match if both are set.")

    _patch_http_identity(pub, args.bind, port)
    _start_stack(args.discover)
    if args.include_self:
        _include_self_in_peers(pub)

    if args.mode == "listen":
        _listen_loop()
        return 0

    if args.mode == "paxos":
        _run_paxos_demo(args.file)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
