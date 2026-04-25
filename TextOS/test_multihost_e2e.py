"""
Multi-host integration tests: two OS processes, two HTTP ports, peer mesh + Paxos wire.

Run from repository root (``protocol/``)::

  python -m unittest TextOS.test_multihost_e2e -v
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
import unittest
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _http_net_path() -> Path:
    return _repo_root() / "network"


def _ensure_net_imports() -> None:
    """Need both repo root (``import network``) and ``network/`` (``import HTTP``)."""
    root = str(_repo_root())
    netp = str(_http_net_path())
    if root not in sys.path:
        sys.path.insert(0, root)
    if netp not in sys.path:
        sys.path.insert(0, netp)


def _run_bootstrap_port(port: int) -> None:
    """Block serving HTTP: peer A (no discover)."""
    _ensure_net_imports()
    import HTTP.connection as h
    import network.connection as nc

    h.URL = f"http://127.0.0.1:{port}"
    h.SERVER_PORT = port
    h.HOST_NAME = "0.0.0.0"
    nc.PROTOCOL = "HTTP"
    h.start_server(connect_peer=None, host=h.HOST_NAME, port=h.SERVER_PORT)
    time.sleep(10_000)


def _run_join_serve(port: int, discover: str) -> None:
    """Node B: ``create("HTTP", discover)`` then keep the HTTP thread alive (daemon test)."""
    _ensure_net_imports()
    import HTTP.connection as h
    import network.connection as nc

    h.URL = f"http://127.0.0.1:{port}"
    h.SERVER_PORT = port
    h.HOST_NAME = "0.0.0.0"
    nc.create("HTTP", discover.rstrip("/"))
    time.sleep(10_000)


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    _, p = s.getsockname()
    s.close()
    return int(p)


def _sub_env() -> dict[str, str]:
    root = str(_repo_root())
    e = os.environ.copy()
    e["PYTHONPATH"] = root + os.pathsep + e.get("PYTHONPATH", "")
    return e


class TestMultihostMesh(unittest.TestCase):
    def test_two_processes_peer_list_mesh(self) -> None:
        """
        Node A: bootstrap. Node B: join A. A's /get_peers must list B; B's must list A.
        """
        import requests

        pa = _free_port()
        pb = _free_port()
        if pa == pb:
            pb = _free_port()

        root = _repo_root()
        script = str(Path(__file__).resolve())
        env = _sub_env()

        proc_a = subprocess.Popen(
            [sys.executable, script, "bootstrap", str(pa)],
            cwd=root,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        proc_b = None
        try:
            time.sleep(0.5)
            proc_b = subprocess.Popen(
                [
                    sys.executable,
                    script,
                    "join-serve",
                    str(pb),
                    f"http://127.0.0.1:{pa}",
                ],
                cwd=root,
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            time.sleep(1.0)
            a_peers = requests.get(
                f"http://127.0.0.1:{pa}/get_peers", timeout=5
            ).json()
            b_peers = requests.get(
                f"http://127.0.0.1:{pb}/get_peers", timeout=5
            ).json()
            self.assertIsInstance(a_peers, list)
            self.assertIsInstance(b_peers, list)
            a_url = f"http://127.0.0.1:{pa}"
            b_url = f"http://127.0.0.1:{pb}"
            self.assertIn(
                b_url,
                a_peers,
                f"A should know B, got a_peers={a_peers!r}",
            )
            self.assertIn(
                a_url,
                b_peers,
                f"B should know A, got b_peers={b_peers!r}",
            )
        finally:
            if proc_b is not None and proc_b.poll() is None:
                proc_b.terminate()
                try:
                    proc_b.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc_b.kill()
            proc_a.terminate()
            try:
                proc_a.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc_a.kill()



class TestMultihostPaxosScript(unittest.TestCase):
    def test_network_multihost_paxos_exits_with_row(self) -> None:
        """
        One process: full Paxos over HTTP (same as ``python -m TextOS.network_multihost_test``).
        """
        port = _free_port()
        root = _repo_root()
        r = subprocess.run(
            [
                sys.executable,
                "-m",
                "TextOS.network_multihost_test",
                "--public-url",
                f"http://127.0.0.1:{port}",
                "--port",
                str(port),
                "--mode",
                "paxos",
                "--file",
                ".__e2e_paxos__.py",
            ],
            cwd=root,
            env=_sub_env(),
            capture_output=True,
            text=True,
            timeout=60,
        )
        out = (r.stdout or "") + (r.stderr or "")
        self.assertEqual(r.returncode, 0, msg=out)
        self.assertIn("prepare_and_acknowledge result:", out)
        self.assertIn("N", out)
        self.assertIn("changes", out)


if __name__ == "__main__":
    if len(sys.argv) >= 2 and sys.argv[1] == "bootstrap" and len(sys.argv) == 3:
        _run_bootstrap_port(int(sys.argv[2]))
    elif len(sys.argv) >= 2 and sys.argv[1] == "join-serve" and len(sys.argv) == 4:
        _run_join_serve(int(sys.argv[2]), sys.argv[3])
    else:
        unittest.main()
