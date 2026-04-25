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


def _run_bootstrap_recv_once(port: int) -> None:
    """Peer A: bootstrap and block until one message is received."""
    _ensure_net_imports()
    import HTTP.connection as h
    import network.connection as nc

    h.URL = f"http://127.0.0.1:{port}"
    h.SERVER_PORT = port
    h.HOST_NAME = "0.0.0.0"
    nc.PROTOCOL = "HTTP"
    h.start_server(connect_peer=None, host=h.HOST_NAME, port=h.SERVER_PORT)
    msg = nc.recv()
    print(msg, flush=True)


def _run_join_send_once(port: int, discover: str, payload: str) -> None:
    """Peer B: join A, send one payload over network.connection.send, then exit."""
    _ensure_net_imports()
    import HTTP.connection as h
    import network.connection as nc

    h.URL = f"http://127.0.0.1:{port}"
    h.SERVER_PORT = port
    h.HOST_NAME = "0.0.0.0"
    nc.create("HTTP", discover.rstrip("/"))
    time.sleep(0.4)
    nc.send(payload)
    time.sleep(0.2)


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

    def test_three_processes_peer_list_full_mesh(self) -> None:
        """
        A bootstrap, B joins A, C joins A. All nodes should end with full mesh peers.
        """
        import requests

        pa = _free_port()
        pb = _free_port()
        pc = _free_port()
        used = {pa}
        while pb in used:
            pb = _free_port()
        used.add(pb)
        while pc in used:
            pc = _free_port()

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
        proc_c = None
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
            time.sleep(0.5)
            proc_c = subprocess.Popen(
                [
                    sys.executable,
                    script,
                    "join-serve",
                    str(pc),
                    f"http://127.0.0.1:{pa}",
                ],
                cwd=root,
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            time.sleep(1.2)

            a_url = f"http://127.0.0.1:{pa}"
            b_url = f"http://127.0.0.1:{pb}"
            c_url = f"http://127.0.0.1:{pc}"

            a_peers = requests.get(f"{a_url}/get_peers", timeout=5).json()
            b_peers = requests.get(f"{b_url}/get_peers", timeout=5).json()
            c_peers = requests.get(f"{c_url}/get_peers", timeout=5).json()

            self.assertIn(b_url, a_peers, f"A should know B, got {a_peers!r}")
            self.assertIn(c_url, a_peers, f"A should know C, got {a_peers!r}")
            self.assertIn(a_url, b_peers, f"B should know A, got {b_peers!r}")
            self.assertIn(c_url, b_peers, f"B should know C, got {b_peers!r}")
            self.assertIn(a_url, c_peers, f"C should know A, got {c_peers!r}")
            self.assertIn(b_url, c_peers, f"C should know B, got {c_peers!r}")
        finally:
            if proc_c is not None and proc_c.poll() is None:
                proc_c.terminate()
                try:
                    proc_c.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc_c.kill()
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

    def test_connection_send_delivers_payload_to_peer(self) -> None:
        """
        Validate network.connection.send -> HTTP /data delivery across two processes.
        """
        pa = _free_port()
        pb = _free_port()
        if pa == pb:
            pb = _free_port()

        payload = "hello-from-send"
        root = _repo_root()
        script = str(Path(__file__).resolve())
        env = _sub_env()

        proc_a = subprocess.Popen(
            [sys.executable, script, "bootstrap-recv-once", str(pa)],
            cwd=root,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        proc_b = None
        try:
            time.sleep(0.6)
            proc_b = subprocess.Popen(
                [
                    sys.executable,
                    script,
                    "join-send-once",
                    str(pb),
                    f"http://127.0.0.1:{pa}",
                    payload,
                ],
                cwd=root,
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            proc_b.wait(timeout=10)
            self.assertEqual(proc_b.returncode, 0)

            out_a, err_a = proc_a.communicate(timeout=10)
            self.assertEqual(proc_a.returncode, 0, msg=err_a)
            self.assertIn(payload, out_a)
        finally:
            if proc_b is not None and proc_b.poll() is None:
                proc_b.terminate()
                try:
                    proc_b.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc_b.kill()
            if proc_a.poll() is None:
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
    elif len(sys.argv) >= 2 and sys.argv[1] == "bootstrap-recv-once" and len(sys.argv) == 3:
        _run_bootstrap_recv_once(int(sys.argv[2]))
    elif len(sys.argv) >= 2 and sys.argv[1] == "join-send-once" and len(sys.argv) == 5:
        _run_join_send_once(int(sys.argv[2]), sys.argv[3], sys.argv[4])
    else:
        unittest.main()
