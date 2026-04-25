import hashlib
import json
import logging
import subprocess
import sys
from pathlib import Path

from TextOS.config import TextOSConfig
from TextOS.messages import Proposal
from TextOS.simulation import run_synod

_log = logging.getLogger("TextOS.paxos")

TEXTOS_DIR = Path(".textos")

# P2P-safe: unlikely to clash with user "hola.txt" or editor temp files.
PAXOS_BALLOT_CACHE_NAME = ".__p2p__textos__ballot__cache__.json"


def storage_filename_for_logical(logical_file: str) -> str:
    """
    Map a logical document path (as used by peers, e.g. ``hola.py``) to the
    on-disk log name under ``TEXTOS_DIR``.

    * ``hola.py`` → ``hola.txt.json``
    * ``src/hola.py`` → ``hola.<hash10>.txt.json`` (same stem in different dirs)
    """
    p = Path(logical_file)
    stem = p.stem
    parent = p.parent
    if parent in (Path("."), Path("")):
        return f"{stem}.txt.json"
    digest = hashlib.sha256(p.as_posix().encode("utf-8")).hexdigest()[:10]
    return f"{stem}.{digest}.txt.json"


_resolved_ballot_cache_path: Path | None = None


def _payload_is_ballot_cache(data) -> bool:
    if not isinstance(data, dict):
        return False
    for k, v in data.items():
        if not isinstance(k, str):
            return False
        try:
            int(v)
        except (TypeError, ValueError):
            return False
    return True


def _file_is_ballot_cache(path: Path) -> bool:
    if not path.is_file():
        return False
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return False
    return _payload_is_ballot_cache(data)


def _iter_ballot_cache_path_candidates():
    base = PAXOS_BALLOT_CACHE_NAME
    yield TEXTOS_DIR / base
    stem = Path(base).stem
    for i in range(1, 64):
        yield TEXTOS_DIR / f"{stem}._{i}.json"
    h = hashlib.sha256(PAXOS_BALLOT_CACHE_NAME.encode("utf-8")).hexdigest()[:16]
    yield TEXTOS_DIR / f".__p2p_ballot_shadow__{h}__.json"


def _resolve_ballot_cache_path() -> Path:
    """
    Pick the first usable path: missing (new), or an existing valid cache JSON.
    If the canonical name is a directory or a non-cache file, try the next
    candidates so we never clobber foreign data and all peers use the same order.
    """
    global _resolved_ballot_cache_path
    if _resolved_ballot_cache_path is not None:
        p = _resolved_ballot_cache_path
        if not p.exists():
            return p
        if p.is_file() and (_file_is_ballot_cache(p) or p.stat().st_size == 0):
            return p
        _resolved_ballot_cache_path = None
    TEXTOS_DIR.mkdir(parents=True, exist_ok=True)
    for p in _iter_ballot_cache_path_candidates():
        if p.is_dir():
            continue
        if not p.exists():
            _resolved_ballot_cache_path = p
            return p
        if p.is_file() and _file_is_ballot_cache(p):
            _resolved_ballot_cache_path = p
            return p
    for n in range(256):
        h = hashlib.sha256(f"{PAXOS_BALLOT_CACHE_NAME}:{n}".encode()).hexdigest()[:12]
        p = TEXTOS_DIR / f".__p2p_ballot_fallback__{h}__.json"
        if p.is_dir():
            continue
        if not p.exists() or (p.is_file() and _file_is_ballot_cache(p)):
            _resolved_ballot_cache_path = p
            return p
    raise RuntimeError("TextOS: no free ballot cache path under TEXTOS_DIR")


def _load_ballot_cache() -> dict[str, int]:
    path = _resolve_ballot_cache_path()
    if not path.is_file():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if _payload_is_ballot_cache(data):
            return {str(k): int(v) for k, v in data.items()}
    except (json.JSONDecodeError, OSError, TypeError, ValueError):
        pass
    return {}


def _save_ballot_cache(cache: dict[str, int]) -> None:
    path = _resolve_ballot_cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(cache, fh, indent=2, sort_keys=True)
    tmp.replace(path)


def _max_n_in_log(data: list) -> int:
    if not data:
        return 0
    return max(int(row.get("N", 0) or 0) for row in data)


def _resolve_store_key(logical_file: str) -> str:
    return storage_filename_for_logical(logical_file)


def _path_for_store_key(store_key: str) -> Path:
    return TEXTOS_DIR / store_key


def load_TextOS(file: str) -> list:
    store_key = _resolve_store_key(file)
    path = _path_for_store_key(store_key)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def save_TextOS(file: str, data: list) -> None:
    store_key = _resolve_store_key(file)
    path = _path_for_store_key(store_key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)
    m = _max_n_in_log(data)
    cache = _load_ballot_cache()
    if m > cache.get(store_key, 0):
        cache[store_key] = m
        _save_ballot_cache(cache)


def _next_ballot(
    logical_file: str,
    data: list,
    *,
    ballot_stride: int = 1,
    ballot_offset: int = 0,
) -> int:
    store_key = _resolve_store_key(logical_file)
    m_disk = _max_n_in_log(data)
    cache = _load_ballot_cache()
    m_cached = cache.get(store_key, 0)
    m = max(m_disk, m_cached)
    if m_disk > m_cached:
        cache[store_key] = m_disk
        _save_ballot_cache(cache)
    if ballot_stride < 1:
        raise ValueError("ballot_stride must be >= 1")
    if ballot_offset < 0 or ballot_offset >= ballot_stride:
        raise ValueError("ballot_offset must satisfy 0 <= ballot_offset < ballot_stride")

    candidate = m + 1
    if ballot_stride == 1 and ballot_offset == 0:
        return candidate

    return candidate + ((ballot_offset - (candidate % ballot_stride)) % ballot_stride)


def prepare_and_acknowledge(
    file: str,
    changes: list[dict],
    *,
    proposer_id: str = "p1",
    acceptor_ids: list[str] | None = None,
    learner_ids: list[str] | None = None,
    local_acceptor_ids: list[str] | None = None,
    local_learner_ids: list[str] | None = None,
    start_proposer: bool = True,
    use_network: bool = False,
    network_idle_loops: int = 10_000,
    network_idle_sleep_s: float = 0.0,
    ballot_stride: int = 1,
    ballot_offset: int = 0,
) -> dict | None:
    """
    One Paxos instance: prepare + accept; persists to the log for ``file`` (logical
    path, mapped to ``{stem}.txt.json``) when the learner sees a weighted majority
    of matching accepts. Ballot high-water is cached in ``PAXOS_BALLOT_CACHE_NAME``
    to avoid O(n) scans on large logs (reconciled with the log when merged P2P).

    When ``use_network`` is True, Paxos frames are sent via
    ``network.connection.send`` and received with ``network.connection.nrecv`` (one
    JSON envelope per logical target). The host must have configured
    ``network.connection`` (for example set ``network.connection.PROTOCOL`` to the
    value expected by the transport, start the server, and establish peers) so that
    ``send`` and ``nrecv`` are operational before calling this. For a purely local
    in-process run, leave ``use_network`` False.

    For one-role-per-host P2P deployments:

    * Configure the full cluster with ``acceptor_ids`` / ``learner_ids``.
    * Set ``local_acceptor_ids`` / ``local_learner_ids`` to roles hosted in this
      process.
    * On proposer host call with ``start_proposer=True``; on passive hosts call
      with ``start_proposer=False`` to only pump inbound/outbound Paxos traffic.
    """
    data_actual = load_TextOS(file)
    if acceptor_ids is None:
        acceptor_ids = ["a1", "a2", "a3"]
    if learner_ids is None:
        learner_ids = ["l1"]

    proposal = None
    if start_proposer:
        n = _next_ballot(
            file,
            data_actual,
            ballot_stride=ballot_stride,
            ballot_offset=ballot_offset,
        )
        proposal = Proposal(n, changes)
        _log.info(
            "round start: file=%s proposer_id=%s ballot N=%s len(changes)=%d",
            file,
            proposer_id,
            proposal.number,
            len(changes),
        )
    else:
        _log.info("passive pump")
    config = TextOSConfig(acceptor_ids, learner_ids)

    row = None

    def on_learned(msg):
        nonlocal row
        row = {"N": msg.proposal.number, "changes": msg.proposal.value}

    run_synod(
        config,
        proposer_id,
        proposal,
        acceptor_ids,
        learner_ids,
        on_learned=on_learned,
        use_network=use_network,
        network_idle_loops=network_idle_loops,
        network_idle_sleep_s=network_idle_sleep_s,
        local_acceptor_ids=local_acceptor_ids,
        local_learner_ids=local_learner_ids,
        start_proposer=start_proposer,
    )
    persisted = False
    if row is not None:
        already_seen = any(int(r.get("N", 0) or 0) == int(row["N"]) for r in data_actual)
        if not already_seen:
            data_actual.append(row)
            save_TextOS(file, data_actual)
            persisted = True
    if row is not None:
        _log.info(
            "round end: file=%s N=%s persisted=%s",
            file,
            row["N"],
            persisted,
        )
    else:
        _log.info("no value learned")
    return row
