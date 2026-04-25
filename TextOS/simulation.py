import logging
from collections import deque
from json import JSONDecodeError
from time import sleep

from TextOS.messages import (
    AcceptMsg,
    AcceptResponseMsg,
    AdjustWeightsMsg,
    PrepareMsg,
    PrepareResponseMsg,
)
from TextOS.paxos_json import decode_envelope, encode_envelope
from TextOS.protocol import (
    BasicTextOSAcceptorProtocol,
    BasicTextOSLearnerProtocol,
    BasicTextOSProposerProtocol,
)

log = logging.getLogger("TextOS.paxos")


def _ids_repr(ids: list[str], *, max_len: int = 200) -> str:
    s = str(ids)
    if len(s) <= max_len:
        return s
    return s[: max_len - 3] + "..."


class LocalTextOSNetwork:
    """In-process FIFO delivery: each send_message schedules deliveries until idle."""

    def __init__(self):
        self._agents = {}
        self._queue = deque()

    def register(self, agent):
        self._agents[agent.pid] = agent
        agent._network = self

    def enqueue(self, targets, msg):
        for t in targets:
            self._queue.append((t, msg))

    def drain(self):
        while self._queue:
            pid, msg = self._queue.popleft()
            agent = self._agents.get(pid)
            if agent is not None:
                agent.dispatch(msg)


def _import_network():
    import network.connection as nc  # type: ignore[import-not-found, import-untyped]

    return nc


class NetworkBroadcastTextOSNetwork:
    """
    Outbound: each (target, message) is encoded with ``encode_envelope`` and sent
    via ``network.connection.send`` (broadcast layer; the ``to`` field routes at the app).
    Inbound: ``drain_quiescent`` polls ``nrecv`` and dispatches to ``agents[to]``.
    """

    def __init__(self):
        self._agents = {}
        self._nc = None
        self._local_queue = deque()

    @property
    def nc(self):
        if self._nc is None:
            self._nc = _import_network()
        return self._nc

    def register(self, agent):
        self._agents[agent.pid] = agent
        agent._network = self

    def enqueue(self, targets, msg):
        for t in targets:
            local_agent = self._agents.get(t)
            if local_agent is not None:
                self._local_queue.append((t, msg))
                log.debug("local: to=%s %s", t, type(msg).__name__)
                continue
            payload = encode_envelope(t, msg)
            self.nc.send(payload)
            log.info("wire out: to=%s %s", t, type(msg).__name__)

    def _drain_local(self) -> int:
        processed = 0
        while self._local_queue:
            pid, msg = self._local_queue.popleft()
            agent = self._agents.get(pid)
            if agent is not None:
                agent.dispatch(msg)
                processed += 1
        return processed

    def drain_quiescent(
        self,
        is_done_fn,
        *,
        max_idle_loops: int = 10_000,
        idle_sleep_s: float = 0.0,
    ):
        """
        Poll ``nrecv`` until ``is_done_fn()`` is true, or there is no more traffic
        and at least ``max_idle_loops`` consecutive empty polls. Malformed wire strings
        are skipped.
        """
        idle = 0
        while not is_done_fn() and idle < max_idle_loops:
            batch = self._drain_local()
            if is_done_fn():
                return
            while True:
                s = self.nc.nrecv()
                if s is None:
                    break
                try:
                    to, pmsg = decode_envelope(s)
                except (JSONDecodeError, TypeError, ValueError, KeyError):
                    continue
                log.info("wire in: to=%s %s", to, type(pmsg).__name__)
                agent = self._agents.get(to)
                if agent is not None:
                    agent.dispatch(pmsg)
                else:
                    log.warning(
                        "wire drop: unknown recipient to=%s (%s); local agents=%s",
                        to,
                        type(pmsg).__name__,
                        sorted(self._agents.keys()),
                    )
                batch += 1
            if is_done_fn():
                return
            if batch == 0:
                idle += 1
                if idle_sleep_s > 0:
                    sleep(idle_sleep_s)
            else:
                idle = 0


def dispatch_paxos_wire(agents: dict, wire: str) -> bool:
    """
    Apply one inbound JSON string to a pid -> TextOSAgent map. True if a message
    was dispatched, False on skip/error.
    """
    try:
        to, pmsg = decode_envelope(wire)
    except (JSONDecodeError, TypeError, ValueError, KeyError):
        return False
    ag = agents.get(to)
    if ag is None:
        return False
    ag.dispatch(pmsg)
    return True


class TextOSAgent:
    def __init__(
        self,
        pid,
        config,
        *,
        role,
        proposal=None,
        analyzer=None,
        on_learned=None,
    ):
        self.pid = pid
        self.config = config
        self.analyzer = analyzer
        self._network = None
        self._on_learned = on_learned
        self.proposer = None
        self.acceptor = None
        self.learner = None
        if role == "proposer":
            if proposal is None:
                raise ValueError("proposer requires proposal")
            self.proposer = BasicTextOSProposerProtocol(self, proposal)
        elif role == "acceptor":
            self.acceptor = BasicTextOSAcceptorProtocol(self)
        elif role == "learner":
            self.learner = BasicTextOSLearnerProtocol(self)
        else:
            raise ValueError("role must be proposer, acceptor, or learner")

    def send_message(self, msg, targets):
        self._network.enqueue(targets, msg)

    def dispatch(self, msg):
        if isinstance(msg, PrepareMsg) and self.acceptor:
            self.acceptor.handle_prepare(msg)
        elif isinstance(msg, PrepareResponseMsg) and self.proposer:
            self.proposer.handle_prepare_response(msg)
        elif isinstance(msg, AcceptMsg) and self.acceptor:
            self.acceptor.handle_accept(msg)
        elif isinstance(msg, AcceptResponseMsg):
            if self.proposer:
                self.proposer.handle_accept_response(msg)
            if self.learner:
                self.learner.handle_accept_response(msg)
        elif isinstance(msg, AdjustWeightsMsg):
            pass

    def log_result(self, msg):
        if self._on_learned:
            self._on_learned(msg)


def run_synod(
    config,
    proposer_pid,
    proposal,
    acceptor_ids,
    learner_ids,
    on_learned=None,
    *,
    use_network: bool = False,
    network_idle_loops: int = 10_000,
    network_idle_sleep_s: float = 0.0,
    local_acceptor_ids: list[str] | None = None,
    local_learner_ids: list[str] | None = None,
    start_proposer: bool = True,
):
    """
    One instance: one proposer, acceptors, learners. Blocks until quiescent.

    * In-process: ``use_network`` is False (default); the local queue is drained
      synchronously.
    * When ``use_network`` is True, outbounds go through ``network.connection.send``;
      ``network.connection.nrecv`` is polled in ``drain_quiescent`` until a learner
      commits (``on_learned``) or the poll budget is exhausted. The application must
      have configured ``network.connection`` (e.g. ``PROTOCOL`` and server) for
      the transport to work.

    Distributed mode knobs (for one-role-per-host deployment):

    * ``local_acceptor_ids``: subset of ``acceptor_ids`` instantiated in *this*
      process (default: all).
    * ``local_learner_ids``: subset of ``learner_ids`` instantiated in *this*
      process (default: all).
    * ``start_proposer=False``: do not instantiate/send from proposer; the node
      behaves as a passive acceptor/learner pump for this call.
    """
    if use_network:
        net: LocalTextOSNetwork | NetworkBroadcastTextOSNetwork = (
            NetworkBroadcastTextOSNetwork()
        )
    else:
        net = LocalTextOSNetwork()
    learned_box: list = []
    learned_flag = [False]

    def _cb(msg):
        if on_learned:
            on_learned(msg)
        learned_box.append(msg.proposal.value)
        learned_flag[0] = True

    run_acceptor_ids = (
        acceptor_ids if local_acceptor_ids is None else list(local_acceptor_ids)
    )
    run_learner_ids = (
        learner_ids if local_learner_ids is None else list(local_learner_ids)
    )
    log.info(
        "synod start: use_network=%s start_proposer=%s proposer_id=%s "
        "local_acceptors=%d %s local_learners=%d %s acceptors=%s learners=%s",
        use_network,
        start_proposer,
        proposer_pid,
        len(run_acceptor_ids),
        _ids_repr(list(run_acceptor_ids)),
        len(run_learner_ids),
        _ids_repr(list(run_learner_ids)),
        _ids_repr(list(acceptor_ids)),
        _ids_repr(list(learner_ids)),
    )

    for aid in run_acceptor_ids:
        if aid in config.acceptor_ids:
            net.register(TextOSAgent(aid, config, role="acceptor"))
    for lid in run_learner_ids:
        if lid in config.learner_ids:
            net.register(TextOSAgent(lid, config, role="learner", on_learned=_cb))

    if start_proposer:
        if proposer_pid is None:
            raise ValueError("proposer_pid is required when start_proposer=True")
        if proposal is None:
            raise ValueError("proposal is required when start_proposer=True")
        proposer = TextOSAgent(proposer_pid, config, role="proposer", proposal=proposal)
        net.register(proposer)
        proposer.proposer.handle_client_request(proposal)
    if use_network:
        assert isinstance(net, NetworkBroadcastTextOSNetwork)
        net.drain_quiescent(
            (lambda: learned_flag[0]) if start_proposer else (lambda: False),
            max_idle_loops=network_idle_loops,
            idle_sleep_s=network_idle_sleep_s,
        )
    else:
        assert isinstance(net, LocalTextOSNetwork)
        net.drain()
    log.info("synod end: learned=%s", learned_flag[0])
    return learned_box[0] if learned_box else None
