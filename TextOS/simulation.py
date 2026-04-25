from collections import deque

from TextOS.messages import (
    AcceptMsg,
    AcceptResponseMsg,
    AdjustWeightsMsg,
    PrepareMsg,
    PrepareResponseMsg,
)
from TextOS.protocol import (
    BasicTextOSAcceptorProtocol,
    BasicTextOSLearnerProtocol,
    BasicTextOSProposerProtocol,
)


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


def run_synod(config, proposer_pid, proposal, acceptor_ids, learner_ids, on_learned=None):
    """
    One instance: one proposer, acceptors, learners. Blocks until message queue empty.
    Returns the learned payload (proposal.value) or None if no learner committed.
    """
    net = LocalTextOSNetwork()
    learned_box = []

    def _cb(msg):
        if on_learned:
            on_learned(msg)
        learned_box.append(msg.proposal.value)

    for aid in acceptor_ids:
        net.register(TextOSAgent(aid, config, role="acceptor"))
    for lid in learner_ids:
        net.register(TextOSAgent(lid, config, role="learner", on_learned=_cb))
    proposer = TextOSAgent(proposer_pid, config, role="proposer", proposal=proposal)
    net.register(proposer)
    proposer.proposer.handle_client_request(proposal)
    net.drain()
    return learned_box[0] if learned_box else None
