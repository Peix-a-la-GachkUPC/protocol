from collections import defaultdict

from TextOS.messages import (
    AcceptMsg,
    AcceptResponseMsg,
    AdjustWeightsMsg,
    PrepareMsg,
    PrepareResponseMsg,
    Proposal,
)


class BasicTextOSProtocol:
    def __init__(self, agent):
        self.agent = agent

    def have_acceptor_majority(self, acceptors):
        """
        Return True or False, depending on whether or not the passed collection
        of acceptors make up a majority.
        """
        config = self.agent.config
        majority_weight = config.total_weight / float(2)
        current_weight = sum(config.weights[i] for i in acceptors if i in config.weights)
        return current_weight > majority_weight

    def tally_outbound_msgs(self):
        if self.agent.analyzer:
            for pid in self.agent.config.acceptor_ids:
                self.agent.analyzer.add_send(pid)

    def tally_inbound_msgs(self, pid):
        if self.agent.analyzer:
            self.agent.analyzer.add_recvd(pid)

    def adjust_weights(self):
        if self.agent.analyzer:
            self.agent.analyzer.check()
            if self.agent.analyzer.weight_changed:
                weights = self.agent.analyzer.weights
                source = self.agent.pid
                msg = AdjustWeightsMsg(source, weights)
                self.agent.send_message(msg, self.agent.config.learner_ids)
                print("--RATIOS--{}".format(self.agent.analyzer.msg_ratios))
                print("--WEIGHTS--{}".format(weights))
                self.agent.analyzer.weight_changed = False


class BasicTextOSProposerProtocol(BasicTextOSProtocol):
    def __init__(self, agent, proposal):
        super().__init__(agent)
        self.request = None
        self.proposal = proposal
        self.prepare_responders = set()
        self.highest_proposal_from_promises = Proposal(-1, None)
        self.accept_responders = set()
        self.client_request_handled = False
        self.state = None
        self.PREPARE_SENT = 0
        self.ACCEPT_SENT = 1

    def handle_client_request(self, proposal):
        self.proposal = proposal
        self.request = proposal.value
        next_msg = PrepareMsg(self.agent.pid, self.proposal)
        self.agent.send_message(next_msg, self.agent.config.acceptor_ids)
        self.tally_outbound_msgs()
        self.state = self.PREPARE_SENT

    def handle_prepare_response(self, msg):
        self.prepare_responders.add(msg.source)
        self.tally_inbound_msgs(msg.source)
        if msg.highest_proposal.number > self.highest_proposal_from_promises.number:
            self.highest_proposal_from_promises = msg.highest_proposal
        if self.state == self.PREPARE_SENT:
            if self.have_acceptor_majority(self.prepare_responders):
                if self.highest_proposal_from_promises.value is not None:
                    self.proposal.value = self.highest_proposal_from_promises.value
                    self.client_request_handled = False
                else:
                    self.proposal.value = self.request
                    self.client_request_handled = True
                next_msg = AcceptMsg(self.agent.pid, self.proposal)
                self.agent.send_message(next_msg, self.agent.config.acceptor_ids)
                self.tally_outbound_msgs()
                self.state = self.ACCEPT_SENT

    def handle_accept_response(self, msg):
        self.accept_responders.add(msg.source)
        self.tally_inbound_msgs(msg.source)
        if self.have_acceptor_majority(self.accept_responders):
            self.adjust_weights()


class BasicTextOSAcceptorProtocol(BasicTextOSProtocol):
    def __init__(self, agent):
        super().__init__(agent)
        self.highest_proposal_promised = Proposal(-1, None)
        self.highest_proposal_accepted = Proposal(-1, None)

    def handle_prepare(self, msg):
        if msg.proposal.number > self.highest_proposal_promised.number:
            self.highest_proposal_promised = msg.proposal
            next_msg = PrepareResponseMsg(
                self.agent.pid, msg.proposal, self.highest_proposal_accepted
            )
            self.agent.send_message(next_msg, [msg.source])

    def handle_accept(self, msg):
        if msg.proposal.number >= self.highest_proposal_promised.number:
            self.highest_proposal_accepted = msg.proposal
            next_msg = AcceptResponseMsg(self.agent.pid, msg.proposal)
            self.agent.send_message(
                next_msg, [msg.source] + list(self.agent.config.learner_ids)
            )


class BasicTextOSLearnerProtocol(BasicTextOSProtocol):
    def __init__(self, agent):
        super().__init__(agent)
        import json

        self._json = json
        self.accept_responders = defaultdict(set)
        self.state = None
        self.RESULT_SENT = 1

    def _value_key(self, value):
        return self._json.dumps(value, sort_keys=True, default=str)

    def handle_accept_response(self, msg):
        key = self._value_key(msg.proposal.value)
        self.accept_responders[key].add(msg.source)
        if self.state == self.RESULT_SENT:
            return
        if self.have_acceptor_majority(self.accept_responders[key]):
            self.agent.log_result(msg)
            self.state = self.RESULT_SENT
