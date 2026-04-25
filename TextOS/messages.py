class Proposal:
    __slots__ = ("number", "value")

    def __init__(self, number, value):
        self.number = number
        self.value = value


class PrepareMsg:
    __slots__ = ("source", "proposal")

    def __init__(self, source, proposal):
        self.source = source
        self.proposal = proposal


class PrepareResponseMsg:
    __slots__ = ("source", "proposal", "highest_proposal")

    def __init__(self, source, proposal, highest_proposal_accepted):
        self.source = source
        self.proposal = proposal
        self.highest_proposal = highest_proposal_accepted


class AcceptMsg:
    __slots__ = ("source", "proposal")

    def __init__(self, source, proposal):
        self.source = source
        self.proposal = proposal


class AcceptResponseMsg:
    __slots__ = ("source", "proposal")

    def __init__(self, source, proposal):
        self.source = source
        self.proposal = proposal


class AdjustWeightsMsg:
    __slots__ = ("source", "weights")

    def __init__(self, source, weights):
        self.source = source
        self.weights = weights
