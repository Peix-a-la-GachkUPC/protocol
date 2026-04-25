class TextOSConfig:
    """Cluster layout and optional per-acceptor weights (weighted majority)."""

    def __init__(self, acceptor_ids, learner_ids, weights=None):
        self.acceptor_ids = list(acceptor_ids)
        self.learner_ids = list(learner_ids)
        if weights is None:
            weights = {i: 1 for i in self.acceptor_ids}
        self.weights = dict(weights)
        self.total_weight = float(sum(self.weights[i] for i in self.acceptor_ids))
