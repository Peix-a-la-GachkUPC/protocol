import network.hyperswarm.hyperswarm as hyperswarm

hs = hyperswarm.HyperswarmInterface()

def create(topic:str):
    hs.create(topic)

def send(data:str):
    hs.send(data)

def recv():
    return hs.recv()

def nrecv():
    return hs.nrecv()

def number_of_peers() -> int:
    return hs.peer_count()