import HTTP.connection

PROTOCOL = ""

def recv():
    match (PROTOCOL):
        case ("HTTP"):
            HTTP.connection.recv()
        case (_):
            raise ValueError("Protocol desconegut")

def nrecv():
    match (PROTOCOL):
        case ("HTTP"):
            HTTP.connection.nrecv()
        case (_):
            raise ValueError("Protocol desconegut")
    
def send(data:str):
    match (PROTOCOL):
        case ("HTTP"):
            HTTP.connection.recv(data)
        case (_):
            raise ValueError("Protocol desconegut")

def create(discover_peer:str):
    match (PROTOCOL):
        case ("HTTP"):
            HTTP.connection.start_server(connect_peer=discover_peer)
        case (_):
            raise ValueError("Protocol desconegut")