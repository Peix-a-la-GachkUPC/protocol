import  network.HTTP.connection as http_connection
import network.hyperswarm.connection as hyperswarm_connection


PROTOCOL = ""

def recv() -> str:
    """Recieves data, if not data has arrived it waits for it

    Raises:
        ValueError: If incorrect protocol is chosen

    Returns:
        str: Recieved data
    """
    match (PROTOCOL):
        case ("HTTP"):
            return http_connection.recv()
        case ("hyperswarm"):
            return hyperswarm_connection.recv()
        case (_):
            raise ValueError("Protocol desconegut")

def nrecv() -> str|None:
    """Recieves data, if no data has arrived it returns None

    Raises:
        ValueError: If incorrect protocol is chosen

    Returns:
        str|None: drecieved data
    """
    match (PROTOCOL):
        case ("HTTP"):
            return http_connection.nrecv()
        case ("hyperswarm"):
            return hyperswarm_connection.nrecv()
        case (_):
            raise ValueError("Protocol desconegut")
    
def send(data:str):
    """Sends data with the chosen protocol

    Args:
        data (str): Data to be sent

    Raises:
        ValueError: If incorrect protocol is chosen
    """
    match (PROTOCOL):
        case ("HTTP"):
            http_connection.send(data)
        case ("hyperswarm"):
            hyperswarm_connection.send(data)
        case (_):
            raise ValueError("Protocol desconegut")

def number_of_peers() -> int:
    match(PROTOCOL):
        case ("HTTP"):
            return http_connection.number_of_peers()
        case ("hyperswarm"):
            return hyperswarm_connection.number_of_peers()
        case (_):
            raise ValueError("Protocol desconegut")

def setup(protocol:str, **kwargs):
    global PROTOCOL
    PROTOCOL = protocol
    match (PROTOCOL):
        case ("HTTP"):
            http_connection.setup(
                host=kwargs.get("host"),
                port=kwargs.get("port"),
            )
        case ("hyperswarm"):
            pass
        case (_):
            raise ValueError("Protocol desconegut")

def create(discover_peer:str):
    """Creates he connection to the network

    Args:
        protocol (str): The chosen protocol
        discover_peer (str): first peer to be connected to

    Raises:
        ValueError: If incorrect protocol is chosen
    """
    global PROTOCOL
    match (PROTOCOL):
        case ("HTTP"):
            http_connection.start_server(connect_peer=discover_peer)
        case ("hyperswarm"):
            hyperswarm_connection.create(discover_peer)
        case (_):
            raise ValueError("Protocol desconegut")
