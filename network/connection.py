from network.HTTP import connection as HTTP_connection
from network.Bluetooth import connection as Bluetooth_connection
from network.hyperswarm import connection as hyperswarm_connection

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
            return HTTP_connection.recv()
        case ("hyperswarm"):
            return hyperswarm_connection.recv()
        case ("Bluetooth"):
            return Bluetooth_connection.recv()
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
            return HTTP_connection.nrecv()
        case ("hyperswarm"):
            return hyperswarm_connection.nrecv()
        case ("Bluetooth"):
            return Bluetooth_connection.nrecv()
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
            HTTP_connection.send(data)
        case ("hyperswarm"):
            hyperswarm_connection.send(data)
        case ("Bluetooth"):
            Bluetooth_connection.send(data)
        case (_):
            raise ValueError("Protocol desconegut")

def create(protocol: str, discover_peer:str):
    """Creates he connection to the network

    Args:
        protocol (str): The chosen protocol
        discover_peer (str): first peer to be connected to

    Raises:
        ValueError: If incorrect protocol is chosen
    """
    global PROTOCOL
    PROTOCOL = protocol
    match (PROTOCOL):
        case ("HTTP"):
            HTTP_connection.start_server(connect_peer=discover_peer)
        case ("hyperswarm"):
            hyperswarm_connection.create(discover_peer)
        case ("Bluetooth"):
            Bluetooth_connection.create(discover_peer)
        case (_):
            raise ValueError("Protocol desconegut")