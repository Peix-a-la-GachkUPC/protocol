import  network.HTTP.connection as http_connection
import network.hyperswarm.connection as hyperswarm_connection

try:
    import conf
except ImportError:
    conf = None


PROTOCOL = ""
_requeue_buffer: list[str] = []


def _normalize_url(raw_url: str) -> str:
    return raw_url.rstrip("/")

def recv() -> str:
    """Recieves data, if not data has arrived it waits for it

    Raises:
        ValueError: If incorrect protocol is chosen

    Returns:
        str: Recieved data
    """
    if _requeue_buffer:
        return _requeue_buffer.pop(0)
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
    if _requeue_buffer:
        return _requeue_buffer.pop(0)
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

def setup(protocol:str):
    global PROTOCOL
    PROTOCOL = protocol
    match (PROTOCOL):
        case ("HTTP"):
            http_connection.setup()
        case ("hyperswarm"):
            pass
        case (_):
            raise ValueError("Protocol desconegut")

def create(discover_peer:str|None):
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
            public_url = getattr(conf, "HTTP_PUBLIC_URL", None) if conf is not None else None
            http_connection.start_server(
                connect_peer=discover_peer,
                public_url=public_url,
            )
        case ("hyperswarm"):
            if discover_peer is not None:
                hyperswarm_connection.create(discover_peer)
        case (_):
            raise ValueError("Protocol desconegut")


def self_url() -> str:
    node_id = getattr(conf, "NETWORK_NODE_ID", None) if conf is not None else None
    if isinstance(node_id, str) and node_id.strip() != "":
        return _normalize_url(node_id)
    match (PROTOCOL):
        case ("HTTP"):
            return _normalize_url(http_connection.URL)
        case ("hyperswarm"):
            return "hyperswarm:local"
        case (_):
            raise ValueError("Protocol desconegut")


def peers() -> list[str]:
    match (PROTOCOL):
        case ("HTTP"):
            return [_normalize_url(p) for p in http_connection.peer_list if isinstance(p, str)]
        case ("hyperswarm"):
            configured = (
                getattr(conf, "NETWORK_PEER_IDENTITIES", [])
                if conf is not None
                else []
            )
            return [
                _normalize_url(p)
                for p in configured
                if isinstance(p, str) and p.strip() != ""
            ]
        case (_):
            raise ValueError("Protocol desconegut")


def requeue(data: str) -> None:
    _requeue_buffer.append(data)
