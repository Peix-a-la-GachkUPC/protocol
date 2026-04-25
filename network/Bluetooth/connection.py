import socket
import json
import threading
import select

CHANNEL = 4
SERVICE_UUID = "00001101-0000-1000-8000-00805F9B34FB"

peer_list = []
recv_list = []
local_mac = ""

def get_local_mac() -> str:
    """Get the local Bluetooth MAC address.
    
    Returns:
        str: MAC address of the local adapter
    """
    try:
        with socket.socket(socket.AF_BLUETOOTH, socket.SOCK_STREAM, socket.BTPROTO_RFCOMM) as s:
            s.bind((socket.BDADDR_ANY, socket.BDADDR_ANY))
            return ":".join(f"{b:02X}" for b in s.getsockname()[0])
    except Exception:
        return "00:00:00:00:00:00"

def parse_message(data: bytes) -> tuple[str, dict]:
    """Parse incoming message in the format 'COMMAND:param1=value1&param2=value2'.
    
    Args:
        data (bytes): Raw message data
        
    Returns:
        tuple: (command, params dict)
    """
    try:
        decoded = data.decode("utf-8").strip()
        if ":" not in decoded:
            return decoded, {}
        command, params_str = decoded.split(":", 1)
        params = {}
        if params_str:
            for param in params_str.split("&"):
                if "=" in param:
                    key, value = param.split("=", 1)
                    params[key] = value
        return command, params
    except Exception:
        return "", {}

def encode_message(command: str, params: dict = None) -> bytes:
    """Encode message to bytes in format 'COMMAND:param1=value1&param2=value2'.
    
    Args:
        command (str): Command name
        params (dict): Parameters dict
        
    Returns:
        bytes: Encoded message
    """
    if params:
        param_str = "&".join(f"{k}={v}" for k, v in params.items())
        return f"{command}:{param_str}".encode("utf-8")
    return f"{command}:".encode("utf-8")

def handle_client(client_sock: socket.socket, addr: tuple):
    """Handle a client connection.
    
    Args:
        client_sock (socket): Client socket
        addr (tuple): Client address (MAC, channel)
    """
    global recv_list, peer_list
    try:
        data = client_sock.recv(4096)
        if not data:
            return
            
        command, params = parse_message(data)
        
        match command:
            case "/data":
                if "value" in params:
                    recv_list.append(params["value"])
                    client_sock.send(encode_message("OK"))
            case "/connect":
                if "mac" in params:
                    peer_list.append(params["mac"])
                    client_sock.send(encode_message("OK"))
            case "/get_peers":
                client_sock.send(encode_message("OK", {"peers": json.dumps(peer_list)}))
            case _:
                client_sock.send(encode_message("ERR"))
    except Exception as e:
        print(f"Error handling client {addr}: {e}")
    finally:
        client_sock.close()

def server_loop(interface: str = socket.BDADDR_ANY, channel: int = CHANNEL):
    """The server loop, listens for incoming connections.
    
    Args:
        interface (str): Bluetooth address to bind to
        channel (int): RFCOMM channel to listen on
    """
    global local_mac
    local_mac = get_local_mac()
    
    server_sock = socket.socket(socket.AF_BLUETOOTH, socket.SOCK_STREAM, socket.BTPROTO_RFCOMM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((interface, channel))
    server_sock.listen(5)
    print(f"Bluetooth server started on channel {channel}")
    
    try:
        while True:
            client_sock, addr = server_sock.accept()
            print(f"Connected by {addr}")
            thread = threading.Thread(target=handle_client, args=(client_sock, addr))
            thread.daemon = True
            thread.start()
    except KeyboardInterrupt:
        pass
    finally:
        server_sock.close()
        print("Bluetooth server stopped.")

def connect_to_peer(mac: str, channel: int = CHANNEL) -> socket.socket | None:
    """Connect to a peer via Bluetooth.
    
    Args:
        mac (str): MAC address of the peer
        channel (int): RFCOMM channel of the peer
        
    Returns:
        socket or None: Connected socket or None if failed
    """
    try:
        sock = socket.socket(socket.AF_BLUETOOTH, socket.SOCK_STREAM, socket.BTPROTO_RFCOMM)
        sock.connect((mac, channel))
        return sock
    except Exception as e:
        print(f"Failed to connect to {mac}: {e}")
        return None

def recv() -> str:
    """Function to recv data, if no data is received it waits until it is.
    
    Returns:
        str: received data
    """
    global recv_list
    while len(recv_list) == 0:
        pass
    return recv_list.pop()

def nrecv() -> str | None:
    """Function to recv data, if no data is received it returns None.
    
    Returns:
        str | None: received data or None
    """
    global recv_list
    if len(recv_list) == 0:
        return None
    return recv_list.pop()

def send(value: str):
    """Function to send data to all peers.
    
    Args:
        value (str): data to send
    """
    global peer_list
    for peer_mac in peer_list:
        sock = connect_to_peer(peer_mac)
        if sock:
            try:
                sock.send(encode_message("/data", {"value": value}))
                response = sock.recv(4096)
                print(f"Sent to {peer_mac}: {response.decode()}")
            except Exception as e:
                print(f"Failed to send to {peer_mac}: {e}")
            finally:
                sock.close()

def create_connections(connect_peer: str):
    """Creates the connections by asking the connected peer for all known peers.
    
    Args:
        connect_peer (str): MAC address of the first peer to connect to
    """
    global peer_list
    sock = connect_to_peer(connect_peer)
    if not sock:
        print(f"Could not connect to {connect_peer}")
        return
        
    try:
        sock.send(encode_message("/connect", {"mac": local_mac}))
        response = sock.recv(4096)
        print(f"Connect response: {response.decode()}")
        
        sock.send(encode_message("/get_peers"))
        response = sock.recv(4096)
        command, params = parse_message(response)
        
        if "peers" in params:
            peer_list = json.loads(params["peers"])
            print(f"Received peers: {peer_list}")
        
        peer_list.append(connect_peer)
        for peer_mac in peer_list:
            if peer_mac != local_mac and peer_mac != connect_peer:
                try:
                    sock2 = connect_to_peer(peer_mac)
                    if sock2:
                        sock2.send(encode_message("/connect", {"mac": local_mac}))
                        sock2.recv(4096)
                        sock2.close()
                except Exception as e:
                    print(f"Failed to connect to {peer_mac}: {e}")
    except Exception as e:
        print(f"Error creating connections: {e}")
    finally:
        sock.close()

def create(discover_peer: str | None = None, channel: int = CHANNEL):
    """Creates the Bluetooth server.
    
    Args:
        discover_peer (str): MAC address of the first peer to connect to
        channel (int): RFCOMM channel to listen on
    """
    global local_mac
    local_mac = get_local_mac()
    
    if discover_peer:
        create_connections(discover_peer)
    
    t = threading.Thread(target=server_loop, args=(socket.BDADDR_ANY, channel))
    t.daemon = True
    t.start()

def start_server(discover_peer: str | None = None, channel: int = CHANNEL):
    """Starts the Bluetooth server (alias for create).
    
    Args:
        discover_peer (str): MAC address of the first peer to connect to
        channel (int): RFCOMM channel to listen on
    """
    create(discover_peer, channel)

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        peer = sys.argv[1] if len(sys.argv) > 1 else None
        ch = int(sys.argv[2]) if len(sys.argv) > 2 else CHANNEL
        start_server(discover_peer=peer, channel=ch)
        
        if peer:
            print("Waiting for messages...")
            msg = recv()
            print(f"Received: {msg}")
            send(f"Echo: {msg}")