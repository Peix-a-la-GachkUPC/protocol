
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from threading import Thread
from sys import argv
import json
import requests
try:
    import conf
except ImportError:
    raise ImportError("Copy conf.py.example to conf.py and configure it")

HOST_NAME = "0.0.0.0"
SERVER_PORT = 8080
URL = f"http://127.0.0.1:{SERVER_PORT}"

peer_list = []
recv_list = []

def _normalize_url(raw_url: str) -> str:
    return raw_url.rstrip("/")

def _add_peer(raw_url: str) -> None:
    global peer_list
    peer_url = _normalize_url(raw_url)
    if peer_url == "":
        return
    if peer_url not in peer_list:
        peer_list.append(peer_url)


def setup():
    global HOST_NAME, SERVER_PORT, URL
    HOST_NAME = conf.HTTP_HOST
    SERVER_PORT = conf.HTTP_PORT
    URL = f"http://127.0.0.1:{SERVER_PORT}"


class MyServer(BaseHTTPRequestHandler):
    def do_GET(self):
        """GET reciever for the http server
        """
        args = parse_qs(urlparse(self.path).query)
        path = urlparse(self.path).path

        match (path):
            case ("/data"):
                self.data(args)
            case ("/get_peers"):
                self.get_peers()
            case ("/connect"):
                self.connect(args)
            case (_):
                self.send_response(404)
                self.send_header("Content-type", "text/html")
                self.end_headers()

    def data(self, args:dict[str, str]):
        """Accepts data by ading it to recv_listn to be retrieved by recv or nrecv

        Args:
            args (dict[str, str]): arguments (value of the data)
        """
        global recv_list
        if not "value" in args.keys():
            self.send_response(400)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            return

        recv_list.append(args["value"][0])

        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()

    def get_peers(self):
        """Sends the known peers to the asker
        """
        global peer_list
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(json.dumps(peer_list).encode("utf-8"))

    def connect(self, args:dict[str, str]):
        """Accepts connections by adding them to peer_list

        Args:
            args (dict[str, str]): arguments (url)
        """
        global peer_list
        if "url" in args.keys() and len(args["url"]) > 0:
            _add_peer(args["url"][0])
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            return

        if "port" not in args.keys():
            self.send_response(400)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            return

        _add_peer(f"http://{self.client_address[0]}:{args['port'][0]}")

        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()

        
def recv() -> str:
    """Funtion to recv data, if no data is recieved it waits untill it is.

    Returns:
        str: recieved data
    """
    global recv_list
    while len(recv_list) == 0: pass
    return recv_list.pop()

def nrecv() -> str|None:
    """Funtion to recv data, if no data is recived it returns null

    Returns:
        str|None: _description_
    """
    global recv_list
    if len(recv_list) == 0: 
        return None
    return recv_list.pop()

def send(value:str):
    """Function to send data

    Args:
        value (str): data to send
    """
    print(peer_list)
    for peer_url in peer_list:
        url = peer_url+"/data"
        params = {'value': value}
        r = requests.get(url = url, params = params, verify=False)

def server_loop(host:str, port:int):
    """The server loop, creates the server in a new thread

    Args:
        host (str): local host
        port (int): local port
    """
    webServer = HTTPServer((host, port), MyServer)
    print("Server started http://%s:%s" % (host, port))

    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass

    webServer.server_close()
    print("Server stopped.")

def create_connections(connect_peer:str, port):
    """Creates the connections by asking the connecte peer for all the known peers
    then connects to all those peers

    Args:
        connect_peer (str): First peer to connect to.
    """
    global peer_list

    print("connect")
    connect_peer = _normalize_url(connect_peer)
    r = requests.get(url = connect_peer+"/get_peers")
    print("hola:",r.text)
    peer_list = []
    for peer_url in json.loads(r.text):
        _add_peer(peer_url)
    print(peer_list, type(peer_list))
    _add_peer(connect_peer)
    for peer_url in peer_list:
        requests.get(
            url=f"{peer_url}/connect",
            params={"url": URL},
            verify=False,
        )

def start_server(
    connect_peer: str | None = None,
    public_url: str | None = None,
):
    """Starts the server

    Args:
        connect_peer (str, optional): The url of the peer to first connect to. Defaults to None.
        host (str, optional): local hostname. Defaults to HOST_NAME.
        port (int, optional): local port. Defaults to SERVER_PORT.
        public_url (str, optional): url advertised to peers. Defaults to None.
    """
    global HOST_NAME, SERVER_PORT, URL

    if public_url is not None:
        URL = public_url.rstrip("/")
    else:
        URL = f"http://{HOST_NAME}:{SERVER_PORT}"

    print("connect_peer", connect_peer)

    if not connect_peer == None:
        create_connections(connect_peer, SERVER_PORT)

    t = Thread(target=server_loop, args=(HOST_NAME, SERVER_PORT,))
    t.daemon = True
    t.start()

if __name__ == "__main__":
    "Only for testing, not as library"
    if len(argv) == 2:
        peer = None
    else:
        peer = argv[2]
    start_server(connect_peer=peer)
    
    if (argv[1] == "8082"):
        print("test")
        send("hello world!")

    try:
        while True:
            print("waiting for data...")
            print(recv(), flush=True)
    except KeyboardInterrupt:
        pass
