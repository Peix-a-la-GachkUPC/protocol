
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from threading import Thread
from sys import argv
import json
import requests

HOST_NAME = "0.0.0.0"
SERVER_PORT = 8080
URL = f"https://{HOST_NAME}:{SERVER_PORT}"

peer_list = []
recv_list = []

class MyServer(BaseHTTPRequestHandler):
    def do_GET(self):
        """GET reciever for the http server
        """
        print(self.path)

        args = parse_qs(urlparse(self.path).query)
        path = urlparse(self.path).path
        print(path, args)

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
        if not "url" in args.keys():
            self.send_response(400)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            return

        peer_list.append(args["url"])

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
    for peer_url in peer_list:
        url = peer_url+"/data"
        params = {'value': value}
        r = requests.get(url = url, params = params)

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

def create_connections(connect_peer:str):
    """Creates the connections by asking the connecte peer for all the known peers
    then connects to all those peers

    Args:
        connect_peer (str): First peer to connect to.
    """
    global peer_list
    r = requests.get(url = connect_peer+"/get_peers")
    peer_list = json.loads(r.text)
    print(peer_list, type(peer_list))
    peer_list.append(connect_peer)
    for peer_url in peer_list:
        r = requests.get(url = f"{peer_url}/connect?url={URL}")


def start_server(connect_peer:str=None, host:str=HOST_NAME, port:int=SERVER_PORT):
    """Starts the server

    Args:
        connect_peer (str, optional): The url of the peer to first connect to. Defaults to None.
        host (str, optional): local hostname. Defaults to HOST_NAME.
        port (int, optional): local port. Defaults to SERVER_PORT.
    """
    if not connect_peer == None:
        create_connections(connect_peer)

    t = Thread(target=server_loop, args=(host, port,))
    t.start()

if __name__ == "__main__":
    "Only for testing, not as library"
    if len(argv) == 2:
        peer = None
    else:
        peer = argv[2]
    start_server(port = int(argv[1]), connect_peer=peer)
    
    if (argv[1] == "8082"):
        print("test")
        send("hello world!")

    if (argv[1] == "8080"):
        print(recv())