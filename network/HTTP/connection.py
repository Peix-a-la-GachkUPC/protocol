
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from threading import Thread
from sys import argv
import json
import requests

HOST_NAME = "localhost"
SERVER_PORT = 8080
URL = f"https://{HOST_NAME}:{SERVER_PORT}"

peer_list = []
recv_list = []

class MyServer(BaseHTTPRequestHandler):
    def do_GET(self):
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
        global peer_list
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(json.dumps(peer_list).encode("utf-8"))

    def connect(self, args:dict[str, str]):
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

        
def recv():
    global recv_list
    while len(recv_list) == 0: pass
    return recv_list.pop()

def nrecv():
    global recv_list
    if len(recv_list) == 0: 
        return None
    return recv_list.pop()

def send(value):
    for peer_url in peer_list:
        url = peer_url+"/data"
        params = {'value': value}
        r = requests.get(url = url, params = params)

def server_loop(host, port):
    webServer = HTTPServer((host, port), MyServer)
    print("Server started http://%s:%s" % (host, port))

    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass

    webServer.server_close()
    print("Server stopped.")

def create_connections(connect_peer:str):
    global peer_list
    r = requests.get(url = connect_peer+"/get_peers")
    peer_list = json.loads(r.text)
    print(peer_list, type(peer_list))
    peer_list.append(connect_peer)
    for peer_url in peer_list:
        r = requests.get(url = f"{peer_url}/connect?url={URL}")


def start_server(connect_peer:str=None, host:str=HOST_NAME, port:int=SERVER_PORT):
    if not connect_peer == None:
        create_connections(connect_peer)

    t = Thread(target=server_loop, args=(host, port,))
    t.start()

if __name__ == "__main__":
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