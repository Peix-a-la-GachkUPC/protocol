import subprocess
import json
import os
import threading


class HyperswarmInterface:
    def __init__(self, node_path='index.js', cwd=None):
        self.node_path = node_path
        self.cwd = cwd or os.getcwd()
        self.proc = subprocess.Popen(
            ['node', node_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            cwd=self.cwd
        )
        self._events = []
        self._events_lock = threading.Lock()
        self._listener_running = True
        self._listener_thread = threading.Thread(target=self._listen_stderr, daemon=True)
        self._listener_thread.start()

    def create(self, topic):
        self._send({'cmd': 'create', 'topic': topic})
        return self._recv()

    def send(self, msg):
        self._send({'cmd': 'send', 'msg': msg})
        return self._recv()

    def recv(self):
        self._send({'cmd': 'recv'})
        return self._recv()['msg']

    def nrecv(self):
        self._send({'cmd': 'nrecv'})
        return self._recv()['msg']

    def peer_count(self):
        self._send({'cmd': 'peers'})
        return self._recv()['count']

    def get_event(self, timeout=0):
        if timeout == 0:
            with self._events_lock:
                if self._events:
                    return self._events.pop(0)
            return None
        import time
        start = time.time()
        while time.time() - start < timeout:
            with self._events_lock:
                if self._events:
                    return self._events.pop(0)
            import time
            time.sleep(0.1)
        return None

    def close(self):
        self._listener_running = False
        self.proc.stdin.close()
        self.proc.wait()

    def kill(self):
        self._listener_running = False
        self.proc.terminate()
        try:
            self.proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()

    def _send(self, obj):
        self.proc.stdin.write(json.dumps(obj) + '\n')
        self.proc.stdin.flush()

    def _recv(self):
        return json.loads(self.proc.stdout.readline())

    def _listen_stderr(self):
        while self._listener_running:
            try:
                line = self.proc.stderr.readline()
                if line:
                    event = json.loads(line.strip())
                    with self._events_lock:
                        self._events.append(event)
            except Exception:
                pass