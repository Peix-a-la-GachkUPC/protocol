from hyperswarm import HyperswarmInterface
import time


hs = HyperswarmInterface()
n = int(input("num: "))
print(hs.create("elmeutopic"))

time.sleep(2)

if n == 1:
    print(f"Peers: {hs.peer_count()}")
    
    while hs.peer_count() == 0:
        time.sleep(1)
        print(f"Waiting for peers... count: {hs.peer_count()}")
    
    print(f"Peers connected: {hs.peer_count()}")
    print(hs.send("hello world!"))

if n == 2:
    print(hs.recv())
    print(hs.send("I <3 club mate"))

if n == 1:
    time.sleep(2)
    print(hs.nrecv())
    print(hs.nrecv())


hs.kill()