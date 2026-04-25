import sys
import time
sys.path.insert(0, "../..")
from network.Bluetooth import connection as bt

if len(sys.argv) < 2:
    print("Usage: python test_bluetooth.py <peer_mac> [channel]")
    print("  peer_mac: MAC address of the first peer to connect to (omit for server-only mode)")
    print("  channel: RFCOMM channel (default: 4)")
    sys.exit(1)

peer_mac = sys.argv[1] if len(sys.argv) > 1 else None
channel = int(sys.argv[2]) if len(sys.argv) > 2 else 4

print("Starting Bluetooth node...")
bt.create(discover_peer=peer_mac, channel=channel)

time.sleep(1)
print(f"Local MAC: {bt.local_mac}")
print(f"Known peers: {bt.peer_list}")

if peer_mac:
    print("\n--- Node 1: Sending message ---")
    time.sleep(1)
    bt.send("hello world via Bluetooth!")
    
    print("Waiting for response...")
    msg = bt.recv()
    print(f"Received: {msg}")
    
    time.sleep(1)
    print(f"nrecv (non-blocking): {bt.nrecv()}")
else:
    print("\n--- Node 2: Waiting for message ---")
    print("Waiting for incoming message...")
    msg = bt.recv()
    print(f"Received: {msg}")
    
    print("Sending response...")
    bt.send("Response via Bluetooth!")
    print("Done.")