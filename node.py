import json
import os
import socket
import sys
import threading
import time
from datetime import datetime

# ------------------------------------------------------------------
# Milestone 0: configuration
# ------------------------------------------------------------------
HOST = "127.0.0.1"
BASE_PORT = 5000
NUM_NODES = 5
PEERS = {i: (HOST, BASE_PORT + i) for i in range(1, NUM_NODES + 1)}

# Timeouts (seconds).  Rule of thumb: CONNECT < IAA/OK < HEARTBEAT < IWON
CONNECT_TIMEOUT = 1.0     # how long to wait when connecting/sending to a peer
HEARTBEAT_INTERVAL = 3.0  # how often to send AYA to the coordinator
IAA_TIMEOUT = 2.0         # how long to wait for IAA after sending AYA
OK_TIMEOUT = 2.0          # how long to wait for an OK after sending ELECTION
IWON_TIMEOUT = 6.0        # after an OK, how long to wait for someone to say IWON
STARTUP_DELAY = 1.0       # pause before a freshly started node calls an election


class Node:
    def __init__(self, node_id):
        if node_id not in PEERS:
            raise ValueError(f"node id must be 1..{NUM_NODES}")
        self.id = node_id
        self.addr = PEERS[node_id]

        # Shared state - always touch under self.lock
        self.lock = threading.Lock()
        self.coordinator_id = max(PEERS)        # node 8 is coordinator by default
        self.election_in_progress = False

        # Events used to wait for replies from other threads
        self.ok_event = threading.Event()       # set when an OK arrives
        self.iwon_event = threading.Event()     # set when an IWON arrives
        self.iaa_event = threading.Event()      # set when an IAA arrives
        self.iaa_from = set()       # higher nodes that answered IAA this round
        self.alive_higher = set()   # higher nodes that answered last round      # set when an IAA arrives

        self.running = True

    # --------------------------------------------------------------
    # Milestone 0: logging
    # --------------------------------------------------------------
    def log(self, msg):
        ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print(f"[{ts}] Node {self.id}: {msg}", flush=True)

    # --------------------------------------------------------------
    # Milestone 1: the listener
    # --------------------------------------------------------------
    def start_listener(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(self.addr)
        server.listen(16)
        self.server = server
        threading.Thread(target=self._accept_loop, daemon=True).start()
        self.log(f"listening on {self.addr[0]}:{self.addr[1]}")

    def _accept_loop(self):
        while self.running:
            try:
                conn, _ = self.server.accept()
            except OSError:
                break  # socket closed during shutdown
            # Each connection is handled in its own thread so the listener never blocks
            threading.Thread(target=self._handle_connection, args=(conn,), daemon=True).start()

    def _handle_connection(self, conn):
        with conn:
            conn.settimeout(CONNECT_TIMEOUT)
            chunks = []
            try:
                while True:
                    data = conn.recv(4096)
                    if not data:
                        break
                    chunks.append(data)
            except (socket.timeout, OSError):
                pass
        if not chunks:
            return
        try:
            msg = json.loads(b"".join(chunks).decode())
        except json.JSONDecodeError:
            self.log("received malformed message, ignoring")
            return
        self.handle_message(msg)

    # --------------------------------------------------------------
    # Milestone 2: the messaging layer
    # --------------------------------------------------------------
    def send(self, target_id, msg_type):
        """Send one message to target_id. Returns True if delivered, False if unreachable."""
        if target_id == self.id:
            return False
        msg = {"type": msg_type, "from": self.id}
        try:
            with socket.create_connection(PEERS[target_id], timeout=CONNECT_TIMEOUT) as s:
                s.sendall(json.dumps(msg).encode())
            if msg_type not in ("AYA", "IAA"):          # keep heartbeat noise down
                self.log(f"-> Node {target_id}: {msg_type}")
            return True
        except (ConnectionRefusedError, socket.timeout, OSError):
            if msg_type not in ("AYA", "IAA"):
                self.log(f"-> Node {target_id}: {msg_type}  (unreachable)")
            return False

    def handle_message(self, msg):
        mtype, sender = msg.get("type"), msg.get("from")

        if mtype == "AYA":
            self.send(sender, "IAA")

        elif mtype == "IAA":
            with self.lock:
                self.iaa_from.add(sender)
            self.iaa_event.set()

        elif mtype == "ELECTION":
            self.log(f"<- Node {sender}: ELECTION")
            if sender < self.id:
                # Bully: tell the lower node it's out, then run our own election
                self.send(sender, "OK")
                threading.Thread(target=self.start_election, daemon=True).start()

        elif mtype == "OK":
            self.log(f"<- Node {sender}: OK  (a higher node is alive, I drop out)")
            self.ok_event.set()

        elif mtype == "IWON":
            self.log(f"<- Node {sender}: IWON")
            with self.lock:
                self.coordinator_id = sender
                self.election_in_progress = False
            self.iwon_event.set()
            self.log(f"*** New coordinator is Node {sender} ***")
            if sender < self.id:
                # A lower node claimed victory but I'm alive and higher: bully it
                self.log(f"Node {sender} is lower than me - challenging")
                threading.Thread(target=self.start_election, daemon=True).start()

        else:
            self.log(f"unknown message {msg}")

    # --------------------------------------------------------------
    # Milestone 3: heartbeats (AYA / IAA)
    # --------------------------------------------------------------
    def heartbeat_loop(self):
        while self.running:
            time.sleep(HEARTBEAT_INTERVAL)
            with self.lock:
                coord = self.coordinator_id
                busy = self.election_in_progress
            if coord == self.id or busy:
                continue  # coordinator doesn't ping itself; don't ping during an election

            with self.lock:
                self.iaa_from.clear()
            self.iaa_event.clear()
            delivered = self.send(coord, "AYA")
            alive = delivered and self.iaa_event.wait(IAA_TIMEOUT)
            if not alive:
                self.log(f"!!! Coordinator Node {coord} is DOWN (no IAA) - starting election")
                self.start_election()

    # --------------------------------------------------------------
    # Milestones 4 & 5: the election
    # --------------------------------------------------------------
    def start_election(self):
        # Only one election at a time inside this node
        with self.lock:
            if self.election_in_progress:
                return
            self.election_in_progress = True

        while self.running:
            self.ok_event.clear()
            self.iwon_event.clear()
            self.log("starting ELECTION")

            higher = [n for n in PEERS if n > self.id]
            for n in higher:
                self.send(n, "ELECTION")

            # Wait for any higher node to answer OK
            if not higher or not self.ok_event.wait(OK_TIMEOUT):
                self.declare_victory()
                return

            # Someone higher is alive - wait for them to announce IWON
            self.log(f"waiting up to {IWON_TIMEOUT}s for IWON")
            if self.iwon_event.wait(IWON_TIMEOUT):
                return  # IWON handler already updated state

            # The higher node died mid-election: try again
            self.log("no IWON received - higher node may have crashed, re-running election")

    def declare_victory(self):
        with self.lock:
            self.coordinator_id = self.id
            self.election_in_progress = False
        self.iwon_event.set()
        self.log("*** No higher node answered - I WON, I am the coordinator ***")
        for n in PEERS:
            if n < self.id:
                self.send(n, "IWON")

    # --------------------------------------------------------------
    # Terminal commands (handy for testing)
    # --------------------------------------------------------------
    def input_loop(self):
        while self.running:
            try:
                cmd = input().strip().lower()
            except (EOFError, KeyboardInterrupt):
                return  # no terminal attached (e.g. running in background)
            if cmd == "status":
                with self.lock:
                    self.log(f"STATUS: coordinator={self.coordinator_id}, "
                             f"election_in_progress={self.election_in_progress}")
            elif cmd == "elect":
                threading.Thread(target=self.start_election, daemon=True).start()
            elif cmd in ("quit", "exit"):
                self.shutdown()
            elif cmd:
                print("commands: status | elect | quit", flush=True)

    # --------------------------------------------------------------
    # Milestone 6: startup + recovery, and clean shutdown
    # --------------------------------------------------------------
    def run(self):
        self.start_listener()
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()
        threading.Thread(target=self.input_loop, daemon=True).start()

        # A node that (re)joins always holds an election - this is how a
        # recovered node 8 bullies its way back to coordinator.
        time.sleep(STARTUP_DELAY)
        threading.Thread(target=self.start_election, daemon=True).start()

        try:
            while self.running:
                time.sleep(0.5)
        except KeyboardInterrupt:
            self.shutdown()

    def shutdown(self):
        self.log("shutting down")
        self.running = False
        try:
            self.server.close()
        except OSError:
            pass
        os._exit(0)


def main():
    if len(sys.argv) != 2 or not sys.argv[1].isdigit():
        print(f"usage: python {sys.argv[0]} <node_id 1..{NUM_NODES}>")
        sys.exit(1)
    try:
        node = Node(int(sys.argv[1]))
    except ValueError as e:
        print(e)
        sys.exit(1)
    try:
        node.run()
    except OSError as e:
        print(f"could not start node: {e}")
        sys.exit(1)
def sub_main():
    print("This is a Standard Bully Election Algorithm implemented in python ! ")


if __name__ == "__main__":
    sub_main()
    main()