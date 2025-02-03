import socket
import threading
import time
from utils import get_local_ip, generate_node_id, generate_key_id, display_finger_table, in_range

# Set the number of bits for the identifier space (m bits)
m = 3  # Adjust as needed

# Global flag for background logging
DEBUG = False

def debug_print(*args, **kwargs):
    """Print debug messages only if DEBUG is True."""
    if DEBUG:
        print(*args, **kwargs)

class ChordNode:
    def __init__(self, port=5000, known_node=None):
        self.ip = get_local_ip()
        self.port = port
        self.node_id = generate_node_id(self.ip, self.port, m)
        self.successor = (self.ip, self.port)
        self.predecessor = (self.ip, self.port)
        self.finger_table = []  # Initialize the finger table

        # Setup UDP socket for asynchronous messaging
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.ip, self.port))

        # Initialize the finger table
        self.initialize_finger_table()

        # If a known node is provided, join the network
        if known_node:
            self.join(known_node)

    def initialize_finger_table(self):
        for i in range(1, m + 1):
            start = (self.node_id + 2**(i - 1)) % (2**m)
            interval = (start, (self.node_id + 2**i) % (2**m))
            successor = (self.ip, self.port)  # Initially assume self
            self.finger_table.append({'start': start, 'interval': interval, 'successor': successor})
        # Display the initial finger table (this is the CLI command output)
        display_finger_table(self.node_id, self.finger_table, m)

    def start(self):
        # Start the asynchronous listener and stabilization routines
        threading.Thread(target=self.listen, daemon=True).start()
        threading.Thread(target=self.stabilize, daemon=True).start()
        threading.Thread(target=self.fix_fingers, daemon=True).start()
        threading.Thread(target=self.check_predecessor, daemon=True).start()
        debug_print(f"Node {self.node_id} started at {self.ip}:{self.port}")

    def listen(self):
        while True:
            data, addr = self.sock.recvfrom(1024)
            message = data.decode()
            debug_print(f"Received message from {addr}: {message}")
            self.handle_message(message, addr)

    def handle_message(self, message, addr):
        parts = message.split()
        if not parts:
            return
        if parts[0] == 'PING':
            self.send_message('PONG', addr)
        elif parts[0] == 'PONG':
            debug_print(f"PONG received from {addr}")
        elif parts[0] == 'FIND_SUCCESSOR':
            start_value = int(parts[1])
            index = int(parts[2]) if len(parts) > 2 else None
            successor = self.find_successor_recursive(start_value)
            if successor is None:
                return
            response = (f"SUCCESSOR {successor[0]} {successor[1]} {index}"
                        if index is not None else
                        f"SUCCESSOR {successor[0]} {successor[1]}")
            self.send_message(response, addr)
        elif parts[0] == 'SUCCESSOR':
            index = int(parts[3]) if len(parts) > 3 else 0
            self.finger_table[index]['successor'] = (parts[1], int(parts[2]))
            if index == 0:
                self.successor = (parts[1], int(parts[2]))
                self.request_predecessor(self.successor)
            # Finger table updates are now silent (unless the CLI asks for them)
        elif parts[0] == 'GET_PREDECESSOR':
            self.send_message(f"PREDECESSOR {self.predecessor[0]} {self.predecessor[1]}", addr)
        elif parts[0] == 'PREDECESSOR':
            # A reply from a GET_PREDECESSOR RPC
            self.predecessor = (parts[1], int(parts[2]))
        elif parts[0] == 'SET_PREDECESSOR':
            self.predecessor = (parts[1], int(parts[2]))
        elif parts[0] == 'UPDATE_FINGER_TABLE':
            node_ip, node_port, i = parts[1], int(parts[2]), int(parts[3])
            self.update_finger_table((node_ip, node_port), i)
        # New message handlers for the recursive RPC lookup:
        elif parts[0] == "GET_SUCCESSOR":
            self.send_message(f"SUCCESSOR_INFO {self.successor[0]} {self.successor[1]}", addr)
        elif parts[0] == "RPC_CLOSEST_PRECEDING":
            query_id = int(parts[1])
            node = self.closest_preceding_finger(query_id)
            self.send_message(f"CLOSEST_PRECEDING {node[0]} {node[1]}", addr)
        # The messages "SUCCESSOR_INFO" and "CLOSEST_PRECEDING" are used in RPC calls.

    def send_message(self, message, target):
        self.sock.sendto(message.encode(), target)

    def rpc(self, message, target, timeout=2, retries=3):
        """A simple RPC mechanism over UDP with retries."""
        for attempt in range(retries):
            temp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            temp_sock.settimeout(timeout)
            try:
                temp_sock.sendto(message.encode(), target)
                data, _ = temp_sock.recvfrom(1024)
                temp_sock.close()
                return data.decode()
            except socket.timeout:
                temp_sock.close()
                continue
        return None

    def join(self, known_node):
        start_value = self.finger_table[0]['start']
        self.send_message(f'FIND_SUCCESSOR {start_value} 0', known_node)

    def find_successor_recursive(self, id):
        current = (self.ip, self.port)
        while True:
            if current == (self.ip, self.port):
                successor = self.successor
            else:
                response = self.rpc("GET_SUCCESSOR", current)
                if response is None:
                    return None
                parts = response.split()
                if parts[0] != "SUCCESSOR_INFO":
                    return None
                successor = (parts[1], int(parts[2]))
            current_node_id = generate_node_id(current[0], current[1], m)
            successor_node_id = generate_node_id(successor[0], successor[1], m)
            if in_range(id, current_node_id, successor_node_id):
                return successor
            else:
                if current == (self.ip, self.port):
                    next_node = self.closest_preceding_finger(id)
                else:
                    response = self.rpc(f"RPC_CLOSEST_PRECEDING {id}", current)
                    if response is None:
                        return None
                    parts = response.split()
                    if parts[0] != "CLOSEST_PRECEDING":
                        return None
                    next_node = (parts[1], int(parts[2]))
                if next_node == current:
                    return successor
                current = next_node

    def closest_preceding_finger(self, id):
        for i in range(m-1, -1, -1):
            finger_node = self.finger_table[i]['successor']
            finger_node_id = generate_node_id(finger_node[0], finger_node[1], m)
            if in_range(finger_node_id, self.node_id, id):
                return finger_node
        return (self.ip, self.port)

    def request_predecessor(self, successor):
        self.send_message('GET_PREDECESSOR', successor)

    def update_finger_table(self, node, i):
        node_id = generate_node_id(node[0], node[1], m)
        current_successor_id = generate_node_id(self.finger_table[i]['successor'][0],
                                                  self.finger_table[i]['successor'][1], m)
        if in_range(node_id, self.node_id, current_successor_id):
            self.finger_table[i]['successor'] = node
            if self.predecessor != node:
                self.send_message(f'UPDATE_FINGER_TABLE {node[0]} {node[1]} {i}', self.predecessor)

    # --- Stabilization Protocols ---

    def stabilize(self):
        """Periodically verify and update the successor, then notify the successor of self."""
        while True:
            try:
                response = self.rpc("GET_PREDECESSOR", self.successor, timeout=1)
                if response:
                    parts = response.split()
                    if parts[0] == "PREDECESSOR":
                        x = (parts[1], int(parts[2]))
                        x_id = generate_node_id(x[0], x[1], m)
                        succ_id = generate_node_id(self.successor[0], self.successor[1], m)
                        if x != (self.ip, self.port) and in_range(x_id, self.node_id, succ_id):
                            self.successor = x
                # Notify the successor about self
                self.send_message(f"SET_PREDECESSOR {self.ip} {self.port}", self.successor)
            except Exception as e:
                debug_print(f"Stabilize error: {e}")
            time.sleep(1)  # Adjust the stabilization period as needed

    def fix_fingers(self):
        """Periodically refresh entries in the finger table."""
        i = 1
        while True:
            try:
                next_start = self.finger_table[i]['start']
                successor = self.find_successor_recursive(next_start)
                if successor:
                    self.finger_table[i]['successor'] = successor
                i = (i + 1) % m
            except Exception as e:
                debug_print(f"Fix fingers error: {e}")
            time.sleep(1)  # Adjust the frequency as needed

    def check_predecessor(self):
        """Periodically check whether the predecessor is still alive."""
        while True:
            try:
                response = self.rpc("PING", self.predecessor, timeout=1)
                if response is None:
                    debug_print("Predecessor not responding; resetting predecessor to self.")
                    self.predecessor = (self.ip, self.port)
            except Exception as e:
                debug_print(f"Check predecessor error: {e}")
                self.predecessor = (self.ip, self.port)
            time.sleep(1)  # Adjust the frequency as needed

def cli(node):
    """A simple command-line interface running concurrently with the node.
    Type 'ft' to display the current finger table.
    Type 'help' to list available commands.
    Type 'quit' to exit.
    """
    help_text = ("Commands:\n"
                 "  ft     - Display the current finger table\n"
                 "  help   - Show this help message\n"
                 "  quit   - Exit the CLI and terminate the node")
    print(help_text)
    while True:
        cmd = input("ChordCLI> ").strip().lower()
        if cmd in ["ft", "finger", "fingers"]:
            display_finger_table(node.node_id, node.finger_table, m)
        elif cmd == "help":
            print(help_text)
        elif cmd == "quit":
            print("Exiting CLI and terminating node.")
            # In a real system you might perform a graceful shutdown here.
            exit(0)
        else:
            print("Unknown command. Type 'help' for available commands.")

if __name__ == "__main__":
    port = int(input("Enter Port: "))
    known_ip = input("Enter known node IP (or press Enter if none): ")
    if known_ip:
        known_port = input("Enter known node Port (or press Enter if none): ")
    else:
        known_port = None
        known_ip = None

    known_node = (known_ip, int(known_port)) if known_ip and known_port else None
    node = ChordNode(port=port, known_node=known_node)
    node.start()

    # Start the CLI in a separate thread so that it doesn't block background operations.
    threading.Thread(target=cli, args=(node,), daemon=False).start()