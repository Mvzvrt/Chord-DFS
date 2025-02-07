import socket
import threading
import time
from utils import get_local_ip, generate_node_id, generate_key_id, display_finger_table, in_range

# Global flag for background logging
DEBUG = False

def debug_print(*args, **kwargs):
    """Print debug messages only if DEBUG is True."""
    if DEBUG:
        print(*args, **kwargs)

class ChordNode:
    def __init__(self, port=5000, known_node=None, m=3):
        self.m = m
        self.ip = get_local_ip()
        self.port = port
        self.node_id = generate_node_id(self.ip, self.port, self.m)
        self.successor = (self.ip, self.port)
        self.predecessor = (self.ip, self.port)
        self.finger_table = []  # Initialize the finger table
        self.lock = threading.RLock()
        self.running = True

        # Setup UDP socket for asynchronous messaging
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.ip, self.port))

        # Initialize the finger table
        self.initialize_finger_table()

        # If a known node is provided, join the network
        if known_node:
            self.join(known_node)

    def initialize_finger_table(self):
        for i in range(1, self.m + 1):
            start = (self.node_id + 2**(i - 1)) % (2**self.m)
            interval = (start, (self.node_id + 2**i) % (2**self.m))
            successor = (self.ip, self.port)  # Initially assume self
            self.finger_table.append({'start': start, 'interval': interval, 'successor': successor})
        # Display the initial finger table (this is the CLI command output)
        display_finger_table(self.node_id, self.finger_table, self.m)

    def start(self):
        # Start the asynchronous listener and stabilization routines
        threading.Thread(target=self.listen, daemon=True).start()
        threading.Thread(target=self.stabilize, daemon=True).start()
        threading.Thread(target=self.fix_fingers, daemon=True).start()
        threading.Thread(target=self.check_predecessor, daemon=True).start()
        debug_print(f"Node {self.node_id} started at {self.ip}:{self.port}")

    def listen(self):
        while self.running:
            try:
                data, addr = self.sock.recvfrom(1024)
                message = data.decode()
                debug_print(f"Received message from {addr}: {message}")
                self.handle_message(message, addr)
            except:
                if self.running:
                    raise

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
            with self.lock:
                self.finger_table[index]['successor'] = (parts[1], int(parts[2]))
                if index == 0:
                    self.successor = (parts[1], int(parts[2]))
                    self.request_predecessor(self.successor)
        elif parts[0] == 'GET_PREDECESSOR':
            self.send_message(f"PREDECESSOR {self.predecessor[0]} {self.predecessor[1]}", addr)
        elif parts[0] == 'PREDECESSOR':
            with self.lock:
                self.predecessor = (parts[1], int(parts[2]))
        elif parts[0] == 'SET_PREDECESSOR':
            with self.lock:
                self.predecessor = (parts[1], int(parts[2]))
        elif parts[0] == 'UPDATE_FINGER_TABLE':
            node_ip, node_port, i = parts[1], int(parts[2]), int(parts[3])
            self.update_finger_table((node_ip, node_port), i)
        elif parts[0] == "GET_SUCCESSOR":
            self.send_message(f"SUCCESSOR_INFO {self.successor[0]} {self.successor[1]}", addr)
        elif parts[0] == "RPC_CLOSEST_PRECEDING":
            query_id = int(parts[1])
            node = self.closest_preceding_finger(query_id)
            self.send_message(f"CLOSEST_PRECEDING {node[0]} {node[1]}", addr)
        elif parts[0] == 'UPDATE_SUCCESSOR':
            new_successor = (parts[1], int(parts[2]))
            with self.lock:
                self.successor = new_successor
                self.finger_table[0]['successor'] = new_successor
        elif parts[0] == 'UPDATE_PREDECESSOR':
            new_predecessor = (parts[1], int(parts[2]))
            with self.lock:
                self.predecessor = new_predecessor

    def send_message(self, message, target):
        try:
            self.sock.sendto(message.encode(), target)
        except:
            debug_print(f"Failed to send message to {target}")

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
                with self.lock:
                    successor = self.successor
            else:
                response = self.rpc("GET_SUCCESSOR", current)
                if response is None:
                    return None
                parts = response.split()
                if parts[0] != "SUCCESSOR_INFO":
                    return None
                successor = (parts[1], int(parts[2]))
            current_node_id = generate_node_id(current[0], current[1], self.m)
            successor_node_id = generate_node_id(successor[0], successor[1], self.m)
            if in_range(id, current_node_id, successor_node_id, self.m):
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
        with self.lock:
            for i in range(self.m-1, -1, -1):
                finger_node = self.finger_table[i]['successor']
                finger_node_id = generate_node_id(finger_node[0], finger_node[1], self.m)
                if in_range(finger_node_id, self.node_id, id, self.m):
                    return finger_node
            return (self.ip, self.port)

    def request_predecessor(self, successor):
        self.send_message('GET_PREDECESSOR', successor)

    def update_finger_table(self, node, i):
        node_id = generate_node_id(node[0], node[1], self.m)
        with self.lock:
            current_successor_id = generate_node_id(self.finger_table[i]['successor'][0],
                                                      self.finger_table[i]['successor'][1], self.m)
            if in_range(node_id, self.node_id, current_successor_id, self.m):
                self.finger_table[i]['successor'] = node
                if self.predecessor != node:
                    self.send_message(f'UPDATE_FINGER_TABLE {node[0]} {node[1]} {i}', self.predecessor)

    # --- Stabilization Protocols ---

    def stabilize(self):
        """Periodically verify and update the successor, then notify the successor of self."""
        while self.running:
            try:
                with self.lock:
                    successor = self.successor
                response = self.rpc("GET_PREDECESSOR", successor, timeout=1)
                if response:
                    parts = response.split()
                    if parts[0] == "PREDECESSOR":
                        x = (parts[1], int(parts[2]))
                        x_id = generate_node_id(x[0], x[1], self.m)
                        succ_id = generate_node_id(successor[0], successor[1], self.m)
                        if x != (self.ip, self.port) and in_range(x_id, self.node_id, succ_id, self.m):
                            with self.lock:
                                self.successor = x
                                self.finger_table[0]['successor'] = x
                # Notify the successor about self
                with self.lock:
                    self.send_message(f"SET_PREDECESSOR {self.ip} {self.port}", self.successor)
            except Exception as e:
                debug_print(f"Stabilize error: {e}")
            time.sleep(1)

    def fix_fingers(self):
        """Periodically refresh entries in the finger table."""
        i = 1
        while self.running:
            try:
                with self.lock:
                    next_start = self.finger_table[i]['start']
                successor = self.find_successor_recursive(next_start)
                if successor:
                    with self.lock:
                        self.finger_table[i]['successor'] = successor
                i = (i + 1) % self.m
            except Exception as e:
                debug_print(f"Fix fingers error: {e}")
            time.sleep(1)

    def check_predecessor(self):
        """Periodically check whether the predecessor is still alive."""
        while self.running:
            try:
                with self.lock:
                    predecessor = self.predecessor
                if predecessor != (self.ip, self.port):
                    response = self.rpc("PING", predecessor, timeout=1)
                    if response is None:
                        debug_print("Predecessor not responding; resetting predecessor to self.")
                        with self.lock:
                            self.predecessor = (self.ip, self.port)
            except Exception as e:
                debug_print(f"Check predecessor error: {e}")
                with self.lock:
                    self.predecessor = (self.ip, self.port)
            time.sleep(1)

    def leave_gracefully(self):
        """Notify predecessor and successor, then shut down."""
        with self.lock:
            pred = self.predecessor
            succ = self.successor
        debug_print("Leaving the network gracefully...")
        if pred != (self.ip, self.port):
            self.send_message(f"UPDATE_SUCCESSOR {succ[0]} {succ[1]}", pred)
        if succ != (self.ip, self.port):
            self.send_message(f"UPDATE_PREDECESSOR {pred[0]} {pred[1]}", succ)
        self.running = False
        self.sock.close()
        debug_print("Node has left the network.")

def cli(node):
    """A simple command-line interface running concurrently with the node."""
    help_text = ("Commands:\n"
                 "  ft     - Display the current finger table\n"
                 "  state  - Show current node state\n"
                 "  debug  - Toggle debug messages\n"
                 "  leave  - Gracefully leave the network\n"
                 "  help   - Show this help message\n"
                 "  quit   - Exit the CLI and terminate the node")
    print(help_text)
    while True:
        cmd = input("ChordCLI> ").strip().lower()
        if cmd in ["ft", "finger", "fingers"]:
            display_finger_table(node.node_id, node.finger_table, node.m)
        elif cmd == "help":
            print(help_text)
        elif cmd == "debug":
            global DEBUG
            DEBUG = not DEBUG
            print(f"Debug mode {'enabled' if DEBUG else 'disabled'}.")
        elif cmd == "state":
            with node.lock:
                print(f"Node ID: {node.node_id}")
                print(f"Predecessor: {node.predecessor}")
                print(f"Successor: {node.successor}")
                display_finger_table(node.node_id, node.finger_table, node.m)
        elif cmd == "leave":
            node.leave_gracefully()
            print("Node is leaving the network. Goodbye!")
            exit(0)
        elif cmd == "quit":
            print("Exiting CLI and terminating node.")
            exit(0)
        else:
            print("Unknown command. Type 'help' for available commands.")

if __name__ == "__main__":
    m = int(input("Enter the number of bits for the identifier space (m, up to 32): "))
    while m < 1 or m > 32:
        m = int(input("Invalid m. Enter a value between 1 and 32: "))
    port = int(input("Enter Port: "))
    known_ip = input("Enter known node IP (or press Enter if none): ")
    known_port = None
    if known_ip:
        known_port = input("Enter known node Port (or press Enter if none): ")
    known_node = (known_ip, int(known_port)) if known_ip and known_port else None
    node = ChordNode(port=port, known_node=known_node, m=m)
    node.start()
    threading.Thread(target=cli, args=(node,), daemon=False).start()