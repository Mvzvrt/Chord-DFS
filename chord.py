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
        self.data = {}  # Local key/value store for the DHT

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
                data, addr = self.sock.recvfrom(4096)
                message = data.decode()
                debug_print(f"Received message from {addr}: {message}")
                self.handle_message(message, addr)
            except Exception as e:
                if self.running:
                    debug_print(f"Listen error: {e}")
                    raise

    def handle_message(self, message, addr):
        parts = message.split()
        if not parts:
            return
        cmd = parts[0]
        if cmd == 'PING':
            self.send_message('PONG', addr)
        elif cmd == 'PONG':
            debug_print(f"PONG received from {addr}")
        elif cmd == 'FIND_SUCCESSOR':
            start_value = int(parts[1])
            index = int(parts[2]) if len(parts) > 2 else None
            successor = self.find_successor_recursive(start_value)
            if successor is None:
                return
            response = (f"SUCCESSOR {successor[0]} {successor[1]} {index}"
                        if index is not None else
                        f"SUCCESSOR {successor[0]} {successor[1]}")
            self.send_message(response, addr)
        elif cmd == 'SUCCESSOR':
            index = int(parts[3]) if len(parts) > 3 else 0
            with self.lock:
                self.finger_table[index]['successor'] = (parts[1], int(parts[2]))
                if index == 0:
                    self.successor = (parts[1], int(parts[2]))
                    self.request_predecessor(self.successor)
        elif cmd == 'GET_PREDECESSOR':
            self.send_message(f"PREDECESSOR {self.predecessor[0]} {self.predecessor[1]}", addr)
        elif cmd == 'PREDECESSOR':
            with self.lock:
                self.predecessor = (parts[1], int(parts[2]))
        elif cmd == 'SET_PREDECESSOR':
            with self.lock:
                self.predecessor = (parts[1], int(parts[2]))
        elif cmd == 'UPDATE_FINGER_TABLE':
            node_ip, node_port, i = parts[1], int(parts[2]), int(parts[3])
            self.update_finger_table((node_ip, node_port), i)
        elif cmd == "GET_SUCCESSOR":
            self.send_message(f"SUCCESSOR_INFO {self.successor[0]} {self.successor[1]}", addr)
        elif cmd == "RPC_CLOSEST_PRECEDING":
            query_id = int(parts[1])
            node = self.closest_preceding_finger(query_id)
            self.send_message(f"CLOSEST_PRECEDING {node[0]} {node[1]}", addr)
        elif cmd == 'UPDATE_SUCCESSOR':
            new_successor = (parts[1], int(parts[2]))
            with self.lock:
                self.successor = new_successor
                self.finger_table[0]['successor'] = new_successor
        elif cmd == 'UPDATE_PREDECESSOR':
            new_predecessor = (parts[1], int(parts[2]))
            with self.lock:
                self.predecessor = new_predecessor
        # --- DHT Operations ---
        elif cmd == 'PUT':
            # Format: PUT <key> <value...>
            key = parts[1]
            value = " ".join(parts[2:]) if len(parts) > 2 else ""
            key_id = generate_key_id(key, self.m)
            if self.is_responsible_for(key_id):
                with self.lock:
                    self.data[key] = value
                self.send_message("PUT_ACK", addr)
                debug_print(f"Stored key '{key}' locally with value: {value}")
            else:
                successor = self.find_successor_recursive(key_id)
                self.send_message(f"PUT {key} {value}", successor)
        elif cmd == 'PUT_ACK':
            debug_print("Received PUT_ACK")
        elif cmd == 'GET':
            # Format: GET <key>
            key = parts[1]
            key_id = generate_key_id(key, self.m)
            if self.is_responsible_for(key_id):
                with self.lock:
                    value = self.data.get(key, "None")
                self.send_message(f"GET_REPLY {key} {value}", addr)
            else:
                successor = self.find_successor_recursive(key_id)
                response = self.rpc(f"GET {key}", successor)
                if response:
                    self.send_message(response, addr)
        elif cmd == 'GET_REPLY':
            # For an RPC response, simply forward the reply.
            self.send_message(message, addr)
        elif cmd == 'DELETE':
            # Format: DELETE <key>
            key = parts[1]
            key_id = generate_key_id(key, self.m)
            if self.is_responsible_for(key_id):
                with self.lock:
                    if key in self.data:
                        del self.data[key]
                        self.send_message(f"DELETE_ACK {key} deleted", addr)
                        debug_print(f"Deleted key '{key}' locally.")
                    else:
                        self.send_message(f"DELETE_ACK {key} not_found", addr)
            else:
                successor = self.find_successor_recursive(key_id)
                response = self.rpc(f"DELETE {key}", successor)
                if response:
                    self.send_message(response, addr)
        elif cmd == 'DELETE_ACK':
            debug_print(f"Received DELETE_ACK: {message}")
        elif cmd == "TRANSFER_KEYS_REQUEST":
            # Format: TRANSFER_KEYS_REQUEST <new_node_ip> <new_node_port> <new_node_id> <new_node_pred_id>
            new_node_ip = parts[1]
            new_node_port = int(parts[2])
            new_node_id = int(parts[3])
            new_node_pred_id = int(parts[4])
            keys_to_transfer = []
            with self.lock:
                # Find keys for which the new node is now responsible.
                for key, value in list(self.data.items()):
                    key_id = generate_key_id(key, self.m)
                    if in_range(key_id, new_node_pred_id, new_node_id, self.m):
                        keys_to_transfer.append((key, value))
                        del self.data[key]
            if keys_to_transfer:
                data_str = ";;".join([f"{k}|{v}" for k, v in keys_to_transfer])
            else:
                data_str = ""
            self.send_message(f"TRANSFER_KEYS_REPLY {data_str}", (new_node_ip, new_node_port))
            debug_print(f"Transferred keys {keys_to_transfer} to new node {new_node_ip}:{new_node_port}")
        elif cmd == "TRANSFER_KEYS_REPLY":
            # Format: TRANSFER_KEYS_REPLY <key1>|<value1>;;<key2>|<value2>;;...
            data_str = " ".join(parts[1:])
            if data_str:
                pairs = data_str.split(";;")
                with self.lock:
                    for pair in pairs:
                        if pair:
                            k, v = pair.split("|", 1)
                            self.data[k] = v
                debug_print(f"Received transferred keys: {self.data}")
        else:
            debug_print(f"Unknown command: {message}")

    def send_message(self, message, target):
        try:
            self.sock.sendto(message.encode(), target)
        except Exception as e:
            debug_print(f"Failed to send message to {target}: {e}")

    def rpc(self, message, target, timeout=2, retries=3):
        """A simple RPC mechanism over UDP with retries."""
        for attempt in range(retries):
            temp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            temp_sock.settimeout(timeout)
            try:
                temp_sock.sendto(message.encode(), target)
                data, _ = temp_sock.recvfrom(4096)
                temp_sock.close()
                return data.decode()
            except Exception as e:
                debug_print(f"RPC error on attempt {attempt+1} to target {target}: {e}")
                temp_sock.close()
                continue
        return None

    def join(self, known_node):
        start_value = self.finger_table[0]['start']
        self.send_message(f'FIND_SUCCESSOR {start_value} 0', known_node)
        # Delay key retrieval so stabilization can finish.
        threading.Thread(target=self.delayed_retrieve_keys, daemon=True).start()
        # Immediately propagate update to affected nodes.
        threading.Thread(target=self.update_others, daemon=True).start()

    def delayed_retrieve_keys(self):
        time.sleep(2)
        with self.lock:
            pred_id = generate_node_id(self.predecessor[0], self.predecessor[1], self.m)
            self_id = self.node_id
            successor = self.successor
        # Ask our successor to transfer keys for which we are now responsible.
        self.send_message(f"TRANSFER_KEYS_REQUEST {self.ip} {self.port} {self_id} {pred_id}", successor)

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

    # --- Immediate Update Propagation Methods ---
    def find_predecessor(self, id):
        """
        Finds the node p such that id is in (p, p.successor].
        This is used during the update_others process.
        """
        p = (self.ip, self.port)
        while True:
            # Get p's successor
            if p == (self.ip, self.port):
                with self.lock:
                    p_successor = self.successor
            else:
                response = self.rpc("GET_SUCCESSOR", p)
                if response:
                    parts = response.split()
                    if parts[0] == "SUCCESSOR_INFO":
                        p_successor = (parts[1], int(parts[2]))
                    else:
                        p_successor = (self.ip, self.port)
                else:
                    p_successor = (self.ip, self.port)
            p_id = generate_node_id(p[0], p[1], self.m)
            ps_id = generate_node_id(p_successor[0], p_successor[1], self.m)
            if in_range(id, p_id, ps_id, self.m):
                return p
            else:
                # Use local closest_preceding_finger if p is self,
                # otherwise ask p for its closest preceding finger.
                if p == (self.ip, self.port):
                    p = self.closest_preceding_finger(id)
                else:
                    response = self.rpc(f"RPC_CLOSEST_PRECEDING {id}", p)
                    if response:
                        parts = response.split()
                        if parts[0] == "CLOSEST_PRECEDING":
                            p = (parts[1], int(parts[2]))
                        else:
                            break
                    else:
                        break
        return p

    def update_others(self):
        """
        For each i from 1 to m, find the last node p whose i-th finger might need to be updated,
        and send it an update message.
        """
        for i in range(1, self.m + 1):
            pred_index = (self.node_id - 2**(i - 1) + 2**self.m) % (2**self.m)
            p = self.find_predecessor(pred_index)
            if p != (self.ip, self.port):
                self.rpc(f"UPDATE_FINGER_TABLE {self.ip} {self.port} {i - 1}", p)
            # Small delay to avoid flooding
            time.sleep(0.1)

    # --- Stabilization Protocols with Timeouts and Fallbacks ---
    def stabilize(self):
        """Periodically verify and update the successor, then notify the successor about self."""
        while self.running:
            try:
                with self.lock:
                    successor = self.successor
                # Attempt to get the predecessor of our successor with a timeout.
                response = self.rpc("GET_PREDECESSOR", successor, timeout=1)
                if response:
                    parts = response.split()
                    if parts[0] == "PREDECESSOR":
                        x = (parts[1], int(parts[2]))
                        x_id = generate_node_id(x[0], x[1], self.m)
                        succ_id = generate_node_id(successor[0], successor[1], self.m)
                        # If x is a better candidate for successor, update it.
                        if x != (self.ip, self.port) and in_range(x_id, self.node_id, succ_id, self.m):
                            with self.lock:
                                self.successor = x
                                self.finger_table[0]['successor'] = x
                else:
                    # No response from current successor; try alternate candidates from the finger table.
                    debug_print(f"Stabilize: Successor {successor} not responding; checking alternate fingers.")
                    alternate = None
                    with self.lock:
                        for entry in self.finger_table:
                            candidate = entry['successor']
                            if candidate != (self.ip, self.port) and candidate != successor:
                                ping_response = self.rpc("PING", candidate, timeout=1)
                                if ping_response:
                                    alternate = candidate
                                    break
                    if alternate:
                        with self.lock:
                            self.successor = alternate
                            self.finger_table[0]['successor'] = alternate
                        debug_print(f"Stabilize: Alternate candidate {alternate} set as new successor.")
                    else:
                        with self.lock:
                            self.successor = (self.ip, self.port)
                            self.finger_table[0]['successor'] = (self.ip, self.port)
                        debug_print("Stabilize: No alternate candidate found; set successor to self.")

                # Notify the (possibly updated) successor about self.
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
        """Periodically check whether the predecessor is still alive using timeouts."""
        while self.running:
            try:
                with self.lock:
                    predecessor = self.predecessor
                if predecessor != (self.ip, self.port):
                    response = self.rpc("PING", predecessor, timeout=1)
                    if response is None:
                        debug_print("Check predecessor: Predecessor not responding; resetting predecessor to self.")
                        with self.lock:
                            self.predecessor = (self.ip, self.port)
            except Exception as e:
                debug_print(f"Check predecessor error: {e}")
                with self.lock:
                    self.predecessor = (self.ip, self.port)
            time.sleep(1)

    def is_responsible_for(self, key_id):
        """
        Determine whether this node is responsible for the given key_id.
        In Chord, a node is responsible for keys in (predecessor, self].
        If the node is alone (predecessor == self) then it is responsible for all keys.
        """
        if self.predecessor == (self.ip, self.port):
            return True
        pred_id = generate_node_id(self.predecessor[0], self.predecessor[1], self.m)
        return in_range(key_id, pred_id, self.node_id, self.m)

    # --- DHT Operations for Clients ---
    def put(self, key, value):
        """Store a key/value pair in the DHT."""
        key_id = generate_key_id(key, self.m)
        if self.is_responsible_for(key_id):
            with self.lock:
                self.data[key] = value
            print(f"Key '{key}' stored locally.")
        else:
            successor = self.find_successor_recursive(key_id)
            response = self.rpc(f"PUT {key} {value}", successor)
            if response and response.startswith("PUT_ACK"):
                print(f"Key '{key}' stored on remote node.")
            else:
                print(f"Failed to store key '{key}'.")

    def get(self, key):
        """Retrieve a key's value from the DHT."""
        key_id = generate_key_id(key, self.m)
        if self.is_responsible_for(key_id):
            with self.lock:
                value = self.data.get(key)
            if value is not None:
                print(f"Value for key '{key}': {value}")
            else:
                print(f"Key '{key}' not found.")
        else:
            successor = self.find_successor_recursive(key_id)
            response = self.rpc(f"GET {key}", successor)
            if response and response.startswith("GET_REPLY"):
                parts = response.split(maxsplit=2)
                if len(parts) >= 3:
                    print(f"Value for key '{key}': {parts[2]}")
                else:
                    print(f"Key '{key}' not found on remote node.")
            else:
                print(f"Failed to retrieve key '{key}'.")

    def delete(self, key):
        """Delete a key/value pair from the DHT."""
        key_id = generate_key_id(key, self.m)
        if self.is_responsible_for(key_id):
            with self.lock:
                if key in self.data:
                    del self.data[key]
                    print(f"Key '{key}' deleted locally.")
                else:
                    print(f"Key '{key}' not found.")
        else:
            successor = self.find_successor_recursive(key_id)
            response = self.rpc(f"DELETE {key}", successor)
            if response and response.startswith("DELETE_ACK"):
                print(f"Key '{key}' deleted on remote node.")
            else:
                print(f"Failed to delete key '{key}'.")

    def leave_gracefully(self):
        """
        Transfer keys to the successor (if any), then notify the predecessor
        and successor so that they update their pointers before shutting down.
        """
        with self.lock:
            pred = self.predecessor
            succ = self.successor
        debug_print("Leaving the network gracefully...")
        # Transfer all keys to successor.
        if succ != (self.ip, self.port) and self.data:
            data_str = ";;".join([f"{k}|{v}" for k, v in self.data.items()])
            self.send_message(f"TRANSFER_KEYS_REPLY {data_str}", succ)
            debug_print(f"Transferred local keys to successor {succ}.")
        if pred != (self.ip, self.port):
            self.send_message(f"UPDATE_SUCCESSOR {succ[0]} {succ[1]}", pred)
        if succ != (self.ip, self.port):
            self.send_message(f"UPDATE_PREDECESSOR {pred[0]} {pred[1]}", succ)
        self.running = False
        self.sock.close()
        debug_print("Node has left the network.")

def cli(node):
    """A simple command-line interface running concurrently with the node."""
    help_text = (
        "Commands:\n"
        "  ft           - Display the current finger table\n"
        "  state        - Show current node state\n"
        "  put <k> <v>  - Store key/value pair in the DHT\n"
        "  get <k>      - Retrieve value for key from the DHT\n"
        "  delete <k>   - Delete key from the DHT\n"
        "  debug        - Toggle debug messages\n"
        "  leave        - Gracefully leave the network\n"
        "  help         - Show this help message\n"
        "  quit         - Exit the CLI and terminate the node"
    )
    print(help_text)
    while True:
        cmd = input("ChordCLI> ").strip()
        if not cmd:
            continue
        tokens = cmd.split()
        command = tokens[0].lower()
        if command in ["ft", "finger", "fingers"]:
            display_finger_table(node.node_id, node.finger_table, node.m)
        elif command == "help":
            print(help_text)
        elif command == "debug":
            global DEBUG
            DEBUG = not DEBUG
            print(f"Debug mode {'enabled' if DEBUG else 'disabled'}.")
        elif command == "state":
            with node.lock:
                print(f"Node ID: {node.node_id}")
                print(f"Predecessor: {node.predecessor}")
                print(f"Successor: {node.successor}")
                display_finger_table(node.node_id, node.finger_table, node.m)
                print("Stored Data:", node.data)
        elif command == "put":
            if len(tokens) < 3:
                print("Usage: put <key> <value>")
            else:
                key = tokens[1]
                value = " ".join(tokens[2:])
                node.put(key, value)
        elif command == "get":
            if len(tokens) < 2:
                print("Usage: get <key>")
            else:
                key = tokens[1]
                node.get(key)
        elif command == "delete":
            if len(tokens) < 2:
                print("Usage: delete <key>")
            else:
                key = tokens[1]
                node.delete(key)
        elif command == "leave":
            node.leave_gracefully()
            print("Node is leaving the network. Goodbye!")
            exit(0)
        elif command == "quit":
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
