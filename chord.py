import socket
import threading
from utils import get_local_ip, generate_node_id, generate_key_id, display_finger_table, in_range

# Set the number of bits for the identifier space (m bits)
m = 3  # You can adjust this as needed

class ChordNode:
    def __init__(self, port=5000, known_node=None):
        self.ip = get_local_ip()
        self.port = port
        self.node_id = generate_node_id(self.ip, self.port, m)
        self.successor = (self.ip, self.port)
        self.predecessor = (self.ip, self.port)
        self.finger_table = []  # Initialize the finger table

        # Setup UDP socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.ip, self.port))

        # Initialize the finger table
        self.initialize_finger_table()

        # Join the ring if a known node is provided
        if known_node:
            self.join(known_node)

    def initialize_finger_table(self):
        for i in range(1, m + 1):
            start = (self.node_id + 2**(i - 1)) % (2**m)
            interval = ((start, (self.node_id + 2**i) % (2**m)))
            successor = (self.ip, self.port)  # Initially, assume itself as the successor
            self.finger_table.append({'start': start, 'interval': interval, 'successor': successor})

        display_finger_table(self.node_id, self.finger_table)

    def start(self):
        threading.Thread(target=self.listen, daemon=True).start()
        print(f"Node {self.node_id} started at {self.ip}:{self.port}")

    def listen(self):
        while True:
            data, addr = self.sock.recvfrom(1024)
            message = data.decode()
            print(f"Received message from {addr}: {message}")
            self.handle_message(message, addr)

    def handle_message(self, message, addr):
        parts = message.split()
        if parts[0] == 'PING':
            self.send_message('PONG', addr)
        elif parts[0] == 'PONG':
            print(f"PONG received from {addr}")
        elif parts[0] == 'FIND_SUCCESSOR':
            start_value = int(parts[1])
            index = int(parts[2]) if len(parts) > 2 else None
            successor = self.find_successor(start_value)
            response = f"SUCCESSOR {successor[0]} {successor[1]} {index}" if index is not None else f"SUCCESSOR {successor[0]} {successor[1]}"
            self.send_message(response, addr)
        elif parts[0] == 'SUCCESSOR':
            index = int(parts[3]) if len(parts) > 3 else 0
            self.finger_table[index]['successor'] = (parts[1], int(parts[2]))
            if index == 0:
                self.successor = (parts[1], int(parts[2]))
                self.request_predecessor(self.successor)
            display_finger_table(self.node_id, self.finger_table)
        elif parts[0] == 'GET_PREDECESSOR':
            self.send_message(f"PREDECESSOR {self.predecessor[0]} {self.predecessor[1]}", addr)
        elif parts[0] == 'PREDECESSOR':
            self.predecessor = (parts[1], int(parts[2]))
            self.notify_successor()
            self.set_finger_table()
        elif parts[0] == 'SET_PREDECESSOR':
            self.predecessor = (parts[1], int(parts[2]))

    def send_message(self, message, target):
        self.sock.sendto(message.encode(), target)

    def join(self, known_node):
        start_value = self.finger_table[0]['start']
        self.send_message(f'FIND_SUCCESSOR {start_value} 0', known_node)

    def find_successor(self, id):
        predecessor = self.find_predecessor(id)
        return predecessor

    def find_predecessor(self, id):
        if in_range(id, self.node_id, generate_node_id(self.finger_table[0]['successor'][0], self.finger_table[0]['successor'][1], m)):
            return (self.ip, self.port)
        else:
            return self.closest_preceding_finger(id)

    def closest_preceding_finger(self, id):
        for i in range(m-1, -1, -1):
            finger_node_id = generate_node_id(self.finger_table[i]['successor'][0], self.finger_table[i]['successor'][1], m)
            if in_range(finger_node_id, self.node_id, id):
                return self.finger_table[i]['successor']
        return (self.ip, self.port)

    def request_predecessor(self, successor):
        self.send_message('GET_PREDECESSOR', successor)

    def notify_successor(self):
        self.send_message(f'SET_PREDECESSOR {self.ip} {self.port}', self.successor)

    def set_finger_table(self):
        for i in range(1, m):  # From second to the last entry
            current_successor = self.finger_table[i - 1]['successor']
            next_start = self.finger_table[i]['start']
            current_node_id = generate_node_id(current_successor[0], current_successor[1], m)

            if in_range(next_start, self.node_id, current_node_id):
                self.finger_table[i]['successor'] = current_successor
            else:
                self.send_message(f'FIND_SUCCESSOR {next_start} {i}', self.successor)

        display_finger_table(self.node_id, self.finger_table)

if __name__ == "__main__":
    port = int(input("Enter Port: "))
    known_ip = input("Enter known node IP (or press Enter if none): ")
    if (known_ip):
        known_port = input("Enter known node Port (or press Enter if none): ")
    else:
        known_port = None
        known_ip = None

    known_node = (known_ip, int(known_port)) if known_ip and known_port else None
    node = ChordNode(port=port, known_node=known_node)
    node.start()

    while True:
        pass
