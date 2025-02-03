import socket
import threading
from utils import get_local_ip, generate_node_id, generate_key_id

# Set the number of bits for the identifier space (m bits)
m = 3  # You can adjust this as needed

class ChordNode:
    def __init__(self, port=5000):
        self.ip = get_local_ip()
        self.port = port
        self.node_id = generate_node_id(self.ip, self.port, m)
        self.successor = None
        self.predecessor = None
        self.finger_table = []  # Initialize the finger table

        # Setup UDP socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.ip, self.port))

        # Initialize the finger table
        self.initialize_finger_table()

    def initialize_finger_table(self):
        for i in range(1, m + 1):
            start = (self.node_id + 2**(i - 1)) % (2**m)
            interval = ((start, (self.node_id + 2**i) % (2**m)))
            successor = (self.ip, self.port)  # Initially, assume itself as the successor
            self.finger_table.append({'start': start, 'interval': interval, 'successor': successor})

        # Print the initialized finger table as a formatted table
        print(f"\nFinger Table for Node {self.node_id}:")
        print(f"{'Entry':<6} {'Start':<10} {'Interval':<20} {'Successor':<20}")
        print("-" * 60)
        for i, entry in enumerate(self.finger_table, start=1):
            print(f"{i:<6} {entry['start']:<10} {str(entry['interval']):<20} {str(entry['successor']):<20}")
        print("-" * 60)

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
        if message == 'PING':
            self.send_message('PONG', addr)
        elif message == 'PONG':
            print(f"PONG received from {addr}")

    def send_message(self, message, target):
        self.sock.sendto(message.encode(), target)

if __name__ == "__main__":
    port = int(input("Enter Port: "))
    node = ChordNode(port=port)
    node.start()

    while True:
        cmd = input("Enter command (ping [IP] [PORT] or hash [DATA]): ")
        if cmd.startswith('ping'):
            _, ip, port = cmd.split()
            node.send_message('PING', (ip, int(port)))
        elif cmd.startswith('hash'):
            _, data = cmd.split(maxsplit=1)
            key_id = generate_key_id(data, m)
            print(f"Key ID for data '{data}' is: {key_id}")
