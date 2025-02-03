import socket
import threading
from utils import get_local_ip, generate_node_id

# Set the number of bits for the identifier space (m bits)
m = 10  # You can adjust this as needed

class ChordNode:
    def __init__(self, port=5000):
        self.ip = get_local_ip()
        self.port = port
        self.node_id = generate_node_id(self.ip, self.port, m)
        self.successor = None
        self.predecessor = None

        # Setup UDP socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.ip, self.port))

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
        cmd = input("Enter command (ping [IP] [PORT]): ")
        if cmd.startswith('ping'):
            _, ip, port = cmd.split()
            node.send_message('PING', (ip, int(port)))
