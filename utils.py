import socket
import hashlib

def get_local_ip():
        # Automatically retrieve the local IP address
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(('8.8.8.8', 80))
            ip = s.getsockname()[0]
        except Exception:
            ip = '127.0.0.1'
        finally:
            s.close()
        return ip

def generate_node_id(ip, port, m):
    identifier = f"{ip}:{port}"
    sha1_hash = hashlib.sha1(identifier.encode()).hexdigest()
    # Convert the hash to an integer and apply a mask to get m bits
    node_id = int(sha1_hash, 16) & ((1 << m) - 1)
    return node_id