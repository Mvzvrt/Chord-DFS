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

def generate_key_id(data, m):
    sha1_hash = hashlib.sha1(data.encode()).hexdigest()
    key_id = int(sha1_hash, 16) & ((1 << m) - 1)
    return key_id

def display_finger_table(node_id, finger_table, m):
    # Print the initialized finger table as a formatted table
    print(f"\nFinger Table for Node {node_id}:")
    print(f"{'Node ID':<6} {'Start':<10} {'Interval':<20} {'Successor':<20}")
    print("-" * 60)
    for i, entry in enumerate(finger_table, start=1):
        print(f"{generate_node_id(entry['successor'][0], entry['successor'][1], m):<6} {entry['start']:<10} {str(entry['interval']):<20} {str(entry['successor']):<20}")
    print("-" * 60)

def in_range(value, start, end):
    if start < end:
        return start < value <= end
    else:
        return value > start or value <= end
