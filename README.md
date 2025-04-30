# Chord-Based Distributed File System

Welcome to the **Chord-Based Distributed File System**! This project implements a distributed hash table (DHT) using the Chord protocol, enabling efficient file storage and retrieval in a decentralized network.

---

## 🚀 Features

- **Decentralized File Storage**: Store and retrieve files across a distributed network of nodes.
- **Fault Tolerance**: Nodes can join and leave the network gracefully without disrupting the system.
- **Efficient Lookup**: Uses the Chord protocol for fast and scalable file lookups.
- **Command-Line Interface (CLI)**: Interact with the system using an intuitive CLI.

---

## 📂 Project Structure

```
├── chord.py          # Main implementation of the Chord protocol

├── utils.py          # Utility functions for hashing, IP retrieval, etc.

├── test.txt          # Sample file for testing

├── README.md         # Documentation for the project
```

---

## 🛠️ Prerequisites

Before running the program, ensure you have the following installed:

1. **Python 3.8+**: [Download Python](https://www.python.org/downloads/)
2. **Required Libraries**: Install dependencies using `pip`:
   
   ```bash
   pip install colorama
   ```

---

## ▶️ How to Run the Program

### Step 1: Clone or Extract the Repository

- **From GitHub**: Clone the repository:
  
  ```bash
  git clone https://github.com/your-username/your-repo-name.git
  cd your-repo-name
  ```
- **From ZIP File**: Extract the ZIP file and navigate to the extracted folder.

---

### Step 2: Start a Node

1. Open a terminal and navigate to the project directory.
2. Run the following command to start a node:
   
   ```bash
   python chord.py
   ```
3. Follow the prompts:
   - **Enter the number of bits for the identifier space (m)**: Choose a value between 1 and 32 (e.g., `3`).
   - **Enter Port**: Specify a port number for the node (e.g., `5000`).
   - **Enter known node IP**: If this is the first node, press Enter. Otherwise, provide the IP of an existing node.
   - **Enter known node Port**: If you entered an IP, provide the port of the known node.

---

### Step 3: Interact with the CLI

Once the node starts, you'll see the **Chord CLI**. Use the following commands to interact with the system:

| Command       | Description                                                                 |
|---------------|-----------------------------------------------------------------------------|
| `help`        | Display a list of available commands.                                      |
| `ft`          | Display the current finger table of the node.                              |
| `state`       | Show the node's state, including its ID, predecessor, successor, and data. |
| `upload`      | Upload a file to the network. Usage: `upload <file_path>`                  |
| `get`         | Retrieve a file from the network. Usage: `get <key>`                      |
| `delete`      | Delete a file from the network. Usage: `delete <key>`                     |
| `files`       | List all files currently stored in the network.                           |
| `debug`       | Toggle debug mode for detailed logs.                                       |
| `leave`       | Gracefully leave the network.                                              |

---

## ✅ Verifying the Program Works

### Test Case 1: Upload and Retrieve a File

1. Start two or more nodes in separate terminals.
2. On one node, upload a file:
   ```bash
   upload test.txt
   ```
3. On another node, retrieve the file using its key:
   ```bash
   get <key>
   ```
   Replace `<key>` with the key generated during the upload.

### Test Case 2: List Files in the Network

1. On any node, list all files stored in the network:
   ```bash
   files
   ```

### Test Case 3: Node Join and Leave

1. Start a new node and join it to the network using the IP and port of an existing node.
2. Verify the new node's state using the `state` command.
3. Gracefully leave the network using the `leave` command and observe how the network stabilizes.

---

## 🛡️ Troubleshooting

- **Port Already in Use**: Ensure the port you specify is not being used by another application.
- **File Not Found**: Ensure the file path is correct when using the `upload` command.
- **Connection Issues**: Verify that all nodes are running on the same network.

---

## 📖 How It Works

This project uses the **Chord protocol** to distribute files across a network of nodes. Each file is assigned a unique key using SHA-1 hashing, and nodes are responsible for storing files whose keys fall within their range. The finger table enables efficient lookups, reducing the number of hops required to find a file.

---

## 🖼️ Screenshots

### Chord CLI
![Chord CLI Screenshot](https://via.placeholder.com/800x400?text=Chord+CLI+Screenshot)

---

## 🚧 Missing Features and Future Improvements

### Missing Features
1. **Replication**: Add redundancy by replicating files across multiple nodes to improve fault tolerance.
2. **File Versioning**: Implement version control for files to handle updates and conflicts.
3. **Dynamic Node Stabilization**: Improve the stabilization process to handle high churn rates more effectively.
4. **Web Interface**: Create a user-friendly web-based interface for non-technical users.
5. **Authentication and Security**: Add encryption and authentication mechanisms to secure file transfers and node communication.

### Future Improvements
1. **Scalability Testing**: Conduct stress tests to evaluate the system's performance with thousands of nodes.
2. **Cross-Network Support**: Enable nodes to communicate across different networks using NAT traversal techniques.
3. **Monitoring and Analytics**: Add tools to monitor network health, node activity, and file distribution.
4. **Integration with Cloud Services**: Allow nodes to offload storage to cloud services for hybrid deployments.
5. **Industry Use Cases**: Adapt the system for specific use cases like IoT data storage, blockchain, or distributed databases.

---

## 🤝 Contributing

Contributions are welcome! Feel free to fork this repository and submit a pull request.

---

## 📜 License

This project is licensed under the MIT License. See the LICENSE file for details.

---

## 💡 Acknowledgments

- **Chord Protocol**: Inspired by the original Chord DHT paper.
- **Colorama**: For cross-platform terminal color support.

---

Thank you for using the **Chord-Based Distributed File System**! If you have any questions or feedback, feel free to reach out.
