import json
import socket
import threading
from dataclasses import dataclass
from typing import List, Optional
import time
import hashlib
import struct
from threading import Lock
import queue
from datetime import datetime
from io import BytesIO
from simpledb import SimpleDB
from simpledb import BinLog



class Node:
    """Single node implementation, independently manages Socket communication with other nodes"""
    def __init__(self, node_id: str, host: str, port: int):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers: List[dict] = []  # Store other node information
        self.server_thread = None

    def start_server(self):
        """Start listening server"""
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(5)
        print(f"Node {self.node_id} server started at {self.host}:{self.port}")

        def handle_client(client_socket, address):
            try:
                data = client_socket.recv(1024).decode('utf-8')
                message = json.loads(data)
                if message['type'] == 'binlog_update':
                    print(f"Node {self.node_id} received binlog update from {address}")
                    binlog_data = bytes.fromhex(message['data'])
                    self.process_binlog(binlog_data)
                elif message['type'] == 'sync_request':
                    print(f"Node {self.node_id} received sync request from {address}")
                    response = {'status': 'ok'}
                    client_socket.send(json.dumps(response).encode('utf-8'))
            except Exception as e:
                print(f"Node {self.node_id} error handling client: {e}")
            finally:
                client_socket.close()

        def listen():
            while True:
                client_socket, address = server.accept()
                threading.Thread(target=handle_client, args=(client_socket, address)).start()

        self.server_thread = threading.Thread(target=listen, daemon=True)
        self.server_thread.start()

    def connect_to_peer(self, peer_id: str, peer_host: str, peer_port: int):
        """Connect to other nodes"""
        self.peers.append({'id': peer_id, 'host': peer_host, 'port': peer_port})
        print(f"Node {self.node_id} connected to node {peer_id} ({peer_host}:{peer_port})")

    def send_binlog_to_peer(self, peer_id: str, binlog_data: bytes):
        """Send binlog to specified node"""
        peer = next((p for p in self.peers if p['id'] == peer_id), None)
        if not peer:
            print(f"Node {peer_id} doesn't exist in peer list of node {self.node_id}")
            return
        try:
            with socket.create_connection((peer['host'], peer['port'])) as sock:
                message = {
                    'type': 'binlog_update',
                    'data': binlog_data.hex()
                }
                sock.send(json.dumps(message).encode('utf-8'))
                print(f"Node {self.node_id} sent binlog to node {peer_id}")
        except Exception as e:
            print(f"Node {self.node_id} error sending binlog to node {peer_id}: {e}")

    def process_binlog(self, binlog_data: bytes):
        """Process received binlog data"""
        print(f"Node {self.node_id} processing binlog data: {binlog_data.hex()}")

        try:
            binlog = BinLog.from_bytes(binlog_data)
            for op in binlog.operations:
                table_name = op['table']
                if table_name not in self.db.get_tables():
                    print(f"Node {self.node_id} detected table {table_name} doesn't exist, creating...")
                    self.db.create_table(table_name)

            self.db.apply_binlog(binlog_data)
            print(f"Node {self.node_id} applied binlog data successfully, current database state:")
            self.db.print_database_state()

        except Exception as e:
            print(f"Node {self.node_id} error parsing binlog: {e}")


@dataclass
class Block:
    """Block structure"""
    previous_hash: bytes      # 32 bytes
    timestamp: float         # 8 bytes
    binlog_data: bytes      # binlog data
    hash: Optional[bytes] = None

    def to_bytes(self) -> bytes:
        """Convert block to binary format"""
        buffer = BytesIO()

        # Magic Number (4 bytes)
        buffer.write(b'BLCK')

        # Write previous_hash (32 bytes)
        buffer.write(self.previous_hash)

        # Write timestamp (8 bytes)
        buffer.write(struct.pack('!d', self.timestamp))

        # Write binlog data length and data
        buffer.write(struct.pack('!I', len(self.binlog_data)))
        buffer.write(self.binlog_data)

        return buffer.getvalue()

    # Read binlog
    @classmethod
    def from_bytes(cls, data: bytes) -> 'Block':
        """Recover block from binary data"""
        buffer = BytesIO(data)

        # Verify Magic Number
        magic = buffer.read(4)
        if magic != b'BLCK':
            raise ValueError("Invalid block format")

        # Read previous_hash
        previous_hash = buffer.read(32)

        # Read timestamp
        timestamp = struct.unpack('!d', buffer.read(8))[0]

        # Read binlog data
        binlog_length = struct.unpack('!I', buffer.read(4))[0]
        binlog_data = buffer.read(binlog_length)

        block = cls(
            previous_hash=previous_hash,
            timestamp=timestamp,
            binlog_data=binlog_data
        )
        block.seal()
        return block

    def calculate_hash(self) -> bytes:
        """Calculate block hash"""
        block_bytes = self.to_bytes()
        return hashlib.sha256(block_bytes).digest()

    def seal(self):
        """Calculate and set block hash"""
        self.hash = self.calculate_hash()

    def print_hex_dump(self, data: bytes) -> str:
        """Format print hexadecimal data"""
        hex_str = ''.join(f'{b:02x}' for b in data)
        return hex_str

    def print_block_info(self):
        """Print block information"""
        print("\n=== Block Information ===")
        print(f"Block hash: {self.hash.hex()}")
        print(f"Previous block hash: {self.previous_hash.hex()}")
        print(f"Timestamp: {datetime.fromtimestamp(self.timestamp)}")
        print(f"Block size: {len(self.to_bytes())} bytes")
        print(f"Binlog size: {len(self.binlog_data)} bytes")

        print("\nBlock binary data:")
        print(self.print_hex_dump(self.to_bytes()))

        if self.binlog_data:
            print("\nBinlog binary data:")
            print(self.print_hex_dump(self.binlog_data))

class Blockchain:
    """Blockchain class"""
    def __init__(self):
        self.chain: List[Block] = []
        self.lock = Lock()
        self.pending_binlogs = queue.Queue()
        self.last_synced_hash: Optional[bytes] = None

        self._create_genesis_block()

    def _create_genesis_block(self):
        """Create genesis block"""
        genesis_block = Block(
            previous_hash=b'\x00' * 32,
            timestamp=time.time(),
            binlog_data=b''
        )
        genesis_block.seal()
        self.chain.append(genesis_block)
        print("Genesis block created")

    def add_binlog(self, binlog_data: bytes):
        """Add new binlog data to pending queue"""
        self.pending_binlogs.put(binlog_data)
        print(f"New binlog data added to pending queue (size: {len(binlog_data)} bytes)")

    def create_block(self) -> Optional[Block]:
        """Create new block from pending binlog data"""
        try:
            binlog_data = self.pending_binlogs.get_nowait()

            with self.lock:
                new_block = Block(
                    previous_hash=self.chain[-1].hash,
                    timestamp=time.time(),
                    binlog_data=binlog_data
                )
                new_block.seal()
                self.chain.append(new_block)
                print("\nNew block created and added to chain")
                return new_block

        except queue.Empty:
            return None

    def validate_chain(self) -> bool:
        """Validate the entire blockchain"""
        for i in range(1, len(self.chain)):
            current_block = self.chain[i]
            previous_block = self.chain[i-1]

            # Verify current block hash
            if current_block.hash != current_block.calculate_hash():
                print(f"Block {i} hash is invalid")
                return False

            # Verify blockchain connection
            if current_block.previous_hash != previous_block.hash:
                print(f"Block {i} connection to previous block is invalid")
                return False

        return True

    def print_chain_info(self):
        """Print blockchain information"""
        print("\n=== Blockchain Information ===")
        print(f"Block count: {len(self.chain)}")
        print(f"Is valid: {self.validate_chain()}")

        for i, block in enumerate(self.chain):
            print(f"\nBlock {i}:")
            block.print_block_info()

    def sync_from_chain(self, db: SimpleDB, last_synced_hash: Optional[str] = None):
        """Sync database from blockchain, only apply unapplied binlog"""
        print("\n=== Starting database synchronization ===")

        # Find the starting synchronization position
        start_index = 0
        if last_synced_hash:
            last_synced_hash_bytes = bytes.fromhex(last_synced_hash)
            for i, block in enumerate(self.chain):
                if block.hash == last_synced_hash_bytes:
                    start_index = i + 1
                    break
            print(f"Synchronize from block {start_index}")
        else:
            print("Synchronize from the beginning")

        # Apply unapplied binlog
        for i, block in enumerate(self.chain[start_index:], start_index):
            if block.binlog_data:
                print(f"\nApplying block {i} binlog:")
                db.apply_binlog(block.binlog_data)
                print(f"Block {i} synchronization completed")

        # Update the last synchronized block hash
        if self.chain:
            self.last_synced_hash = self.chain[-1].hash
            print(f"\nSynchronization completed, latest block hash: {self.last_synced_hash.hex()}")



def test_multiple_nodes():
    """Test multiple node blockchain synchronization"""

    print("=== Creating Blockchain ===")
    blockchain = Blockchain()

    # Create node A
    node_a = Node('A', '127.0.0.1', 5000)
    node_a.start_server()

    # Create node B
    node_b = Node('B', '127.0.0.1', 5001)
    node_b.start_server()

    # Create node C
    node_c = Node('C', '127.0.0.1', 5002)
    node_c.start_server()

    print("\n=== Initializing Database for Each Node ===")
    for node in [node_a, node_b, node_c]:
        db = SimpleDB()
        db.create_table('users')  # Create `users` table
        db.print_database_state()
        node.db = db

    # Node A connects to B and C
    node_a.connect_to_peer('B', '127.0.0.1', 5001)
    node_a.connect_to_peer('C', '127.0.0.1', 5002)

    # Simulate block generation
    print("\n=== Simulating Block Generation ===")

    # First block operation
    db1 = SimpleDB()
    db1.create_table('users')
    db1.insert('users', {'id': 1, 'name': 'Alice', 'age': 25})
    db1.insert('users', {'id': 2, 'name': 'Bob', 'age': 30})
    binlog1 = db1.create_binlog()
    blockchain.add_binlog(binlog1)
    block1 = blockchain.create_block()

    # Broadcast the binlog of the first block
    stop_event = threading.Event()

    def send_binlog_and_stop():
        node_a.send_binlog_to_peer('B', block1.binlog_data)
        node_a.send_binlog_to_peer('C', block1.binlog_data)
        time.sleep(1)  # Ensure reception is complete
        stop_event.set()

    t1 = threading.Thread(target=send_binlog_and_stop)
    t1.start()

    # Wait for test to complete
    stop_event.wait()
    print("\n=== Test Complete ===")

if __name__ == "__main__":
    test_multiple_nodes()
