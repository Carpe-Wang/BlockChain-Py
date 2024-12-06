# BlockchainDB

A simple blockchain-based distributed database system that implements data synchronization between multiple nodes using binlog and blockchain technology.

## Features

- **Distributed Database**: Nodes maintain independent databases and synchronize through blockchain and binlog updates.
- **Blockchain Integration**: Uses a blockchain to ensure data integrity and secure transaction logging.
- **Binlog Support**: Records database operations for playback and synchronization.
- **Multi-Node Communication**: Nodes can connect, communicate, and exchange updates in a peer-to-peer manner.
- **CRUD Operations**: SimpleDB provides support for creating, reading, updating, and deleting records in tables.
- **Data Synchronization**: Ensures consistent state across distributed nodes using binlog and blockchain mechanisms.

---

## Requirements

- Python >= 3.7 (required for `dataclasses` and other features)
- No external dependencies; built entirely with Python's standard library

---

## Project Structure

- `blockchaindb.py`: Main blockchain implementation with node communication and data synchronization.
- `simpledb.py`: Lightweight database with binlog support.
- `requirements.txt`: Python version requirement.
- `README.md`: Documentation for the project.

---

## Key Components

### 1. **SimpleDB**
A simple database with the following features:
- **Table Management**: Create tables with a simple schema-less structure.
- **CRUD Operations**: Insert, update, delete, and query records.
- **Binlog Support**:
  - Logs all operations into a binary format for replay and synchronization.
  - Supports serialization and deserialization of operations into a compact binary format.
- **Database State Management**: Print and maintain the current state of the database.

---

### 2. **BlockchainDB**
A blockchain-based system with the following capabilities:
- **Blockchain Implementation**: Ensures data integrity using block hashes and links.
- **Block Management**:
  - Automatically creates a genesis block.
  - Adds new blocks containing binlog data.
  - Validates the entire chain for integrity.
- **Node Communication**:
  - Peer-to-peer networking for block and binlog updates.
  - Multi-threaded server to handle incoming requests.
- **Synchronization**:
  - Synchronizes database state across nodes using binlog updates.
  - Maintains a consistent and distributed blockchain.

---

## Usage Example

### Testing SimpleDB
1. **Run Basic Database Operations**:
    ```python
    original_db = SimpleDB()
    original_db.create_table('users')
    original_db.insert('users', {'id': 1, 'name': 'Alice', 'age': 25})
    original_db.insert('users', {'id': 2, 'name': 'Bob', 'age': 30})
    original_db.update('users', {'id': 1}, {'age': 26})
    ```

2. **Generate Binlog**:
    ```python
    binlog_data = original_db.create_binlog()
    original_db.print_binlog(binlog_data)
    ```

3. **Apply Binlog to a New Database**:
    ```python
    new_db = SimpleDB()
    new_db.create_table('users')
    new_db.apply_binlog(binlog_data)
    ```

4. **Compare Database States**:
    ```python
    original_db.print_database_state()
    new_db.print_database_state()
    ```

---

### Testing Blockchain and Node Communication
1. **Create Blockchain**:
    ```python
    blockchain = Blockchain()
    ```

2. **Add Binlog and Create Blocks**:
    ```python
    db = SimpleDB()
    db.create_table('users')
    db.insert('users', {'id': 1, 'name': 'Alice', 'age': 25})
    binlog = db.create_binlog()
    blockchain.add_binlog(binlog)
    blockchain.create_block()
    ```

3. **Start Nodes and Establish Communication**:
    ```python
    node_a = Node('A', '127.0.0.1', 5000)
    node_a.start_server()
    node_a.connect_to_peer('B', '127.0.0.1', 5001)
    ```

4. **Broadcast Binlog Updates**:
    ```python
    node_a.send_binlog_to_peer('B', binlog)
    ```

---

## How It Works

1. **Database Operations**:
   - Perform operations (INSERT, UPDATE, DELETE) on a local database.
   - Generate a binlog for operations performed within a specific timeframe.

2. **Blockchain Integration**:
   - Add binlog data to the blockchain as a new block.
   - Ensure integrity by linking each block to its predecessor using hashes.

3. **Node Communication**:
   - Nodes exchange binlog updates to synchronize databases.
   - Peers validate and apply binlog data to maintain consistency.

4. **Validation**:
   - Validate the blockchain to ensure integrity.
   - Replay binlog data on nodes for synchronization.

---

## Running Tests

1. **Run SimpleDB Tests**:
    ```bash
    python simpledb.py
    ```

2. **Run BlockchainDB Tests**:
    ```bash
    python blockchaindb.py
    ```

---

