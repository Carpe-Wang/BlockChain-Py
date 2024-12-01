# BlockchainDB

A simple blockchain-based distributed database system that implements data synchronization between multiple nodes using binlog and blockchain technology.

## Features

- Distributed database with blockchain-based synchronization
- Multi-node support with socket-based communication
- Binary log (binlog) for operation recording and replay
- Simple key-value storage with basic CRUD operations
- Block creation and validation
- Peer-to-peer networking capabilities

## Requirements

- Python >= 3.7 (required for dataclasses and other features)
- All dependencies are from Python standard library

## Project Structure

- `blockchaindb.py`: Main blockchain implementation with node communication
- `simpledb.py`: Basic database implementation with binlog support
- `requirements.txt`: Project dependencies (Python version requirement)

## Key Components

### SimpleDB
- Basic database operations (CREATE, INSERT, UPDATE, DELETE)
- Binlog generation and application
- Database state management

### BlockchainDB
- Block creation and chain management
- Node communication and synchronization
- Binlog distribution across nodes

## Usage Example 