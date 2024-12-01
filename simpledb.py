from dataclasses import dataclass
from typing import Dict, List, Any, Optional
from datetime import datetime
import json
import time
import struct
import binascii
from collections import defaultdict
from io import BytesIO

@dataclass
class BinLog:
    """Represents all database operations within a time period"""
    start_time: float
    end_time: float
    operations: List[Dict[str, Any]]
    
    def to_bytes(self) -> bytes:
        """Convert the entire time period's operations to binary format"""
        buffer = BytesIO()
        
        # Magic Number (4 bytes)
        buffer.write(b'BLOG')
        
        # Write start and end time (16 bytes)
        buffer.write(struct.pack('!dd', self.start_time, self.end_time))
        
        # Write operation count (4 bytes)
        buffer.write(struct.pack('!I', len(self.operations)))
        
        # Write each operation
        for op in self.operations:
            # Operation type (1 byte)
            buffer.write(op['type'][0].encode('ascii'))
            
            # Timestamp (8 bytes)
            buffer.write(struct.pack('!d', op['timestamp']))
            
            # Table name (ASCII encoded)
            table_bytes = op['table'].encode('ascii')
            buffer.write(struct.pack('!H', len(table_bytes)))
            buffer.write(table_bytes)
            
            # Data (JSON encoded in ASCII)
            data_bytes = json.dumps(op['data'], ensure_ascii=True).encode('ascii')
            buffer.write(struct.pack('!I', len(data_bytes)))
            buffer.write(data_bytes)
            
            # If it's an UPDATE operation, write old data
            if op['type'] == 'UPDATE':
                old_data_bytes = json.dumps(op['old_data'], ensure_ascii=True).encode('ascii')
                buffer.write(struct.pack('!I', len(old_data_bytes)))
                buffer.write(old_data_bytes)
        
        return buffer.getvalue()
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'BinLog':
        """Recover BinLog object from binary data"""
        buffer = BytesIO(data)
        
        # Verify Magic Number
        magic = buffer.read(4)
        if magic != b'BLOG':
            raise ValueError("Invalid binlog format")
        
        # Read timestamp
        start_time, end_time = struct.unpack('!dd', buffer.read(16))
        
        # Read operation count
        op_count = struct.unpack('!I', buffer.read(4))[0]
        
        operations = []
        for _ in range(op_count):
            # Read operation type
            op_type_byte = buffer.read(1).decode('ascii')
            op_type = {'I': 'INSERT', 'U': 'UPDATE', 'D': 'DELETE'}[op_type_byte]
            
            # Read timestamp
            timestamp = struct.unpack('!d', buffer.read(8))[0]
            
            # Read table name
            table_len = struct.unpack('!H', buffer.read(2))[0]
            table = buffer.read(table_len).decode('ascii')
            
            # Read data
            data_len = struct.unpack('!I', buffer.read(4))[0]
            data = json.loads(buffer.read(data_len).decode('ascii'))
            
            op = {
                'type': op_type,
                'timestamp': timestamp,
                'table': table,
                'data': data
            }
            
            # If it's an UPDATE operation, read old data
            if op_type == 'UPDATE':
                old_data_len = struct.unpack('!I', buffer.read(4))[0]
                old_data = json.loads(buffer.read(old_data_len).decode('ascii'))
                op['old_data'] = old_data
            
            operations.append(op)
        
        return cls(start_time, end_time, operations)

    def print_hex_dump(self, data: bytes) -> str:
        """Format hex data for printing"""
        def format_hex_line(offset: int, data: bytes) -> str:
            hex_part = ' '.join(f'{b:02x}' for b in data)
            ascii_part = ''.join(chr(b) if 32 <= b <= 126 else '.' for b in data)
            return f'{offset:08x}  {hex_part:<48}  |{ascii_part}|'

        result = []
        for i in range(0, len(data), 16):
            chunk = data[i:i+16]
            result.append(format_hex_line(i, chunk))
        return '\n'.join(result)

class SimpleDB:
    def __init__(self):
        self.tables: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.current_operations: List[Dict[str, Any]] = []
        self.binlog_start_time: float = time.time()
    
    def create_table(self, table_name: str):
        """Create table"""
        if table_name not in self.tables:
            self.tables[table_name] = []
            print(f"Table {table_name} created successfully")
        else:
            print(f"Table {table_name} already exists")

    def insert(self, table: str, data: Dict[str, Any]):
        """Insert data"""
        if table not in self.tables:
            raise ValueError(f"Table {table} does not exist")
        
        self.tables[table].append(data)
        self.current_operations.append({
            'type': 'INSERT',
            'table': table,
            'data': data,
            'timestamp': time.time()
        })
        print(f"Inserted data into table {table}: {data}")

    def update(self, table: str, condition: Dict[str, Any], new_data: Dict[str, Any]):
        """Update data"""
        if table not in self.tables:
            raise ValueError(f"Table {table} does not exist")
        
        for record in self.tables[table]:
            if all(record.get(k) == v for k, v in condition.items()):
                old_data = record.copy()
                record.update(new_data)
                self.current_operations.append({
                    'type': 'UPDATE',
                    'table': table,
                    'data': record.copy(),
                    'old_data': old_data,
                    'timestamp': time.time()
                })
                print(f"Updated data in table {table}: {old_data} -> {record}")

    def delete(self, table: str, condition: Dict[str, Any]):
        """Delete data"""
        if table not in self.tables:
            raise ValueError(f"Table {table} does not exist")
        
        self.tables[table] = [
            record for record in self.tables[table]
            if not all(record.get(k) == v for k, v in condition.items())
        ]
        self.current_operations.append({
            'type': 'DELETE',
            'table': table,
            'data': condition,
            'timestamp': time.time()
        })
        print(f"Deleted data from table {table} matching condition: {condition}")

    def create_binlog(self) -> bytes:
        """Create binlog for current time period"""
        if not self.current_operations:
            return None
            
        end_time = time.time()
        binlog = BinLog(
            start_time=self.binlog_start_time,
            end_time=end_time,
            operations=self.current_operations
        )
        
        # Reset current operations list and start time
        self.current_operations = []
        self.binlog_start_time = end_time
        
        return binlog.to_bytes()

    def print_binlog(self, binlog_data: bytes):
        """Print binlog binary content"""
        print("\n=== Binlog Binary Content ===")
        print(f"Total size: {len(binlog_data)} bytes")
        
        # Print formatted hex data
        print("\nHex dump:")
        binlog = BinLog.from_bytes(binlog_data)
        print(binlog.print_hex_dump(binlog_data))
        
        # Print parsed information
        print(f"\nStart time: {datetime.fromtimestamp(binlog.start_time)}")
        print(f"End time: {datetime.fromtimestamp(binlog.end_time)}")
        print(f"Operation count: {len(binlog.operations)}")
        
        for i, op in enumerate(binlog.operations, 1):
            print(f"\nOperation {i}:")
            print(f"  Type: {op['type']}")
            print(f"  Time: {datetime.fromtimestamp(op['timestamp'])}")
            print(f"  Table: {op['table']}")
            print(f"  Data: {op['data']}")
            if 'old_data' in op:
                print(f"  Old Data: {op['old_data']}")

    def apply_binlog(self, binlog_data: bytes):
        """Apply binlog data to database"""
        binlog = BinLog.from_bytes(binlog_data)
        print("\n=== Starting to Apply Binlog ===")
        print(f"Time range: {datetime.fromtimestamp(binlog.start_time)} -> {datetime.fromtimestamp(binlog.end_time)}")
        print(f"Operation count: {len(binlog.operations)}")
        
        for i, op in enumerate(binlog.operations, 1):
            print(f"\nExecuting operation {i}/{len(binlog.operations)}:")
            print(f"Type: {op['type']}")
            print(f"Time: {datetime.fromtimestamp(op['timestamp'])}")
            
            if op['type'] == 'INSERT':
                self.insert(op['table'], op['data'])
            elif op['type'] == 'UPDATE':
                # Extract condition from old_data
                condition = {k: v for k, v in op['old_data'].items() if k in op['data']}
                new_data = {k: v for k, v in op['data'].items() if k not in condition}
                self.update(op['table'], condition, new_data)
            elif op['type'] == 'DELETE':
                self.delete(op['table'], op['data'])
            
            print("-" * 50)

    def print_database_state(self):
        """Print current database state"""
        print("\n=== Current Database State ===")
        for table_name, records in self.tables.items():
            print(f"\nTable {table_name}:")
            if not records:
                print("  (Empty)")
            for record in records:
                print(f"  {record}")

    def get_tables(self) -> List[str]:
        """Return all table names in current database"""
        return list(self.tables.keys())

def test_simple_db():
    """Test SimpleDB functionality, including binlog serialization and deserialization"""
    # Create original database and perform operations
    print("=== Creating Original Database and Performing Operations ===")
    original_db = SimpleDB()
    original_db.create_table('users')
    original_db.insert('users', {'id': 1, 'name': 'Alice', 'age': 25})
    original_db.insert('users', {'id': 2, 'name': 'Bob', 'age': 30})
    original_db.update('users', {'id': 1}, {'age': 26})
    
    # Create binlog
    print("\n=== Creating Binlog ===")
    binlog_data = original_db.create_binlog()
    if binlog_data:
        original_db.print_binlog(binlog_data)
    
    # Create new database instance and apply binlog
    print("\n=== Creating New Database and Applying Binlog ===")
    new_db = SimpleDB()
    new_db.create_table('users')  # Need to create table first
    new_db.apply_binlog(binlog_data)
    
    # Print both database states for comparison
    print("\n=== Comparing Database States ===")
    print("\nOriginal Database State:")
    original_db.print_database_state()
    print("\nNew Database State:")
    new_db.print_database_state()

if __name__ == '__main__':
    test_simple_db() 