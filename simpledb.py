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
    """表示一段时间内的所有数据库操作"""
    start_time: float
    end_time: float
    operations: List[Dict[str, Any]]
    
    def to_bytes(self) -> bytes:
        """将整个时间段的操作转换为二进制格式"""
        buffer = BytesIO()
        
        # Magic Number (4 bytes)
        buffer.write(b'BLOG')
        
        # 写入开始和结束时间 (16 bytes)
        buffer.write(struct.pack('!dd', self.start_time, self.end_time))
        
        # 写入操作数量 (4 bytes)
        buffer.write(struct.pack('!I', len(self.operations)))
        
        # 写入每个操作
        for op in self.operations:
            # 操作类型 (1 byte)
            buffer.write(op['type'][0].encode('ascii'))
            
            # 时间戳 (8 bytes)
            buffer.write(struct.pack('!d', op['timestamp']))
            
            # 表名 (使用ASCII编码)
            table_bytes = op['table'].encode('ascii')
            buffer.write(struct.pack('!H', len(table_bytes)))
            buffer.write(table_bytes)
            
            # 数据 (使用ASCII编码的JSON)
            data_bytes = json.dumps(op['data'], ensure_ascii=True).encode('ascii')
            buffer.write(struct.pack('!I', len(data_bytes)))
            buffer.write(data_bytes)
            
            # 如果是UPDATE操作，写入旧数据
            if op['type'] == 'UPDATE':
                old_data_bytes = json.dumps(op['old_data'], ensure_ascii=True).encode('ascii')
                buffer.write(struct.pack('!I', len(old_data_bytes)))
                buffer.write(old_data_bytes)
        
        return buffer.getvalue()
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'BinLog':
        """从二进制数据恢复BinLog对象"""
        buffer = BytesIO(data)
        
        # 验证Magic Number
        magic = buffer.read(4)
        if magic != b'BLOG':
            raise ValueError("Invalid binlog format")
        
        # 读取时间戳
        start_time, end_time = struct.unpack('!dd', buffer.read(16))
        
        # 读取操作数量
        op_count = struct.unpack('!I', buffer.read(4))[0]
        
        operations = []
        for _ in range(op_count):
            # 读取操作类型
            op_type_byte = buffer.read(1).decode('ascii')
            op_type = {'I': 'INSERT', 'U': 'UPDATE', 'D': 'DELETE'}[op_type_byte]
            
            # 读取时间戳
            timestamp = struct.unpack('!d', buffer.read(8))[0]
            
            # 读取表名
            table_len = struct.unpack('!H', buffer.read(2))[0]
            table = buffer.read(table_len).decode('ascii')
            
            # 读取数据
            data_len = struct.unpack('!I', buffer.read(4))[0]
            data = json.loads(buffer.read(data_len).decode('ascii'))
            
            op = {
                'type': op_type,
                'timestamp': timestamp,
                'table': table,
                'data': data
            }
            
            # 如果是UPDATE操作，读取旧数据
            if op_type == 'UPDATE':
                old_data_len = struct.unpack('!I', buffer.read(4))[0]
                old_data = json.loads(buffer.read(old_data_len).decode('ascii'))
                op['old_data'] = old_data
            
            operations.append(op)
        
        return cls(start_time, end_time, operations)

    def print_hex_dump(self, data: bytes) -> str:
        """格式化打印十六进制数据"""
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
        """创建表"""
        if table_name not in self.tables:
            self.tables[table_name] = []
            print(f"表 {table_name} 创建成功")
        else:
            print(f"表 {table_name} 已存在")

    def insert(self, table: str, data: Dict[str, Any]):
        """插入数据"""
        if table not in self.tables:
            raise ValueError(f"表 {table} 不存在")
        
        self.tables[table].append(data)
        self.current_operations.append({
            'type': 'INSERT',
            'table': table,
            'data': data,
            'timestamp': time.time()
        })
        print(f"向表 {table} 插入数据: {data}")

    def update(self, table: str, condition: Dict[str, Any], new_data: Dict[str, Any]):
        """更新数据"""
        if table not in self.tables:
            raise ValueError(f"表 {table} 不存在")
        
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
                print(f"更新表 {table} 中的数据: {old_data} -> {record}")

    def delete(self, table: str, condition: Dict[str, Any]):
        """删除数据"""
        if table not in self.tables:
            raise ValueError(f"表 {table} 不存在")
        
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
        print(f"从表 {table} 删除满足条件的数据: {condition}")

    def create_binlog(self) -> bytes:
        """创建当前时间段的binlog"""
        if not self.current_operations:
            return None
            
        end_time = time.time()
        binlog = BinLog(
            start_time=self.binlog_start_time,
            end_time=end_time,
            operations=self.current_operations
        )
        
        # 重置当前操作列表和开始时间
        self.current_operations = []
        self.binlog_start_time = end_time
        
        return binlog.to_bytes()

    def print_binlog(self, binlog_data: bytes):
        """打印binlog的二进制内容"""
        print("\n=== Binlog Binary Content ===")
        print(f"Total size: {len(binlog_data)} bytes")
        
        # 打印格式化的十六进制数据
        print("\nHex dump:")
        binlog = BinLog.from_bytes(binlog_data)
        print(binlog.print_hex_dump(binlog_data))
        
        # 打印解析后的信息
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
        """应用binlog数据到数据库"""
        binlog = BinLog.from_bytes(binlog_data)
        print("\n=== 开始应用 Binlog ===")
        print(f"时间范围: {datetime.fromtimestamp(binlog.start_time)} -> {datetime.fromtimestamp(binlog.end_time)}")
        print(f"操作数量: {len(binlog.operations)}")
        
        for i, op in enumerate(binlog.operations, 1):
            print(f"\n执行操作 {i}/{len(binlog.operations)}:")
            print(f"类型: {op['type']}")
            print(f"时间: {datetime.fromtimestamp(op['timestamp'])}")
            
            if op['type'] == 'INSERT':
                self.insert(op['table'], op['data'])
            elif op['type'] == 'UPDATE':
                # 从old_data中提取条件
                condition = {k: v for k, v in op['old_data'].items() if k in op['data']}
                new_data = {k: v for k, v in op['data'].items() if k not in condition}
                self.update(op['table'], condition, new_data)
            elif op['type'] == 'DELETE':
                self.delete(op['table'], op['data'])
            
            print("-" * 50)

    def print_database_state(self):
        """打印当前数据库状态"""
        print("\n=== 数据库当前状态 ===")
        for table_name, records in self.tables.items():
            print(f"\n表 {table_name}:")
            if not records:
                print("  (空)")
            for record in records:
                print(f"  {record}")

def test_simple_db():
    """测试SimpleDB的功能，包括binlog的序列化和反序列化"""
    # 创建原始数据库并执行操作
    print("=== 创建原始数据库并执行操作 ===")
    original_db = SimpleDB()
    original_db.create_table('users')
    original_db.insert('users', {'id': 1, 'name': 'Alice', 'age': 25})
    original_db.insert('users', {'id': 2, 'name': 'Bob', 'age': 30})
    original_db.update('users', {'id': 1}, {'age': 26})
    #original_db.delete('users', {'id': 2})
    
    # 创建binlog
    print("\n=== 创建Binlog ===")
    binlog_data = original_db.create_binlog()
    if binlog_data:
        original_db.print_binlog(binlog_data)
    
    # 创建新的数据库实例并应用binlog
    print("\n=== 创建新数据库并应用Binlog ===")
    new_db = SimpleDB()
    new_db.create_table('users')  # 需要先创建表
    new_db.apply_binlog(binlog_data)
    
    # 打印两个数据库的状态进行比较
    print("\n=== 比较数据库状态 ===")
    print("\n原始数据库状态:")
    original_db.print_database_state()
    print("\n新数据库状态:")
    new_db.print_database_state()

if __name__ == '__main__':
    test_simple_db() 