from dataclasses import dataclass
from typing import List, Optional, Dict, Any
import time
import hashlib
import struct
from threading import Lock
import queue
from datetime import datetime
from io import BytesIO
from simpledb import SimpleDB


@dataclass
class Block:
    """区块结构"""
    previous_hash: bytes      # 32 bytes
    timestamp: float         # 8 bytes
    binlog_data: bytes      # binlog数据
    hash: Optional[bytes] = None
    
    def to_bytes(self) -> bytes:
        """将区块转换为二进制格式"""
        buffer = BytesIO()
        
        # Magic Number (4 bytes)
        buffer.write(b'BLCK')
        
        # 写入previous_hash (32 bytes)
        buffer.write(self.previous_hash)
        
        # 写入timestamp (8 bytes)
        buffer.write(struct.pack('!d', self.timestamp))
        
        # 写入binlog数据长度和数据
        buffer.write(struct.pack('!I', len(self.binlog_data)))
        buffer.write(self.binlog_data)
        
        return buffer.getvalue()
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'Block':
        """从二进制数据恢复区块"""
        buffer = BytesIO(data)
        
        # 验证Magic Number
        magic = buffer.read(4)
        if magic != b'BLCK':
            raise ValueError("Invalid block format")
        
        # 读取previous_hash
        previous_hash = buffer.read(32)
        
        # 读取timestamp
        timestamp = struct.unpack('!d', buffer.read(8))[0]
        
        # 读取binlog数据
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
        """计算区块哈希"""
        block_bytes = self.to_bytes()
        return hashlib.sha256(block_bytes).digest()
    
    def seal(self):
        """计算并设置区块哈希"""
        self.hash = self.calculate_hash()
    
    def print_hex_dump(self, data: bytes) -> str:
        """格式化打印十六进制数据"""
        hex_str = ''.join(f'{b:02x}' for b in data)
        return hex_str
    
    def print_block_info(self):
        """打印区块信息"""
        print("\n=== 区块信息 ===")
        print(f"区块哈希: {self.hash.hex()}")
        print(f"前一个区块哈希: {self.previous_hash.hex()}")
        print(f"时间戳: {datetime.fromtimestamp(self.timestamp)}")
        print(f"区块大小: {len(self.to_bytes())} bytes")
        print(f"Binlog大小: {len(self.binlog_data)} bytes")
        
        # 打印完整的区块二进制数据
        print("\n区块二进制数据:")
        print(self.print_hex_dump(self.to_bytes()))
        
        # 打印binlog二进制数据
        if self.binlog_data:
            print("\nBinlog二进制数据:")
            print(self.print_hex_dump(self.binlog_data))

class Blockchain:
    """区块链类"""
    def __init__(self):
        self.chain: List[Block] = []
        self.lock = Lock()
        self.pending_binlogs = queue.Queue()
        self.last_synced_hash: Optional[bytes] = None
        
        # 创建创世区块
        self._create_genesis_block()
    
    def _create_genesis_block(self):
        """创建创世区块"""
        genesis_block = Block(
            previous_hash=b'\x00' * 32,
            timestamp=time.time(),
            binlog_data=b''
        )
        genesis_block.seal()
        self.chain.append(genesis_block)
        print("创世区块已创建")
    
    def add_binlog(self, binlog_data: bytes):
        """添加新的binlog数据到待处理队列"""
        self.pending_binlogs.put(binlog_data)
        print(f"新的binlog数据已添加到待处理队列 (大小: {len(binlog_data)} bytes)")
    
    def create_block(self) -> Optional[Block]:
        """从待处理的binlog数据创建新区块"""
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
                print("\n新区块已创建并添加到链中")
                return new_block
                
        except queue.Empty:
            return None
    
    def validate_chain(self) -> bool:
        """验证整个区块链"""
        for i in range(1, len(self.chain)):
            current_block = self.chain[i]
            previous_block = self.chain[i-1]
            
            # 验证当前区块的哈希
            if current_block.hash != current_block.calculate_hash():
                print(f"区块 {i} 的哈希值无效")
                return False
            
            # 验证区块链接
            if current_block.previous_hash != previous_block.hash:
                print(f"区块 {i} 与前一个区块的链接无效")
                return False
        
        return True
    
    def print_chain_info(self):
        """打印区块链信息"""
        print("\n=== 区块链信息 ===")
        print(f"区块数量: {len(self.chain)}")
        print(f"是否有效: {self.validate_chain()}")
        
        for i, block in enumerate(self.chain):
            print(f"\n区块 {i}:")
            block.print_block_info()
    
    def sync_from_chain(self, db: SimpleDB, last_synced_hash: Optional[str] = None):
        """从区块链同步数据库，只应用未同步的binlog"""
        print("\n=== 开始同步数据库 ===")
        
        # 找到开始同步的位置
        start_index = 0
        if last_synced_hash:
            last_synced_hash_bytes = bytes.fromhex(last_synced_hash)
            for i, block in enumerate(self.chain):
                if block.hash == last_synced_hash_bytes:
                    start_index = i + 1
                    break
            print(f"从区块 {start_index} 开始同步")
        else:
            print("从头开始同步")
        
        # 应用未同步的binlog
        for i, block in enumerate(self.chain[start_index:], start_index):
            if block.binlog_data:
                print(f"\n应用区块 {i} 的binlog:")
                db.apply_binlog(block.binlog_data)
                print(f"区块 {i} 同步完成")
        
        # 更新最后同步的区块哈希
        if self.chain:
            self.last_synced_hash = self.chain[-1].hash
            print(f"\n同步完成，最新区块哈希: {self.last_synced_hash.hex()}")

def test_blockchain_sync():
    """测试区块链的同步功能"""
    from simpledb import SimpleDB
    
    # 创建区块链
    print("=== 创建区块链 ===")
    blockchain = Blockchain()
    
    # 创建多个区块
    print("\n=== 创建多个区块 ===")
    
    # 第一个区块的操作
    db1 = SimpleDB()
    db1.create_table('users')
    db1.insert('users', {'id': 1, 'name': 'Alice', 'age': 25})
    binlog1 = db1.create_binlog()
    blockchain.add_binlog(binlog1)
    block1 = blockchain.create_block()
    
    # 第二个区块的操作
    db2 = SimpleDB()
    db2.create_table('users')
    db2.insert('users', {'id': 2, 'name': 'Bob', 'age': 30})
    binlog2 = db2.create_binlog()
    blockchain.add_binlog(binlog2)
    block2 = blockchain.create_block()
    
    # 创建一个新的数据库，只包含部分数据
    print("\n=== 创建待同步的数据库 ===")
    target_db = SimpleDB()
    target_db.create_table('users')
    target_db.insert('users', {'id': 1, 'name': 'Alice', 'age': 25})
    print("\n当前数据库状态:")
    target_db.print_database_state()
    
    # 同步数据库
    print("\n=== 开始同步 ===")
    # 从第一个区块之后开始同步
    blockchain.sync_from_chain(target_db, block1.hash.hex())
    
    print("\n同步后的数据库状态:")
    target_db.print_database_state()

if __name__ == "__main__":
    test_blockchain_sync()
