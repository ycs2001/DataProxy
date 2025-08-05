#!/usr/bin/env python3
"""
数据库配置管理器 - 重构后的简化配置管理

专注于数据库连接、路径管理和基础配置
"""

import os
import json
import hashlib
import sqlite3
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from pathlib import Path


@dataclass
class DatabaseInfo:
    """数据库信息"""
    path: str
    name: str
    type: str
    description: str
    tables: List[str]
    config_file: Optional[str] = None


class DatabaseConfigManager:
    """
    数据库配置管理器 - 简化版本
    
    职责：
    1. 管理数据库连接和路径
    2. 加载和保存数据库配置
    3. 提供数据库基础信息
    """
    
    def __init__(self):
        self.current_database: Optional[str] = None
        self.database_info: Optional[DatabaseInfo] = None
        self.config_dir = Path("configs/database_contexts")
        self.config_dir.mkdir(parents=True, exist_ok=True)
        
        # 发现可用数据库
        self.available_databases = self._discover_databases()
        
        # 自动选择数据库（如果只有一个）
        if len(self.available_databases) == 1:
            self.switch_database(self.available_databases[0])
    
    def _discover_databases(self) -> List[str]:
        """发现可用的数据库文件"""
        databases = []
        
        # 在当前目录和flask_backend目录中查找.db文件
        search_paths = [".", "flask_backend"]
        
        for search_path in search_paths:
            if os.path.exists(search_path):
                for file in os.listdir(search_path):
                    if file.endswith('.db'):
                        db_path = os.path.join(search_path, file)
                        if os.path.isfile(db_path):
                            databases.append(db_path)
        
        return databases
    
    def switch_database(self, database_path: str) -> bool:
        """
        切换数据库
        
        Args:
            database_path: 数据库文件路径
            
        Returns:
            bool: 切换是否成功
        """
        try:
            if not os.path.exists(database_path):
                print(f"[ERROR] 数据库文件不存在: {database_path}")
                return False
            
            # 获取数据库基础信息
            db_info = self._get_database_info(database_path)
            
            if db_info:
                self.current_database = database_path
                self.database_info = db_info
                print(f"[INFO] 数据库切换成功: {database_path}")
                return True
            else:
                print(f"[ERROR] 无法获取数据库信息: {database_path}")
                return False
                
        except Exception as e:
            print(f"[ERROR] 数据库切换失败: {e}")
            return False
    
    def _get_database_info(self, database_path: str) -> Optional[DatabaseInfo]:
        """获取数据库基础信息"""
        try:
            # 连接数据库获取表信息
            conn = sqlite3.connect(database_path)
            cursor = conn.cursor()
            
            # 获取所有表名
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = [row[0] for row in cursor.fetchall()]
            
            conn.close()
            
            # 生成数据库信息
            db_name = os.path.splitext(os.path.basename(database_path))[0]
            
            return DatabaseInfo(
                path=database_path,
                name=db_name,
                type="sqlite",
                description=f"SQLite数据库: {db_name}",
                tables=tables,
                config_file=self._get_config_file_path(database_path)
            )
            
        except Exception as e:
            print(f"[ERROR] 获取数据库信息失败: {e}")
            return None
    
    def _get_config_file_path(self, database_path: str) -> str:
        """生成配置文件路径"""
        db_name = os.path.splitext(os.path.basename(database_path))[0]
        db_hash = hashlib.md5(database_path.encode()).hexdigest()
        return str(self.config_dir / f"{db_name}_{db_hash}.json")
    
    def get_current_database(self) -> Optional[str]:
        """获取当前数据库路径"""
        return self.current_database
    
    def get_database_info(self) -> Optional[DatabaseInfo]:
        """获取当前数据库信息"""
        return self.database_info
    
    def get_available_databases(self) -> List[str]:
        """获取可用数据库列表"""
        return self.available_databases
    
    def is_valid(self) -> bool:
        """检查配置是否有效"""
        return (
            self.current_database is not None and
            os.path.exists(self.current_database) and
            self.database_info is not None
        )


# 全局实例
_database_config_manager = None


def get_database_config() -> DatabaseConfigManager:
    """获取数据库配置管理器实例"""
    global _database_config_manager
    if _database_config_manager is None:
        _database_config_manager = DatabaseConfigManager()
    return _database_config_manager


def reset_database_config():
    """重置数据库配置管理器"""
    global _database_config_manager
    _database_config_manager = None
