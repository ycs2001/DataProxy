#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库执行模块
用于实际执行SQL查询并保存结果
"""

import os
import csv
import logging
import pandas as pd
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import sqlite3

class DatabaseExecutor:
    """数据库执行器"""

    def __init__(self, db_type: str = "sqlite", connection_params: Dict[str, Any] = None, database_path: str = None):
        """
        初始化数据库执行器

        Args:
            db_type: 数据库类型 (sqlite, mysql, postgresql等)
            connection_params: 数据库连接参数
            database_path: 数据库文件路径（用于SQLite）
        """
        self.db_type = db_type.lower()
        self.connection_params = connection_params or {}
        self.database_path = database_path
        self.logger = logging.getLogger(__name__)
        self.connection = None

        # 连接到指定数据库或创建示例数据库
        if database_path and os.path.exists(database_path):
            self._connect_to_database(database_path)
        elif not connection_params:
            self._create_sample_database()

    def _connect_to_database(self, database_path: str):
        """连接到指定的数据库文件"""
        try:
            self.connection = sqlite3.connect(database_path)
            self.logger.info(f"已连接到数据库: {database_path}")
        except Exception as e:
            self.logger.error(f"连接数据库失败: {e}")
            raise

    def _create_sample_database(self):
        """创建示例数据库和数据"""
        db_path = "sample_bank_data.db"
        
        try:
            self.connection = sqlite3.connect(db_path)
            self.database_path = db_path
            cursor = self.connection.cursor()
            
            # 创建示例表
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS customers (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    balance REAL,
                    account_type TEXT,
                    created_date TEXT
                )
            ''')
            
            # 插入示例数据
            sample_data = [
                (1, '张三', 150000.0, '对公账户', '2023-01-15'),
                (2, '李四', 80000.0, '个人账户', '2023-02-20'),
                (3, '王五', 200000.0, '对公账户', '2023-03-10'),
                (4, '赵六', 50000.0, '个人账户', '2023-04-05'),
                (5, '钱七', 300000.0, '对公账户', '2023-05-12')
            ]
            
            cursor.executemany('''
                INSERT OR REPLACE INTO customers (id, name, balance, account_type, created_date)
                VALUES (?, ?, ?, ?, ?)
            ''', sample_data)
            
            self.connection.commit()
            self.logger.info(f"创建示例数据库: {db_path}")
            
        except Exception as e:
            self.logger.error(f"创建示例数据库失败: {e}")
            raise

    def execute_query(self, sql: str) -> Tuple[bool, List[Dict[str, Any]], str]:
        """
        执行SQL查询

        Args:
            sql: SQL查询语句

        Returns:
            (是否成功, 查询结果, 错误信息)
        """
        try:
            if not self.connection:
                return False, [], "数据库连接未建立"

            # 使用pandas执行查询，方便处理结果
            df = pd.read_sql_query(sql, self.connection)

            # 转换为字典列表
            results = df.to_dict('records')

            self.logger.info(f"查询执行成功，返回{len(results)}条记录")
            return True, results, ""

        except Exception as e:
            error_msg = f"查询执行失败: {str(e)}"
            self.logger.error(error_msg)
            return False, [], error_msg

    def save_to_csv(self, data: List[Dict[str, Any]], filename: str) -> bool:
        """
        保存查询结果到CSV文件

        Args:
            data: 查询结果数据
            filename: 文件名

        Returns:
            是否保存成功
        """
        try:
            if not data:
                self.logger.warning("没有数据需要保存")
                return False

            # 确保输出目录存在
            output_dir = "output"
            os.makedirs(output_dir, exist_ok=True)

            # 构建完整文件路径
            if not filename.endswith('.csv'):
                filename += '.csv'
            
            filepath = os.path.join(output_dir, filename)

            # 保存到CSV
            df = pd.DataFrame(data)
            df.to_csv(filepath, index=False, encoding='utf-8-sig')

            self.logger.info(f"数据已保存到: {filepath}")
            return True

        except Exception as e:
            self.logger.error(f"保存CSV文件失败: {str(e)}")
            return False

    def execute_and_save(self, sql: str, filename: str, description: str = "") -> Dict[str, Any]:
        """
        执行查询并保存结果

        Args:
            sql: SQL查询语句
            filename: 保存的文件名
            description: 查询描述

        Returns:
            执行结果信息
        """
        result_info = {
            "success": False,
            "sql": sql,
            "filename": filename,
            "description": description,
            "record_count": 0,
            "error_message": "",
            "execution_time": datetime.now().isoformat()
        }

        try:
            # 执行查询
            success, data, error_msg = self.execute_query(sql)

            if not success:
                result_info["error_message"] = error_msg
                return result_info

            # 保存到CSV
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_filename = f"{timestamp}_{filename}"

            if self.save_to_csv(data, csv_filename):
                result_info["success"] = True
                result_info["record_count"] = len(data)
                result_info["filename"] = csv_filename
                result_info["data_preview"] = data[:5] if data else []  # 预览前5条
            else:
                result_info["error_message"] = "保存CSV文件失败"

        except Exception as e:
            result_info["error_message"] = f"执行过程异常: {str(e)}"

        return result_info

    def get_table_info(self) -> Dict[str, Any]:
        """
        获取数据库表信息

        Returns:
            表信息字典
        """
        try:
            if not self.connection:
                return {"error": "数据库连接未建立"}

            cursor = self.connection.cursor()
            
            # 获取所有表名
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            
            table_info = {}
            for table in tables:
                table_name = table[0]
                
                # 获取表结构
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                
                # 获取记录数
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                row_count = cursor.fetchone()[0]
                
                table_info[table_name] = {
                    "columns": [{"name": col[1], "type": col[2]} for col in columns],
                    "row_count": row_count
                }
            
            return table_info

        except Exception as e:
            self.logger.error(f"获取表信息失败: {str(e)}")
            return {"error": str(e)}

    def close(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            self.logger.info("数据库连接已关闭")
