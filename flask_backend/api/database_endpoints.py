#!/usr/bin/env python3
"""
数据库管理API端点 - 支持前端数据分析模块
"""

import os
import sqlite3
import glob
from flask import Blueprint, jsonify, request
from .error_handlers import APIErrorHandler

def create_database_blueprint():
    """创建数据库管理API蓝图"""
    
    db_bp = Blueprint('database', __name__, url_prefix='/api/v1/databases')
    
    # 全局变量存储当前数据库
    current_database = None
    
    @db_bp.route('', methods=['GET'])
    def get_databases():
        """获取可用数据库列表"""
        try:
            # 扫描多个目录
            search_dirs = [
                './databases/imported',
                './databases/temp',
                './databases',
                os.getenv('DATAPROXY_DATA_DIR', './data'),
                './data'  # 添加默认数据目录
            ]

            databases = []
            seen_paths = set()

            for data_dir in search_dirs:
                if not os.path.exists(data_dir):
                    continue

                db_files = glob.glob(os.path.join(data_dir, '**/*.db'), recursive=True)

                for db_file in db_files:
                    if db_file in seen_paths:
                        continue
                    seen_paths.add(db_file)

                    try:
                        conn = sqlite3.connect(db_file)
                        cursor = conn.cursor()
                        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                        tables = cursor.fetchall()
                        conn.close()

                        # 获取文件信息
                        file_stat = os.stat(db_file)

                        databases.append({
                            'name': os.path.splitext(os.path.basename(db_file))[0],
                            'path': db_file,
                            'table_count': len(tables),
                            'description': f'数据库包含 {len(tables)} 个表',
                            'size': file_stat.st_size,
                            'status': 'connected',
                            'last_modified': file_stat.st_mtime
                        })
                    except Exception as e:
                        # 记录损坏的数据库但不跳过
                        databases.append({
                            'name': os.path.splitext(os.path.basename(db_file))[0],
                            'path': db_file,
                            'table_count': 0,
                            'description': f'数据库文件损坏: {str(e)}',
                            'size': 0,
                            'status': 'error',
                            'last_modified': 0
                        })

            # 按修改时间排序
            databases.sort(key=lambda x: x.get('last_modified', 0), reverse=True)

            return jsonify({
                'success': True,
                'databases': databases,
                'current_database': current_database,
                'total_count': len(databases)
            })

        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)
    
    @db_bp.route('/switch', methods=['POST'])
    def switch_database():
        """切换当前使用的数据库 (旧格式支持)"""
        try:
            data = request.get_json()
            if not data or 'database_path' not in data:
                return APIErrorHandler.handle_validation_error(
                    'database_path is required', 'database_path'
                )

            database_path = data['database_path']
            return _switch_database_internal(database_path)

        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)

    @db_bp.route('/<path:database_id>/switch', methods=['POST'])
    def switch_database_by_id(database_id):
        """切换当前使用的数据库 (新格式支持)"""
        try:
            # database_id 可能是路径或者数据库名
            return _switch_database_internal(database_id)

        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)

    def _switch_database_internal(database_path):
        """内部数据库切换逻辑"""
        # 如果传入的不是完整路径，尝试查找数据库
        if not os.path.exists(database_path):
            # 尝试在各个目录中查找
            search_dirs = [
                './databases/imported',
                './databases/temp',
                './databases',
                os.getenv('DATAPROXY_DATA_DIR', './data')
            ]

            found_path = None
            for data_dir in search_dirs:
                if not os.path.exists(data_dir):
                    continue

                # 尝试直接匹配
                potential_path = os.path.join(data_dir, database_path)
                if os.path.exists(potential_path):
                    found_path = potential_path
                    break

                # 尝试添加.db扩展名
                potential_path = os.path.join(data_dir, f"{database_path}.db")
                if os.path.exists(potential_path):
                    found_path = potential_path
                    break

                # 递归搜索
                for root, dirs, files in os.walk(data_dir):
                    for file in files:
                        if file == database_path or file == f"{database_path}.db":
                            found_path = os.path.join(root, file)
                            break
                    if found_path:
                        break

            if found_path:
                database_path = found_path
            else:
                return APIErrorHandler.handle_validation_error(
                    'Database file not found', 'database_path'
                )

        # 验证数据库文件可访问
        try:
            conn = sqlite3.connect(database_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            conn.close()

            global current_database
            current_database = database_path

            # 同时更新DataProxy系统的当前数据库
            try:
                from core_modules.config import get_unified_config
                config = get_unified_config()
                config.current_database_path = database_path
                print(f"[INFO] 数据库切换成功: {database_path}")
            except Exception as e:
                print(f"[WARNING] 无法更新统一配置: {e}")

            return jsonify({
                'success': True,
                'message': 'Database switched successfully',
                'current_database': current_database,
                'database_info': {
                    'path': database_path,
                    'name': os.path.splitext(os.path.basename(database_path))[0],
                    'table_count': len(tables)
                }
            })

        except sqlite3.Error as e:
            return APIErrorHandler.handle_validation_error(
                f'Invalid database file: {str(e)}', 'database_path'
            )
    
    @db_bp.route('/<path:database_path>/tables', methods=['GET'])
    def get_tables(database_path):
        """获取指定数据库的表列表"""
        try:
            if not os.path.exists(database_path):
                return APIErrorHandler.handle_validation_error(
                    'Database file not found', 'database_path'
                )
            
            conn = sqlite3.connect(database_path)
            cursor = conn.cursor()
            
            # 获取表信息
            cursor.execute("""
                SELECT name, sql FROM sqlite_master 
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
            """)
            tables_info = cursor.fetchall()
            
            tables = []
            for table_name, table_sql in tables_info:
                # 获取表的行数
                cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                row_count = cursor.fetchone()[0]
                
                # 获取列信息
                cursor.execute(f"PRAGMA table_info(`{table_name}`)")
                columns_info = cursor.fetchall()
                
                columns = [
                    {
                        'name': col[1],
                        'type': col[2],
                        'not_null': bool(col[3]),
                        'primary_key': bool(col[5])
                    }
                    for col in columns_info
                ]
                
                tables.append({
                    'name': table_name,
                    'row_count': row_count,
                    'column_count': len(columns),
                    'columns': columns,
                    'sql': table_sql
                })
            
            conn.close()
            
            return jsonify({
                'success': True,
                'database_path': database_path,
                'tables': tables,
                'table_count': len(tables)
            })
            
        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)
    
    @db_bp.route('/<path:database_path>/tables/<table_name>/data', methods=['GET'])
    def get_table_data(database_path, table_name):
        """获取表数据（支持分页和搜索）"""
        try:
            if not os.path.exists(database_path):
                return APIErrorHandler.handle_validation_error(
                    'Database file not found', 'database_path'
                )
            
            # 获取查询参数
            limit = request.args.get('limit', 100, type=int)
            offset = request.args.get('offset', 0, type=int)
            search = request.args.get('search', '')
            
            conn = sqlite3.connect(database_path)
            conn.row_factory = sqlite3.Row  # 使结果可以按列名访问
            cursor = conn.cursor()
            
            # 构建查询
            base_query = f"SELECT * FROM `{table_name}`"
            count_query = f"SELECT COUNT(*) FROM `{table_name}`"
            
            params = []
            if search:
                # 获取列名用于搜索
                cursor.execute(f"PRAGMA table_info(`{table_name}`)")
                columns = [col[1] for col in cursor.fetchall()]
                
                search_conditions = []
                for col in columns:
                    search_conditions.append(f"`{col}` LIKE ?")
                    params.append(f'%{search}%')
                
                where_clause = " WHERE " + " OR ".join(search_conditions)
                base_query += where_clause
                count_query += where_clause
            
            # 获取总数
            cursor.execute(count_query, params)
            total_count = cursor.fetchone()[0]
            
            # 获取数据
            data_query = base_query + f" LIMIT {limit} OFFSET {offset}"
            cursor.execute(data_query, params)
            rows = cursor.fetchall()
            
            # 转换为字典列表
            data = [dict(row) for row in rows]
            
            conn.close()
            
            return jsonify({
                'success': True,
                'database_path': database_path,
                'table_name': table_name,
                'data': data,
                'pagination': {
                    'total_count': total_count,
                    'limit': limit,
                    'offset': offset,
                    'has_more': offset + limit < total_count
                },
                'search': search
            })
            
        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)
    
    return db_bp
