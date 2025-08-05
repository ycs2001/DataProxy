#!/usr/bin/env python3
"""
上下文管理API端点 - 支持前端数据分析模块
"""

import os
import json
import time
import glob
import sqlite3
from datetime import datetime
from flask import Blueprint, jsonify, request
from .error_handlers import APIErrorHandler

def create_context_blueprint():
    """创建上下文管理API蓝图"""

    context_bp = Blueprint('context', __name__, url_prefix='/api/v1/contexts')

    def get_context_file_path(database_path):
        """获取数据库对应的上下文文件路径"""
        db_name = os.path.splitext(os.path.basename(database_path))[0]
        # 直接使用configs目录（相对于DataProxy根目录）
        context_dir = os.path.join('configs', 'database_contexts')
        return os.path.join(context_dir, f"{db_name}_context.json")

    def format_timestamp(timestamp):
        """将Unix时间戳转换为ISO 8601格式"""
        if isinstance(timestamp, str):
            # 如果已经是ISO格式，直接返回
            return timestamp
        elif isinstance(timestamp, (int, float)):
            # Unix时间戳转换
            return datetime.fromtimestamp(timestamp).isoformat() + 'Z'
        else:
            # 当前时间
            return datetime.now().isoformat() + 'Z'

    def convert_business_terms(business_terms):
        """转换业务术语格式为前端期望的对象格式"""
        converted = {}
        for term_name, term_value in business_terms.items():
            if isinstance(term_value, dict):
                # 已经是新格式，确保包含所有必需字段
                converted[term_name] = {
                    "definition": term_value.get("definition", term_name),
                    "calculation": term_value.get("calculation", ""),
                    "applicable_tables": term_value.get("applicable_tables", []),
                    "examples": term_value.get("examples", [])
                }
            else:
                # 旧格式，需要转换
                converted[term_name] = {
                    "definition": term_name,  # 使用术语名作为定义
                    "calculation": term_value if term_value else "",
                    "applicable_tables": [],
                    "examples": []
                }
        return converted

    def get_table_schema(database_path, table_name):
        """获取表的schema信息"""
        try:
            conn = sqlite3.connect(database_path)
            cursor = conn.cursor()

            # 获取列信息
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns_info = cursor.fetchall()

            conn.close()
            return columns_info
        except Exception as e:
            print(f"获取表schema失败: {e}")
            return []
    
    @context_bp.route('', methods=['GET'])
    def get_contexts():
        """获取所有数据库的上下文列表 - 符合前端期望格式"""
        try:
            from datetime import datetime

            # 直接从文件系统扫描上下文配置文件
            context_dir = os.path.join('configs', 'database_contexts')
            contexts = []

            if os.path.exists(context_dir):
                context_files = glob.glob(os.path.join(context_dir, '*_context.json'))

                for context_file in context_files:
                    try:
                        # 读取上下文配置文件
                        with open(context_file, 'r', encoding='utf-8') as f:
                            context_data = json.load(f)

                        # 构建上下文信息 - 符合前端期望格式，适配新的配置文件格式
                        if 'database_info' in context_data:
                            # 新格式：有database_info字段
                            database_name = context_data['database_info'].get('name', '')
                            database_path = context_data['database_info'].get('path', '')
                            generated_at = context_data['database_info'].get('created_at', time.time())
                            table_count = len(context_data.get('tables', {}))
                            business_terms_count = len(context_data.get('business_terms', {}))
                        else:
                            # 旧格式：直接在根级别
                            database_name = context_data.get('database_name', '')
                            database_path = context_data.get('database_path', '')
                            generated_at = context_data.get('created_time', time.time())
                            table_count = len(context_data.get('tables', {}))
                            business_terms_count = len(context_data.get('business_terms', {}))

                        # 如果没有数据库路径，从文件名推断
                        if not database_path:
                            db_name = os.path.splitext(os.path.basename(context_file))[0].replace('_context', '')
                            database_path = f"../databases/imported/{db_name}.db"

                        # 统一路径格式：确保路径相对于Flask应用工作目录
                        if database_path and database_path.startswith('../'):
                            database_path = database_path[3:]  # 移除 "../" 前缀
                        if database_path and not database_path.startswith('./'):
                            database_path = f"./{database_path}"

                        context_info = {
                            "database_path": database_path,
                            "database_name": database_name or os.path.splitext(os.path.basename(database_path))[0],
                            "database_type": "sqlite",
                            "generated_at": format_timestamp(generated_at),
                            "table_count": table_count,
                            "business_terms_count": business_terms_count
                        }
                        contexts.append(context_info)

                    except Exception as e:
                        print(f"[WARNING] 无法读取上下文文件 {context_file}: {e}")
                        continue

            else:
                # 如果目录不存在，创建它
                os.makedirs(context_dir, exist_ok=True)

            return jsonify({
                "success": True,
                "contexts": contexts,
                "total_count": len(contexts)
            })

        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)
    
    @context_bp.route('/<path:database_path>', methods=['GET'])
    def get_context_by_path(database_path):
        """获取指定数据库的上下文详情"""
        try:
            context_file = get_context_file_path(database_path)
            
            if not os.path.exists(context_file):
                return APIErrorHandler.handle_validation_error(
                    'Context not found for this database', 'database_path'
                )
            
            with open(context_file, 'r', encoding='utf-8') as f:
                context_data = json.load(f)
            
            return jsonify({
                'success': True,
                'database_path': database_path,
                'context': context_data,
                'context_file': context_file
            })
            
        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)
    
    @context_bp.route('', methods=['POST'])
    def get_context_detail():
        """获取指定数据库的详细上下文信息 - 符合前端期望格式"""
        try:
            from datetime import datetime

            # 获取请求参数
            data = request.get_json()
            if not data or 'database_path' not in data:
                return jsonify({
                    "success": False,
                    "error": {
                        "code": "VALIDATION_ERROR",
                        "message": "缺少database_path参数",
                        "details": {
                            "field": "database_path",
                            "expected": "字符串类型的数据库路径"
                        }
                    },
                    "timestamp": int(time.time())
                }), 400

            database_path = data['database_path']
            print(f"[DEBUG] 获取上下文详情: {database_path}")

            # 直接检查数据库文件是否存在
            if not os.path.exists(database_path):
                return jsonify({
                    "success": False,
                    "error": {
                        "code": "DATABASE_NOT_FOUND",
                        "message": f"数据库不存在: {database_path}",
                        "details": {
                            "database_path": database_path,
                            "available_databases": [
                                f for f in glob.glob("./databases/imported/*.db")
                                if os.path.exists(f)
                            ]
                        }
                    },
                    "timestamp": int(time.time())
                }), 404

            # 加载上下文信息
            context_file = get_context_file_path(database_path)
            context_data = None

            if os.path.exists(context_file):
                with open(context_file, 'r', encoding='utf-8') as f:
                    context_data = json.load(f)

            if not context_data:
                return jsonify({
                    "success": False,
                    "error": {
                        "code": "CONTEXT_NOT_FOUND",
                        "message": "未找到上下文信息",
                        "details": {
                            "database_path": database_path,
                            "suggestion": "请先上传数据库文件或生成上下文配置"
                        }
                    },
                    "timestamp": int(time.time())
                }), 404

            # 构建详细的上下文信息 - 符合前端期望格式，适配新的配置文件格式
            if 'database_info' in context_data:
                # 新格式
                database_name = context_data['database_info'].get('name', os.path.basename(database_path))
                generated_at = context_data['database_info'].get('created_at', time.time())
            else:
                # 旧格式
                database_name = context_data.get('database_name', os.path.basename(database_path))
                generated_at = context_data.get('created_time', time.time())

            context_detail = {
                "database_path": database_path,
                "database_name": database_name,
                "tables": [],
                "business_terms": convert_business_terms(context_data.get('business_terms', {}))
            }

            # 添加表结构信息 - 适配新的配置文件格式
            tables_data = context_data.get('tables', {})
            field_descriptions = context_data.get('field_descriptions', {})

            if tables_data:
                for table_name, table_info in tables_data.items():
                    table_detail = {
                        "name": table_name,
                        "chinese_name": table_info.get('chinese_name', table_name),
                        "description": table_info.get('description', ''),
                        "columns": []
                    }

                    # 获取数据库schema信息
                    schema_columns = get_table_schema(database_path, table_name)
                    schema_dict = {col[1]: col for col in schema_columns}  # col[1] is column name

                    # 新格式的列信息处理
                    if 'columns' in table_info and isinstance(table_info['columns'], list):
                        # 新格式：columns是列表
                        for col_name in table_info['columns']:
                            # 从schema获取详细信息
                            schema_info = schema_dict.get(col_name)

                            # 从field_descriptions获取中文名和描述
                            field_key = f"{table_name}.{col_name}"
                            field_desc = field_descriptions.get(field_key, {})

                            column_detail = {
                                "name": col_name,
                                "chinese_name": field_desc.get('chinese_name', col_name),
                                "type": schema_info[2] if schema_info else "VARCHAR",  # col[2] is type
                                "description": field_desc.get('business_meaning', ''),
                                "nullable": not bool(schema_info[3]) if schema_info else True,  # col[3] is notnull
                                "primary_key": col_name in table_info.get('primary_keys', [])
                            }
                            table_detail["columns"].append(column_detail)
                    elif 'columns' in table_info and isinstance(table_info['columns'], dict):
                        # 旧格式：columns是字典
                        for col_name, col_info in table_info['columns'].items():
                            schema_info = schema_dict.get(col_name)

                            column_detail = {
                                "name": col_name,
                                "chinese_name": col_info.get('chinese_name', col_name),
                                "type": col_info.get('type', schema_info[2] if schema_info else 'VARCHAR'),
                                "description": col_info.get('description', ''),
                                "nullable": col_info.get('nullable', not bool(schema_info[3]) if schema_info else True),
                                "primary_key": col_info.get('primary_key', False)
                            }
                            table_detail["columns"].append(column_detail)

                    context_detail["tables"].append(table_detail)

            return jsonify({
                "success": True,
                "data": context_detail,
                "metadata": {
                    "generated_at": format_timestamp(generated_at),
                    "table_count": len(context_detail["tables"]),
                    "business_terms_count": len(context_detail["business_terms"])
                }
            })

        except Exception as e:
            print(f"[ERROR] 获取上下文详情失败: {e}")
            return jsonify({
                "success": False,
                "error": {
                    "code": "INTERNAL_SERVER_ERROR",
                    "message": "服务器内部错误",
                    "details": {
                        "error_id": f"ERR_{int(time.time())}"
                    }
                },
                "timestamp": int(time.time())
            }), 500

    @context_bp.route('/create', methods=['POST'])
    def create_context():
        """为数据库创建新的上下文"""
        try:
            data = request.get_json()
            if not data or 'database_path' not in data:
                return APIErrorHandler.handle_validation_error(
                    'database_path is required', 'database_path'
                )

            database_path = data['database_path']

            # 验证数据库文件存在
            if not os.path.exists(database_path):
                return APIErrorHandler.handle_validation_error(
                    'Database file not found', 'database_path'
                )

            # 创建基础上下文结构
            context_data = {
                'database_name': os.path.splitext(os.path.basename(database_path))[0],
                'database_path': database_path,
                'created_time': time.time(),
                'description': data.get('description', ''),
                'business_terms': data.get('business_terms', {}),
                'field_mappings': data.get('field_mappings', {}),
                'query_scope_rules': data.get('query_scope_rules', []),
                'tables': data.get('tables', {}),
                'relationships': data.get('relationships', [])
            }

            # 保存上下文文件
            context_file = get_context_file_path(database_path)
            os.makedirs(os.path.dirname(context_file), exist_ok=True)

            with open(context_file, 'w', encoding='utf-8') as f:
                json.dump(context_data, f, ensure_ascii=False, indent=2)

            return jsonify({
                'success': True,
                'message': 'Context created successfully',
                'database_path': database_path,
                'context_file': context_file,
                'context': context_data
            })

        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)
    
    @context_bp.route('/<path:database_path>', methods=['PUT'])
    def update_context(database_path):
        """更新数据库上下文信息"""
        try:
            context_file = get_context_file_path(database_path)
            
            if not os.path.exists(context_file):
                return APIErrorHandler.handle_validation_error(
                    'Context not found for this database', 'database_path'
                )
            
            # 读取现有上下文
            with open(context_file, 'r', encoding='utf-8') as f:
                context_data = json.load(f)
            
            # 更新数据
            update_data = request.get_json()
            if update_data:
                context_data.update(update_data)
                context_data['modified_time'] = time.time()
            
            # 保存更新后的上下文
            with open(context_file, 'w', encoding='utf-8') as f:
                json.dump(context_data, f, ensure_ascii=False, indent=2)
            
            return jsonify({
                'success': True,
                'message': 'Context updated successfully',
                'database_path': database_path,
                'context': context_data
            })
            
        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)
    
    @context_bp.route('/<path:database_path>/refresh', methods=['POST'])
    def refresh_context(database_path):
        """刷新数据库上下文"""
        try:
            # 这里可以实现重新分析数据库结构的逻辑
            # 目前返回成功状态
            return jsonify({
                'success': True,
                'message': 'Context refresh initiated',
                'database_path': database_path,
                'timestamp': time.time()
            })
            
        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)
    
    @context_bp.route('/<path:database_path>', methods=['DELETE'])
    def delete_context(database_path):
        """删除数据库上下文"""
        try:
            context_file = get_context_file_path(database_path)
            
            if not os.path.exists(context_file):
                return APIErrorHandler.handle_validation_error(
                    'Context not found for this database', 'database_path'
                )
            
            os.remove(context_file)
            
            return jsonify({
                'success': True,
                'message': 'Context deleted successfully',
                'database_path': database_path
            })
            
        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)
    
    # 业务术语管理端点
    @context_bp.route('/<path:database_id>/business-terms', methods=['GET'])
    def get_business_terms(database_id):
        """获取数据库的业务术语"""
        try:
            context_file = get_context_file_path(database_id)

            if not os.path.exists(context_file):
                return jsonify({
                    'success': True,
                    'business_terms': {},
                    'total_count': 0
                })

            with open(context_file, 'r', encoding='utf-8') as f:
                context_data = json.load(f)

            business_terms = context_data.get('business_terms', {})

            return jsonify({
                'success': True,
                'database_id': database_id,
                'business_terms': business_terms,
                'total_count': len(business_terms)
            })

        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)

    @context_bp.route('/<path:database_id>/business-terms', methods=['POST'])
    def add_business_term(database_id):
        """添加业务术语"""
        try:
            data = request.get_json()
            if not data or 'term' not in data or 'definition' not in data:
                return APIErrorHandler.handle_validation_error(
                    'term and definition are required', 'term'
                )

            context_file = get_context_file_path(database_id)

            # 读取或创建上下文
            if os.path.exists(context_file):
                with open(context_file, 'r', encoding='utf-8') as f:
                    context_data = json.load(f)
            else:
                context_data = {
                    'database_name': os.path.splitext(os.path.basename(database_id))[0],
                    'database_path': database_id,
                    'created_time': time.time(),
                    'business_terms': {},
                    'field_mappings': {},
                    'query_scope_rules': [],
                    'tables': {},
                    'relationships': []
                }
                os.makedirs(os.path.dirname(context_file), exist_ok=True)

            # 添加业务术语
            term_name = data['term']
            term_data = {
                'definition': data['definition'],
                'calculation': data.get('calculation', ''),
                'applicable_tables': data.get('applicable_tables', []),
                'examples': data.get('examples', []),
                'created_time': time.time()
            }

            if 'business_terms' not in context_data:
                context_data['business_terms'] = {}

            context_data['business_terms'][term_name] = term_data
            context_data['modified_time'] = time.time()

            # 保存上下文
            with open(context_file, 'w', encoding='utf-8') as f:
                json.dump(context_data, f, ensure_ascii=False, indent=2)

            return jsonify({
                'success': True,
                'message': 'Business term added successfully',
                'database_id': database_id,
                'term': term_name,
                'term_data': term_data
            })

        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)

    @context_bp.route('/<path:database_id>/business-terms/<term_id>', methods=['PUT'])
    def update_business_term(database_id, term_id):
        """更新业务术语"""
        try:
            data = request.get_json()
            if not data:
                return APIErrorHandler.handle_validation_error(
                    'Request body is required', 'body'
                )

            context_file = get_context_file_path(database_id)

            if not os.path.exists(context_file):
                return APIErrorHandler.handle_validation_error(
                    'Context not found for this database', 'database_id'
                )

            with open(context_file, 'r', encoding='utf-8') as f:
                context_data = json.load(f)

            if 'business_terms' not in context_data or term_id not in context_data['business_terms']:
                return APIErrorHandler.handle_validation_error(
                    'Business term not found', 'term_id'
                )

            # 更新业务术语
            term_data = context_data['business_terms'][term_id]
            if 'definition' in data:
                term_data['definition'] = data['definition']
            if 'calculation' in data:
                term_data['calculation'] = data['calculation']
            if 'applicable_tables' in data:
                term_data['applicable_tables'] = data['applicable_tables']
            if 'examples' in data:
                term_data['examples'] = data['examples']

            term_data['modified_time'] = time.time()
            context_data['modified_time'] = time.time()

            # 保存上下文
            with open(context_file, 'w', encoding='utf-8') as f:
                json.dump(context_data, f, ensure_ascii=False, indent=2)

            return jsonify({
                'success': True,
                'message': 'Business term updated successfully',
                'database_id': database_id,
                'term': term_id,
                'term_data': term_data
            })

        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)

    @context_bp.route('/<path:database_id>/business-terms/<term_id>', methods=['DELETE'])
    def delete_business_term(database_id, term_id):
        """删除业务术语"""
        try:
            context_file = get_context_file_path(database_id)

            if not os.path.exists(context_file):
                return APIErrorHandler.handle_validation_error(
                    'Context not found for this database', 'database_id'
                )

            with open(context_file, 'r', encoding='utf-8') as f:
                context_data = json.load(f)

            if 'business_terms' not in context_data or term_id not in context_data['business_terms']:
                return APIErrorHandler.handle_validation_error(
                    'Business term not found', 'term_id'
                )

            # 删除业务术语
            del context_data['business_terms'][term_id]
            context_data['modified_time'] = time.time()

            # 保存上下文
            with open(context_file, 'w', encoding='utf-8') as f:
                json.dump(context_data, f, ensure_ascii=False, indent=2)

            return jsonify({
                'success': True,
                'message': 'Business term deleted successfully',
                'database_id': database_id,
                'term': term_id
            })

        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)

    # 字段映射管理端点
    @context_bp.route('/<path:database_id>/field-mappings', methods=['GET'])
    def get_field_mappings(database_id):
        """获取数据库的字段映射"""
        try:
            context_file = get_context_file_path(database_id)

            if not os.path.exists(context_file):
                return jsonify({
                    'success': True,
                    'field_mappings': {},
                    'total_count': 0
                })

            with open(context_file, 'r', encoding='utf-8') as f:
                context_data = json.load(f)

            # 使用 field_descriptions 字段，与配置文件格式保持一致
            field_mappings = context_data.get('field_descriptions', {})

            return jsonify({
                'success': True,
                'database_id': database_id,
                'field_mappings': field_mappings,
                'total_count': len(field_mappings)
            })

        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)

    @context_bp.route('/<path:database_id>/field-mappings', methods=['PUT'])
    def update_field_mappings(database_id):
        """更新数据库的字段映射"""
        try:
            data = request.get_json()
            if not data:
                return APIErrorHandler.handle_validation_error(
                    'Request body is required', 'body'
                )

            context_file = get_context_file_path(database_id)

            # 读取或创建上下文
            if os.path.exists(context_file):
                with open(context_file, 'r', encoding='utf-8') as f:
                    context_data = json.load(f)
            else:
                context_data = {
                    'database_name': os.path.splitext(os.path.basename(database_id))[0],
                    'database_path': database_id,
                    'created_time': time.time(),
                    'business_terms': {},
                    'field_descriptions': {},
                    'query_scope_rules': [],
                    'tables': {},
                    'relationships': []
                }
                os.makedirs(os.path.dirname(context_file), exist_ok=True)

            # 验证字段映射格式
            field_mappings = data.get('field_mappings', {})
            for field_key, field_info in field_mappings.items():
                if not isinstance(field_info, dict):
                    return APIErrorHandler.handle_validation_error(
                        f'Field mapping for {field_key} must be an object', 'field_mappings'
                    )

                # 验证必需字段
                if 'chinese_name' not in field_info:
                    return APIErrorHandler.handle_validation_error(
                        f'chinese_name is required for field {field_key}', 'field_mappings'
                    )

            # 更新字段映射，使用 field_descriptions 字段
            context_data['field_descriptions'] = field_mappings
            context_data['modified_time'] = time.time()

            # 保存上下文
            with open(context_file, 'w', encoding='utf-8') as f:
                json.dump(context_data, f, ensure_ascii=False, indent=2)

            return jsonify({
                'success': True,
                'message': 'Field mappings updated successfully',
                'database_id': database_id,
                'field_mappings': context_data['field_descriptions'],
                'updated_count': len(field_mappings)
            })

        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)

    # 查询范围规则管理端点
    @context_bp.route('/<path:database_id>/query-scope-rules', methods=['GET'])
    def get_query_scope_rules(database_id):
        """获取数据库的查询范围规则"""
        try:
            context_file = get_context_file_path(database_id)

            if not os.path.exists(context_file):
                return jsonify({
                    'success': True,
                    'query_scope_rules': [],
                    'total_count': 0
                })

            with open(context_file, 'r', encoding='utf-8') as f:
                context_data = json.load(f)

            query_scope_rules = context_data.get('query_scope_rules', [])

            # 为每个规则添加ID（使用数组索引）
            rules_with_id = []
            for index, rule in enumerate(query_scope_rules):
                rule_with_id = rule.copy()
                rule_with_id['id'] = index
                rules_with_id.append(rule_with_id)

            return jsonify({
                'success': True,
                'database_id': database_id,
                'query_scope_rules': rules_with_id,
                'total_count': len(rules_with_id)
            })

        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)

    @context_bp.route('/<path:database_id>/query-scope-rules', methods=['POST'])
    def add_query_scope_rule(database_id):
        """添加查询范围规则"""
        try:
            data = request.get_json()
            if not data:
                return APIErrorHandler.handle_validation_error(
                    'Request body is required', 'body'
                )

            # 验证必需字段
            required_fields = ['rule_type', 'description', 'condition', 'table_name']
            for field in required_fields:
                if field not in data:
                    return APIErrorHandler.handle_validation_error(
                        f'{field} is required', field
                    )

            context_file = get_context_file_path(database_id)

            # 读取或创建上下文
            if os.path.exists(context_file):
                with open(context_file, 'r', encoding='utf-8') as f:
                    context_data = json.load(f)
            else:
                context_data = {
                    'database_name': os.path.splitext(os.path.basename(database_id))[0],
                    'database_path': database_id,
                    'created_time': time.time(),
                    'business_terms': {},
                    'field_descriptions': {},
                    'query_scope_rules': [],
                    'tables': {},
                    'relationships': []
                }
                os.makedirs(os.path.dirname(context_file), exist_ok=True)

            # 添加查询范围规则，使用配置文件格式
            rule_data = {
                'rule_type': data['rule_type'],
                'description': data['description'],
                'condition': data['condition'],
                'table_name': data['table_name']
            }

            if 'query_scope_rules' not in context_data:
                context_data['query_scope_rules'] = []

            context_data['query_scope_rules'].append(rule_data)
            context_data['modified_time'] = time.time()

            # 保存上下文
            with open(context_file, 'w', encoding='utf-8') as f:
                json.dump(context_data, f, ensure_ascii=False, indent=2)

            # 返回带ID的规则数据
            rule_data_with_id = rule_data.copy()
            rule_data_with_id['id'] = len(context_data['query_scope_rules']) - 1

            return jsonify({
                'success': True,
                'message': 'Query scope rule added successfully',
                'database_id': database_id,
                'rule_data': rule_data_with_id
            })

        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)

    @context_bp.route('/<path:database_id>/query-scope-rules/<int:rule_id>', methods=['PUT'])
    def update_query_scope_rule(database_id, rule_id):
        """更新查询范围规则"""
        try:
            data = request.get_json()
            if not data:
                return APIErrorHandler.handle_validation_error(
                    'Request body is required', 'body'
                )

            context_file = get_context_file_path(database_id)

            if not os.path.exists(context_file):
                return jsonify({
                    'success': False,
                    'error': {
                        'code': 'CONTEXT_NOT_FOUND',
                        'message': f'Context not found for database: {database_id}'
                    },
                    'timestamp': int(time.time())
                }), 404

            with open(context_file, 'r', encoding='utf-8') as f:
                context_data = json.load(f)

            query_scope_rules = context_data.get('query_scope_rules', [])

            # 验证规则ID
            if rule_id < 0 or rule_id >= len(query_scope_rules):
                return jsonify({
                    'success': False,
                    'error': {
                        'code': 'RULE_NOT_FOUND',
                        'message': f'Rule with ID {rule_id} not found'
                    },
                    'timestamp': int(time.time())
                }), 404

            # 验证必需字段
            required_fields = ['rule_type', 'description', 'condition', 'table_name']
            for field in required_fields:
                if field not in data:
                    return APIErrorHandler.handle_validation_error(
                        f'{field} is required', field
                    )

            # 更新规则
            updated_rule = {
                'rule_type': data['rule_type'],
                'description': data['description'],
                'condition': data['condition'],
                'table_name': data['table_name']
            }

            context_data['query_scope_rules'][rule_id] = updated_rule
            context_data['modified_time'] = time.time()

            # 保存上下文
            with open(context_file, 'w', encoding='utf-8') as f:
                json.dump(context_data, f, ensure_ascii=False, indent=2)

            # 返回带ID的规则数据
            rule_data_with_id = updated_rule.copy()
            rule_data_with_id['id'] = rule_id

            return jsonify({
                'success': True,
                'message': 'Query scope rule updated successfully',
                'database_id': database_id,
                'rule_data': rule_data_with_id
            })

        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)

    @context_bp.route('/<path:database_id>/query-scope-rules/<int:rule_id>', methods=['DELETE'])
    def delete_query_scope_rule(database_id, rule_id):
        """删除查询范围规则"""
        try:
            context_file = get_context_file_path(database_id)

            if not os.path.exists(context_file):
                return jsonify({
                    'success': False,
                    'error': {
                        'code': 'CONTEXT_NOT_FOUND',
                        'message': f'Context not found for database: {database_id}'
                    },
                    'timestamp': int(time.time())
                }), 404

            with open(context_file, 'r', encoding='utf-8') as f:
                context_data = json.load(f)

            query_scope_rules = context_data.get('query_scope_rules', [])

            # 验证规则ID
            if rule_id < 0 or rule_id >= len(query_scope_rules):
                return jsonify({
                    'success': False,
                    'error': {
                        'code': 'RULE_NOT_FOUND',
                        'message': f'Rule with ID {rule_id} not found'
                    },
                    'timestamp': int(time.time())
                }), 404

            # 删除规则
            deleted_rule = context_data['query_scope_rules'].pop(rule_id)
            context_data['modified_time'] = time.time()

            # 保存上下文
            with open(context_file, 'w', encoding='utf-8') as f:
                json.dump(context_data, f, ensure_ascii=False, indent=2)

            return jsonify({
                'success': True,
                'message': 'Query scope rule deleted successfully',
                'database_id': database_id,
                'deleted_rule': deleted_rule,
                'remaining_count': len(context_data['query_scope_rules'])
            })

        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)

    return context_bp
