#!/usr/bin/env python3
"""
文件管理API端点 - 支持前端数据分析模块
"""

import os
import time
from flask import Blueprint, jsonify, request
from werkzeug.utils import secure_filename
from .error_handlers import APIErrorHandler

def create_file_blueprint():
    """创建文件管理API蓝图"""
    
    file_bp = Blueprint('file', __name__, url_prefix='/api/v1')
    
    # 允许的文件扩展名
    ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv', 'json'}
    MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB
    
    def allowed_file(filename):
        """检查文件扩展名是否允许"""
        return '.' in filename and \
               filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS
    
    @file_bp.route('/files/upload', methods=['POST'])
    def upload_file():
        """文件上传并生成数据库上下文 - 符合前端期望格式"""
        try:
            from datetime import datetime
            import tempfile
            import shutil

            # 检查文件是否存在
            if 'file' not in request.files:
                return jsonify({
                    "success": False,
                    "error": {
                        "code": "VALIDATION_ERROR",
                        "message": "未找到上传文件",
                        "details": {
                            "field": "file",
                            "expected": "Excel或CSV文件"
                        }
                    }
                }), 400

            file = request.files['file']
            if file.filename == '':
                return jsonify({
                    "success": False,
                    "error": {
                        "code": "VALIDATION_ERROR",
                        "message": "文件名不能为空"
                    }
                }), 400

            # 获取其他参数
            description = request.form.get('description', '')
            business_terms = request.form.get('business_terms', '')
            data_dictionary = request.form.get('data_dictionary', '')

            # 检查文件类型
            allowed_extensions = {'.xlsx', '.xls', '.csv'}
            file_ext = os.path.splitext(file.filename)[1].lower()
            if file_ext not in allowed_extensions:
                return jsonify({
                    "success": False,
                    "error": {
                        "code": "VALIDATION_ERROR",
                        "message": f"不支持的文件类型: {file_ext}",
                        "details": {
                            "supported_types": list(allowed_extensions)
                        }
                    }
                }), 400

            # 确保全局状态已初始化
            try:
                from flask_backend.app import global_state
                if not global_state.initialized:
                    success = global_state.initialize()
                    if not success:
                        return jsonify({
                            "success": False,
                            "error": "系统初始化失败"
                        }), 500
            except Exception as global_error:
                print(f"[WARNING] 无法访问全局状态: {global_error}")

            # 生成唯一文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{timestamp}_{secure_filename(file.filename)}"

            # 创建临时文件路径
            temp_dir = tempfile.gettempdir()
            temp_file_path = os.path.join(temp_dir, filename)

            # 保存上传的文件
            file.save(temp_file_path)
            print(f"[INFO] 文件已保存到: {temp_file_path}")

            # 转换文件为SQLite数据库
            db_filename = os.path.splitext(filename)[0] + '.db'

            try:
                # 使用文件转换器处理文件
                processed_result = _process_uploaded_file_advanced(
                    temp_file_path,
                    description=description,
                    business_terms=business_terms,
                    data_dictionary=data_dictionary
                )

                if processed_result.get('success'):
                    # 移动数据库文件到正式目录
                    databases_dir = './databases/imported'
                    os.makedirs(databases_dir, exist_ok=True)

                    final_db_path = os.path.join(databases_dir, db_filename)

                    # 如果转换器生成了数据库文件，移动它
                    temp_db_path = processed_result.get('database_path')
                    if temp_db_path and os.path.exists(temp_db_path):
                        shutil.move(temp_db_path, final_db_path)

                    # 刷新可用数据库列表
                    try:
                        global_state.available_databases = global_state.unified_config.get_available_databases()
                    except:
                        pass

                    # 清理临时文件
                    if os.path.exists(temp_file_path):
                        os.remove(temp_file_path)

                    return jsonify({
                        "success": True,
                        "message": "文件上传并处理成功",
                        "data": {
                            "file_path": final_db_path,
                            "database_name": processed_result.get('database_name', db_filename),
                            "table_count": processed_result.get('table_count', 0),
                            "context_generated": processed_result.get('context_generated', False),
                            "business_terms_count": processed_result.get('business_terms_count', 0)
                        }
                    })
                else:
                    return jsonify({
                        "success": False,
                        "error": {
                            "code": "FILE_PROCESSING_ERROR",
                            "message": "文件处理失败，无法生成数据库"
                        }
                    }), 500

            except Exception as processing_error:
                print(f"[ERROR] 文件处理失败: {processing_error}")
                # 清理临时文件
                if os.path.exists(temp_file_path):
                    os.remove(temp_file_path)

                return jsonify({
                    "success": False,
                    "error": {
                        "code": "FILE_PROCESSING_ERROR",
                        "message": f"文件处理失败: {str(processing_error)}"
                    }
                }), 500

        except Exception as e:
            print(f"[ERROR] 文件上传失败: {e}")
            return jsonify({
                "success": False,
                "error": {
                    "code": "INTERNAL_SERVER_ERROR",
                    "message": str(e)
                }
            }), 500

    def _process_uploaded_file_advanced(file_path, description, business_terms, data_dictionary):
        """高级文件处理 - 生成数据库和上下文"""
        try:
            import pandas as pd
            import sqlite3

            # 读取文件
            file_ext = os.path.splitext(file_path)[1].lower()

            if file_ext in ['.xlsx', '.xls']:
                # 读取Excel文件
                df = pd.read_excel(file_path)
            elif file_ext == '.csv':
                # 读取CSV文件
                df = pd.read_csv(file_path, encoding='utf-8')
            else:
                raise Exception(f"不支持的文件类型: {file_ext}")

            # 生成数据库文件名
            base_name = os.path.splitext(os.path.basename(file_path))[0]
            db_filename = f"{base_name}.db"
            temp_db_path = os.path.join(os.path.dirname(file_path), db_filename)

            # 创建SQLite数据库
            conn = sqlite3.connect(temp_db_path)

            # 将数据写入数据库
            table_name = base_name.replace('-', '_').replace(' ', '_')
            df.to_sql(table_name, conn, if_exists='replace', index=False)

            # 获取表结构信息
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns_info = cursor.fetchall()

            conn.close()

            # 生成上下文信息
            context_data = {
                'database_name': base_name,
                'database_path': temp_db_path,
                'created_time': time.time(),
                'description': description,
                'business_terms': _parse_business_terms(business_terms),
                'field_mappings': _parse_data_dictionary(data_dictionary),
                'query_scope_rules': [],
                'tables': {
                    table_name: {
                        'chinese_name': table_name,
                        'description': f'从文件 {os.path.basename(file_path)} 导入的数据',
                        'columns': {}
                    }
                },
                'relationships': []
            }

            # 添加列信息
            for col_info in columns_info:
                col_name = col_info[1]
                col_type = col_info[2]
                context_data['tables'][table_name]['columns'][col_name] = {
                    'chinese_name': col_name,
                    'type': col_type,
                    'description': '',
                    'nullable': not col_info[3],
                    'primary_key': col_info[5] == 1
                }

            # 保存上下文文件
            try:
                from flask_backend.api.context_endpoints import get_context_file_path
                context_file = get_context_file_path(temp_db_path)
                os.makedirs(os.path.dirname(context_file), exist_ok=True)

                with open(context_file, 'w', encoding='utf-8') as f:
                    json.dump(context_data, f, ensure_ascii=False, indent=2)

                context_generated = True
            except Exception as context_error:
                print(f"[WARNING] 上下文保存失败: {context_error}")
                context_generated = False

            return {
                'success': True,
                'database_path': temp_db_path,
                'database_name': base_name,
                'table_count': 1,
                'context_generated': context_generated,
                'business_terms_count': len(context_data['business_terms']),
                'record_count': len(df)
            }

        except Exception as e:
            print(f"[ERROR] 高级文件处理失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    def _parse_business_terms(business_terms_text):
        """解析业务术语文本"""
        terms = {}
        if business_terms_text:
            try:
                # 简单的解析逻辑，每行一个术语
                lines = business_terms_text.strip().split('\n')
                for line in lines:
                    if ':' in line:
                        term, definition = line.split(':', 1)
                        terms[term.strip()] = {
                            'definition': definition.strip(),
                            'calculation': '',
                            'applicable_tables': []
                        }
            except Exception as e:
                print(f"[WARNING] 业务术语解析失败: {e}")
        return terms

    def _parse_data_dictionary(data_dictionary_text):
        """解析数据字典文本"""
        mappings = {}
        if data_dictionary_text:
            try:
                # 简单的解析逻辑，每行一个映射
                lines = data_dictionary_text.strip().split('\n')
                for line in lines:
                    if ':' in line:
                        field, chinese_name = line.split(':', 1)
                        mappings[field.strip()] = chinese_name.strip()
            except Exception as e:
                print(f"[WARNING] 数据字典解析失败: {e}")
        return mappings

    def _process_uploaded_file(file_path, description, business_terms, data_dictionary):
        """处理上传的文件，生成数据库和上下文 - 兼容性函数"""
        try:
            # 调用高级处理函数
            result = _process_uploaded_file_advanced(file_path, description, business_terms, data_dictionary)

            if result.get('success'):
                return {
                    'status': 'processed',
                    'database_generated': True,
                    'context_generated': result.get('context_generated', False),
                    'message': 'File processing completed successfully'
                }
            else:
                return {
                    'status': 'failed',
                    'database_generated': False,
                    'context_generated': False,
                    'message': result.get('error', 'File processing failed')
                }
        except Exception as e:
            raise Exception(f"File processing failed: {str(e)}")
    
    @file_bp.route('/files', methods=['GET'])
    def list_files():
        """获取已上传文件列表"""
        try:
            upload_dir = os.getenv('DATAPROXY_DATA_DIR', './data')
            
            if not os.path.exists(upload_dir):
                return jsonify({
                    'success': True,
                    'files': [],
                    'total_count': 0
                })
            
            files = []
            for filename in os.listdir(upload_dir):
                file_path = os.path.join(upload_dir, filename)
                if os.path.isfile(file_path):
                    file_stat = os.stat(file_path)
                    files.append({
                        'name': filename,
                        'path': file_path,
                        'size': file_stat.st_size,
                        'modified_time': file_stat.st_mtime,
                        'extension': filename.rsplit('.', 1)[1].lower() if '.' in filename else ''
                    })
            
            # 按修改时间排序
            files.sort(key=lambda x: x['modified_time'], reverse=True)
            
            return jsonify({
                'success': True,
                'files': files,
                'total_count': len(files),
                'upload_directory': upload_dir
            })
            
        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)
    
    @file_bp.route('/files/<filename>', methods=['DELETE'])
    def delete_file(filename):
        """删除指定文件"""
        try:
            upload_dir = os.getenv('DATAPROXY_DATA_DIR', './data')
            file_path = os.path.join(upload_dir, secure_filename(filename))
            
            if not os.path.exists(file_path):
                return APIErrorHandler.handle_validation_error(
                    'File not found', 'filename'
                )
            
            os.remove(file_path)
            
            return jsonify({
                'success': True,
                'message': 'File deleted successfully',
                'filename': filename
            })
            
        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)
    
    return file_bp
