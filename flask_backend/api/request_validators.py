#!/usr/bin/env python3
"""
请求验证器 - 统一的请求验证逻辑
"""

from typing import Dict, Any
from flask import Request


def validate_query_request(request: Request) -> Dict[str, Any]:
    """
    验证查询请求
    
    Args:
        request: Flask请求对象
        
    Returns:
        Dict[str, Any]: 验证结果
    """
    try:
        # 检查Content-Type
        if not request.is_json:
            return {
                'valid': False,
                'error': '请求必须是JSON格式',
                'field': 'content-type'
            }
        
        # 获取JSON数据
        data = request.get_json()
        if not data:
            return {
                'valid': False,
                'error': '请求体不能为空',
                'field': 'body'
            }
        
        # 验证必需字段
        if 'query' not in data:
            return {
                'valid': False,
                'error': '缺少必需字段: query',
                'field': 'query'
            }
        
        query = data['query']
        
        # 验证query字段
        if not isinstance(query, str):
            return {
                'valid': False,
                'error': 'query字段必须是字符串',
                'field': 'query'
            }
        
        if not query.strip():
            return {
                'valid': False,
                'error': 'query字段不能为空',
                'field': 'query'
            }
        
        if len(query.strip()) > 1000:
            return {
                'valid': False,
                'error': 'query字段长度不能超过1000字符',
                'field': 'query'
            }
        
        # 验证可选字段
        analysis_mode = data.get('analysis_mode', 'auto')
        if analysis_mode not in ['auto', 'simple', 'detailed']:
            return {
                'valid': False,
                'error': 'analysis_mode必须是auto、simple或detailed之一',
                'field': 'analysis_mode'
            }
        
        enable_statistics = data.get('enable_statistics', True)
        if not isinstance(enable_statistics, bool):
            return {
                'valid': False,
                'error': 'enable_statistics必须是布尔值',
                'field': 'enable_statistics'
            }
        
        return {
            'valid': True,
            'data': {
                'query': query.strip(),
                'analysis_mode': analysis_mode,
                'enable_statistics': enable_statistics
            }
        }
        
    except Exception as e:
        return {
            'valid': False,
            'error': f'请求解析失败: {str(e)}',
            'field': 'request'
        }


def validate_database_switch_request(request: Request) -> Dict[str, Any]:
    """
    验证数据库切换请求
    
    Args:
        request: Flask请求对象
        
    Returns:
        Dict[str, Any]: 验证结果
    """
    try:
        # 检查Content-Type
        if not request.is_json:
            return {
                'valid': False,
                'error': '请求必须是JSON格式',
                'field': 'content-type'
            }
        
        # 获取JSON数据
        data = request.get_json()
        if not data:
            return {
                'valid': False,
                'error': '请求体不能为空',
                'field': 'body'
            }
        
        # 验证必需字段
        if 'database_path' not in data:
            return {
                'valid': False,
                'error': '缺少必需字段: database_path',
                'field': 'database_path'
            }
        
        database_path = data['database_path']
        
        # 验证database_path字段
        if not isinstance(database_path, str):
            return {
                'valid': False,
                'error': 'database_path字段必须是字符串',
                'field': 'database_path'
            }
        
        if not database_path.strip():
            return {
                'valid': False,
                'error': 'database_path字段不能为空',
                'field': 'database_path'
            }
        
        return {
            'valid': True,
            'data': {
                'database_path': database_path.strip()
            }
        }
        
    except Exception as e:
        return {
            'valid': False,
            'error': f'请求解析失败: {str(e)}',
            'field': 'request'
        }


def validate_file_upload_request(request: Request) -> Dict[str, Any]:
    """
    验证文件上传请求
    
    Args:
        request: Flask请求对象
        
    Returns:
        Dict[str, Any]: 验证结果
    """
    try:
        # 检查是否有文件
        if 'file' not in request.files:
            return {
                'valid': False,
                'error': '缺少文件字段',
                'field': 'file'
            }
        
        file = request.files['file']
        
        # 检查文件是否为空
        if file.filename == '':
            return {
                'valid': False,
                'error': '未选择文件',
                'field': 'file'
            }
        
        # 检查文件扩展名
        allowed_extensions = {'.xlsx', '.xls', '.csv'}
        if not any(file.filename.lower().endswith(ext) for ext in allowed_extensions):
            return {
                'valid': False,
                'error': f'不支持的文件格式，支持的格式: {", ".join(allowed_extensions)}',
                'field': 'file'
            }
        
        return {
            'valid': True,
            'data': {
                'file': file
            }
        }
        
    except Exception as e:
        return {
            'valid': False,
            'error': f'文件验证失败: {str(e)}',
            'field': 'file'
        }
