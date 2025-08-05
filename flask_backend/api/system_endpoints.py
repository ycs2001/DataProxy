#!/usr/bin/env python3
"""
系统管理API端点 - 支持前端数据分析模块
"""

import time
import os
from flask import Blueprint, jsonify, request
from .error_handlers import APIErrorHandler

def create_system_blueprint():
    """创建系统管理API蓝图"""
    
    system_bp = Blueprint('system', __name__)
    
    @system_bp.route('/health', methods=['GET'])
    def health_check():
        """健康检查端点"""
        try:
            return jsonify({
                'status': 'healthy',
                'service': 'DataProxy API',
                'version': '2.0.0',
                'timestamp': time.time(),
                'api_key_configured': bool(os.getenv('DEEPSEEK_API_KEY'))
            })
        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)
    
    @system_bp.route('/api/v1/system/status', methods=['GET'])
    def system_status():
        """系统详细状态信息"""
        try:
            # 检查各个模块状态
            status_info = {
                'system': {
                    'status': 'running',
                    'uptime': time.time(),
                    'version': '2.0.0'
                },
                'modules': {
                    'llm_service': bool(os.getenv('DEEPSEEK_API_KEY')),
                    'database_service': True,
                    'visualization_service': True,
                    'file_service': True
                },
                'configuration': {
                    'api_key_configured': bool(os.getenv('DEEPSEEK_API_KEY')),
                    'data_directory': os.getenv('DATAPROXY_DATA_DIR', './data'),
                    'max_file_size': '100MB'
                },
                'statistics': {
                    'total_queries': 0,  # 可以从数据库获取
                    'active_databases': 0,  # 可以从文件系统获取
                    'total_uploads': 0
                }
            }
            
            return jsonify({
                'success': True,
                'status': status_info,
                'timestamp': time.time()
            })
            
        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)
    
    return system_bp
