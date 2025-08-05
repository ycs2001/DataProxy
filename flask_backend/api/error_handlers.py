#!/usr/bin/env python3
"""
API错误处理器 - 统一的错误处理和响应格式
"""

import os
import time
import traceback
from typing import Dict, Any, Optional
from flask import jsonify


class APIErrorHandler:
    """API错误处理器"""
    
    @staticmethod
    def create_error_response(
        error_message: str,
        error_code: str = "INTERNAL_ERROR",
        status_code: int = 500,
        details: Optional[Dict[str, Any]] = None
    ) -> tuple:
        """
        创建标准化错误响应
        
        Args:
            error_message: 错误消息
            error_code: 错误代码
            status_code: HTTP状态码
            details: 额外的错误详情
            
        Returns:
            tuple: (response, status_code)
        """
        response = {
            "success": False,
            "error": {
                "message": error_message,
                "code": error_code,
                "timestamp": time.time()
            }
        }
        
        if details:
            response["error"]["details"] = details
        
        return jsonify(response), status_code
    
    @staticmethod
    def create_success_response(
        data: Any,
        message: str = "操作成功",
        metadata: Optional[Dict[str, Any]] = None
    ) -> tuple:
        """
        创建标准化成功响应
        
        Args:
            data: 响应数据
            message: 成功消息
            metadata: 元数据
            
        Returns:
            tuple: (response, status_code)
        """
        response = {
            "success": True,
            "message": message,
            "data": data,
            "timestamp": time.time()
        }
        
        if metadata:
            response["metadata"] = metadata
        
        return jsonify(response), 200

    @staticmethod
    def success_response(data: Any, message: str = "操作成功") -> tuple:
        """简化的成功响应方法（兼容性）"""
        return APIErrorHandler.create_success_response(data, message)

    @staticmethod
    def handle_validation_error(error_message: str, field: str = None) -> tuple:
        """处理验证错误"""
        details = {"field": field} if field else None
        return APIErrorHandler.create_error_response(
            error_message,
            "VALIDATION_ERROR",
            400,
            details
        )
    
    @staticmethod
    def handle_not_found_error(resource: str = "资源") -> tuple:
        """处理资源未找到错误"""
        return APIErrorHandler.create_error_response(
            f"{resource}未找到",
            "NOT_FOUND",
            404
        )
    
    @staticmethod
    def handle_database_error(error: Exception) -> tuple:
        """处理数据库错误"""
        return APIErrorHandler.create_error_response(
            "数据库操作失败",
            "DATABASE_ERROR",
            500,
            {"error_type": type(error).__name__}
        )
    
    @staticmethod
    def handle_configuration_error(error_message: str) -> tuple:
        """处理配置错误"""
        return APIErrorHandler.create_error_response(
            f"配置错误: {error_message}",
            "CONFIGURATION_ERROR",
            500
        )
    
    @staticmethod
    def handle_query_error(error: Exception) -> tuple:
        """处理查询错误"""
        return APIErrorHandler.create_error_response(
            f"查询执行失败: {str(error)}",
            "QUERY_ERROR",
            500,
            {"error_type": type(error).__name__}
        )
    
    @staticmethod
    def handle_unexpected_error(error: Exception) -> tuple:
        """处理未预期的错误"""
        print(f"[ERROR] 未预期的错误: {error}")
        print(f"[ERROR] 错误堆栈: {traceback.format_exc()}")
        
        return APIErrorHandler.create_error_response(
            "服务器内部错误",
            "INTERNAL_ERROR",
            500,
            {
                "error_type": type(error).__name__,
                "debug_info": str(error) if os.getenv('FLASK_DEBUG') == 'true' else None
            }
        )


def register_error_handlers(app):
    """注册全局错误处理器"""
    
    @app.errorhandler(400)
    def bad_request(error):
        return APIErrorHandler.create_error_response(
            "请求格式错误",
            "BAD_REQUEST",
            400
        )
    
    @app.errorhandler(404)
    def not_found(error):
        return APIErrorHandler.create_error_response(
            "API端点未找到",
            "NOT_FOUND",
            404
        )
    
    @app.errorhandler(405)
    def method_not_allowed(error):
        return APIErrorHandler.create_error_response(
            "HTTP方法不允许",
            "METHOD_NOT_ALLOWED",
            405
        )
    
    @app.errorhandler(500)
    def internal_error(error):
        return APIErrorHandler.create_error_response(
            "服务器内部错误",
            "INTERNAL_ERROR",
            500
        )
    
    print("✅ API错误处理器: 已注册全局错误处理器")
