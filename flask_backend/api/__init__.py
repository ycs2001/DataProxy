#!/usr/bin/env python3
"""
API模块 - 重构后的Flask API组件

提供模块化的API端点和统一的错误处理
"""

from .error_handlers import APIErrorHandler, register_error_handlers
from .query_endpoints import create_query_blueprint
from .request_validators import (
    validate_query_request,
    validate_database_switch_request,
    validate_file_upload_request
)

__all__ = [
    'APIErrorHandler',
    'register_error_handlers',
    'create_query_blueprint',
    'validate_query_request',
    'validate_database_switch_request',
    'validate_file_upload_request'
]
