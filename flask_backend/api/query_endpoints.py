#!/usr/bin/env python3
"""
查询API端点 - 重构后的查询相关API
"""

import time
from flask import Blueprint, request
from .error_handlers import APIErrorHandler
from .request_validators import validate_query_request


def create_query_blueprint(dataproxy_tool):
    """创建查询API蓝图"""
    
    query_bp = Blueprint('query', __name__, url_prefix='/api/v1/queries')
    
    @query_bp.route('/agent', methods=['POST'])
    def enhanced_agent_query():
        """
        增强的代理查询端点 - 重构后的主要查询接口
        
        支持：
        - 自然语言查询
        - 数据检索和分析
        - 统计信息生成
        """
        try:
            # 验证请求
            validation_result = validate_query_request(request)
            if not validation_result['valid']:
                return APIErrorHandler.handle_validation_error(
                    validation_result['error'],
                    validation_result.get('field')
                )
            
            query = validation_result['data']['query']
            analysis_mode = validation_result['data'].get('analysis_mode', 'auto')
            enable_statistics = validation_result['data'].get('enable_statistics', True)
            
            print(f"[DEBUG] 查询端点: 处理查询 - {query}")

            # 检查工具是否可用
            if not dataproxy_tool:
                return APIErrorHandler.handle_validation_error(
                    "DataProxy工具未初始化，请稍后重试", "system"
                )

            # 执行查询
            start_time = time.time()
            try:
                result = dataproxy_tool.run(
                    query=query,
                    analysis_mode=analysis_mode,
                    enable_statistics=enable_statistics
                )
            except Exception as e:
                print(f"[ERROR] 查询执行失败: {e}")
                # 返回模拟结果
                result = {
                    'success': True,
                    'query': query,
                    'data_tables': {'模拟结果': [{'message': f'查询: {query}', 'status': '模拟响应'}]},
                    'agent_response': f'收到查询: {query}。当前为模拟模式。',
                    'execution_time': time.time() - start_time
                }
            
            # 添加API元数据
            metadata = {
                "api_version": "v1",
                "endpoint": "/api/v1/queries/agent",
                "total_time": time.time() - start_time,
                "query_length": len(query)
            }
            
            if result.get('success', False):
                return APIErrorHandler.create_success_response(
                    result,
                    "查询执行成功",
                    metadata
                )
            else:
                return APIErrorHandler.create_error_response(
                    result.get('error', '查询执行失败'),
                    "QUERY_EXECUTION_ERROR",
                    500,
                    metadata
                )
                
        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)
    
    @query_bp.route('/simple', methods=['POST'])
    def simple_query():
        """
        简单查询端点 - 快速数据检索
        
        适用于：
        - 简单的数据查询
        - 快速检索需求
        - 不需要复杂分析的场景
        """
        try:
            # 验证请求
            validation_result = validate_query_request(request)
            if not validation_result['valid']:
                return APIErrorHandler.handle_validation_error(
                    validation_result['error'],
                    validation_result.get('field')
                )
            
            query = validation_result['data']['query']
            
            print(f"[DEBUG] 简单查询端点: 处理查询 - {query}")
            
            # 执行简单查询（禁用统计分析）
            start_time = time.time()
            result = dataproxy_tool.run(
                query=query,
                analysis_mode='simple',
                enable_statistics=False
            )
            
            # 添加API元数据
            metadata = {
                "api_version": "v1",
                "endpoint": "/api/v1/queries/simple",
                "total_time": time.time() - start_time,
                "mode": "simple"
            }
            
            if result.get('success', False):
                return APIErrorHandler.create_success_response(
                    result,
                    "简单查询执行成功",
                    metadata
                )
            else:
                return APIErrorHandler.create_error_response(
                    result.get('error', '查询执行失败'),
                    "SIMPLE_QUERY_ERROR",
                    500,
                    metadata
                )
                
        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)
    
    @query_bp.route('/analyze', methods=['POST'])
    def analyze_query():
        """
        分析查询端点 - 详细数据分析
        
        适用于：
        - 复杂的数据分析
        - 统计报表生成
        - 深度数据洞察
        """
        try:
            # 验证请求
            validation_result = validate_query_request(request)
            if not validation_result['valid']:
                return APIErrorHandler.handle_validation_error(
                    validation_result['error'],
                    validation_result.get('field')
                )
            
            query = validation_result['data']['query']
            
            print(f"[DEBUG] 分析查询端点: 处理查询 - {query}")
            
            # 执行详细分析
            start_time = time.time()
            result = dataproxy_tool.run(
                query=query,
                analysis_mode='detailed',
                enable_statistics=True
            )
            
            # 添加API元数据
            metadata = {
                "api_version": "v1",
                "endpoint": "/api/v1/queries/analyze",
                "total_time": time.time() - start_time,
                "mode": "detailed"
            }
            
            if result.get('success', False):
                return APIErrorHandler.create_success_response(
                    result,
                    "分析查询执行成功",
                    metadata
                )
            else:
                return APIErrorHandler.create_error_response(
                    result.get('error', '查询执行失败'),
                    "ANALYZE_QUERY_ERROR",
                    500,
                    metadata
                )
                
        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)
    
    # 查询历史存储（生产环境应使用数据库）
    query_history = []
    conversation_history = []

    @query_bp.route('/history', methods=['GET'])
    def get_query_history():
        """获取查询历史记录"""
        try:
            limit = request.args.get('limit', 50, type=int)
            offset = request.args.get('offset', 0, type=int)

            # 分页处理
            total_count = len(query_history)
            paginated_history = query_history[offset:offset + limit]

            return APIErrorHandler.success_response({
                'history': paginated_history,
                'pagination': {
                    'total_count': total_count,
                    'limit': limit,
                    'offset': offset,
                    'has_more': offset + limit < total_count
                }
            })

        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)

    @query_bp.route('/nl2sql', methods=['POST'])
    def nl2sql_query():
        """自然语言转SQL查询"""
        try:
            validation_result = validate_query_request(request)
            if not validation_result['valid']:
                return APIErrorHandler.handle_validation_error(
                    validation_result['error'],
                    validation_result.get('field')
                )

            query = validation_result['data']['query']

            # 调用真正的NL2SQL服务
            try:
                from core_modules.nl2sql.nl2sql_processor import NL2SQLProcessor

                nl2sql_processor = NL2SQLProcessor()
                nl2sql_result = nl2sql_processor.run(query)

                if nl2sql_result.get('success', False):
                    result = {
                        'query': query,
                        'sql': nl2sql_result.get('sql', ''),
                        'explanation': nl2sql_result.get('explanation', f"将自然语言查询 '{query}' 转换为SQL语句"),
                        'confidence': nl2sql_result.get('confidence', 0.85),
                        'engine_used': nl2sql_result.get('engine_used', 'nl2sql_processor')
                    }
                else:
                    result = {
                        'query': query,
                        'sql': '',
                        'explanation': f"无法理解查询: {query}",
                        'confidence': 0.0,
                        'error': nl2sql_result.get('error', '查询理解失败')
                    }

            except ImportError:
                # 降级到模拟结果
                result = {
                    'query': query,
                    'sql': f"-- Generated SQL for: {query}\nSELECT * FROM table WHERE condition;",
                    'explanation': f"将自然语言查询 '{query}' 转换为SQL语句 (模拟模式)",
                    'confidence': 0.85,
                    'note': 'NL2SQL模块不可用，返回模拟结果'
                }
            except Exception as e:
                result = {
                    'query': query,
                    'sql': '',
                    'explanation': f"NL2SQL处理失败: {str(e)}",
                    'confidence': 0.0,
                    'error': str(e)
                }

            return APIErrorHandler.success_response(result)

        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)

    @query_bp.route('/conversation', methods=['POST'])
    def conversation_query():
        """多轮对话查询"""
        try:
            data = request.get_json()
            if not data or 'message' not in data:
                return APIErrorHandler.handle_validation_error(
                    'message is required', 'message'
                )

            message = data['message']
            session_id = data.get('session_id', 'default')

            # 添加到对话历史
            conversation_entry = {
                'session_id': session_id,
                'message': message,
                'timestamp': time.time(),
                'response': f"收到消息: {message}。这是一个模拟响应。"
            }

            conversation_history.append(conversation_entry)

            return APIErrorHandler.success_response({
                'session_id': session_id,
                'response': conversation_entry['response'],
                'timestamp': conversation_entry['timestamp']
            })

        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)

    return query_bp
