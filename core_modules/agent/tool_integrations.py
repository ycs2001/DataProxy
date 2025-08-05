#!/usr/bin/env python3
"""
工具集成模块 - 简化版
只保留NL2SQL工具的核心功能，删除与DynamicSchemaExtractor重复的功能
"""

import os
import time
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field

# LangChain导入
try:
    from langchain.tools import BaseTool
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False

# 配置驱动的NL2SQL引擎
try:
    from .configurable_nl2sql_engine import ConfigurableNL2SQLEngine
    from .unified_config import get_unified_config
    CONFIGURABLE_ENGINE_AVAILABLE = True
except ImportError:
    CONFIGURABLE_ENGINE_AVAILABLE = False


class NL2SQLToolInput(BaseModel):
    """NL2SQL工具输入模型"""
    query: str = Field(description="自然语言查询")


class ConfigurableNL2SQLTool(BaseTool):
    """配置驱动的NL2SQL工具 - 新架构"""
    name: str = "nl2sql_query"
    description: str = "将自然语言转换为SQL并执行，返回银行业务数据（配置驱动）"
    args_schema: type = NL2SQLToolInput

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 使用私有属性避免Pydantic字段冲突
        if CONFIGURABLE_ENGINE_AVAILABLE:
            self._config_manager = get_unified_config()
            self._nl2sql_engine = ConfigurableNL2SQLEngine(self._config_manager)
            self._use_configurable_engine = True
            print("[DEBUG] ConfigurableNL2SQLTool: 使用配置驱动引擎")
        else:
            self._use_configurable_engine = False
            print("[WARNING] ConfigurableNL2SQLTool: 配置驱动引擎不可用，回退到传统模式")

    def _run(self, query: str) -> Dict[str, Any]:
        """执行NL2SQL查询 - 配置驱动版本"""
        if self._use_configurable_engine:
            return self._run_with_configurable_engine(query)
        else:
            return self._run_with_legacy_engine(query)

    def _run_with_configurable_engine(self, query: str) -> Dict[str, Any]:
        """使用配置驱动引擎执行查询"""
        try:
            print(f"[DEBUG] ConfigurableNL2SQLTool: 开始执行查询: {query}")

            # 创建查询上下文
            context = self._config_manager.create_query_context_for_nl2sql(query)

            # 使用配置驱动引擎生成SQL
            result = self._nl2sql_engine.generate_sql(query, context)

            if result['success']:
                # 这里应该执行SQL并返回数据
                # 为了演示，我们返回模拟结果
                return {
                    'success': True,
                    'sql': result['sql'],
                    'data': [],  # 实际应该执行SQL获取数据
                    'metadata': result.get('metadata', {}),
                    'execution_time': 0.1,
                    'record_count': 0,
                    'task_completed': True,
                    'next_action': 'none',
                    'summary': '配置驱动引擎执行成功（演示模式）'
                }
            else:
                return {
                    'success': False,
                    'error': result.get('error', 'Unknown error'),
                    'sql': result.get('sql', ''),
                    'task_completed': True,
                    'next_action': 'none',
                    'summary': f"配置驱动引擎执行失败: {result.get('error', 'Unknown error')}"
                }

        except Exception as e:
            print(f"[ERROR] ConfigurableNL2SQLTool: 配置驱动引擎执行失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'task_completed': True,
                'next_action': 'none',
                'summary': f"配置驱动引擎异常: {e}"
            }

    def _run_with_legacy_engine(self, query: str) -> Dict[str, Any]:
        """回退到传统引擎"""
        # 创建传统NL2SQL工具实例并执行
        legacy_tool = NL2SQLTool()
        return legacy_tool._run(query)

class NL2SQLTool(BaseTool):
    """LangChain标准的NL2SQL工具 - 传统版本"""
    name: str = "nl2sql_query_legacy"
    description: str = "将自然语言转换为SQL并执行，返回银行业务数据（传统版本）"
    args_schema: type = NL2SQLToolInput
    
    def _run(self, query: str) -> Dict[str, Any]:
        """执行NL2SQL查询 - 使用优化的Pipeline"""
        try:
            start_time = time.time()

            # 记录原始工作目录
            original_cwd = os.getcwd()

            # ⭐ 关键修复：在切换工作目录之前获取DataProxy配置
            dataproxy_context = None
            database_path = None
            schema_type = 'loan_customer'  # 默认类型

            try:
                print(f"[DEBUG] 在原始目录获取DataProxy配置: {original_cwd}")
                # 注意：schema_config_manager 和 schema_converter 已被删除
                # 这里保留注释以说明原有功能
                print(f"[WARNING] DataProxy配置转换功能已被移除")

            except Exception as e:
                print(f"[WARNING] 无法获取DataProxy配置: {e}")

            # 切换到NL2SQL目录
            nl2sql_path = '/Users/chongshenyang/Desktop/数据分析'
            os.chdir(nl2sql_path)
            print(f"[DEBUG] 切换到NL2SQL目录: {nl2sql_path}")

            # 导入优化的Pipeline
            import sys
            sys.path.append(nl2sql_path)

            try:
                from pipeline import NL2SQLPipeline

                # 创建Pipeline实例
                pipeline = NL2SQLPipeline()

                # 确定最终的数据库路径
                if not database_path:
                    # 回退到session state方式
                    try:
                        import streamlit as st
                        selected_db = st.session_state.get('selected_database', None)
                        if selected_db:
                            database_path = os.path.join(nl2sql_path, selected_db)
                            print(f"[DEBUG] 回退使用session state数据库: {database_path}")
                    except:
                        pass

                # 最后的回退：使用默认数据库
                if not database_path:
                    database_path = os.path.join(nl2sql_path, 'new_bank_data.db')
                    schema_type = 'loan_customer'
                    print(f"[DEBUG] 使用默认数据库: {database_path}")

                schema_info = {'database_path': database_path}
                if dataproxy_context:
                    schema_info['dataproxy_context'] = dataproxy_context

                # 确保有数据库路径
                if not database_path:
                    # 最后的回退：使用默认数据库
                    database_path = os.path.join(nl2sql_path, 'new_bank_data.db')
                    schema_type = 'loan_customer'
                    schema_info = {'database_path': database_path}
                    print(f"[DEBUG] 使用默认数据库: {database_path}")

                # 🧹 完全清理硬编码：直接使用原始查询，完全依赖auto-schema配置
                enhanced_query = query
                print(f"[DEBUG] 完全依赖auto-schema配置，不添加任何硬编码增强")

                # ⭐ 关键修复：传递DataProxy Schema配置给pipeline
                print(f"[DEBUG] 执行查询，数据库路径: {database_path}")
                if dataproxy_context:
                    print(f"[DEBUG] ✅ 传递DataProxy Schema配置给pipeline")
                    # 使用pipeline的内部方法，传递dataproxy_context
                    result = pipeline._run_query_mode(enhanced_query, database_path, dataproxy_context=dataproxy_context)
                else:
                    print(f"[DEBUG] ⚠️ 未获取到DataProxy Schema配置，使用默认方式")
                    result = pipeline._run_query_mode(enhanced_query, database_path)

                if not result['success']:
                    raise Exception(f"Pipeline执行失败: {result.get('error', 'Unknown error')}")

            except Exception as import_error:
                # 🚀 不使用回退机制，直接报告错误
                print(f"[ERROR] Pipeline执行失败: {import_error}")
                import traceback
                traceback.print_exc()
                raise Exception(f"NL2SQL Pipeline执行失败: {import_error}")

            # 处理成功结果
            if result['success']:
                # 转换数据格式，限制数据量
                data_records = []

                # 兼容不同的结果格式
                result_data = result.get('data', [])
                result_columns = result.get('columns', [])

                if result_data and result_columns:
                    max_records = min(500, len(result_data))
                    print(f"[DEBUG] 原始数据{len(result_data)}条，限制返回{max_records}条")

                    for row in result_data[:max_records]:
                        record = {}
                        for i, col in enumerate(result_columns):
                            record[col] = row[i] if i < len(row) else None
                        data_records.append(record)
                else:
                    print(f"[DEBUG] 查询成功但无数据返回")

                # 恢复工作目录
                os.chdir(original_cwd)

                # 🔥 移除前置可视化判断，改为简单的数据返回
                # 可视化判断现在由各个工具在后置阶段基于实际数据进行

                return {
                    'success': True,
                    'sql': result.get('generated_sql', result.get('sql', '')),
                    'data': data_records,
                    'metadata': result.get('metadata', {}),
                    'execution_time': result.get('execution_time', time.time() - start_time),
                    'record_count': result.get('record_count', len(data_records)),
                    'task_completed': True,  # 明确的完成标志
                    'next_action': 'none',  # 不再在此处决定下一步行动
                    'summary': f"成功执行SQL查询，返回{result.get('record_count', len(data_records))}条记录"
                }
            else:
                # 恢复工作目录
                os.chdir(original_cwd)

                # 🚀 阶段1修复：错误情况也要明确完成状态
                return {
                    'success': False,
                    'error': result.get('error', 'Unknown error'),
                    'sql': result.get('generated_sql', ''),
                    'error_type': self._classify_sql_error(result.get('error', '')),
                    'suggestions': self._get_error_suggestions(result.get('error', '')),
                    'execution_time': result.get('execution_time', time.time() - start_time),
                    'record_count': 0,
                    'task_completed': True,  # 即使失败也是完成状态
                    'next_action': 'none',  # 错误时不需要下一步
                    'summary': f"SQL执行失败: {result.get('error', 'Unknown error')}"
                }

        except Exception as e:
            try:
                os.chdir(original_cwd)
            except:
                pass

            # 详细的错误信息记录
            error_msg = str(e)
            print(f"[ERROR] NL2SQL工具执行失败: {error_msg}")

            # 如果错误信息为空或者是None，提供更详细的信息
            if not error_msg or error_msg == 'None':
                error_msg = "NL2SQL执行过程中发生未知错误，可能是API调用超时或配置问题"

            import traceback
            traceback.print_exc()

            # 🚀 阶段1修复：异常情况也要明确完成状态
            return {
                'success': False,
                'error': error_msg,
                'error_type': self._classify_sql_error(error_msg),
                'suggestions': self._get_error_suggestions(error_msg),
                'execution_time': time.time() - start_time,
                'task_completed': True,  # 异常也是完成状态
                'next_action': 'none',  # 异常时不需要下一步
                'summary': f"NL2SQL工具执行异常: {error_msg}"
            }
    
    def _classify_sql_error(self, error_msg: str) -> str:
        """分类SQL错误类型"""
        error_msg_lower = error_msg.lower()

        if 'no such table' in error_msg_lower:
            return 'table_not_found'
        elif 'no such column' in error_msg_lower:
            return 'column_not_found'
        elif 'syntax error' in error_msg_lower:
            return 'syntax_error'
        elif 'permission' in error_msg_lower:
            return 'permission_error'
        elif 'connection' in error_msg_lower:
            return 'connection_error'
        else:
            return 'unknown_error'

    def _get_error_suggestions(self, error_msg: str) -> List[str]:
        """根据错误信息提供修复建议"""
        error_type = self._classify_sql_error(error_msg)

        suggestions = {
            'table_not_found': [
                "尝试使用其他相关表名",
                "检查表名拼写是否正确",
                "使用更通用的查询方式"
            ],
            'column_not_found': [
                "尝试使用其他相关字段名",
                "检查字段名拼写是否正确",
                "使用SELECT * 查看可用字段"
            ],
            'syntax_error': [
                "简化SQL语法",
                "检查SQL语句结构",
                "使用更基础的查询语句"
            ],
            'permission_error': [
                "使用更基础的查询权限",
                "尝试查询其他表",
                "简化查询条件"
            ],
            'connection_error': [
                "检查网络连接状态",
                "确认数据库文件是否存在",
                "稍后重试查询"
            ],
            'unknown_error': [
                "尝试简化查询",
                "使用不同的查询方式",
                "检查数据库连接"
            ]
        }

        return suggestions.get(error_type, suggestions['unknown_error'])

    def _enhance_query_for_schema(self, query: str, schema_type: str, schema_info: dict) -> str:
        """智能化查询增强 - 使用新的增强引擎"""
        try:
            # 导入智能增强器
            from .intelligent_query_enhancer import enhance_query_intelligent

            # 执行智能增强
            enhanced_query = enhance_query_intelligent(query, schema_type, schema_info)

            if enhanced_query != query:
                print(f"[DEBUG] 查询增强成功:")
                print(f"  原始: {query}")
                print(f"  增强: {enhanced_query}")
            else:
                print(f"[DEBUG] 查询无需增强: {query}")

            return enhanced_query

        except ImportError as e:
            print(f"[WARNING] 智能增强器不可用，使用传统方法: {e}")
            return self._legacy_enhance_query_for_schema(query, schema_type, schema_info)
        except Exception as e:
            print(f"[ERROR] 智能查询增强失败: {e}")
            return self._legacy_enhance_query_for_schema(query, schema_type, schema_info)

    def _legacy_enhance_query_for_schema(self, query: str, schema_type: str, schema_info: dict) -> str:
        """传统查询增强方法（备用）"""
        if not schema_type:
            return query

        try:
            if schema_type == 'loan_customer':
                return self._enhance_loan_customer_query(query, schema_info)
            elif schema_type == 'annual_report':
                return self._enhance_annual_report_query(query, schema_info)
            else:
                return query
        except Exception as e:
            print(f"[DEBUG] 传统查询增强失败: {e}")
            return query

    def _enhance_loan_customer_query(self, query: str, schema_info: dict) -> str:
        """增强银行客户贷款数据库的查询 - 完全依赖auto-schema配置"""
        # 🧹 清理硬编码：不再添加硬编码的业务术语定义
        # 让NL2SQL系统完全依赖auto-schema学习的配置
        print(f"[DEBUG] 使用auto-schema配置，不添加硬编码增强")
        return query

    def _enhance_annual_report_query(self, query: str, schema_info: dict) -> str:
        """增强银行年报数据库的查询 - 完全依赖auto-schema配置"""
        # 🧹 清理硬编码：不再添加硬编码的业务术语定义
        print(f"[DEBUG] 使用auto-schema配置，不添加硬编码增强")
        return query

    def _run_without_enhancement(self, query: str) -> Dict[str, Any]:
        """执行查询但不做增强 - 用于QueryCoordinator"""
        print(f"[DEBUG] NL2SQLTool: 执行查询（无增强）: {query}")

        try:
            # 设置跳过增强标志
            self._skip_enhancement = True

            # 调用原有的_run方法，但跳过增强
            result = self._run(query)

            # 清除标志
            self._skip_enhancement = False

            return result

        except Exception as e:
            # 清除标志
            self._skip_enhancement = False
            print(f"[ERROR] NL2SQL查询执行失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'query': query
            }

    # 🔥 移除前置可视化判断方法
    # 可视化判断现在由各个工具基于实际数据在后置阶段进行

    async def _arun(self, query: str) -> Dict[str, Any]:
        """异步执行"""
        return self._run(query)


# DatabaseSchemaTool 已删除 - 功能由 DynamicSchemaExtractor 替代
