#!/usr/bin/env python3
"""
NL2SQL处理器 - 重构后的统一NL2SQL工具

替代原有的ConsolidatedNL2SQLTool，提供简化的接口
"""

import time
from typing import Dict, Any, Optional, Type
from pydantic import BaseModel, Field

try:
    from langchain.tools import BaseTool
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False

from .sql_query_engine import SQLQueryEngine, SQLQueryResult

try:
    from ..config import get_context_manager
except ImportError:
    # 兼容性处理
    def get_context_manager():
        from ..config import get_unified_config
        return get_unified_config()


class NL2SQLInput(BaseModel):
    """NL2SQL工具输入模型"""
    query: str = Field(description="自然语言查询")
    engine_type: str = Field(default="auto", description="引擎类型: auto|simple|advanced")


class NL2SQLProcessor(BaseTool if LANGCHAIN_AVAILABLE else object):
    """
    NL2SQL处理器 - 统一的自然语言到SQL转换工具
    
    功能：
    1. 自然语言查询理解
    2. SQL生成和执行
    3. 结果格式化和返回
    """
    
    name: str = "nl2sql_processor"
    description: str = """自然语言到SQL转换工具

功能：
- 将自然语言查询转换为SQL语句
- 执行SQL查询并返回结果
- 支持复杂的业务查询和数据分析

适用场景：
- 银行业务数据查询
- 客户信息检索
- 统计分析和报表生成
- 数据探索和分析

输入参数：
- query: 自然语言查询
- engine_type: 引擎类型（可选）
"""
    
    if LANGCHAIN_AVAILABLE:
        args_schema: Type[BaseModel] = NL2SQLInput
    
    def __init__(self, **kwargs):
        if LANGCHAIN_AVAILABLE:
            super().__init__(**kwargs)

        # 初始化组件（不作为Pydantic字段）
        object.__setattr__(self, 'sql_engine', SQLQueryEngine())
        object.__setattr__(self, 'context_manager', get_context_manager())

        print("[DEBUG] NL2SQLProcessor: 初始化完成")
    
    def _run(self, query: str, engine_type: str = "auto") -> Dict[str, Any]:
        """
        执行NL2SQL转换
        
        Args:
            query: 用户的自然语言查询
            engine_type: 引擎类型
            
        Returns:
            Dict[str, Any]: 查询结果
        """
        start_time = time.time()
        
        try:
            print(f"[DEBUG] NL2SQLProcessor: 处理查询: {query}")
            
            # 创建查询上下文
            context = self._create_query_context(query)
            
            if not context:
                return self._create_error_result(
                    "无法创建查询上下文",
                    start_time
                )
            
            # 使用SQL引擎生成和执行查询
            result = self.sql_engine.generate_sql(query, context.to_dict())
            
            # 转换为统一格式
            return self._convert_result(result, start_time)
            
        except Exception as e:
            print(f"[ERROR] NL2SQLProcessor: 处理失败: {e}")
            return self._create_error_result(str(e), start_time)
    
    def _create_query_context(self, query: str) -> Optional[Any]:
        """创建查询上下文"""
        try:
            return self.context_manager.create_query_context(query)
        except Exception as e:
            print(f"[ERROR] NL2SQLProcessor: 创建上下文失败: {e}")
            return None
    
    def _convert_result(self, result: SQLQueryResult, start_time: float) -> Dict[str, Any]:
        """转换查询结果为统一格式"""
        total_time = time.time() - start_time
        
        return {
            "success": result.success,
            "sql": result.sql,
            "data": result.data,
            "record_count": result.record_count,
            "execution_time": total_time,
            "engine_time": result.execution_time,
            "engine_used": result.engine_used,
            "tool": "NL2SQLProcessor",
            "error": result.error if not result.success else None
        }
    
    def _create_error_result(self, error_message: str, start_time: float) -> Dict[str, Any]:
        """创建错误结果"""
        return {
            "success": False,
            "sql": "",
            "data": [],
            "record_count": 0,
            "execution_time": time.time() - start_time,
            "engine_time": 0,
            "engine_used": "none",
            "tool": "NL2SQLProcessor",
            "error": error_message
        }
    
    def run(self, query: str, engine_type: str = "auto") -> Dict[str, Any]:
        """
        公共接口：执行NL2SQL转换
        
        Args:
            query: 自然语言查询
            engine_type: 引擎类型
            
        Returns:
            Dict[str, Any]: 查询结果
        """
        return self._run(query, engine_type)


def create_nl2sql_processor() -> NL2SQLProcessor:
    """创建NL2SQL处理器实例"""
    return NL2SQLProcessor()


# 向后兼容接口
class ConsolidatedNL2SQLTool(NL2SQLProcessor):
    """向后兼容的ConsolidatedNL2SQLTool"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        print("[DEBUG] ConsolidatedNL2SQLTool: 使用重构后的NL2SQLProcessor")


def create_consolidated_nl2sql_tool() -> ConsolidatedNL2SQLTool:
    """创建ConsolidatedNL2SQLTool实例（向后兼容）"""
    return ConsolidatedNL2SQLTool()
