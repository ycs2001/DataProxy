#!/usr/bin/env python3
"""
数据查询处理器 - 重构后的统一数据查询工具

替代原有的SimplifiedDataProxyTool，提供更清晰的功能定义
"""

import time
from typing import Dict, Any, Type, List, Optional
from pydantic import BaseModel, Field

try:
    from langchain.tools import BaseTool
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False

from ..config import get_database_config, get_context_manager
from ..nl2sql import NL2SQLProcessor


class DataQueryInput(BaseModel):
    """数据查询输入模型"""
    query: str = Field(description="业务查询，支持简单查询和复杂分析")
    analysis_mode: str = Field(
        default="auto", 
        description="分析模式: auto(自动) | simple(简单) | detailed(详细)"
    )
    enable_statistics: bool = Field(
        default=True,
        description="是否启用统计分析"
    )


class DataQueryProcessor(BaseTool if LANGCHAIN_AVAILABLE else object):
    """
    数据查询处理器 - 统一的数据查询和分析工具
    
    功能：
    1. 自然语言查询处理
    2. 数据检索和分析
    3. 统计信息生成
    4. 结果格式化
    
    设计原则：
    - 单一职责：专注数据查询处理
    - 简化接口：减少复杂性
    - 统一输出：标准化结果格式
    """
    
    name: str = "data_query_processor"
    description: str = """数据查询处理器 - 统一的数据分析工具

核心能力：
1. 智能查询处理：自动将自然语言转换为SQL查询
2. 数据检索分析：执行查询并返回结构化数据
3. 统计信息生成：自动计算数据统计摘要
4. 结果标准化：提供统一的输出格式

分析模式：
- auto（默认）：根据查询复杂度自动选择处理方式
- simple：快速查询，适合简单数据检索
- detailed：详细分析，包含统计和洞察

适用场景：
- 银行业务数据查询（客户信息、存款、贷款等）
- 统计分析需求（各支行数据对比、趋势分析等）
- 快速数据检索（特定条件的数据查找）
- 业务报表生成（汇总数据和关键指标）

输入参数：
- query: 自然语言查询
- analysis_mode: 分析模式（可选）
- enable_statistics: 是否启用统计分析（可选）
"""
    
    if LANGCHAIN_AVAILABLE:
        args_schema: Type[BaseModel] = DataQueryInput

    def __init__(self, **kwargs):
        if LANGCHAIN_AVAILABLE:
            super().__init__(**kwargs)

        # 初始化组件（不作为Pydantic字段）
        object.__setattr__(self, 'database_config', get_database_config())
        object.__setattr__(self, 'context_manager', get_context_manager())
        object.__setattr__(self, 'nl2sql_processor', NL2SQLProcessor())

        print("[DEBUG] DataQueryProcessor: 初始化完成")

    def _run(self, query: str, analysis_mode: str = "auto", enable_statistics: bool = True) -> Dict[str, Any]:
        """
        执行数据查询处理
        
        Args:
            query: 用户的业务查询
            analysis_mode: 分析模式
            enable_statistics: 是否启用统计分析
            
        Returns:
            Dict[str, Any]: 查询结果
        """
        start_time = time.time()
        
        try:
            print(f"[DEBUG] DataQueryProcessor: 处理查询: {query}")
            print(f"[DEBUG] 分析模式: {analysis_mode}")
            
            # 检查数据库配置
            if not self.database_config.is_valid():
                return self._create_error_result(
                    "数据库配置无效，请先切换到有效的数据库",
                    start_time
                )
            
            # 使用NL2SQL处理器执行查询
            nl2sql_result = self.nl2sql_processor.run(query)
            
            if not nl2sql_result.get('success', False):
                return self._create_error_result(
                    f"查询执行失败: {nl2sql_result.get('error', '未知错误')}",
                    start_time
                )
            
            # 构建结果
            result = {
                "success": True,
                "query": query,
                "analysis_mode": analysis_mode,
                "sql": nl2sql_result.get('sql', ''),
                "data_tables": [
                    {
                        "name": "查询结果",
                        "data": nl2sql_result.get('data', []),
                        "record_count": nl2sql_result.get('record_count', 0)
                    }
                ],
                "execution_time": time.time() - start_time,
                "engine_time": nl2sql_result.get('engine_time', 0),
                "tool": "DataQueryProcessor"
            }
            
            # 添加统计信息（如果启用）
            if enable_statistics and nl2sql_result.get('data'):
                result["statistics"] = self._generate_statistics(nl2sql_result['data'])
            
            # 添加数据库信息
            db_info = self.database_config.get_database_info()
            if db_info:
                result["database_info"] = {
                    "name": db_info.name,
                    "path": db_info.path,
                    "type": db_info.type
                }
            
            print(f"[DEBUG] DataQueryProcessor: 查询成功，返回 {result['data_tables'][0]['record_count']} 条记录")
            return result
            
        except Exception as e:
            print(f"[ERROR] DataQueryProcessor: 处理失败: {e}")
            return self._create_error_result(str(e), start_time)
    
    def _generate_statistics(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """生成数据统计信息"""
        if not data:
            return {}
        
        try:
            stats = {
                "total_records": len(data),
                "fields_count": len(data[0]) if data else 0,
                "field_types": {}
            }
            
            # 分析字段类型
            if data:
                for field_name, field_value in data[0].items():
                    if isinstance(field_value, (int, float)):
                        stats["field_types"][field_name] = "numeric"
                    elif isinstance(field_value, str):
                        stats["field_types"][field_name] = "text"
                    else:
                        stats["field_types"][field_name] = "other"
            
            # 数值字段统计
            numeric_fields = [k for k, v in stats["field_types"].items() if v == "numeric"]
            if numeric_fields:
                stats["numeric_summary"] = {}
                for field in numeric_fields:
                    values = [row.get(field, 0) for row in data if isinstance(row.get(field), (int, float))]
                    if values:
                        stats["numeric_summary"][field] = {
                            "count": len(values),
                            "sum": sum(values),
                            "avg": sum(values) / len(values),
                            "min": min(values),
                            "max": max(values)
                        }
            
            return stats
            
        except Exception as e:
            print(f"[WARNING] DataQueryProcessor: 统计生成失败: {e}")
            return {"error": "统计生成失败"}
    
    def _create_error_result(self, error_message: str, start_time: float) -> Dict[str, Any]:
        """创建错误结果"""
        return {
            "success": False,
            "query": "",
            "analysis_mode": "error",
            "sql": "",
            "data_tables": [],
            "execution_time": time.time() - start_time,
            "engine_time": 0,
            "tool": "DataQueryProcessor",
            "error": error_message
        }
    
    def run(self, query: str, analysis_mode: str = "auto", enable_statistics: bool = True) -> Dict[str, Any]:
        """
        公共接口：执行数据查询处理
        
        Args:
            query: 自然语言查询
            analysis_mode: 分析模式
            enable_statistics: 是否启用统计分析
            
        Returns:
            Dict[str, Any]: 查询结果
        """
        return self._run(query, analysis_mode, enable_statistics)


def create_data_query_processor() -> DataQueryProcessor:
    """创建数据查询处理器实例"""
    return DataQueryProcessor()


# 向后兼容接口
class SimplifiedDataProxyTool(DataQueryProcessor):
    """向后兼容的SimplifiedDataProxyTool"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        print("[DEBUG] SimplifiedDataProxyTool: 使用重构后的DataQueryProcessor")


def create_simplified_dataproxy_tool() -> SimplifiedDataProxyTool:
    """创建SimplifiedDataProxyTool实例（向后兼容）"""
    return SimplifiedDataProxyTool()
