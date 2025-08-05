#!/usr/bin/env python3
"""
NL2SQL Module - 重构后的自然语言到SQL转换系统

重构特性：
- 统一的SQL查询引擎
- 简化的工具接口
- 优化的配置管理
- 向后兼容支持

核心组件：
- SQLQueryEngine: 统一的SQL查询引擎
- NL2SQLProcessor: 重构后的NL2SQL处理器
- ConsolidatedNL2SQLTool: 向后兼容接口

使用示例：
    from core_modules.nl2sql import NL2SQLProcessor, ConsolidatedNL2SQLTool

    # 使用新的处理器（推荐）
    processor = NL2SQLProcessor()

    # 或使用向后兼容接口
    tool = ConsolidatedNL2SQLTool()
"""

# 导入重构后的核心组件
try:
    _imported_modules = {}

    # 导入新的重构后组件
    try:
        from .sql_query_engine import SQLQueryEngine, SQLQueryResult
        _imported_modules['SQLQueryEngine'] = SQLQueryEngine
        _imported_modules['SQLQueryResult'] = SQLQueryResult
    except ImportError as e:
        print(f"警告: sql_query_engine导入失败 - {e}")

    try:
        from .nl2sql_processor import NL2SQLProcessor, create_nl2sql_processor
        _imported_modules['NL2SQLProcessor'] = NL2SQLProcessor
        _imported_modules['create_nl2sql_processor'] = create_nl2sql_processor
    except ImportError as e:
        print(f"警告: nl2sql_processor导入失败 - {e}")

    # ConsolidatedNL2SQLTool已被CoreDataEngine替代
    print("ℹ️  NL2SQL: ConsolidatedNL2SQLTool已被CoreDataEngine替代")

    # 原有组件已被重构整合到新的统一引擎中
    print("ℹ️  原有的SimpleNL2SQLEngine和ConfigurableNL2SQLEngine已被SQLQueryEngine替代")

    # 重构信息
    print("ℹ️  NL2SQL: 模块已重构，推荐使用新组件:")
    print("   - NL2SQLProcessor: 新的统一处理器")
    print("   - SQLQueryEngine: 重构后的查询引擎")
    print("   - ConsolidatedNL2SQLTool: 向后兼容接口")

    try:
        from .models import (
            SQLResult, QueryContext, NL2SQLSchema, 
            BusinessContext, PromptTemplates
        )
        _imported_modules.update({
            'SQLResult': SQLResult,
            'QueryContext': QueryContext,
            'NL2SQLSchema': NL2SQLSchema,
            'BusinessContext': BusinessContext,
            'PromptTemplates': PromptTemplates
        })
    except ImportError as e:
        print(f"警告: models导入失败 - {e}")

    # 将成功导入的模块添加到当前命名空间
    globals().update(_imported_modules)

    # 动态生成__all__列表，只包含成功导入的模块
    __all__ = list(_imported_modules.keys())

    NL2SQL_MODULES_AVAILABLE = len(_imported_modules) > 0

    if NL2SQL_MODULES_AVAILABLE:
        print(f"✅ DataProxy NL2SQL: 成功导入 {len(_imported_modules)} 个模块: {', '.join(__all__)}")
    else:
        print("❌ DataProxy NL2SQL: 没有成功导入任何模块")

except Exception as e:
    print(f"警告: DataProxy NL2SQL 模块导入过程失败 - {e}")
    NL2SQL_MODULES_AVAILABLE = False
    __all__ = []

# 版本信息
__version__ = "1.0.0"
__author__ = "DataProxy Team"
__description__ = "DataProxy 原生 NL2SQL 系统 - 配置驱动的高性能引擎"

# 便捷函数
def create_engine(config=None):
    """创建 DataProxy NL2SQL 引擎实例"""
    if not NL2SQL_MODULES_AVAILABLE:
        raise ImportError("DataProxy NL2SQL 模块不可用")

    if config is None:
        from ..unified_config import get_unified_config
        config = get_unified_config()

    return SimpleNL2SQLEngine(config)

def get_available_templates():
    """获取可用的提示模板列表（简化版本不需要模板）"""
    return ["simple_banking_query"]
