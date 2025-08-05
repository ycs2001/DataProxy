#!/usr/bin/env python3
"""
DataProxy 核心模块包 - 简化后的统一架构

🚀 新的简化架构：
- core_engine: 统一的数据处理引擎 (推荐使用)
- agent: LangChain agents (向后兼容)
- analytics: 统计分析和业务洞察
- config: 配置管理
- utils: 工具函数
- visualization: 可视化工具
- nl2sql: 自然语言转SQL

🎯 推荐使用方式：
    from core_modules import CoreDataEngine, quick_query

    # 创建引擎
    engine = CoreDataEngine("path/to/database.db")

    # 执行查询
    result = engine.query("查询对公有效户数量")

    # 或者快速查询
    result = quick_query("path/to/database.db", "查询不良贷款余额")

📚 向后兼容：
    from core_modules.agent import create_data_query_processor
    from core_modules.config import get_unified_config
"""

# 🚀 导入简化后的核心引擎 (推荐使用)
try:
    from .core_engine import CoreDataEngine, create_core_engine, quick_query
    print("✅ 核心引擎加载成功: CoreDataEngine")
except ImportError as e:
    print(f"⚠️ 核心引擎加载失败: {e}")

# 导入重构后的核心组件，保持向后兼容性
try:
    _imported_modules = {}

    # 从agent模块导入（仅导入可用组件）
    try:
        from .agent import (
            SimplifiedDataProxyTool,
            create_simplified_dataproxy_tool,
            get_tool_registry
        )
        _imported_modules.update({
            'SimplifiedDataProxyTool': SimplifiedDataProxyTool,
            'create_simplified_dataproxy_tool': create_simplified_dataproxy_tool,
            'get_tool_registry': get_tool_registry
        })
        print("✅ Agent: 成功导入可用代理组件")
    except ImportError as e:
        print(f"ℹ️  Agent: 部分组件不可用 - {e}")

    # 从config模块导入（仅导入可用组件）
    try:
        from .config import (
            get_database_config,
            QueryContext,
            get_unified_config,
            reset_unified_config,
            UnifiedConfig
        )
        _imported_modules.update({
            'get_database_config': get_database_config,
            'QueryContext': QueryContext,
            'get_unified_config': get_unified_config,
            'reset_unified_config': reset_unified_config,
            'UnifiedConfig': UnifiedConfig
        })
        print("✅ Config: 成功导入可用配置组件")
    except ImportError as e:
        print(f"ℹ️  Config: 部分组件不可用 - {e}")

    # 从utils模块导入
    try:
        from .utils import (
            DynamicSchemaExtractor,
            extract_database_schema,
            determine_database_type,
            build_schema_info_for_llm,
            FileConverter,
            DatabaseExecutor
        )
        _imported_modules.update({
            'DynamicSchemaExtractor': DynamicSchemaExtractor,
            'extract_database_schema': extract_database_schema,
            'determine_database_type': determine_database_type,
            'build_schema_info_for_llm': build_schema_info_for_llm,
            'FileConverter': FileConverter,
            'DatabaseExecutor': DatabaseExecutor
        })
    except ImportError as e:
        print(f"警告: utils模块导入失败 - {e}")

    # 从data_processing模块导入（仅导入可用组件）
    try:
        from .data_processing import QueryAnalysisTool
        _imported_modules.update({
            'QueryAnalysisTool': QueryAnalysisTool
        })
        print("✅ Data Processing: 成功导入查询分析工具")
    except ImportError as e:
        print(f"警告: data_processing模块导入失败 - {e}")

    # 将成功导入的模块添加到当前命名空间
    globals().update(_imported_modules)

    # 生成__all__列表，包含核心引擎和其他模块
    __all__ = ['CoreDataEngine', 'create_core_engine', 'quick_query'] + list(_imported_modules.keys())

    CORE_MODULES_AVAILABLE = len(_imported_modules) > 0

    if CORE_MODULES_AVAILABLE:
        print(f"✅ 简化架构成功导入 {len(_imported_modules)} 个模块 + 核心引擎")
    else:
        print("⚠️ 只有核心引擎可用，其他模块导入失败")

except Exception as e:
    print(f"警告: 核心模块导入过程失败 - {e}")
    CORE_MODULES_AVAILABLE = False
    __all__ = []

# 版本信息
__version__ = "2.0.0-simplified"
__author__ = "DataProxy Team"
__description__ = "简化架构的统一数据分析引擎"
