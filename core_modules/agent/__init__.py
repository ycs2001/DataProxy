#!/usr/bin/env python3
"""
Agent Module - 重构后的代理和工具编排系统

重构特性：
- 统一的数据查询处理器
- 简化的工具注册表
- 向后兼容接口
"""

# 导入重构后的核心组件
try:
    from .data_query_processor import DataQueryProcessor, create_data_query_processor
    print("✅ Agent: 成功导入重构后的数据查询处理器")
except ImportError as e:
    print(f"⚠️ Agent: 数据查询处理器导入失败 - {e}")

# 注意：SimplifiedDataProxyTool已被CoreDataEngine替代
print("ℹ️  Agent: SimplifiedDataProxyTool已被CoreDataEngine替代，请使用新的统一接口")

# 保留原有组件（如果需要）
try:
    from .streamlit_agent import LangChainDataAnalysisAgent, StreamlitCallbackHandler, ToolCallResult
    from .tool_integrations import NL2SQLTool
    print("✅ Agent: 成功导入原有代理组件")
except ImportError as e:
    print(f"⚠️ Agent: 原有代理组件导入失败 - {e}")

# 工具注册表已简化，使用CoreDataEngine替代
print("ℹ️  Agent: 工具注册表已简化，请直接使用CoreDataEngine")

# 导出组件
try:
    __all__ = [
        # 重构后的核心组件
        'DataQueryProcessor',
        'create_data_query_processor',

        # 注意：向后兼容接口已被CoreDataEngine替代

        # 原有代理组件
        'LangChainDataAnalysisAgent',
        'StreamlitCallbackHandler',
        'ToolCallResult',
        'NL2SQLTool',

        # 工具注册表已简化
    ]
except NameError:
    __all__ = []
