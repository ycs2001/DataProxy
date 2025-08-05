#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DataProxy 数据导入模块

提供智能数据导入和转换功能
- IntelligentDataImporter: 基于规则的传统导入器
- LLMIntelligentDataImporter: 基于LLM的智能导入器 (推荐)
"""

# 导入纯LLM智能导入器（最新推荐）
try:
    from .intelligent_data_importer import (
        IntelligentDataImporter,
        create_intelligent_importer,
        quick_intelligent_import
    )
    PURE_LLM_IMPORTER_AVAILABLE = True
    print("✅ 纯LLM智能导入器可用")
except ImportError as e:
    PURE_LLM_IMPORTER_AVAILABLE = False
    print(f"⚠️ 纯LLM智能导入器不可用: {e}")

# 导入LLM智能导入器（向后兼容）
try:
    from .llm_intelligent_importer import (
        LLMIntelligentDataImporter,
        create_llm_intelligent_importer,
        quick_llm_import
    )
    LLM_IMPORTER_AVAILABLE = True
    print("✅ LLM智能导入器可用")
except ImportError as e:
    LLM_IMPORTER_AVAILABLE = False
    print(f"⚠️ LLM智能导入器不可用: {e}")

# 导出列表
__all__ = []

# 优先导出纯LLM导入器
if PURE_LLM_IMPORTER_AVAILABLE:
    __all__.extend([
        'IntelligentDataImporter',
        'create_intelligent_importer',
        'quick_intelligent_import'
    ])

# 向后兼容的LLM导入器
if LLM_IMPORTER_AVAILABLE:
    __all__.extend([
        'LLMIntelligentDataImporter',
        'create_llm_intelligent_importer',
        'quick_llm_import'
    ])

# 便捷别名（推荐使用纯LLM版本）
if PURE_LLM_IMPORTER_AVAILABLE:
    # 最新推荐接口 - 纯LLM驱动
    SmartDataImporter = IntelligentDataImporter
    create_smart_importer = create_intelligent_importer
    smart_import = quick_intelligent_import

    # 纯LLM专用别名
    PureLLMDataImporter = IntelligentDataImporter
    create_pure_llm_importer = create_intelligent_importer
    pure_llm_import = quick_intelligent_import

    __all__.extend([
        'SmartDataImporter',
        'create_smart_importer',
        'smart_import',
        'PureLLMDataImporter',
        'create_pure_llm_importer',
        'pure_llm_import'
    ])
elif LLM_IMPORTER_AVAILABLE:
    # 回退到LLM版本
    SmartDataImporter = LLMIntelligentDataImporter
    create_smart_importer = create_llm_intelligent_importer
    smart_import = quick_llm_import

    __all__.extend([
        'SmartDataImporter',
        'create_smart_importer',
        'smart_import'
    ])

print(f"📦 数据导入模块加载完成，可用组件: {__all__}")
