#!/usr/bin/env python3
"""
DataProxy 配置管理模块 - 重构后的简化配置系统

提供简化的配置管理接口：
- DatabaseConfigManager: 数据库配置和连接管理
- ContextManager: 查询上下文管理
- ConfigurationRegistry: 统一配置注册表

设计原则：
- 单一职责：每个组件专注特定功能
- 简化接口：减少配置复杂性
- 统一管理：避免配置冲突
"""

# 简化后的配置组件
try:
    from .database_config_manager import DatabaseConfigManager, get_database_config

    # 向后兼容接口
    from .unified_config import UnifiedConfig, get_unified_config, reset_unified_config

    # 添加兼容性函数
    def get_configuration_registry():
        """兼容性函数 - 返回配置注册表对象"""
        class ConfigurationRegistry:
            def __init__(self):
                self.database_config = get_database_config()
                self.unified_config = get_unified_config()

            def get_nl2sql_config(self):
                """获取NL2SQL配置"""
                if hasattr(self.unified_config, 'nl2sql_config'):
                    return self.unified_config.nl2sql_config
                else:
                    # 返回默认配置
                    return {
                        'prompt_templates': {
                            'simple_query': '''你是一个专业的银行数据分析师，需要将自然语言查询转换为准确的SQL语句。

数据库结构：
{schema_info}

业务术语定义：
{business_terms}

字段映射：
{field_mappings}

用户查询：{query}

请根据以上信息生成准确的SQL语句。注意：
1. 只返回SQL语句，不要包含任何解释或注释
2. 使用正确的表名和字段名
3. 对于业务术语，使用对应的SQL条件
4. 确保SQL语法正确
5. 如果需要计算，使用适当的聚合函数

SQL语句：''',
                            'complex_query': '''你是一个专业的银行数据分析师，需要将复杂的自然语言查询转换为准确的SQL语句。

数据库结构：
{schema_info}

业务术语定义：
{business_terms}

字段映射：
{field_mappings}

用户查询：{query}

请根据以上信息生成准确的SQL语句。注意：
1. 只返回SQL语句，不要包含任何解释或注释
2. 使用正确的表名和字段名
3. 对于业务术语，使用对应的SQL条件和计算公式
4. 包含必要的JOIN和WHERE条件
5. 使用适当的聚合函数和GROUP BY
6. 确保SQL语法正确

SQL语句：'''
                        },
                        'query_modes': ['simple', 'complex', 'analytical'],
                        'constraints': [
                            '只返回SQL语句，不要包含解释',
                            '使用标准SQL语法',
                            '确保字段名正确',
                            '添加适当的WHERE条件',
                            '限制结果数量以提高性能'
                        ]
                    }

        return ConfigurationRegistry()

    def get_context_manager():
        """返回真正的统一配置管理器"""
        return get_unified_config()

    __all__ = [
        # 简化后组件
        'DatabaseConfigManager',
        'get_database_config',

        # 向后兼容
        'UnifiedConfig',
        'get_unified_config',
        'reset_unified_config',

        # 兼容性函数
        'get_configuration_registry',
        'get_context_manager'
    ]

    print("✅ 配置管理模块: 简化后组件加载成功")
    print("ℹ️  Config: ContextManager和ConfigurationRegistry已被CoreDataEngine替代")

except ImportError as e:
    print(f"⚠️ 配置管理模块: 部分组件加载失败 - {e}")

    # 回退到原有组件
    try:
        from .unified_config import UnifiedConfig, get_unified_config, reset_unified_config

        __all__ = [
            'UnifiedConfig', 'get_unified_config', 'reset_unified_config'
        ]
        print("✅ 配置管理模块: 使用原有组件")

    except ImportError as fallback_e:
        print(f"❌ 配置管理模块: 完全加载失败 - {fallback_e}")
        __all__ = []
