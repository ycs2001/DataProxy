#!/usr/bin/env python3
"""
统一配置管理模块
整合所有分散的配置系统，提供一致的配置接口
"""

import os
import json
from dataclasses import dataclass
from typing import Dict, Any, List, Optional
# 定义本地的数据类
from dataclasses import dataclass

@dataclass
class BusinessTerm:
    """业务术语定义"""
    name: str
    definition: str
    data_representation: str
    sql_conditions: str
    examples: List[str]

@dataclass
class QueryScope:
    """查询范围规则"""
    query_pattern: str
    scope_type: str  # 'all', 'filtered', 'specific'
    filter_conditions: str
    description: str


@dataclass
class QueryContext:
    """统一的查询上下文 - 包含查询执行所需的所有信息"""
    user_query: str
    database_path: str
    database_type: str
    business_terms: Dict[str, BusinessTerm]
    schema_info: Dict[str, Any]
    query_scope_rules: List[QueryScope]
    table_relationships: Dict[str, Any] = None

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式，用于传递给SQL引擎"""
        # 转换业务术语为字典格式
        business_terms_dict = {}
        for term_name, term in self.business_terms.items():
            if hasattr(term, 'definition'):
                business_terms_dict[term_name] = {
                    'name': term.name,
                    'definition': term.definition,
                    'sql_conditions': term.sql_conditions
                }
            else:
                business_terms_dict[term_name] = term

        # 转换schema信息为简化格式
        tables = []
        table_details = {}
        for table_name, table_info in self.schema_info.items():
            if table_name not in ['description', 'database_type', 'total_tables']:
                tables.append(table_name)
                if isinstance(table_info, dict):
                    columns = table_info.get('columns', [])
                    if isinstance(columns, list) and columns:
                        if isinstance(columns[0], dict):
                            # 如果是字典格式，提取列名
                            column_names = [col.get('name', str(col)) for col in columns]
                        else:
                            # 如果是字符串格式，直接使用
                            column_names = [str(col) for col in columns]
                    else:
                        column_names = []

                    table_details[table_name] = {
                        'columns': column_names,
                        'row_count': table_info.get('row_count', 0)
                    }

        return {
            'user_query': self.user_query,
            'database_path': self.database_path,
            'database_type': self.database_type,
            'business_terms': business_terms_dict,
            'tables': tables,
            'table_details': table_details,
            'schema_info': self.schema_info,
            'query_scope_rules': [
                {
                    'query_pattern': rule.query_pattern,
                    'scope_type': rule.scope_type,
                    'filter_conditions': rule.filter_conditions,
                    'description': rule.description
                } for rule in self.query_scope_rules
            ],
            'table_relationships': self.table_relationships or {}
        }

    def to_full_prompt(self) -> str:
        """生成包含所有上下文的完整提示词"""
        # 🚀 修复：只包含查询中明确提到的业务术语
        terms_text = ""
        for term_name, term in self.business_terms.items():
            # 检查查询中是否明确提到了这个术语
            if (term_name in self.user_query or
                term.name in self.user_query or
                any(keyword in self.user_query for keyword in term.name.split())):
                terms_text += f"- {term.name}：{term.definition}\n"
                terms_text += f"  数据表示：{term.data_representation}\n"
        
        # 格式化查询范围规则
        rules_text = ""
        for rule in self.query_scope_rules:
            rules_text += f"- {rule.description}\n"
            rules_text += f"  筛选条件：{rule.filter_conditions}\n"

        # 构建表关系信息
        schema_summary = ""
        if self.table_relationships:
            schema_summary = "【重要：表关系和 JOIN 条件】\n"
            schema_summary += "生成 SQL 时必须严格按照以下 JOIN 条件：\n"
            for rel_name, rel_info in self.table_relationships.items():
                from_table = rel_info.get('from_table', '')
                to_table = rel_info.get('to_table', '')
                from_field = rel_info.get('from_field', '')
                to_field = rel_info.get('to_field', '')
                schema_summary += f"- {from_table}.{from_field} = {to_table}.{to_field}\n"

            schema_summary += "\n⚠️ 注意：字段名包含中文，请使用准确的字段名：\n"
            schema_summary += "- CORP_LOAN_CONTRACT_INFO 表的客户字段是：客户编号\n"
            schema_summary += "- CORP_LOAN_CONTRACT_INFO 表的合同字段是：合同编号\n"
            schema_summary += "- CONT_RACTCLASSIFY 表的合同字段是：CONTRACT_NO\n"

        # 添加通用数据库指导
        general_guidance = f"""
【重要查询指导】
用户查询：{self.user_query}

查询理解指导：

【用户意图分析思路】
请仔细分析用户查询的真实意图：
- 用户说"分析各个银行存款情况"时，思考：他们想了解的是整体存款状况还是特定客户群体？
- 用户说"统计客户数量"时，思考：是要统计所有客户还是某个特定分类的客户？
- 当用户使用通用词汇（如"分析"、"统计"、"查询"）时，优先考虑提供全面的数据视角

【业务术语识别策略】
观察用户查询中的关键信号：
- 明确的业务术语（如"对公有效户"、"不良贷款"）→ 应用相应的业务定义
- 通用的描述性词汇（如"存款"、"客户"、"贷款"）→ 考虑提供完整数据集
- 模糊的表达（如"主要客户"、"重要指标"）→ 可以询问用户具体需求

【数据范围判断原则】
在构建SQL时思考：
- 用户的查询目标是什么？是要看全貌还是特定切片？
- 如果用户没有明确限定条件，是否应该提供更全面的数据？
- 添加过滤条件是否会遗漏用户真正关心的信息？

【银行业务时间概念理解】
理解银行业务中的时间语义：
- "截至某日期"通常指业务统计时点，数据本身已体现该时点状态
- 关注业务状态字段（如合同状态、分类结果）比时间字段更重要
- 特殊编码值（如"0001-01-01"）往往有业务含义，需要正确解读
- 思考：用户关心的是业务状态还是时间筛选？

【建议的思考流程】
1. 解析用户查询的核心意图
2. 识别明确的业务术语和隐含需求
3. 判断是否需要应用特定的业务规则
4. 构建能够回答用户真实问题的SQL查询
5. 如有疑问，优先选择提供更全面的数据视角
"""

        return f"""
【业务背景知识】

数据库信息：{self.schema_info.get('description', '银行业务数据库')}
数据库路径：{self.database_path}
数据库类型：{self.database_type}

{schema_summary}

{general_guidance}

业务术语定义（仅在查询明确提到时应用）：
{terms_text}

查询范围规则：
{rules_text}

用户查询：{self.user_query}
"""

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式，用于传递给NL2SQL系统"""
        return {
            'user_query': self.user_query,
            'database_path': self.database_path,
            'database_type': self.database_type,
            'business_context': self.to_full_prompt(),
            'schema_info': self.schema_info
        }


class UnifiedConfig:
    """统一的配置管理器 - 整合所有配置系统，消除配置冲突"""

    def __init__(self):
        self.database_path: Optional[str] = None
        self.database_type: Optional[str] = None
        self.business_terms: Dict[str, BusinessTerm] = {}
        self.schema_info: Dict[str, Any] = {}
        self.query_scope_rules: List[QueryScope] = []
        self.field_mappings: Dict[str, Any] = {}  # 统一字段映射
        self.table_relationships: Dict[str, Any] = {}  # 表关系

        # 🔥 新增：NL2SQL配置整合
        self.nl2sql_config: Dict[str, Any] = {}  # NL2SQL引擎配置
        self.prompt_templates: Dict[str, str] = {}  # 提示词模板
        self.query_patterns: List[Dict[str, Any]] = []  # 查询模式
        self.sql_constraints: Dict[str, Any] = {}  # SQL约束规则

        # 初始化配置
        self._load_all_configs()

        # 验证配置一致性
        self._validate_config_consistency()
    
    def _load_all_configs(self):
        """加载所有配置信息 - 统一管理，消除冲突"""
        print("[DEBUG] UnifiedConfig: 开始加载所有配置...")

        # 1. 加载数据库路径配置（不自动选择数据库）
        self._load_database_config()

        # 2. 加载业务知识配置（可以预加载）
        self._load_business_knowledge()

        # 3. 🔥 新增：加载NL2SQL配置（统一管理）
        self._load_nl2sql_config()

        # 4. 只有在有数据库路径时才加载Schema相关配置
        if self.database_path:
            print("[DEBUG] UnifiedConfig: 有数据库路径，加载Schema相关配置")
            self._load_schema_info()
            self._load_field_mappings()
            self._load_table_relationships()
        else:
            print("[DEBUG] UnifiedConfig: 无数据库路径，跳过Schema相关配置加载")
            self.schema_info = {}
            self.field_mappings = {}
            self.table_relationships = {}

        print(f"[DEBUG] UnifiedConfig: 配置加载完成")
        print(f"  - 数据库路径: {self.database_path}")
        print(f"  - 数据库类型: {self.database_type}")
        print(f"  - 业务术语数量: {len(self.business_terms)}")
        print(f"  - 查询规则数量: {len(self.query_scope_rules)}")
        print(f"  - 字段映射数量: {len(self.field_mappings)}")
        print(f"  - 表关系数量: {len(self.table_relationships)}")
    
    def _load_database_config(self):
        """加载数据库配置 - 自动发现并切换到可用数据库"""
        try:
            print("[DEBUG] UnifiedConfig: 开始自动发现和加载数据库...")

            # 首先尝试从智能上下文管理器获取
            try:
                from .intelligent_context_manager import IntelligentContextManager
                context_manager = IntelligentContextManager()
                all_contexts = context_manager.get_available_databases()

                available_count = len(all_contexts)
                print(f"[INFO] UnifiedConfig: 发现 {available_count} 个可用数据库配置")

                # 如果有可用的数据库配置，自动选择第一个
                if all_contexts:
                    first_db = list(all_contexts.keys())[0]
                    print(f"[INFO] UnifiedConfig: 自动选择数据库: {first_db}")
                    if self.switch_database(first_db):
                        return

            except ImportError:
                print("[DEBUG] UnifiedConfig: 智能上下文管理器不可用，使用自动发现")

            # 自动发现数据库文件
            discovered_db = self._discover_database()
            if discovered_db:
                print(f"[INFO] UnifiedConfig: 发现数据库文件: {discovered_db}")
                # 自动切换到发现的数据库
                if self._auto_switch_database(discovered_db):
                    print(f"[INFO] UnifiedConfig: 自动切换数据库成功: {discovered_db}")
                    return

            print("[WARNING] UnifiedConfig: 未找到可用数据库，使用空配置")
            self.database_path = None
            self.database_type = None

        except Exception as e:
            print(f"[WARNING] UnifiedConfig: 检查数据库配置失败: {e}")
            self.database_path = None
            self.database_type = None

    def _discover_database(self) -> Optional[str]:
        """自动发现可用的数据库文件"""
        import glob

        # 搜索数据库目录
        search_paths = [
            './databases/**/*.db',
            './databases/*.db',
            './data/**/*.db',
            './data/*.db'
        ]

        for pattern in search_paths:
            db_files = glob.glob(pattern, recursive=True)
            if db_files:
                # 返回第一个找到的数据库文件
                return db_files[0]

        return None

    def _auto_switch_database(self, database_path: str) -> bool:
        """自动切换到指定数据库并配置字段映射"""
        try:
            # 检查数据库是否存在
            if not os.path.exists(database_path):
                print(f"[ERROR] 数据库文件不存在: {database_path}")
                return False

            # 设置数据库路径和类型
            self.database_path = database_path
            self.database_type = 'sqlite'  # 假设是SQLite数据库

            print(f"[INFO] UnifiedConfig: 自动切换数据库到: {database_path}")

            # 加载数据库相关配置
            self._determine_database_type()
            self._load_business_knowledge()
            self._load_schema_info()
            self._load_field_mappings()
            self._load_table_relationships()

            # 自动配置缺失的字段映射
            self._auto_configure_field_mappings()

            print(f"[INFO] ✅ UnifiedConfig自动数据库配置完成!")
            print(f"  - 数据库路径: {self.database_path}")
            print(f"  - 数据库类型: {self.database_type}")
            print(f"  - Schema表数量: {len([k for k in self.schema_info.keys() if k not in ['description', 'database_type', 'total_tables']])}")
            print(f"  - 字段映射数量: {len(self.field_mappings)}")

            return True

        except Exception as e:
            print(f"[ERROR] 自动切换数据库失败: {e}")
            return False

    def switch_database(self, database_path: str) -> bool:
        """
        切换数据库 - 使用智能上下文管理器

        Args:
            database_path: 新的数据库路径

        Returns:
            bool: 切换是否成功
        """
        try:
            print(f"[INFO] UnifiedConfig: 开始切换数据库到: {database_path}")

            # 检查数据库是否存在
            if not os.path.exists(database_path):
                print(f"[ERROR] 数据库文件不存在: {database_path}")
                return False

            # 使用智能上下文管理器进行切换
            from .intelligent_context_manager import get_context_manager

            context_manager = get_context_manager()
            success = context_manager.switch_context(database_path)

            if success:
                # 更新UnifiedConfig的状态
                old_database_path = self.database_path
                self.database_path = database_path

                # 重新加载所有配置（现在使用动态上下文）
                self._determine_database_type()
                self._load_business_knowledge()  # 添加业务术语加载
                self._load_schema_info()
                self._load_field_mappings()
                self._load_table_relationships()

                print(f"[INFO] ✅ UnifiedConfig数据库切换成功!")
                print(f"  - 原数据库: {old_database_path}")
                print(f"  - 新数据库: {self.database_path}")
                print(f"  - 数据库类型: {self.database_type}")
                print(f"  - Schema表数量: {len([k for k in self.schema_info.keys() if k not in ['description', 'database_type', 'total_tables']])}")
                print(f"  - 字段映射数量: {len(self.field_mappings)}")
                print(f"  - 表关系数量: {len(self.table_relationships)}")

                return True
            else:
                print(f"[ERROR] 智能上下文管理器切换失败")
                return False

        except Exception as e:
            print(f"[ERROR] UnifiedConfig: 数据库切换失败: {e}")
            return False

    def get_available_databases(self) -> Dict[str, Dict[str, Any]]:
        """
        获取所有可用的数据库列表 - 使用智能上下文管理器

        Returns:
            Dict[str, Dict[str, Any]]: 数据库路径 -> 数据库信息
        """
        try:
            from .intelligent_context_manager import get_context_manager

            context_manager = get_context_manager()
            all_contexts = context_manager.get_available_databases()

            available_databases = {}
            for context_info in all_contexts:
                db_path = context_info['database_path']

                # 🔥 修复：检查当前项目目录中是否有同名数据库文件
                current_project_db_path = None
                if not os.path.exists(db_path):
                    # 尝试在当前项目目录中查找同名数据库
                    db_filename = os.path.basename(db_path)
                    current_project_db_path = os.path.join(os.getcwd(), db_filename)

                    if os.path.exists(current_project_db_path):
                        print(f"[INFO] 找到当前项目中的数据库文件: {current_project_db_path}")
                        db_path = current_project_db_path
                    else:
                        print(f"[WARNING] 数据库文件不存在: {context_info['database_path']}")
                        print(f"[WARNING] 当前项目中也未找到: {current_project_db_path}")
                        # 仍然添加到列表中，但标记为不可用
                        available_databases[context_info['database_path']] = {
                            'name': context_info['database_name'],
                            'business_type': context_info['database_type'],
                            'description': context_info.get('description', ''),
                            'generated_at': context_info['generated_at'],
                            'table_count': 0,
                            'total_tables': 0,
                            'is_current': False,
                            'file_exists': False,
                            'status': 'file_not_found'
                        }
                        continue

                # 加载完整上下文以获取表数量
                try:
                    from .dynamic_context_generator import ContextFileManager
                    file_manager = ContextFileManager()
                    context = file_manager.load_context(db_path)
                    table_count = len(context.tables) if context else 0
                except:
                    table_count = 0

                available_databases[db_path] = {
                    'name': context_info['database_name'],
                    'business_type': context_info['database_type'],
                    'description': context_info.get('description', ''),
                    'generated_at': context_info['generated_at'],
                    'table_count': table_count,
                    'total_tables': table_count,  # 兼容性字段
                    'is_current': db_path == self.database_path,
                    'file_exists': True,
                    'status': 'available'
                }

            return available_databases

        except Exception as e:
            print(f"[ERROR] UnifiedConfig: 获取可用数据库列表失败: {e}")
            return {}
    
    def _determine_database_type(self):
        """确定数据库类型"""
        if not self.database_path:
            return
            
        try:
            from .dynamic_schema_extractor import determine_database_type
            self.database_type = determine_database_type(self.database_path)
            print(f"[DEBUG] UnifiedConfig: 确定数据库类型: {self.database_type}")
        except Exception as e:
            print(f"[WARNING] UnifiedConfig: 确定数据库类型失败: {e}")
            self.database_type = 'unknown'
    
    def _load_business_knowledge(self):
        """加载业务知识配置 - 优先从数据库特定上下文加载"""
        try:
            # 如果有数据库路径，优先从数据库特定上下文加载业务术语
            if self.database_path:
                print(f"[DEBUG] UnifiedConfig: 从数据库特定上下文加载业务知识")

                # 尝试从智能上下文管理器加载
                try:
                    from .intelligent_context_manager import get_context_manager
                    context_manager = get_context_manager()
                    db_context = context_manager.get_or_create_context(self.database_path)

                    if db_context:
                        self._load_from_context_manager(db_context)
                        return

                except ImportError:
                    print(f"[DEBUG] UnifiedConfig: 智能上下文管理器不可用，从配置文件加载")

                # 从配置文件直接加载
                self._load_from_config_files()


            else:
                print(f"[DEBUG] UnifiedConfig: 无数据库路径，使用空业务知识配置")
                self.business_terms = {}
                self.query_scope_rules = []

            print(f"[DEBUG] UnifiedConfig: 业务知识加载成功")
        except Exception as e:
            print(f"[WARNING] UnifiedConfig: 加载业务知识失败: {e}")
            import traceback
            traceback.print_exc()
            # 确保有默认值
            self.business_terms = {}
            self.query_scope_rules = []

    def _load_from_context_manager(self, db_context):
        """从上下文管理器加载业务知识"""
        # 将DatabaseContext对象转换为字典格式
        db_context_data = None
        if db_context:
            db_context_data = {
                'database_specific_business_terms': getattr(db_context, 'database_specific_business_terms', None),
                'database_specific_field_mappings': getattr(db_context, 'database_specific_field_mappings', None),
                'database_specific_query_scopes': getattr(db_context, 'database_specific_query_scopes', None)
            }

        if db_context_data and db_context_data.get('database_specific_business_terms'):
            # 从数据库特定上下文加载业务术语
            db_specific_terms = db_context_data['database_specific_business_terms']
            self.business_terms = {}

            # 转换为标准格式
            for term_name, term_data in db_specific_terms.items():
                self.business_terms[term_name] = BusinessTerm(
                    name=term_name,
                    definition=term_data.get('definition', ''),
                    data_representation=term_data.get('category', '数据库特定'),
                    sql_conditions=term_data.get('sql_conditions', ''),
                    examples=[]
                )

            print(f"[DEBUG] UnifiedConfig: 从数据库特定上下文加载业务术语 {len(self.business_terms)} 个")

            # 加载数据库特定的查询范围
            if db_context_data.get('database_specific_query_scopes'):
                self.query_scope_rules = []
                for rule in db_context_data['database_specific_query_scopes']:
                    if isinstance(rule, dict):
                        self.query_scope_rules.append(QueryScope(
                            query_pattern=rule.get('table_name', ''),
                            scope_type=rule.get('rule_type', ''),
                            filter_conditions=rule.get('condition', ''),
                            description=rule.get('description', '')
                        ))
                print(f"[DEBUG] UnifiedConfig: 从数据库特定上下文加载查询范围 {len(self.query_scope_rules)} 个")
            else:
                self.query_scope_rules = []

        elif db_context and hasattr(db_context, 'business_terms') and db_context.business_terms:
            # 回退到原始业务术语
            print(f"[DEBUG] UnifiedConfig: 使用原始业务术语作为回退")
            self.business_terms = {}

            # 转换原始业务术语格式
            for term_name, term_data in db_context.business_terms.items():
                if isinstance(term_data, dict):
                    self.business_terms[term_name] = BusinessTerm(
                        name=term_name,
                        definition=term_data.get('definition', ''),
                        data_representation='银行业务',
                        sql_conditions=term_data.get('calculation', ''),
                        examples=[]
                    )
                else:
                    self.business_terms[term_name] = BusinessTerm(
                        name=term_name,
                        definition=str(term_data),
                        data_representation='银行业务',
                        sql_conditions='',
                        examples=[]
                    )

            print(f"[DEBUG] UnifiedConfig: 从原始业务术语加载 {len(self.business_terms)} 个")
            self.query_scope_rules = []

        else:
            print(f"[DEBUG] UnifiedConfig: 数据库上下文无业务术语，使用空配置")
            self.business_terms = {}
            self.query_scope_rules = []

    def _load_from_config_files(self):
        """从配置文件直接加载业务知识"""
        import json
        import glob

        # 根据数据库路径确定配置文件
        db_name = os.path.splitext(os.path.basename(self.database_path))[0]
        config_pattern = f"configs/database_contexts/{db_name}_context.json"

        config_files = glob.glob(config_pattern)
        if not config_files:
            print(f"[WARNING] UnifiedConfig: 未找到配置文件: {config_pattern}")
            self.business_terms = {}
            self.query_scope_rules = []
            return

        config_file = config_files[0]
        print(f"[INFO] UnifiedConfig: 从配置文件加载: {config_file}")

        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config_data = json.load(f)

            # 加载业务术语
            self.business_terms = {}
            business_terms = config_data.get('business_terms', {})
            for term, definition in business_terms.items():
                self.business_terms[term] = BusinessTerm(
                    name=term,
                    definition=definition,
                    data_representation="银行业务",
                    sql_conditions=definition if any(keyword in definition for keyword in ['SELECT', 'WHERE', 'CASE', '=']) else "",
                    examples=[]
                )

            print(f"[INFO] UnifiedConfig: 从配置文件加载业务术语 {len(self.business_terms)} 个")

            # 加载查询范围规则
            self.query_scope_rules = []
            query_rules = config_data.get('query_scope_rules', [])
            for rule in query_rules:
                if isinstance(rule, dict):
                    self.query_scope_rules.append(QueryScope(
                        query_pattern=rule.get('table_name', ''),
                        scope_type=rule.get('rule_type', ''),
                        filter_conditions=rule.get('condition', ''),
                        description=rule.get('description', '')
                    ))

            print(f"[INFO] UnifiedConfig: 从配置文件加载查询规则 {len(self.query_scope_rules)} 个")

        except Exception as e:
            print(f"[ERROR] UnifiedConfig: 配置文件加载失败: {e}")
            self.business_terms = {}
            self.query_scope_rules = []

    def _load_schema_info(self):
        """加载Schema信息 - 使用动态上下文管理器"""
        try:
            if self.database_path:
                print(f"[DEBUG] UnifiedConfig: 加载Schema信息")

                # 尝试使用智能上下文管理器
                try:
                    from .intelligent_context_manager import get_context_manager
                    context_manager = get_context_manager()
                    db_context = context_manager.get_or_create_context(self.database_path)

                    if db_context:
                        print(f"[INFO] UnifiedConfig: 使用智能上下文管理器加载Schema")
                        self._load_schema_from_context(db_context)
                        return

                except ImportError:
                    print("[DEBUG] UnifiedConfig: 智能上下文管理器不可用，使用直接数据库读取")

                # 直接从SQLite数据库读取Schema
                self.schema_info = self._extract_sqlite_schema()

            else:
                print(f"[WARNING] UnifiedConfig: 数据库路径为空，无法加载Schema")
                self.schema_info = {}

        except Exception as e:
            print(f"[ERROR] UnifiedConfig: Schema加载失败: {e}")
            self.schema_info = {}

    def _load_schema_from_context(self, db_context):
        """从数据库上下文加载Schema信息"""
        # 转换为UnifiedConfig格式的schema_info
        self.schema_info = {
            'description': db_context.description,
            'database_type': db_context.database_type,
            'total_tables': len(db_context.tables)
        }

        # 添加表信息
        for table_name, table_schema in db_context.tables.items():
            # 安全地提取列名
            columns = table_schema.columns
            if isinstance(columns, list):
                if columns and isinstance(columns[0], dict):
                    # 如果是字典列表，提取name字段
                    column_names = [col.get('name', str(col)) for col in columns]
                else:
                    # 如果是字符串列表，直接使用
                    column_names = [str(col) for col in columns]
            else:
                # 如果不是列表，转换为列表
                column_names = [str(columns)]

            self.schema_info[table_name] = {
                'columns': column_names,
                'description': table_schema.description,
                'row_count': table_schema.row_count,
                'key_fields': self._extract_key_fields_from_table(table_schema)
            }

        # 更新数据库类型
        self.database_type = db_context.database_type

        print(f"[DEBUG] UnifiedConfig: 动态Schema加载成功")
        print(f"  - 数据库类型: {self.database_type}")
        print(f"  - 表数量: {len(db_context.tables)}")

    def _extract_sqlite_schema(self) -> dict:
        """直接从SQLite数据库提取Schema信息"""
        try:
            import sqlite3

            conn = sqlite3.connect(self.database_path)
            cursor = conn.cursor()

            schema_info = {
                'database_type': 'sqlite',
                'description': f'SQLite数据库: {os.path.basename(self.database_path)}'
            }

            # 获取所有表
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = cursor.fetchall()

            schema_info['total_tables'] = len(tables)

            for table_name_tuple in tables:
                table_name = table_name_tuple[0]

                # 获取表的列信息
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns_info = cursor.fetchall()

                # 获取表的行数
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                row_count = cursor.fetchone()[0]

                # 构建表信息
                columns = []
                for col_info in columns_info:
                    columns.append({
                        'name': col_info[1],
                        'type': col_info[2],
                        'not_null': bool(col_info[3]),
                        'default_value': col_info[4],
                        'primary_key': bool(col_info[5])
                    })

                schema_info[table_name] = {
                    'columns': columns,
                    'row_count': row_count,
                    'description': f'表 {table_name}，包含 {len(columns)} 列，{row_count} 行数据'
                }

            conn.close()

            print(f"[INFO] UnifiedConfig: 直接提取Schema成功，包含 {len(tables)} 个表")
            return schema_info

        except Exception as e:
            print(f"[ERROR] UnifiedConfig: 直接Schema提取失败: {e}")
            return {}

    def _extract_key_fields_from_table(self, table_schema) -> Dict[str, str]:
        """从表结构中提取关键字段映射"""
        key_fields = {}

        try:
            columns = table_schema.columns
            if isinstance(columns, list):
                for column in columns:
                    # 安全地获取列名
                    if isinstance(column, dict):
                        col_name = column.get('name', str(column))
                    else:
                        col_name = str(column)

                    col_lower = col_name.lower()

                    # 识别关键字段类型
                    if 'org' in col_lower and 'name' in col_lower:
                        key_fields['branch_field'] = col_name
                    elif col_name in ['CUST_ID', 'customer_id', 'cust_id']:
                        key_fields['customer_id'] = col_name
                    elif 'cust' in col_lower and 'name' in col_lower:
                        key_fields['customer_name'] = col_name
                    elif 'deposit' in col_lower and 'bal' in col_lower:
                        key_fields['deposit_balance'] = col_name
                    elif 'loan' in col_lower and 'bal' in col_lower:
                        key_fields['loan_balance'] = col_name
        except Exception as e:
            print(f"[WARNING] 提取关键字段失败: {e}")

        return key_fields
    
    def create_context(self, user_query: str) -> QueryContext:
        """为用户查询创建完整的上下文"""
        print(f"[DEBUG] UnifiedConfig: 为查询创建上下文: {user_query}")
        
        # 获取适用的查询范围规则
        applicable_rules = self._get_applicable_rules(user_query)
        
        context = QueryContext(
            user_query=user_query,
            database_path=self.database_path or "",
            database_type=self.database_type or "unknown",
            business_terms=self.business_terms,
            schema_info=self.schema_info,
            query_scope_rules=applicable_rules,
            table_relationships=self.table_relationships
        )
        
        print(f"[DEBUG] UnifiedConfig: 上下文创建完成")
        return context
    
    def _get_applicable_rules(self, query: str) -> List[QueryScope]:
        """获取适用于查询的范围规则"""
        applicable_rules = []
        for rule in self.query_scope_rules:
            # 处理字典格式的规则
            if isinstance(rule, dict):
                query_pattern = rule.get('query_pattern', '')
                if query_pattern and query_pattern in query:
                    # 转换为QueryScope对象
                    scope_rule = QueryScope(
                        query_pattern=query_pattern,
                        scope_type=rule.get('scope_type', 'filtered'),
                        filter_conditions=rule.get('filter_conditions', ''),
                        description=rule.get('description', '')
                    )
                    applicable_rules.append(scope_rule)
            elif hasattr(rule, 'query_pattern'):
                # 处理QueryScope对象
                if rule.query_pattern in query:
                    applicable_rules.append(rule)
        return applicable_rules
    
    def is_valid(self) -> bool:
        """检查配置是否有效"""
        # 🔧 修复：放宽验证条件，允许数据库切换后的临时状态
        basic_valid = (
            self.database_path is not None and
            os.path.exists(self.database_path)
        )

        # 如果基础条件满足，即使business_terms为空也认为是有效的
        # 这允许数据库切换后的配置重新加载过程
        if basic_valid:
            print(f"[DEBUG] UnifiedConfig.is_valid(): 基础验证通过")
            print(f"  - 数据库路径: {self.database_path}")
            print(f"  - 文件存在: {os.path.exists(self.database_path) if self.database_path else False}")
            print(f"  - 业务术语数量: {len(self.business_terms)}")
            return True

        print(f"[DEBUG] UnifiedConfig.is_valid(): 验证失败")
        print(f"  - 数据库路径: {self.database_path}")
        print(f"  - 文件存在: {os.path.exists(self.database_path) if self.database_path else False}")
        return False
    
    def get_status(self) -> Dict[str, Any]:
        """获取配置状态"""
        return {
            'database_path': self.database_path,
            'database_exists': os.path.exists(self.database_path) if self.database_path else False,
            'database_type': self.database_type,
            'business_terms_count': len(self.business_terms),
            'query_rules_count': len(self.query_scope_rules),
            'field_mappings_count': len(self.field_mappings),
            'table_relationships_count': len(self.table_relationships),
            'is_valid': self.is_valid(),
            'config_consistency': self._check_consistency()
        }

    def _load_field_mappings(self):
        """加载统一字段映射 - 优先从数据库特定上下文加载"""
        try:
            # 如果有数据库路径，优先从数据库特定上下文加载字段映射
            if self.database_path:
                print(f"[DEBUG] UnifiedConfig: 加载字段映射")

                # 尝试从智能上下文管理器加载
                try:
                    from .intelligent_context_manager import get_context_manager
                    context_manager = get_context_manager()
                    db_context = context_manager.get_or_create_context(self.database_path)

                    # 将DatabaseContext对象转换为字典格式
                    db_context_data = None
                    if db_context:
                        db_context_data = {
                            'database_specific_field_mappings': getattr(db_context, 'database_specific_field_mappings', None),
                            'field_mappings': getattr(db_context, 'field_mappings', {})
                        }

                    if db_context_data and 'database_specific_field_mappings' in db_context_data:
                        # 从数据库特定上下文加载字段映射
                        self.field_mappings = db_context_data['database_specific_field_mappings']
                        print(f"[DEBUG] UnifiedConfig: 从数据库特定上下文加载字段映射 {len(self.field_mappings)} 个")
                        return
                    elif db_context_data and 'field_mappings' in db_context_data:
                        # 回退到原有的字段映射
                        self.field_mappings = db_context_data['field_mappings']
                        print(f"[DEBUG] UnifiedConfig: 从数据库上下文加载原有字段映射 {len(self.field_mappings)} 个")
                        return

                except ImportError:
                    print(f"[DEBUG] UnifiedConfig: 智能上下文管理器不可用，使用基础字段映射")

                # 使用基础字段映射配置，但保留已有的映射
                if not hasattr(self, 'field_mappings') or not self.field_mappings:
                    self.field_mappings = {}
                print(f"[DEBUG] UnifiedConfig: 使用基础字段映射配置，当前有 {len(self.field_mappings)} 个映射")
            else:
                print(f"[DEBUG] UnifiedConfig: 无数据库路径，使用空字段映射配置")
                self.field_mappings = {}

            print(f"[DEBUG] UnifiedConfig: 字段映射加载完成，共 {len(self.field_mappings)} 个")
        except Exception as e:
            print(f"[ERROR] UnifiedConfig: 字段映射加载失败: {e}")
            import traceback
            traceback.print_exc()
            self.field_mappings = {}

    def _load_table_relationships(self):
        """加载表关系配置 - 基于当前数据库的实际表结构"""
        try:
            if not self.database_path or not self.schema_info:
                print("[DEBUG] UnifiedConfig: 无数据库路径或Schema信息，跳过表关系加载")
                self.table_relationships = {}
                return

            # 尝试从配置文件加载表关系
            self.table_relationships = {}

            # 从配置文件加载表关系
            if self._load_table_relationships_from_config():
                print(f"[INFO] UnifiedConfig: 从配置文件加载表关系 {len(self.table_relationships)} 个")
            else:
                # 基于当前数据库的实际表结构动态生成表关系
                self._generate_table_relationships_from_schema()
                print(f"[DEBUG] UnifiedConfig: 基于Schema生成表关系 {len(self.table_relationships)} 个")

        except Exception as e:
            print(f"[ERROR] UnifiedConfig: 表关系加载失败: {e}")
            self.table_relationships = {}

    def _load_table_relationships_from_config(self) -> bool:
        """从配置文件加载表关系"""
        try:
            import json
            import glob

            # 根据数据库路径确定配置文件
            db_name = os.path.splitext(os.path.basename(self.database_path))[0]
            config_pattern = f"configs/database_contexts/{db_name}_context.json"

            config_files = glob.glob(config_pattern)
            if not config_files:
                return False

            config_file = config_files[0]

            with open(config_file, 'r', encoding='utf-8') as f:
                config_data = json.load(f)

            # 从database_description中加载表关系
            db_description = config_data.get('database_description', {})
            table_relationships = db_description.get('table_relationships', [])

            for rel in table_relationships:
                if isinstance(rel, dict):
                    rel_name = f"{rel.get('table1', '')}_to_{rel.get('table2', '')}"
                    self.table_relationships[rel_name] = {
                        "from_table": rel.get('table1', ''),
                        "from_field": rel.get('common_field', ''),
                        "to_table": rel.get('table2', ''),
                        "to_field": rel.get('common_field', ''),
                        "relationship_type": rel.get('relationship_type', 'one_to_many'),
                        "description": rel.get('description', '')
                    }

            return len(self.table_relationships) > 0

        except Exception as e:
            print(f"[ERROR] UnifiedConfig: 从配置文件加载表关系失败: {e}")
            return False

    def _generate_table_relationships_from_schema(self):
        """基于Schema生成表关系"""
        # 获取当前数据库的表列表（排除元数据字段）
        actual_tables = [k for k in self.schema_info.keys()
                       if k not in ['description', 'database_type', 'total_tables']]

        print(f"[DEBUG] UnifiedConfig: 当前数据库表: {actual_tables}")

        # 如果是banking_indicators单表数据库，不需要表关系
        if len(actual_tables) == 1 and 'banking_indicators' in actual_tables:
            print("[DEBUG] UnifiedConfig: 单表数据库，无需表关系配置")
            self.table_relationships = {}

        # 如果是多表数据库，根据实际表结构定义关系
        elif len(actual_tables) > 1:
            # 检查是否包含客户贷款相关表
            if 'corp_deposit_balance' in actual_tables and 'corp_loan_contract_info' in actual_tables:
                self.table_relationships["corp_deposit_balance_to_corp_loan"] = {
                    "from_table": "corp_deposit_balance",
                    "from_field": "CUST_ID",
                    "to_table": "corp_loan_contract_info",
                    "to_field": "cust_no",
                    "relationship_type": "one_to_many"
                }

            if 'corp_loan_contract_info' in actual_tables and 'contract_classification' in actual_tables:
                self.table_relationships["corp_loan_to_classification"] = {
                    "from_table": "corp_loan_contract_info",
                    "from_field": "CONTRACT_NO",
                    "to_table": "contract_classification",
                    "to_field": "CONTRACT_NO",
                    "relationship_type": "one_to_one"
                }

    def _auto_configure_field_mappings(self):
        """自动配置缺失的字段映射，基于数据库实际结构"""
        try:
            if not self.database_path or not self.schema_info:
                print("[DEBUG] UnifiedConfig: 无数据库信息，跳过自动字段映射配置")
                return

            print("[DEBUG] UnifiedConfig: 开始自动配置字段映射...")

            # 获取数据库中的实际字段
            actual_fields = set()
            for table_name, table_info in self.schema_info.items():
                if table_name not in ['description', 'database_type', 'total_tables']:
                    if isinstance(table_info, dict) and 'columns' in table_info:
                        for column in table_info['columns']:
                            if isinstance(column, dict) and 'name' in column:
                                actual_fields.add(column['name'])
                            elif isinstance(column, str):
                                actual_fields.add(column)

            print(f"[DEBUG] UnifiedConfig: 发现数据库字段: {sorted(actual_fields)}")

            # 定义关键字段的自动映射规则
            auto_mapping_rules = {
                'host_org_name': ['host_org_name', 'BRANCH_NAME', 'acg_org_blng_lv1_branch_name', 'org_name', 'branch'],
                'CUST_ID': ['CUST_ID', 'cust_id', 'customer_id', 'cust_no', 'CUST_NO'],
                'corp_deposit_y_avg_bal': ['corp_deposit_y_avg_bal', 'avg_deposit', 'yearly_avg_balance', 'deposit_avg'],
                'CUST_NAME': ['CUST_NAME', 'cust_name', 'customer_name', 'name'],
                'corp_deposit_bal': ['corp_deposit_bal', 'deposit_balance', 'current_balance'],
                'loan_bal_rmb': ['loan_bal_rmb', 'loan_balance', 'loan_bal'],
                'CONTRACT_CL_RESULT': ['CONTRACT_CL_RESULT', 'classification_result', 'cl_result']
            }

            # 自动匹配字段
            auto_mapped_count = 0
            for logical_field, possible_names in auto_mapping_rules.items():
                if logical_field not in self.field_mappings:
                    # 查找匹配的实际字段
                    for possible_name in possible_names:
                        if possible_name in actual_fields:
                            self.field_mappings[logical_field] = possible_name
                            auto_mapped_count += 1
                            print(f"[INFO] UnifiedConfig: 自动映射 {logical_field} -> {possible_name}")
                            break

                    if logical_field not in self.field_mappings:
                        print(f"[WARNING] UnifiedConfig: 未找到字段 {logical_field} 的匹配")

            print(f"[INFO] UnifiedConfig: 自动配置了 {auto_mapped_count} 个字段映射")

        except Exception as e:
            print(f"[ERROR] UnifiedConfig: 自动字段映射配置失败: {e}")

    def _validate_config_consistency(self):
        """验证配置一致性"""
        print("[DEBUG] UnifiedConfig: 验证配置一致性...")

        # 验证关键字段映射
        critical_fields = ["host_org_name", "CUST_ID", "corp_deposit_y_avg_bal"]
        for field in critical_fields:
            if field not in self.field_mappings:
                print(f"[WARNING] 关键字段 {field} 缺失映射配置")

        print("[DEBUG] UnifiedConfig: 配置一致性验证完成")

    def _check_consistency(self) -> bool:
        """检查配置一致性"""
        try:
            # 检查关键配置项
            if not self.database_path or not self.business_terms:
                return False

            # 检查字段映射完整性
            critical_fields = ["host_org_name", "CUST_ID"]
            for field in critical_fields:
                if field not in self.field_mappings:
                    return False

            return True
        except Exception:
            return False

    def get_nl2sql_config(self, database_type: str = None) -> Dict[str, Any]:
        """获取NL2SQL配置"""
        try:
            if database_type is None:
                database_type = self.database_type

            # 从database_contexts获取NL2SQL配置
            # 简化：直接使用第一个匹配的数据库类型配置
            context = {}
            for key, config in self.database_contexts.items():
                if database_type in key:
                    context = config
                    break
            nl2sql_config = context.get('nl2sql_config', {})

            if not nl2sql_config:
                print(f"[WARNING] 未找到数据库类型 {database_type} 的NL2SQL配置，使用默认配置")
                nl2sql_config = self._get_default_nl2sql_config()

            return nl2sql_config

        except Exception as e:
            print(f"[ERROR] 获取NL2SQL配置失败: {e}")
            return self._get_default_nl2sql_config()

    def _get_default_nl2sql_config(self) -> Dict[str, Any]:
        """获取默认NL2SQL配置"""
        return {
            "prompt_templates": {
                "default": "你是一个专业的SQL生成专家。请基于以下信息生成SQL查询：\n\n用户查询：{user_query}\n\n请生成SQL语句："
            },
            "sql_generation_rules": {
                "constraints": {
                    "forbidden_time_filters": [],
                    "forbidden_reason": ""
                }
            },
            "query_type_mapping": {
                "patterns": [
                    {
                        "pattern": ".*",
                        "type": "simple_query",
                        "complexity": "simple",
                        "priority": 1
                    }
                ]
            }
        }

    def create_query_context_for_nl2sql(self, query: str) -> Dict[str, Any]:
        """为NL2SQL创建查询上下文"""
        try:
            print(f"[DEBUG] UnifiedConfig: 为NL2SQL创建查询上下文: {query}")

            context = {
                "database_info": {
                    "path": self.database_path,
                    "type": self.database_type
                },
                "business_terms": self.business_terms,
                "field_mappings": self.field_mappings,
                "table_relationships": self.table_relationships,
                "user_query": query,
                "database_schema": self._build_schema_summary()
            }

            print(f"[DEBUG] UnifiedConfig: NL2SQL查询上下文创建完成")
            return context

        except Exception as e:
            print(f"[ERROR] UnifiedConfig: 创建NL2SQL查询上下文失败: {e}")
            return {"user_query": query}

    def _build_schema_summary(self) -> str:
        """构建数据库结构摘要"""
        try:
            schema_parts = []

            # 添加表关系信息
            if self.table_relationships:
                schema_parts.append("【表关系】")
                for rel_name, rel_info in self.table_relationships.items():
                    from_table = rel_info.get('from_table', '')
                    to_table = rel_info.get('to_table', '')
                    from_field = rel_info.get('from_field', '')
                    to_field = rel_info.get('to_field', '')
                    schema_parts.append(f"- {from_table}.{from_field} = {to_table}.{to_field}")

            return "\n".join(schema_parts)

        except Exception as e:
            print(f"[ERROR] 构建数据库结构摘要失败: {e}")
            return ""

    def create_query_context(self, query: str, database_path: str = None) -> 'QueryContext':
        """创建查询上下文"""
        try:
            # 使用提供的数据库路径或当前数据库路径
            db_path = database_path or self.database_path

            # 如果数据库路径不匹配，需要切换
            if db_path and db_path != self.database_path:
                print(f"[INFO] 切换数据库上下文: {db_path}")
                self.switch_database(db_path)

            # 转换业务术语格式
            business_terms = {}
            for term_name, term_data in self.business_terms.items():
                if isinstance(term_data, dict):
                    business_terms[term_name] = BusinessTerm(
                        name=term_data.get('name', term_name),
                        definition=term_data.get('definition', ''),
                        data_representation=term_data.get('sql_conditions', ''),
                        sql_conditions=term_data.get('sql_conditions', ''),
                        examples=term_data.get('examples', [])
                    )

            # 转换查询范围规则格式
            scope_rules = []
            for rule_data in self.query_scope_rules:
                if isinstance(rule_data, dict):
                    scope_rules.append(QueryScope(
                        query_pattern=rule_data.get('query_pattern', ''),
                        scope_type=rule_data.get('scope_type', 'all'),
                        filter_conditions=rule_data.get('filter_conditions', ''),
                        description=rule_data.get('description', '')
                    ))

            return QueryContext(
                user_query=query,
                database_path=self.database_path or '',
                database_type=self.database_type or 'unknown',
                business_terms=business_terms,
                schema_info=self.schema_info,
                query_scope_rules=scope_rules,
                table_relationships=self.table_relationships
            )

        except Exception as e:
            print(f"[ERROR] 创建查询上下文失败: {e}")
            # 返回最小化的上下文
            return QueryContext(
                user_query=query,
                database_path=database_path or '',
                database_type='unknown',
                business_terms={},
                schema_info={},
                query_scope_rules=[],
                table_relationships={}
            )

    def _load_nl2sql_config(self):
        """🔥 新增：加载NL2SQL配置 - 统一管理所有NL2SQL相关配置"""
        try:
            print("[DEBUG] UnifiedConfig: 开始加载NL2SQL配置...")

            # 默认NL2SQL配置
            self.nl2sql_config = {
                "engine_type": "simple",  # simple | configurable | enhanced
                "enable_caching": True,
                "max_retries": 2,
                "timeout_seconds": 30
            }

            # 默认提示词模板
            self.prompt_templates = {
                "default": """你是一个专业的SQL查询生成专家。请根据用户的自然语言查询生成准确的SQL语句。

用户查询：{user_query}

数据库结构：
{database_schema}

业务术语：
{business_terms}

约束条件：
{constraints}

请生成准确的SQL查询语句。""",

                "simple_query": """根据以下信息生成SQL查询：

查询：{user_query}
数据库结构：{database_schema}

请直接生成SQL语句。""",

                "complex_analysis": """你是银行数据分析专家。请分析以下查询并生成相应的SQL：

业务查询：{user_query}

数据库结构：
{database_schema}

业务术语定义：
{business_terms}

请生成能够满足业务分析需求的SQL查询。"""
            }

            # 默认查询模式
            self.query_patterns = [
                {
                    "pattern": r"查询.*客户.*信息",
                    "type": "simple_query",
                    "complexity": "simple",
                    "priority": 1
                },
                {
                    "pattern": r"分析.*分布|统计.*情况",
                    "type": "complex_analysis",
                    "complexity": "complex",
                    "priority": 2
                },
                {
                    "pattern": r"对公有效户|不良贷款",
                    "type": "business_analysis",
                    "complexity": "complex",
                    "priority": 3
                }
            ]

            # 默认SQL约束
            self.sql_constraints = {
                "forbidden_time_filters": [
                    "WHERE.*日期.*=.*'{date}'",
                    "AND.*时间.*BETWEEN"
                ],
                "forbidden_reason": "避免硬编码时间过滤条件",
                "max_result_limit": 10000,
                "allowed_operations": ["SELECT"],
                "forbidden_operations": ["DROP", "DELETE", "UPDATE", "INSERT"]
            }

            print(f"[DEBUG] UnifiedConfig: NL2SQL配置加载完成")
            print(f"  - 提示词模板: {len(self.prompt_templates)} 个")
            print(f"  - 查询模式: {len(self.query_patterns)} 个")
            print(f"  - 约束规则: {len(self.sql_constraints)} 个")

        except Exception as e:
            print(f"[WARNING] UnifiedConfig: NL2SQL配置加载失败: {e}")
            # 设置最小配置
            self.nl2sql_config = {"engine_type": "simple"}
            self.prompt_templates = {}
            self.query_patterns = []
            self.sql_constraints = {}

    def get_nl2sql_config(self) -> Dict[str, Any]:
        """获取NL2SQL配置"""
        return {
            "engine_config": self.nl2sql_config,
            "prompt_templates": self.prompt_templates,
            "query_type_mapping": {
                "patterns": self.query_patterns
            },
            "sql_generation_rules": {
                "constraints": self.sql_constraints
            }
        }

    def update_nl2sql_config(self, config_updates: Dict[str, Any]):
        """更新NL2SQL配置"""
        try:
            if "prompt_templates" in config_updates:
                self.prompt_templates.update(config_updates["prompt_templates"])

            if "query_patterns" in config_updates:
                self.query_patterns.extend(config_updates["query_patterns"])

            if "sql_constraints" in config_updates:
                self.sql_constraints.update(config_updates["sql_constraints"])

            print("[DEBUG] UnifiedConfig: NL2SQL配置更新完成")

        except Exception as e:
            print(f"[ERROR] UnifiedConfig: NL2SQL配置更新失败: {e}")


# 全局统一配置实例
_global_unified_config = None

def get_unified_config() -> UnifiedConfig:
    """获取全局统一配置实例"""
    global _global_unified_config
    if _global_unified_config is None:
        _global_unified_config = UnifiedConfig()
    return _global_unified_config

def reload_unified_config():
    """重新加载全局统一配置"""
    global _global_unified_config
    print("[DEBUG] 重新加载全局统一配置...")
    _global_unified_config = None
    _global_unified_config = UnifiedConfig()
    print("[DEBUG] 全局统一配置重新加载完成")
    return _global_unified_config

def update_global_config_database(database_path: str):
    """更新全局配置的数据库路径"""
    global _global_unified_config
    if _global_unified_config is not None:
        print(f"[DEBUG] 更新全局配置数据库路径: {database_path}")
        _global_unified_config.database_path = database_path
        # 重新加载相关配置
        _global_unified_config._load_schema_info()
        _global_unified_config._load_field_mappings()
        _global_unified_config._load_table_relationships()
        print("[DEBUG] 全局配置数据库更新完成")

def reset_unified_config():
    """重置全局统一配置实例"""
    global _global_unified_config
    _global_unified_config = None
