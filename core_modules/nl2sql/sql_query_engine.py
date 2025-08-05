#!/usr/bin/env python3
"""
SQL查询引擎 - 重构后的统一NL2SQL引擎

整合原有的SimpleNL2SQLEngine和ConsolidatedNL2SQLTool功能
"""

import time
import sqlite3
import re
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass

try:
    from ..config import get_configuration_registry
except ImportError:
    # 兼容性处理
    def get_configuration_registry():
        class SimpleConfigRegistry:
            def get_nl2sql_config(self):
                return {
                    'prompt_templates': {
                        'simple_query': '''基于以下数据库结构，将自然语言查询转换为SQL语句：

数据库结构：
{schema_info}

字段映射：
{field_mappings}

查询：{query}

请生成准确的SQL语句：''',
                        'complex_query': '''基于以下数据库结构，将复杂的自然语言查询转换为SQL语句：

数据库结构：
{schema_info}

字段映射：
{field_mappings}

业务规则：
{business_rules}

查询：{query}

请生成准确的SQL语句，包含必要的JOIN和WHERE条件：'''
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
        return SimpleConfigRegistry()


@dataclass
class SQLQueryResult:
    """SQL查询结果 - 统一格式"""
    success: bool
    sql: str = ""
    data: List[Dict[str, Any]] = None
    record_count: int = 0
    execution_time: float = 0.0
    error: Optional[str] = None
    engine_used: str = "sql_query_engine"
    
    def __post_init__(self):
        if self.data is None:
            self.data = []
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'success': self.success,
            'sql': self.sql,
            'data': self.data,
            'record_count': self.record_count,
            'execution_time': self.execution_time,
            'error': self.error,
            'engine_used': self.engine_used
        }


class SQLQueryEngine:
    """
    SQL查询引擎 - 统一的NL2SQL处理器
    
    功能：
    1. 自然语言到SQL的转换
    2. SQL执行和结果处理
    3. 错误处理和重试机制
    """
    
    def __init__(self):
        # 初始化组件（避免Pydantic字段冲突）
        object.__setattr__(self, 'config_registry', get_configuration_registry())
        object.__setattr__(self, 'llm_client', None)
        self._init_llm_client()
    
    def _init_llm_client(self):
        """初始化LLM客户端"""
        try:
            # 使用OpenAI兼容的DeepSeek API
            from langchain_openai import ChatOpenAI
            import os

            api_key = os.getenv('DEEPSEEK_API_KEY')
            if api_key:
                # DeepSeek API是OpenAI兼容的
                self.llm_client = ChatOpenAI(
                    api_key=api_key,
                    base_url="https://api.deepseek.com",
                    model="deepseek-chat",
                    temperature=0.1
                )
                print("[DEBUG] SQLQueryEngine: 使用 DeepSeek API (OpenAI兼容)")
            else:
                print("[WARNING] SQLQueryEngine: 未找到 DEEPSEEK_API_KEY")

        except ImportError:
            print("[WARNING] SQLQueryEngine: LangChain OpenAI 不可用")
    
    def generate_sql(self, query: str, context: Optional[Dict[str, Any]] = None) -> SQLQueryResult:
        """
        生成SQL查询
        
        Args:
            query: 用户的自然语言查询
            context: 查询上下文
            
        Returns:
            SQLQueryResult: 查询结果
        """
        start_time = time.time()
        
        try:
            print(f"[DEBUG] SQLQueryEngine: 开始处理查询: {query}")
            
            if not self.llm_client:
                return SQLQueryResult(
                    success=False,
                    error="LLM客户端未初始化",
                    execution_time=time.time() - start_time
                )
            
            # 构建提示词
            prompt = self._build_prompt(query, context)
            print(f"[DEBUG] SQLQueryEngine: 构建的提示词长度: {len(prompt) if prompt else 0}")
            print(f"[DEBUG] SQLQueryEngine: 提示词内容: {prompt[:1000]}..." if prompt and len(prompt) > 1000 else f"[DEBUG] SQLQueryEngine: 提示词内容: {prompt}")

            # 调用LLM生成SQL
            llm_response = self._call_llm(prompt)
            print(f"[DEBUG] SQLQueryEngine: LLM响应长度: {len(llm_response) if llm_response else 0}")
            print(f"[DEBUG] SQLQueryEngine: LLM原始响应: {llm_response[:500]}..." if llm_response and len(llm_response) > 500 else f"[DEBUG] SQLQueryEngine: LLM原始响应: {llm_response}")

            # 提取SQL
            sql = self._extract_sql(llm_response)

            if not sql:
                print(f"[ERROR] SQLQueryEngine: 未能提取到有效的SQL语句")
                print(f"[DEBUG] SQLQueryEngine: 尝试的提取模式失败，完整响应: {llm_response}")
                return SQLQueryResult(
                    success=False,
                    error="未能提取到有效的SQL语句",
                    execution_time=time.time() - start_time
                )
            
            # 清理和修复SQL
            sql = self._clean_sql(sql)
            print(f"[DEBUG] SQLQueryEngine: 生成的SQL: {sql}")
            
            # 执行SQL
            data, record_count = self._execute_sql(sql, context)
            
            return SQLQueryResult(
                success=True,
                sql=sql,
                data=data,
                record_count=record_count,
                execution_time=time.time() - start_time
            )
            
        except Exception as e:
            print(f"[ERROR] SQLQueryEngine: 查询失败: {e}")
            return SQLQueryResult(
                success=False,
                error=str(e),
                execution_time=time.time() - start_time
            )
    
    def _build_prompt(self, query: str, context: Optional[Dict[str, Any]] = None) -> str:
        """构建LLM提示词"""
        try:
            # 获取配置
            nl2sql_config = self.config_registry.get_nl2sql_config()
            print(f"[DEBUG] SQLQueryEngine: NL2SQL配置获取成功: {bool(nl2sql_config)}")
            print(f"[DEBUG] SQLQueryEngine: 配置内容: {nl2sql_config}")

            # 如果配置为空或不完整，使用默认配置
            if not nl2sql_config or not nl2sql_config.get('prompt_templates'):
                print(f"[WARNING] SQLQueryEngine: 配置不完整，使用默认配置")
                nl2sql_config = {
                    'prompt_templates': {
                        'simple_query': '''你是一个专业的银行数据分析师，需要将自然语言查询转换为准确的SQL语句。

数据库结构：
{schema_info}

业务术语定义：
{business_terms}

字段映射：
{field_mappings}

用户查询：{query}

重要说明：
- 对公有效户：指corp_deposit_y_avg_bal > 0的客户
- 不良贷款：指CONTRACT_CL_RESULT IN ('2','3','4') 或 CONTRACT_CL_RESULT = '不良'
- 请使用上面显示的真实表名，不要使用占位符如"your_table_name"
- 优先使用包含最多相关字段的表

重要的SQL语法规则：
- GROUP BY子句中不能使用聚合函数(SUM, COUNT, AVG等)
- 如需按聚合结果分组，应使用子查询或HAVING子句
- CASE WHEN表达式如果包含聚合函数，不能直接用于GROUP BY
- 正确的做法：先计算聚合值，再在外层查询中分类

请根据以上信息生成准确的SQL语句。注意：
1. 只返回SQL语句，不要包含任何解释或注释
2. 使用上面显示的真实表名和字段名
3. 对于业务术语，使用对应的SQL条件
4. 确保SQL语法正确，特别注意GROUP BY规则
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

重要的SQL语法规则：
- GROUP BY子句中不能使用聚合函数(SUM, COUNT, AVG等)
- 如需按聚合结果分组，应使用子查询或HAVING子句
- CASE WHEN表达式如果包含聚合函数，不能直接用于GROUP BY
- 正确的做法：先计算聚合值，再在外层查询中分类

请根据以上信息生成准确的SQL语句。注意：
1. 只返回SQL语句，不要包含任何解释或注释
2. 使用正确的表名和字段名
3. 对于业务术语，使用对应的SQL条件和计算公式
4. 包含必要的JOIN和WHERE条件
5. 使用适当的聚合函数和GROUP BY
6. 确保SQL语法正确

SQL语句：'''
                    }
                }

            # 判断查询复杂度
            if self._is_complex_query(query):
                template = nl2sql_config.get('prompt_templates', {}).get('complex_query', '')
                print(f"[DEBUG] SQLQueryEngine: 使用复杂查询模板，长度: {len(template)}")
            else:
                template = nl2sql_config.get('prompt_templates', {}).get('simple_query', '')
                print(f"[DEBUG] SQLQueryEngine: 使用简单查询模板，长度: {len(template)}")

            if not template:
                print(f"[ERROR] SQLQueryEngine: 模板为空！可用模板: {list(nl2sql_config.get('prompt_templates', {}).keys())}")
                return ""

            # 构建上下文信息
            schema_info = self._format_schema_info(context)
            business_terms = self._format_business_terms(context)
            field_mappings = self._format_field_mappings(context)

            print(f"[DEBUG] SQLQueryEngine: Schema信息长度: {len(schema_info)}")
            print(f"[DEBUG] SQLQueryEngine: 业务术语长度: {len(business_terms)}")
            print(f"[DEBUG] SQLQueryEngine: 字段映射长度: {len(field_mappings)}")

            # 填充模板
            try:
                prompt = template.format(
                    query=query,
                    schema_info=schema_info,
                    business_terms=business_terms,
                    field_mappings=field_mappings,
                    business_rules=business_terms  # 兼容性
                )
            except KeyError as e:
                print(f"[WARNING] SQLQueryEngine: 模板格式化失败: {e}，使用简化版本")
                # 如果模板缺少某些字段，使用简化版本
                prompt = template.format(
                    query=query,
                    schema_info=schema_info,
                    business_terms=business_terms
                )

            return prompt

        except Exception as e:
            print(f"[ERROR] SQLQueryEngine: 提示词构建失败: {e}")
            return ""

    def execute_query(self, sql_query: str, database_path: str = None) -> 'SQLQueryResult':
        """执行SQL查询"""
        import sqlite3
        import os

        start_time = time.time()

        try:
            # 确定数据库路径
            if not database_path:
                import glob
                data_dir = os.getenv('DATAPROXY_DATA_DIR', './databases')

                # 扫描数据库文件（包括子目录）
                db_files = glob.glob(os.path.join(data_dir, '**/*.db'), recursive=True)
                db_files.extend(glob.glob(os.path.join(data_dir, '*.db')))

                if not db_files:
                    return SQLQueryResult(
                        success=False,
                        error="没有找到可用的数据库文件",
                        execution_time=time.time() - start_time
                    )
                database_path = db_files[0]  # 使用第一个找到的数据库

            # 连接数据库并执行查询
            conn = sqlite3.connect(database_path)
            conn.row_factory = sqlite3.Row  # 使结果可以按列名访问
            cursor = conn.cursor()

            cursor.execute(sql_query)
            rows = cursor.fetchall()

            # 转换为字典列表
            data = [dict(row) for row in rows]
            record_count = len(data)

            conn.close()

            return SQLQueryResult(
                success=True,
                sql=sql_query,
                data=data,
                record_count=record_count,
                execution_time=time.time() - start_time
            )

        except Exception as e:
            return SQLQueryResult(
                success=False,
                sql=sql_query,
                error=f"SQL执行失败: {str(e)}",
                execution_time=time.time() - start_time
            )

    def _is_complex_query(self, query: str) -> bool:
        """判断是否为复杂查询"""
        complex_indicators = [
            "的.*的",  # 如"对公有效户的不良贷款余额"
            "统计.*并输出",  # 多步骤要求
            "截至.*末",  # 时间条件
            ".*余额.*客户.*名称",  # 复合查询要求
            "分析.*分布",  # 分析类查询
            "各.*统计",  # 分组统计
        ]
        return any(re.search(indicator, query) for indicator in complex_indicators)
    
    def _format_schema_info(self, context: Optional[Dict[str, Any]]) -> str:
        """格式化数据库结构信息"""
        if not context:
            return "数据库结构信息不可用"

        schema_text = "数据库表结构：\n"

        # 使用table_details如果可用
        if 'table_details' in context and context['table_details']:
            for table_name, table_info in context['table_details'].items():
                columns = table_info.get('columns', [])
                row_count = table_info.get('row_count', 0)
                schema_text += f"\n表 {table_name} ({row_count} 行数据):\n"
                for col in columns[:15]:  # 显示前15列
                    if isinstance(col, dict):
                        col_name = col.get('name', str(col))
                        col_type = col.get('type', '')
                        schema_text += f"  - {col_name} ({col_type})\n"
                    else:
                        schema_text += f"  - {col}\n"
                if len(columns) > 15:
                    schema_text += f"  ... 还有 {len(columns) - 15} 列\n"
        elif 'tables' in context and context['tables']:
            for table in context['tables']:
                schema_text += f"- {table}\n"
        else:
            # 尝试从schema_info获取
            if 'schema_info' in context and context['schema_info']:
                for table_name, table_info in context['schema_info'].items():
                    if table_name not in ['description', 'database_type', 'total_tables']:
                        if isinstance(table_info, dict):
                            columns = table_info.get('columns', [])
                            row_count = table_info.get('row_count', 0)
                            schema_text += f"\n表 {table_name} ({row_count} 行数据):\n"
                            for col in columns[:15]:
                                if isinstance(col, dict):
                                    col_name = col.get('name', str(col))
                                    col_type = col.get('type', '')
                                    schema_text += f"  - {col_name} ({col_type})\n"
                                else:
                                    schema_text += f"  - {col}\n"
                            if len(columns) > 15:
                                schema_text += f"  ... 还有 {len(columns) - 15} 列\n"
            else:
                schema_text += "无可用表信息\n"

        return schema_text
    
    def _format_business_terms(self, context: Optional[Dict[str, Any]]) -> str:
        """格式化业务术语"""
        if not context:
            return "业务术语定义不可用"

        terms_text = "业务术语定义：\n"

        # 检查business_terms
        if 'business_terms' in context and context['business_terms']:
            for term_name, term_info in context['business_terms'].items():
                if isinstance(term_info, dict):
                    definition = term_info.get('definition', '')
                    sql_conditions = term_info.get('sql_conditions', '')
                    terms_text += f"- {term_name}: {definition}"
                    if sql_conditions:
                        terms_text += f" (SQL: {sql_conditions})"
                    terms_text += "\n"
                else:
                    terms_text += f"- {term_name}: {term_info}\n"
        else:
            # 尝试从统一配置获取业务术语
            try:
                from ..config import get_unified_config
                config = get_unified_config()

                if config.business_terms:
                    for term_name, term_obj in list(config.business_terms.items())[:10]:  # 限制显示前10个
                        if hasattr(term_obj, 'definition'):
                            definition = term_obj.definition
                            sql_conditions = getattr(term_obj, 'sql_conditions', '')
                            terms_text += f"- {term_name}: {definition}"
                            if sql_conditions:
                                terms_text += f" (SQL: {sql_conditions})"
                            terms_text += "\n"
                        else:
                            terms_text += f"- {term_name}: {term_obj}\n"
                else:
                    terms_text += "无业务术语定义\n"
            except Exception as e:
                terms_text += "业务术语获取失败\n"

        return terms_text

    def _format_field_mappings(self, context: Optional[Dict[str, Any]]) -> str:
        """格式化字段映射信息"""
        if not context:
            return "字段映射信息不可用"

        # 从统一配置获取字段映射
        try:
            from ..config import get_unified_config
            config = get_unified_config()

            if config.field_mappings:
                mappings_text = "字段映射：\n"
                for logical_field, actual_field in config.field_mappings.items():
                    mappings_text += f"- {logical_field} → {actual_field}\n"
                return mappings_text
            else:
                return "无字段映射配置"
        except Exception as e:
            return f"字段映射获取失败: {e}"
    
    def _call_llm(self, prompt: str) -> str:
        """调用LLM"""
        try:
            if hasattr(self.llm_client, 'invoke'):
                # LangChain 客户端
                from langchain.schema.messages import HumanMessage
                response = self.llm_client.invoke([HumanMessage(content=prompt)])
                return response.content
            else:
                raise Exception("未知的LLM客户端类型")
                
        except Exception as e:
            raise Exception(f"LLM调用失败: {e}")
    
    def _extract_sql(self, response: str) -> Optional[str]:
        """提取SQL语句"""
        if not response:
            return None
        
        # SQL提取模式
        patterns = [
            r'```sql\s*(.*?)\s*```',
            r'```\s*(SELECT.*?)\s*```', 
            r'SQL:\s*(SELECT.*?)(?:\n|$)',
            r'(SELECT.*?)(?:\n\n|$)',
        ]
        
        # 尝试各种模式
        for pattern in patterns:
            matches = re.findall(pattern, response, re.DOTALL | re.IGNORECASE)
            if matches:
                sql = matches[0].strip()
                if self._is_valid_sql(sql):
                    return sql
        
        return None
    
    def _is_valid_sql(self, sql: str) -> bool:
        """验证SQL语句"""
        if not sql or len(sql.strip()) < 10:
            return False
        
        sql_upper = sql.upper().strip()
        return sql_upper.startswith('SELECT')
    
    def _clean_sql(self, sql: str) -> str:
        """清理SQL语句"""
        if not sql:
            return sql
        
        # 移除markdown标记
        sql = re.sub(r'```sql|```', '', sql)
        
        # 移除注释
        sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
        
        # 清理多余空格
        sql = ' '.join(sql.split())
        
        # 确保以分号结尾
        if not sql.endswith(';'):
            sql += ';'
        
        return sql
    
    def _execute_sql(self, sql: str, context: Optional[Dict[str, Any]] = None) -> Tuple[List[Dict[str, Any]], int]:
        """执行SQL查询"""
        try:
            # 获取数据库路径
            db_path = None
            if context and 'database_path' in context:
                db_path = context['database_path']
            
            if not db_path:
                print("[WARNING] SQLQueryEngine: 无法获取数据库路径")
                return [], 0
            
            # 连接数据库并执行SQL
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute(sql)
            rows = cursor.fetchall()
            
            # 转换为字典列表
            data = [dict(row) for row in rows]
            
            conn.close()
            
            print(f"[DEBUG] SQLQueryEngine: SQL执行成功，返回 {len(data)} 条记录")
            return data, len(data)
            
        except Exception as e:
            print(f"[ERROR] SQLQueryEngine: SQL执行失败: {e}")
            return [], 0
