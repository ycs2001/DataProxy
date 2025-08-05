#!/usr/bin/env python3
"""
查询分析模块
从langchain_streamlit_app.py提取的成熟查询分析组件
"""

import os
from typing import Dict, Any, List
from pydantic import BaseModel, Field


# LangChain导入
try:
    from langchain.tools import BaseTool
    from langchain_openai import ChatOpenAI
    from langchain.schema.messages import HumanMessage
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False


class QueryAnalysisToolInput(BaseModel):
    """查询分析工具输入模型"""
    query: str = Field(description="用户的自然语言查询")


# 删除硬编码的SchemaBasedDecomposer，完全使用动态Schema


# 删除独立的LLMQueryDecomposer，功能已集成到QueryAnalysisTool中


class QueryAnalysisTool(BaseTool):
    """增强的查询分析工具 - 集成LLM和Schema分解"""
    name: str = "analyze_query"
    description: str = "智能分析用户查询，基于LLM和数据库Schema进行查询分解，制定详细的执行计划"
    args_schema: type = QueryAnalysisToolInput

# 不再需要硬编码的Schema分解器，使用动态Schema

    def get_llm(self):
        """获取LLM实例"""
        if not hasattr(self, '_llm'):
            try:
                from langchain_openai import ChatOpenAI
                self._llm = ChatOpenAI(
                    model="deepseek-chat",
                    openai_api_key=os.getenv('DEEPSEEK_API_KEY'),
                    openai_api_base="https://api.deepseek.com/v1",
                    temperature=0.1
                )
            except Exception as e:
                print(f"[WARNING] LLM初始化失败: {e}")
                self._llm = None
        return self._llm

    def _run(self, query: str) -> Dict[str, Any]:
        """查询分析和拆解 - 自动获取数据库路径"""
        print(f"[DEBUG] ===== QueryAnalysisTool: 查询分析和拆解 - {query} =====")

        try:
            # 尝试从配置管理器获取数据库路径
            database_path = self._get_database_path_from_config()

            if database_path:
                print(f"[DEBUG] 从配置获取到数据库路径: {database_path}")
                # 使用带数据库路径的方法
                return self._run_with_database_path(query, database_path)
            else:
                print(f"[DEBUG] 未找到数据库路径配置，使用简化分析")
                # 使用简化的分析方法
                return self._run_simplified_analysis(query)

        except Exception as e:
            print(f"[DEBUG] 查询分析失败: {e}")
            import traceback
            traceback.print_exc()

            return {
                'success': False,
                'error': str(e),
                'original_query': query
            }

    def _get_database_path_from_config(self) -> str:
        """从配置管理器获取数据库路径"""
        try:
            # 尝试导入配置管理器
            from .schema_config_manager import get_schema_config_manager
            config_manager = get_schema_config_manager()

            # 获取所有配置
            all_configs = config_manager.get_all_configs()

            if all_configs:
                # 返回第一个可用的数据库路径
                for db_path in all_configs.keys():
                    if os.path.exists(db_path):
                        return db_path

            print(f"[DEBUG] 未找到可用的数据库路径配置")
            return None

        except Exception as e:
            print(f"[DEBUG] 获取数据库路径失败: {e}")
            return None

    def _run_simplified_analysis(self, query: str) -> Dict[str, Any]:
        """简化的查询分析 - 不依赖数据库路径"""
        print(f"[DEBUG] 使用简化分析模式")

        try:
            # 基于查询内容进行简单的业务逻辑分析
            sub_queries = self._simple_query_decomposition(query)

            return {
                'success': True,
                'original_query': query,
                'schema_analysis': {'mode': 'simplified'},
                'schema_type': 'unknown',
                'sub_queries': sub_queries,
                'analysis_summary': f'简化分析完成：拆解出{len(sub_queries)}个子查询'
            }

        except Exception as e:
            print(f"[ERROR] 简化分析失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'original_query': query
            }

    def _simple_query_decomposition(self, query: str) -> List[Dict[str, Any]]:
        """简单的查询拆解 - 基于关键词"""
        sub_queries = []

        # 银行业务查询的常见模式
        if "对公有效户" in query and "不良贷款" in query:
            sub_queries = [
                {
                    'id': 'effective_customers',
                    'query_text': '筛选对公有效户客户信息',
                    'priority': 1,
                    'description': '根据存款余额筛选有效客户'
                },
                {
                    'id': 'bad_loans',
                    'query_text': '筛选不良贷款合同信息',
                    'priority': 2,
                    'description': '根据五级分类筛选不良贷款'
                },
                {
                    'id': 'join_and_aggregate',
                    'query_text': '关联客户和贷款数据并汇总余额',
                    'priority': 3,
                    'description': '关联数据并按客户汇总不良贷款余额'
                }
            ]
        elif "各分行" in query and "贷款" in query:
            sub_queries = [
                {
                    'id': 'branch_loans',
                    'query_text': '统计各分行的贷款情况',
                    'priority': 1,
                    'description': '按分行统计贷款数据'
                }
            ]
        else:
            # 默认不拆解
            sub_queries = [
                {
                    'id': 'original_query',
                    'query_text': query,
                    'priority': 1,
                    'description': '原始查询（未拆解）'
                }
            ]

        return sub_queries

    def _determine_schema_type(self, schema_analysis: Dict[str, Any]) -> str:
        """确定数据库类型 - 使用DynamicSchemaExtractor"""
        try:
            # 如果有数据库路径，使用DynamicSchemaExtractor
            if 'database_path' in schema_analysis:
                from .dynamic_schema_extractor import determine_database_type

                database_path = schema_analysis['database_path']
                print(f"[DEBUG] 正在确定数据库类型: {database_path}")

                schema_type = determine_database_type(database_path)
                print(f"[DEBUG] 确定的Schema类型: {schema_type}")

                return schema_type

            # 如果没有数据库路径，返回未知类型
            print(f"[DEBUG] 缺少数据库路径信息，无法确定Schema类型")
            return 'unknown'

        except Exception as e:
            print(f"[DEBUG] 确定Schema类型失败: {e}")
            return 'unknown'

    def _decompose_to_subqueries(self, query: str, schema_analysis: Dict[str, Any], schema_type: str) -> List[Dict[str, Any]]:
        """智能拆解查询为多个具体的子查询"""
        try:
            print(f"[DEBUG] 开始智能拆解查询: {query}")

            # 使用LLM进行智能拆解
            sub_queries = self._llm_decompose_query(query, schema_analysis, schema_type)

            if sub_queries:
                print(f"[DEBUG] 智能拆解成功，生成 {len(sub_queries)} 个子查询")
                return sub_queries

            # 如果LLM拆解失败，返回原查询作为单个子查询
            print(f"[DEBUG] 智能拆解失败，保持原查询")
            return [{
                'id': 'original_query',
                'query_text': query,
                'priority': 1,
                'tables': [],
                'schema_type': schema_type,
                'description': '原始查询（未拆解）'
            }]

        except Exception as e:
            print(f"[DEBUG] 查询拆解异常: {e}")
            return [{
                'id': 'error_fallback',
                'query_text': query,
                'priority': 1,
                'tables': [],
                'schema_type': schema_type,
                'description': f'拆解失败: {str(e)}'
            }]

    def _llm_decompose_query(self, query: str, schema_analysis: Dict[str, Any], schema_type: str) -> List[Dict[str, Any]]:
        """使用LLM智能拆解查询"""
        try:
            if not LANGCHAIN_AVAILABLE:
                return []

            llm = self.get_llm()
            if not llm:
                return []

            # 构建拆解提示
            prompt = f"""
你是一个银行业务分析专家。请分析用户查询，理解业务术语的含义，并基于数据库结构自主推导出业务逻辑步骤。

【业务术语定义】
- 对公有效户：存款余额年日均大于等于10万元的对公客户
- 不良贷款余额：五级分类为次级、可疑、损失的贷款余额

【数据库类型】
{schema_type}

【分析任务】
请分析用户查询中涉及的业务概念，理解每个术语的具体含义，然后推导出实现这个查询需要的逻辑步骤。

【分析要求】
1. 识别查询中的关键业务术语
2. 理解每个术语的业务定义和数据表示
3. 推导出数据处理的逻辑顺序
4. 每个步骤要说明：做什么、用哪个表、什么条件
5. 返回自然语言描述，不是SQL

【用户查询】
{query}

请按照以下格式分析：
分析结果：
1. 业务术语识别：[识别出的关键术语]
2. 数据处理步骤：
   Step1: [第一步要做什么，基于什么业务逻辑]
   Step2: [第二步要做什么，基于什么业务逻辑]
   Step3: [第三步要做什么，基于什么业务逻辑]
3. 子查询列表：[具体的子查询描述]

请返回JSON格式的拆解结果：
{{
    "sub_queries": [
        {{
            "id": "具体的查询标识",
            "query_text": "具体的、简单的自然语言查询",
            "priority": 1,
            "description": "这个子查询的目的"
        }}
    ]
}}

示例：
输入: "分析各分行贷款"
输出: {{
    "sub_queries": [
        {{
            "id": "branch_loan_count",
            "query_text": "查询各分行的贷款客户数量",
            "priority": 1,
            "description": "统计每个分行有多少贷款客户"
        }},
        {{
            "id": "branch_loan_amount",
            "query_text": "查询各分行的贷款余额总额",
            "priority": 2,
            "description": "计算每个分行的贷款总金额"
        }},
        {{
            "id": "branch_avg_loan",
            "query_text": "查询各分行的平均贷款金额",
            "priority": 3,
            "description": "计算每个分行的平均贷款额度"
        }}
    ]
}}

注意：query_text必须是自然语言，不要生成SQL语句！

请严格按照JSON格式返回：
"""

            from langchain.schema.messages import HumanMessage
            response = llm.invoke([HumanMessage(content=prompt)])

            # 解析LLM响应
            import json
            try:
                # 清理响应内容，移除markdown标记
                content = response.content.strip()
                if content.startswith('```json'):
                    content = content[7:]  # 移除 ```json
                if content.endswith('```'):
                    content = content[:-3]  # 移除 ```
                content = content.strip()

                result = json.loads(content)
                sub_queries_data = result.get('sub_queries', [])

                # 转换为标准格式
                sub_queries = []
                for sq in sub_queries_data:
                    sub_query = {
                        'id': sq.get('id', f'query_{len(sub_queries)+1}'),
                        'query_text': sq.get('query_text', query),
                        'priority': sq.get('priority', len(sub_queries)+1),
                        'tables': schema_analysis.get('involved_tables', []),
                        'schema_type': schema_type,
                        'description': sq.get('description', '')
                    }
                    sub_queries.append(sub_query)

                return sub_queries

            except json.JSONDecodeError:
                print(f"[WARNING] LLM返回的JSON格式错误: {response.content}")
                return []

        except Exception as e:
            print(f"[WARNING] LLM拆解失败: {e}")
            return []





    def _run_with_database_path(self, query: str, database_path: str) -> Dict[str, Any]:
        """带数据库路径的查询分析 - 业务知识增强版"""
        print(f"[DEBUG] ===== QueryAnalysisTool: 业务知识增强查询分析 - {query} =====")
        print(f"[DEBUG] 数据库路径: {database_path}")

        try:
            # 第一步：获取统一配置
            from .unified_config import get_unified_config
            unified_config = get_unified_config()

            # 生成查询上下文
            query_context = unified_config.create_query_context(query, database_path)
            business_context = query_context.to_full_prompt()
            print(f"[DEBUG] 业务上下文生成完成")

            # 第二步：动态Schema分析（使用缓存）
            print(f"[DEBUG] 开始动态Schema分析...")
            schema_analysis = {'database_path': database_path, 'business_context': business_context}

            # 第三步：确定数据库类型（使用动态方法）
            schema_type = self._determine_schema_type(schema_analysis)
            print(f"[DEBUG] 动态确定的Schema类型: {schema_type}")

            # 第四步：业务知识增强的查询拆解
            sub_queries = self._business_enhanced_decompose_query(query, schema_analysis, schema_type, unified_config)
            print(f"[DEBUG] 业务增强拆解出 {len(sub_queries)} 个子查询")

            # 🚀 阶段1修复：返回明确的完成状态和下一步指导
            return {
                'success': True,
                'original_query': query,
                'schema_analysis': schema_analysis,
                'schema_type': schema_type,
                'sub_queries': sub_queries,
                'business_context': business_context,
                'analysis_summary': f'业务知识增强分析完成：类型={schema_type}，拆解出{len(sub_queries)}个子查询',
                'task_completed': True,  # 明确的完成标志
                'next_action': 'nl2sql_query',  # 指导下一步应该执行SQL查询
                'enhanced_query': ', '.join(sub_queries) + ', ' + query  # 提供增强后的查询
            }

        except Exception as e:
            print(f"[ERROR] 业务知识增强查询分析失败: {e}")
            import traceback
            traceback.print_exc()

            # 🚀 阶段1修复：错误情况也要明确完成状态
            return {
                'success': False,
                'error': str(e),
                'original_query': query,
                'task_completed': True,  # 即使失败也是完成状态
                'next_action': 'none',  # 错误时不需要下一步
                'summary': f"查询分析失败: {str(e)}"
            }

    def _business_enhanced_decompose_query(self, query: str, schema_analysis: Dict[str, Any],
                                          schema_type: str, unified_config) -> List[str]:
        """业务知识增强的查询拆解"""
        try:
            # 获取适用的查询范围规则
            scope_rules = unified_config.query_scope_rules

            # 构建业务知识增强的提示词
            business_context = schema_analysis.get('business_context', '')

            prompt = f"""
你是银行业务分析专家。请基于业务知识对用户查询进行智能分解。

{business_context}

【用户查询】
{query}

【数据库类型】
{schema_type}

【适用的业务规则】
"""

            for rule in scope_rules:
                prompt += f"- {rule.description}\n"
                prompt += f"  范围类型: {rule.scope_type}\n"
                prompt += f"  筛选条件: {rule.filter_conditions}\n"

            prompt += """

【分析要求】
1. 基于业务术语的精确定义进行分析
2. 根据查询范围规则确定数据范围
3. 按照业务逻辑步骤进行分解
4. 每个子查询要明确业务目标和数据来源

【输出格式】
请返回具体的子查询列表，每个子查询一行：
1. [第一个子查询]
2. [第二个子查询]
...

请开始分析：
"""

            # 调用LLM进行业务增强分析
            llm = self.get_llm()
            if not llm:
                # LLM不可用时的备用方案
                return self._fallback_decompose_query(query, schema_type)

            from langchain.schema.messages import HumanMessage
            response = llm.invoke([HumanMessage(content=prompt)])

            # 解析LLM响应
            sub_queries = []
            lines = response.content.strip().split('\n')

            for line in lines:
                line = line.strip()
                if line and (line.startswith(('1.', '2.', '3.', '4.', '5.')) or line.startswith('-')):
                    # 提取子查询内容
                    if '.' in line:
                        sub_query = line.split('.', 1)[1].strip()
                    elif line.startswith('-'):
                        sub_query = line[1:].strip()
                    else:
                        sub_query = line

                    if sub_query:
                        sub_queries.append(sub_query)

            return sub_queries if sub_queries else [query]

        except Exception as e:
            print(f"[WARNING] 业务增强查询拆解失败: {e}")
            return self._fallback_decompose_query(query, schema_type)

    def _fallback_decompose_query(self, query: str, schema_type: str) -> List[str]:
        """备用查询拆解方法"""
        # 简单的规则拆解
        if "对公有效户" in query and "不良贷款" in query:
            return [
                "筛选对公有效户客户信息",
                "筛选不良贷款合同信息",
                "关联客户和贷款数据并汇总余额"
            ]
        elif "各分行" in query and "贷款余额" in query:
            return [
                "统计各分行的贷款合同数量",
                "计算各分行的贷款余额总计"
            ]
        else:
            return [query]

    async def _arun(self, query: str) -> Dict[str, Any]:
        """异步执行"""
        return self._run(query)
