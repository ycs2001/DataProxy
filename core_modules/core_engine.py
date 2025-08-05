#!/usr/bin/env python3
"""
DataProxy 统一核心引擎

替代复杂的多层工具架构，提供简单、直接、高效的数据查询和分析功能。
遵循KISS原则，专注核心功能，消除过度工程化。
"""

import os
import time
import sqlite3
import hashlib
from typing import Dict, Any, List, Optional
from pathlib import Path


class CoreDataEngine:
    """
    DataProxy 统一核心引擎
    
    设计原则：
    1. 单一入口 - 所有查询通过一个接口
    2. 直接处理 - 无中间层转换
    3. 内置功能 - NL2SQL、分析、可视化一体化
    4. 简单配置 - 基于数据库路径自动配置
    """
    
    def __init__(self, database_path: str):
        """
        初始化核心引擎
        
        Args:
            database_path: 数据库文件路径
        """
        self.database_path = database_path
        self.database_name = Path(database_path).stem
        
        # 简化的配置
        self.business_terms = self._load_business_terms()
        self.schema_info = self._load_schema_info()
        
        # LLM客户端
        self.llm_client = self._init_llm()
        
        print(f"[INFO] CoreDataEngine 初始化完成: {self.database_name}")
    
    def query(self, user_query: str, analysis_mode: str = "auto") -> Dict[str, Any]:
        """
        统一查询接口
        
        Args:
            user_query: 用户自然语言查询
            analysis_mode: 分析模式 (auto|simple|detailed)
            
        Returns:
            统一的查询结果
        """
        start_time = time.time()
        
        try:
            print(f"[INFO] 处理查询: {user_query}")
            
            # 1. NL2SQL转换和执行
            sql_result = self._process_nl2sql(user_query)
            
            if not sql_result['success']:
                return sql_result
            
            # 2. 数据分析（根据模式）
            if analysis_mode in ["auto", "detailed"] and len(sql_result['data']) > 0:
                sql_result['statistics'] = self._generate_statistics(sql_result['data'])
                sql_result['insights'] = self._generate_insights(sql_result['data'], user_query)
            
            # 3. 可视化准备
            if len(sql_result['data']) > 1:
                sql_result['visualization'] = self._prepare_visualization(sql_result['data'], user_query)
            
            # 4. 添加元数据
            sql_result['execution_time'] = time.time() - start_time
            sql_result['database'] = self.database_name
            sql_result['query_mode'] = analysis_mode
            
            print(f"[INFO] 查询完成，耗时: {sql_result['execution_time']:.2f}秒")
            return sql_result
            
        except Exception as e:
            print(f"[ERROR] 查询处理失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'execution_time': time.time() - start_time,
                'database': self.database_name
            }
    
    def _process_nl2sql(self, user_query: str) -> Dict[str, Any]:
        """NL2SQL处理 - 简化版本"""
        try:
            # 1. 生成SQL
            sql = self._generate_sql(user_query)
            
            if not sql:
                return {
                    'success': False,
                    'error': 'SQL生成失败',
                    'data': []
                }
            
            # 2. 执行SQL
            data, record_count = self._execute_sql(sql)
            
            return {
                'success': True,
                'sql': sql,
                'data': data,
                'record_count': record_count
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'NL2SQL处理失败: {e}',
                'data': []
            }
    
    def _generate_sql(self, user_query: str) -> Optional[str]:
        """生成SQL - 使用LLM"""
        try:
            # 构建提示词
            prompt = self._build_sql_prompt(user_query)
            
            # 调用LLM
            response = self._call_llm(prompt)
            
            # 提取SQL
            sql = self._extract_sql_from_response(response)
            
            print(f"[DEBUG] 生成SQL: {sql}")
            return sql
            
        except Exception as e:
            print(f"[ERROR] SQL生成失败: {e}")
            return None
    
    def _build_sql_prompt(self, user_query: str) -> str:
        """构建SQL生成提示词"""
        # 获取表结构信息
        tables_info = []
        for table_name, table_info in self.schema_info.items():
            columns = ", ".join([f"{col['name']}({col['type']})" for col in table_info['columns']])
            tables_info.append(f"表 {table_name}: {columns}")
        
        schema_text = "\n".join(tables_info)
        
        # 业务术语
        terms_text = "\n".join([f"- {term}: {definition}" for term, definition in self.business_terms.items()])
        
        prompt = f"""你是一个SQL专家，请根据用户查询生成准确的SQL语句。

数据库结构:
{schema_text}

业务术语定义:
{terms_text}

用户查询: {user_query}

请生成对应的SQL查询语句，只返回SQL代码，不要其他解释。
SQL语句要求:
1. 使用正确的表名和字段名
2. 应用相关的业务规则
3. 确保语法正确
4. 适当使用JOIN关联表

SQL:"""
        
        return prompt
    
    def _call_llm(self, prompt: str) -> str:
        """调用LLM"""
        try:
            if not self.llm_client:
                raise Exception("LLM客户端未初始化")
            
            from langchain.schema.messages import HumanMessage
            response = self.llm_client.invoke([HumanMessage(content=prompt)])
            return response.content
            
        except Exception as e:
            raise Exception(f"LLM调用失败: {e}")
    
    def _extract_sql_from_response(self, response: str) -> str:
        """从LLM响应中提取SQL"""
        # 移除markdown格式
        if "```sql" in response:
            sql = response.split("```sql")[1].split("```")[0].strip()
        elif "```" in response:
            sql = response.split("```")[1].strip()
        else:
            sql = response.strip()
        
        # 清理SQL
        sql = sql.replace("SQL:", "").strip()
        
        return sql
    
    def _execute_sql(self, sql: str) -> tuple:
        """执行SQL查询"""
        try:
            conn = sqlite3.connect(self.database_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute(sql)
            rows = cursor.fetchall()
            
            # 转换为字典列表
            data = [dict(row) for row in rows]
            record_count = len(data)
            
            conn.close()
            
            print(f"[DEBUG] SQL执行成功，返回 {record_count} 条记录")
            return data, record_count
            
        except Exception as e:
            print(f"[ERROR] SQL执行失败: {e}")
            return [], 0
    
    def _generate_statistics(self, data: List[Dict]) -> Dict[str, Any]:
        """生成统计信息 - 简化版本"""
        if not data:
            return {}
        
        try:
            import pandas as pd
            df = pd.DataFrame(data)
            
            stats = {
                'total_records': len(data),
                'columns': list(df.columns),
                'numeric_summary': {}
            }
            
            # 数值列统计
            numeric_cols = df.select_dtypes(include=['number']).columns
            for col in numeric_cols:
                stats['numeric_summary'][col] = {
                    'count': int(df[col].count()),
                    'sum': float(df[col].sum()),
                    'mean': float(df[col].mean()),
                    'min': float(df[col].min()),
                    'max': float(df[col].max())
                }
            
            return stats
            
        except Exception as e:
            print(f"[WARN] 统计生成失败: {e}")
            return {'total_records': len(data)}
    
    def _generate_insights(self, data: List[Dict], user_query: str) -> List[str]:
        """生成业务洞察 - 简化版本"""
        insights = []
        
        try:
            if not data:
                return ["查询未返回数据"]
            
            # 基本洞察
            insights.append(f"查询返回 {len(data)} 条记录")
            
            # 数值分析洞察
            if len(data) > 1:
                import pandas as pd
                df = pd.DataFrame(data)
                numeric_cols = df.select_dtypes(include=['number']).columns
                
                for col in numeric_cols:
                    if df[col].count() > 0:
                        max_val = df[col].max()
                        min_val = df[col].min()
                        insights.append(f"{col}: 最大值 {max_val}, 最小值 {min_val}")
            
            return insights[:5]  # 最多5个洞察
            
        except Exception as e:
            print(f"[WARN] 洞察生成失败: {e}")
            return [f"数据分析完成，共 {len(data)} 条记录"]
    
    def _prepare_visualization(self, data: List[Dict], user_query: str) -> Dict[str, Any]:
        """准备可视化数据 - 简化版本"""
        try:
            import pandas as pd
            df = pd.DataFrame(data)
            
            viz_data = {
                'chart_ready': True,
                'data_tables': {
                    'main': {
                        'data': data,
                        'columns': list(df.columns),
                        'record_count': len(data)
                    }
                },
                'suggested_charts': []
            }
            
            # 简单的图表建议
            numeric_cols = df.select_dtypes(include=['number']).columns
            categorical_cols = df.select_dtypes(include=['object']).columns
            
            if len(categorical_cols) > 0 and len(numeric_cols) > 0:
                viz_data['suggested_charts'] = ['bar', 'pie']
            elif len(numeric_cols) > 1:
                viz_data['suggested_charts'] = ['line', 'scatter']
            else:
                viz_data['suggested_charts'] = ['table']
            
            return viz_data
            
        except Exception as e:
            print(f"[WARN] 可视化准备失败: {e}")
            return {'chart_ready': False}
    
    def _load_business_terms(self) -> Dict[str, str]:
        """加载业务术语 - 简化版本"""
        return {
            "对公有效户": "corp_deposit_y_avg_bal >= 100000",
            "不良贷款": "CONTRACT_CL_RESULT IN (2, 3, 4)",
            "个人客户": "customer_type = '个人'",
            "对公客户": "customer_type = '对公'"
        }
    
    def _load_schema_info(self) -> Dict[str, Any]:
        """加载数据库schema - 自动提取"""
        try:
            conn = sqlite3.connect(self.database_path)
            cursor = conn.cursor()
            
            # 获取所有表
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            
            schema_info = {}
            for (table_name,) in tables:
                # 获取表结构
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                
                schema_info[table_name] = {
                    'columns': [
                        {
                            'name': col[1],
                            'type': col[2],
                            'not_null': bool(col[3]),
                            'primary_key': bool(col[5])
                        }
                        for col in columns
                    ]
                }
            
            conn.close()
            print(f"[DEBUG] Schema加载完成，共 {len(schema_info)} 个表")
            return schema_info
            
        except Exception as e:
            print(f"[ERROR] Schema加载失败: {e}")
            return {}
    
    def _init_llm(self):
        """初始化LLM客户端"""
        try:
            from langchain_openai import ChatOpenAI
            
            api_key = os.getenv('DEEPSEEK_API_KEY')
            if not api_key:
                print("[WARN] 未找到DEEPSEEK_API_KEY，LLM功能不可用")
                return None
            
            client = ChatOpenAI(
                model="deepseek-chat",
                openai_api_key=api_key,
                openai_api_base="https://api.deepseek.com",
                temperature=0.1
            )
            
            print("[DEBUG] LLM客户端初始化成功")
            return client
            
        except Exception as e:
            print(f"[ERROR] LLM初始化失败: {e}")
            return None


# 便捷函数
def create_core_engine(database_path: str) -> CoreDataEngine:
    """创建核心引擎实例"""
    return CoreDataEngine(database_path)


def quick_query(database_path: str, query: str, mode: str = "auto") -> Dict[str, Any]:
    """快速查询函数"""
    engine = create_core_engine(database_path)
    return engine.query(query, mode)
