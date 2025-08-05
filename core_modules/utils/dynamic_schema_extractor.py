#!/usr/bin/env python3
"""
动态Schema提取器
从query_analyzer.py和intelligent_query_enhancer.py中提取的纯净Schema功能
"""

import sqlite3
import os
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
# 注意：schema_config_manager 已被删除，相关功能已移除


@dataclass
class TableSchema:
    """表结构信息"""
    table_name: str
    columns: List[Dict[str, str]]
    row_count: int
    sample_data: List[Dict[str, Any]]


@dataclass
class DatabaseSchema:
    """数据库结构信息"""
    database_path: str
    database_type: str
    tables: Dict[str, TableSchema]
    relationships: List[Dict[str, str]]


class DynamicSchemaExtractor:
    """纯净的动态Schema提取器 - 从其他组件中提取的功能"""

    def __init__(self):
        self.cache = {}  # 缓存提取的schema信息

    def extract_database_schema(self, database_path: str, use_cache: bool = True) -> DatabaseSchema:
        """提取数据库完整schema信息（支持高级缓存）"""
        # 检查内存缓存
        if database_path in self.cache:
            return self.cache[database_path]

        # 注意：Schema配置管理器已被删除，跳过配置检查

        print(f"[INFO] 开始分析数据库Schema: {database_path}")

        try:
            # 确定数据库类型
            database_type = self.determine_database_type(database_path)

            # 连接数据库
            conn = sqlite3.connect(database_path)
            cursor = conn.cursor()

            # 获取所有表名
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            table_names = [row[0] for row in cursor.fetchall()]

            tables = {}
            for table_name in table_names:
                table_schema = self._extract_table_schema(cursor, table_name)
                if table_schema:
                    tables[table_name] = table_schema

            # 提取表关系
            relationships = self._extract_relationships(cursor, table_names)

            conn.close()

            # 创建数据库schema对象
            db_schema = DatabaseSchema(
                database_path=database_path,
                database_type=database_type,
                tables=tables,
                relationships=relationships
            )

            # 缓存结果到内存
            self.cache[database_path] = db_schema

            # 注意：配置保存功能已被删除

            return db_schema

        except Exception as e:
            print(f"[ERROR] 提取数据库schema失败: {e}")
            return None

    def determine_database_type(self, database_path: str) -> str:
        """智能确定数据库类型 - 使用LLM分析"""
        try:
            # 首先尝试LLM智能分析
            llm_result = self._llm_determine_database_type(database_path)
            if llm_result and llm_result != 'unknown':
                return llm_result

            # LLM失败时使用简单规则作为备用
            return self._rule_based_determine_database_type(database_path)

        except Exception as e:
            print(f"[WARNING] 确定数据库类型失败: {e}")
            return 'unknown'

    def _llm_determine_database_type(self, database_path: str) -> str:
        """使用LLM智能确定数据库类型"""
        try:
            # 获取数据库基本信息
            conn = sqlite3.connect(database_path)
            cursor = conn.cursor()

            # 获取表名
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            table_names = [row[0] for row in cursor.fetchall()]

            # 获取每个表的字段信息和样本数据
            table_info = {}
            for table_name in table_names[:5]:  # 只分析前5个表
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [col[1] for col in cursor.fetchall()]

                # 获取样本数据
                try:
                    cursor.execute(f"SELECT * FROM {table_name} LIMIT 2")
                    sample_rows = cursor.fetchall()
                    table_info[table_name] = {
                        'columns': columns,
                        'sample_count': len(sample_rows),
                        'sample_data': sample_rows[:1] if sample_rows else []
                    }
                except:
                    table_info[table_name] = {
                        'columns': columns,
                        'sample_count': 0,
                        'sample_data': []
                    }

            conn.close()

            # 构建LLM分析提示
            filename = os.path.basename(database_path)

            table_descriptions = []
            for table_name, info in table_info.items():
                columns_str = ", ".join(info['columns'][:10])  # 只显示前10个字段
                table_descriptions.append(f"表名: {table_name}, 字段: {columns_str}, 数据行数: {info['sample_count']}")

            prompt = f"""
你是数据库专家。请根据数据库文件名和表结构信息，智能判断数据库的业务类型。

数据库文件名: {filename}

表结构信息:
{chr(10).join(table_descriptions)}

请从以下类型中选择最合适的一个：
1. loan_customer - 银行客户贷款相关数据库
2. annual_report - 银行年报财务数据库
3. unknown - 无法确定类型

判断依据：
- 如果包含客户、贷款、信贷、分行等相关表和字段，选择 loan_customer
- 如果包含年报、财务、资产、负债等相关表和字段，选择 annual_report
- 如果无法明确判断，选择 unknown

请只返回类型名称（loan_customer、annual_report 或 unknown），不要返回其他内容：
"""

            # 调用LLM
            llm = self._get_llm()
            if not llm:
                return 'unknown'

            from langchain.schema.messages import HumanMessage
            response = llm.invoke([HumanMessage(content=prompt)])
            result = response.content.strip().lower()

            # 验证返回结果
            valid_types = ['loan_customer', 'annual_report', 'unknown']
            if result in valid_types:
                print(f"[DEBUG] LLM智能确定数据库类型: {result}")
                return result
            else:
                print(f"[WARNING] LLM返回无效类型: {result}")
                return 'unknown'

        except Exception as e:
            print(f"[WARNING] LLM确定数据库类型失败: {e}")
            return 'unknown'

    def _rule_based_determine_database_type(self, database_path: str) -> str:
        """基于规则确定数据库类型（备用方案）"""
        try:
            # 根据文件名判断
            filename = os.path.basename(database_path).lower()

            if 'annual' in filename or 'report' in filename:
                return 'annual_report'
            elif 'loan' in filename or 'customer' in filename or 'bank_data' in filename:
                return 'loan_customer'

            # 根据表名判断
            conn = sqlite3.connect(database_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            table_names = [row[0].upper() for row in cursor.fetchall()]
            conn.close()

            # 检查特征表名
            if any('ANNUAL_REPORT' in name for name in table_names):
                return 'annual_report'
            elif any('CUST_INFO' in name or 'LOAN' in name for name in table_names):
                return 'loan_customer'

            return 'unknown'

        except Exception as e:
            print(f"[WARNING] 规则确定数据库类型失败: {e}")
            return 'unknown'

    def _get_llm(self):
        """获取LLM实例"""
        try:
            from langchain_openai import ChatOpenAI
            return ChatOpenAI(
                model="deepseek-chat",
                openai_api_key=os.getenv('DEEPSEEK_API_KEY'),
                openai_api_base="https://api.deepseek.com/v1",
                temperature=0.1
            )
        except Exception as e:
            print(f"[WARNING] LLM初始化失败: {e}")
            return None

    def build_schema_info_for_llm(self, database_path: str) -> str:
        """构建用于LLM的Schema信息 - 突出业务字段含义"""
        try:
            db_schema = self.extract_database_schema(database_path)
            if not db_schema:
                return ""

            # 构建业务导向的Schema信息
            table_info = "【数据库表结构 - 业务字段说明】\n"

            for table_name, table_schema in db_schema.tables.items():
                # 添加表的业务含义
                business_purpose = self._get_table_business_purpose(table_name)
                table_info += f"\n表名: {table_name} (共{table_schema.row_count}行) - {business_purpose}\n"

                # 分类显示字段
                key_fields, business_fields, other_fields = self._categorize_fields(table_schema.columns)

                if key_fields:
                    table_info += "关键字段:\n"
                    for column in key_fields:
                        business_meaning = self._get_field_business_meaning(column['字段名'])
                        table_info += f"  - {column['字段名']} ({column['数据类型']}) - {business_meaning}\n"

                if business_fields:
                    table_info += "业务字段:\n"
                    for column in business_fields[:5]:  # 限制显示数量
                        business_meaning = self._get_field_business_meaning(column['字段名'])
                        table_info += f"  - {column['字段名']} ({column['数据类型']}) - {business_meaning}\n"

                # 添加关键样本数据
                if table_schema.sample_data:
                    table_info += "关键数据示例:\n"
                    for sample in table_schema.sample_data[:1]:  # 只显示1行关键数据
                        key_sample = self._extract_key_sample_data(sample, key_fields + business_fields[:3])
                        table_info += f"  {key_sample}\n"

            # 构建业务关系信息
            relationship_info = ""
            if db_schema.relationships:
                relationship_info = "\n【表关联关系 - 业务逻辑】\n"
                for rel in db_schema.relationships[:3]:
                    business_context = self._get_relationship_business_context(rel)
                    relationship_info += f"- {rel['from_table']}.{rel['from_column']} → {rel['to_table']}.{rel['to_column']} ({business_context})\n"

            return table_info + relationship_info

        except Exception as e:
            print(f"[ERROR] 构建LLM Schema信息失败: {e}")
            return ""

    def _get_table_business_purpose(self, table_name: str) -> str:
        """使用LLM自主分析表的业务用途"""
        try:
            llm = self._get_llm()
            if not llm:
                return f'{table_name}表'

            prompt = f"""
你是银行业务数据专家。请分析表名的业务用途。

表名: {table_name}

请分析这个表在银行业务中的用途，考虑：
1. 表名的含义
2. 银行业务的数据分类
3. 可能存储的业务信息

要求：
- 只返回简洁的业务用途描述（不超过15字）
- 不要解释分析过程

请返回表的业务用途：
"""

            from langchain.schema.messages import HumanMessage
            response = llm.invoke([HumanMessage(content=prompt)])

            purpose = response.content.strip()
            if len(purpose) > 30:
                purpose = purpose[:30] + '...'

            return purpose if purpose else f'{table_name}表'

        except Exception as e:
            print(f"[WARNING] LLM表用途分析失败: {e}")
            return f'{table_name}表'

    def _categorize_fields(self, columns):
        """使用LLM自主分类字段 - 无硬编码规则"""
        try:
            # 使用LLM进行字段分类
            llm = self._get_llm()
            if not llm:
                # LLM不可用时的简单分类
                return self._simple_categorize_fields(columns)

            # 构建字段列表
            field_names = [col['字段名'] for col in columns]
            field_list = ", ".join(field_names)

            prompt = f"""
你是数据库专家。请将以下字段分类为三类：

字段列表: {field_list}

分类标准：
1. 关键字段：主键、外键、标识符、名称等核心识别字段
2. 业务字段：金额、余额、分类、状态等业务数据字段
3. 其他字段：辅助信息、描述性字段等

请返回JSON格式：
{{
    "key_fields": ["字段1", "字段2"],
    "business_fields": ["字段3", "字段4"],
    "other_fields": ["字段5", "字段6"]
}}

只返回JSON，不要其他解释：
"""

            from langchain.schema.messages import HumanMessage
            response = llm.invoke([HumanMessage(content=prompt)])

            # 解析LLM响应
            import json
            try:
                content = response.content.strip()
                if content.startswith('```json'):
                    content = content[7:]
                if content.endswith('```'):
                    content = content[:-3]
                content = content.strip()

                result = json.loads(content)

                # 根据LLM分类结果重新组织字段
                key_fields = []
                business_fields = []
                other_fields = []

                for column in columns:
                    field_name = column['字段名']
                    if field_name in result.get('key_fields', []):
                        key_fields.append(column)
                    elif field_name in result.get('business_fields', []):
                        business_fields.append(column)
                    else:
                        other_fields.append(column)

                return key_fields, business_fields, other_fields

            except json.JSONDecodeError:
                print(f"[WARNING] LLM字段分类JSON解析失败，使用简单分类")
                return self._simple_categorize_fields(columns)

        except Exception as e:
            print(f"[WARNING] LLM字段分类失败: {e}")
            return self._simple_categorize_fields(columns)

    def _simple_categorize_fields(self, columns):
        """简单的字段分类备用方案"""
        key_fields = []
        business_fields = []
        other_fields = []

        for column in columns:
            # 简单的启发式分类，但不依赖硬编码关键词
            if column.get('主键') == 'PRIMARY KEY':
                key_fields.append(column)
            elif column['数据类型'].upper() in ['INTEGER', 'REAL', 'NUMERIC']:
                business_fields.append(column)
            else:
                other_fields.append(column)

        return key_fields, business_fields, other_fields

    def _get_field_business_meaning(self, field_name: str) -> str:
        """使用LLM自主分析字段的业务含义 - 完全无硬编码"""
        try:
            # 使用LLM进行字段语义分析
            llm = self._get_llm()
            if not llm:
                return f'字段({field_name})'

            prompt = f"""
你是银行业务数据专家。请分析字段名的业务含义。

字段名: {field_name}

请分析这个字段在银行业务中可能代表什么含义，考虑：
1. 字段名的组成部分
2. 银行业务的常见概念
3. 可能的业务用途

要求：
- 只返回简洁的业务含义描述（不超过20字）
- 不要解释分析过程
- 如果不确定，返回通用描述

请返回字段的业务含义：
"""

            from langchain.schema.messages import HumanMessage
            response = llm.invoke([HumanMessage(content=prompt)])

            # 清理和验证响应
            meaning = response.content.strip()
            if len(meaning) > 50:  # 限制长度
                meaning = meaning[:50] + '...'

            return meaning if meaning else f'字段({field_name})'

        except Exception as e:
            print(f"[WARNING] LLM字段语义分析失败: {e}")
            return f'字段({field_name})'

    def _extract_key_sample_data(self, sample, key_fields):
        """提取关键样本数据"""
        key_data = []
        for field in key_fields[:3]:  # 只显示前3个关键字段
            field_name = field['字段名']
            if field_name in sample:
                value = sample[field_name]
                if isinstance(value, str) and len(value) > 20:
                    value = value[:20] + '...'
                key_data.append(f"{field_name}={value}")
        return ", ".join(key_data)

    def _get_relationship_business_context(self, relationship):
        """使用LLM自主分析关系的业务上下文"""
        try:
            llm = self._get_llm()
            if not llm:
                return '数据关联'

            from_table = relationship['from_table']
            to_table = relationship['to_table']
            from_column = relationship['from_column']
            to_column = relationship['to_column']

            prompt = f"""
你是银行业务数据专家。请分析表关联的业务含义。

关联关系: {from_table}.{from_column} → {to_table}.{to_column}

请分析这个关联在银行业务中的含义，考虑：
1. 两个表的业务用途
2. 关联字段的含义
3. 业务流程中的逻辑关系

要求：
- 只返回简洁的业务关联描述（不超过10字）
- 不要解释分析过程

请返回关联的业务含义：
"""

            from langchain.schema.messages import HumanMessage
            response = llm.invoke([HumanMessage(content=prompt)])

            context = response.content.strip()
            if len(context) > 20:
                context = context[:20] + '...'

            return context if context else '数据关联'

        except Exception as e:
            print(f"[WARNING] LLM关系分析失败: {e}")
            return '数据关联'

    def _extract_table_schema(self, cursor, table_name: str) -> TableSchema:
        """提取单个表的schema信息"""
        try:
            # 获取表结构
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns_info = cursor.fetchall()

            columns = []
            for col_info in columns_info:
                columns.append({
                    '字段名': col_info[1],
                    '数据类型': col_info[2],
                    '是否非空': 'NOT NULL' if col_info[3] else 'NULL',
                    '默认值': col_info[4] if col_info[4] else '',
                    '主键': 'PRIMARY KEY' if col_info[5] else ''
                })

            # 获取行数
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]

            # 获取样本数据
            sample_data = []
            try:
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
                rows = cursor.fetchall()
                column_names = [col['字段名'] for col in columns]

                for row in rows:
                    sample_record = {}
                    for i, value in enumerate(row):
                        if i < len(column_names):
                            # 截断长文本
                            if isinstance(value, str) and len(value) > 50:
                                value = value[:50] + '...'
                            sample_record[column_names[i]] = value
                    sample_data.append(sample_record)
            except Exception as e:
                print(f"[WARNING] 获取表 {table_name} 样本数据失败: {e}")

            return TableSchema(
                table_name=table_name,
                columns=columns,
                row_count=row_count,
                sample_data=sample_data
            )

        except Exception as e:
            print(f"[ERROR] 提取表 {table_name} schema失败: {e}")
            return None

    def _extract_relationships(self, cursor, table_names: List[str]) -> List[Dict[str, str]]:
        """提取表之间的关系"""
        relationships = []

        try:
            for table_name in table_names:
                # 获取外键信息
                cursor.execute(f"PRAGMA foreign_key_list({table_name})")
                fk_info = cursor.fetchall()

                for fk in fk_info:
                    relationships.append({
                        'from_table': table_name,
                        'from_column': fk[3],
                        'to_table': fk[2],
                        'to_column': fk[4],
                        'relationship_type': 'foreign_key'
                    })

            # 基于字段名推断可能的关系
            relationships.extend(self._infer_relationships(cursor, table_names))

        except Exception as e:
            print(f"[WARNING] 提取表关系失败: {e}")

        return relationships

    def _infer_relationships(self, cursor, table_names: List[str]) -> List[Dict[str, str]]:
        """智能推断表关系 - 使用LLM分析"""
        try:
            # 首先尝试LLM智能推断
            llm_relationships = self._llm_infer_relationships(cursor, table_names)
            if llm_relationships:
                return llm_relationships

            # LLM失败时使用规则推断作为备用
            return self._rule_based_infer_relationships(cursor, table_names)

        except Exception as e:
            print(f"[WARNING] 推断表关系失败: {e}")
            return []

    def _llm_infer_relationships(self, cursor, table_names: List[str]) -> List[Dict[str, str]]:
        """使用LLM智能推断表关系"""
        try:
            if len(table_names) < 2:
                return []

            # 获取表结构信息
            table_schemas = {}
            for table_name in table_names:
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns_info = cursor.fetchall()

                columns = []
                for col_info in columns_info:
                    columns.append({
                        'name': col_info[1],
                        'type': col_info[2],
                        'primary_key': bool(col_info[5])
                    })

                table_schemas[table_name] = columns

            # 构建LLM分析提示
            schema_descriptions = []
            for table_name, columns in table_schemas.items():
                column_strs = []
                for col in columns:
                    pk_mark = " (主键)" if col['primary_key'] else ""
                    column_strs.append(f"{col['name']} ({col['type']}){pk_mark}")

                schema_descriptions.append(f"表名: {table_name}\n字段: {', '.join(column_strs)}")

            prompt = f"""
你是数据库关系专家。请分析以下表结构，智能推断表之间可能的关联关系。

表结构信息:
{chr(10).join(schema_descriptions)}

请分析：
1. 哪些字段可能是外键关系
2. 字段名相似或相同的可能关联
3. 业务逻辑上的合理关联

请返回JSON格式的关系列表：
{{
    "relationships": [
        {{
            "from_table": "表名1",
            "from_column": "字段名1",
            "to_table": "表名2",
            "to_column": "字段名2",
            "relationship_type": "inferred",
            "confidence": "high/medium/low",
            "reason": "推断原因"
        }}
    ]
}}

注意：
- 只返回有合理依据的关系
- confidence表示推断的可信度
- reason简要说明推断依据
- 如果没有明显关系，返回空的relationships数组

请返回JSON格式：
"""

            # 调用LLM
            llm = self._get_llm()
            if not llm:
                return []

            from langchain.schema.messages import HumanMessage
            response = llm.invoke([HumanMessage(content=prompt)])

            # 解析LLM响应
            import json
            try:
                # 清理响应内容
                content = response.content.strip()
                if content.startswith('```json'):
                    content = content[7:]
                if content.endswith('```'):
                    content = content[:-3]
                content = content.strip()

                result = json.loads(content)
                relationships_data = result.get('relationships', [])

                # 转换为标准格式
                relationships = []
                for rel in relationships_data:
                    relationship = {
                        'from_table': rel.get('from_table', ''),
                        'from_column': rel.get('from_column', ''),
                        'to_table': rel.get('to_table', ''),
                        'to_column': rel.get('to_column', ''),
                        'relationship_type': 'llm_inferred',
                        'confidence': rel.get('confidence', 'medium'),
                        'reason': rel.get('reason', '')
                    }
                    relationships.append(relationship)

                print(f"[DEBUG] LLM智能推断出 {len(relationships)} 个表关系")
                return relationships

            except json.JSONDecodeError:
                print(f"[WARNING] LLM返回的JSON格式错误: {response.content}")
                return []

        except Exception as e:
            print(f"[WARNING] LLM推断表关系失败: {e}")
            return []

    def _rule_based_infer_relationships(self, cursor, table_names: List[str]) -> List[Dict[str, str]]:
        """基于规则推断表关系（备用方案）"""
        inferred_relationships = []

        try:
            # 获取所有表的字段信息
            table_columns = {}
            for table_name in table_names:
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [col[1] for col in cursor.fetchall()]
                table_columns[table_name] = columns

            # 智能字段匹配（不使用硬编码模式）
            for table1, columns1 in table_columns.items():
                for table2, columns2 in table_columns.items():
                    if table1 >= table2:  # 避免重复比较
                        continue

                    # 寻找相同或相似的字段名
                    for col1 in columns1:
                        for col2 in columns2:
                            # 完全相同的字段名
                            if col1.lower() == col2.lower():
                                inferred_relationships.append({
                                    'from_table': table1,
                                    'from_column': col1,
                                    'to_table': table2,
                                    'to_column': col2,
                                    'relationship_type': 'name_match'
                                })
                            # 包含ID的字段匹配
                            elif ('id' in col1.lower() and 'id' in col2.lower() and
                                  len(col1) > 2 and len(col2) > 2):
                                # 检查是否有共同的前缀或后缀
                                col1_clean = col1.lower().replace('_', '').replace('id', '')
                                col2_clean = col2.lower().replace('_', '').replace('id', '')

                                if col1_clean and col2_clean and (
                                    col1_clean in col2_clean or col2_clean in col1_clean
                                ):
                                    inferred_relationships.append({
                                        'from_table': table1,
                                        'from_column': col1,
                                        'to_table': table2,
                                        'to_column': col2,
                                        'relationship_type': 'id_pattern_match'
                                    })

        except Exception as e:
            print(f"[WARNING] 规则推断表关系失败: {e}")

        return inferred_relationships


# 全局实例
_schema_extractor = None

def get_schema_extractor() -> DynamicSchemaExtractor:
    """获取全局schema提取器实例"""
    global _schema_extractor
    if _schema_extractor is None:
        _schema_extractor = DynamicSchemaExtractor()
    return _schema_extractor

def extract_database_schema(database_path: str) -> DatabaseSchema:
    """提取数据库schema的便捷函数"""
    extractor = get_schema_extractor()
    return extractor.extract_database_schema(database_path)

def determine_database_type(database_path: str) -> str:
    """确定数据库类型的便捷函数"""
    extractor = get_schema_extractor()
    return extractor.determine_database_type(database_path)

def build_schema_info_for_llm(database_path: str) -> str:
    """构建用于LLM的Schema信息的便捷函数"""
    extractor = get_schema_extractor()
    return extractor.build_schema_info_for_llm(database_path)
