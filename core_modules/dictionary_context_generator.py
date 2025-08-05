#!/usr/bin/env python3
"""
数据字典驱动的上下文文件生成器
直接导入业务数据，利用数据字典生成丰富的上下文配置文件
"""

import os
import sqlite3
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
import json
import requests
from datetime import datetime
from pathlib import Path

class DictionaryContextGenerator:
    """数据字典驱动的上下文文件生成器"""

    def __init__(self, output_db_path: str, api_key: Optional[str] = None):
        self.output_db_path = output_db_path
        self.api_key = api_key or os.getenv('DEEPSEEK_API_KEY')
        self.dictionary_files = []  # 数据字典文件信息
        self.business_data_files = []  # 业务数据文件信息
        self.imported_tables = []  # 导入的表信息
        self.business_terms = {}  # 业务术语词典
        self.field_descriptions = {}  # 字段描述映射
        self.query_scope_rules = []  # 查询范围规则
        self.database_description = {}  # 数据库描述信息
        self.table_name_config = self._load_table_name_config()  # 表名映射配置

    def _load_table_name_config(self) -> Dict[str, Any]:
        """加载表名映射配置"""
        try:
            config_path = "configs/table_name_mappings.json"
            if os.path.exists(config_path):
                with open(config_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                print(f"⚠️ 表名映射配置文件不存在: {config_path}")
                return {}
        except Exception as e:
            print(f"⚠️ 加载表名映射配置失败: {e}")
            return {}

    def generate_database_with_context(self, data_dir: str) -> Dict[str, Any]:
        """数据字典驱动的数据库和上下文生成工作流"""
        print(f"🏗️ 开始数据字典驱动的数据库和上下文生成...")
        print(f"📁 数据目录: {data_dir}")
        print(f"📁 输出数据库: {self.output_db_path}")

        try:
            # 步骤1：直接导入业务数据到数据库（保持原始结构）
            self._step1_import_business_data_directly(data_dir)

            # 步骤2：读取和分析数据字典
            self._step2_analyze_data_dictionaries(data_dir)

            # 步骤3：使用LLM生成业务术语词典
            self._step3_generate_business_terms_with_llm()

            # 步骤4：生成完整的数据库上下文配置文件
            self._step4_generate_context_configuration()

            # 生成报告
            report = self._generate_comprehensive_report()
            print(f"✅ 数据库和上下文生成完成！")
            print(f"   📊 导入表数: {len(self.imported_tables)}")
            print(f"   📚 业务术语: {len(self.business_terms)}")
            print(f"   📋 字段描述: {len(self.field_descriptions)}")
            print(f"   📏 查询规则: {len(self.query_scope_rules)}")

            return report

        except Exception as e:
            print(f"❌ 数据库和上下文生成失败: {e}")
            raise
    
    def _step1_import_business_data_directly(self, data_dir: str):
        """步骤1：直接导入业务数据到数据库（保持原始结构）"""
        print(f"\n📊 步骤1：直接导入业务数据（保持原始结构）")

        # 查找业务数据文件（非数据字典文件）
        business_files = []
        for root, dirs, files in os.walk(data_dir):
            for file in files:
                if file.endswith(('.xlsx', '.xls', '.csv')) and '数据字典' not in file:
                    file_path = os.path.join(root, file)
                    business_files.append(file_path)

        print(f"📊 发现 {len(business_files)} 个业务数据文件")

        # 创建数据库连接
        conn = sqlite3.connect(self.output_db_path)

        try:
            for business_file in business_files:
                file_name = os.path.basename(business_file)
                print(f"📊 导入业务数据: {file_name}")

                try:
                    # 读取数据文件
                    if file_name.endswith('.csv'):
                        df = pd.read_csv(business_file)
                    else:
                        df = pd.read_excel(business_file)

                    # 生成表名（基于文件名）
                    table_name = self._generate_table_name_from_filename(file_name)

                    # 直接导入到数据库，保持原始字段名和结构
                    df.fillna('').to_sql(table_name, conn, if_exists='replace', index=False)

                    # 记录导入信息
                    table_info = {
                        'table_name': table_name,
                        'source_file': file_name,
                        'source_path': business_file,
                        'rows': len(df),
                        'columns': len(df.columns),
                        'column_names': list(df.columns),
                        'chinese_name': self._extract_chinese_name_from_filename(file_name)
                    }
                    self.imported_tables.append(table_info)
                    self.business_data_files.append(table_info)

                    print(f"✅ 导入成功: {table_name} ({len(df)}行, {len(df.columns)}列)")

                except Exception as e:
                    print(f"❌ 导入失败: {file_name} - {e}")

        finally:
            conn.close()

        print(f"📊 业务数据导入完成，共导入 {len(self.imported_tables)} 个表")

    def _generate_table_name_from_filename(self, filename: str) -> str:
        """智能生成标准化的表名 - 支持多种银行机构和命名规范"""
        try:
            # 1. 首先尝试配置文件映射
            table_name = self._try_config_mapping(filename)
            if table_name:
                print(f"📋 使用配置映射: {filename} -> {table_name}")
                return table_name

            # 2. 尝试LLM智能推断
            table_name = self._try_llm_table_name_inference(filename)
            if table_name:
                print(f"🧠 LLM推断表名: {filename} -> {table_name}")
                return table_name

            # 3. 使用模式匹配（保持向后兼容）
            table_name = self._try_pattern_matching(filename)
            if table_name:
                print(f"🔍 模式匹配: {filename} -> {table_name}")
                return table_name

            # 4. 回退到通用命名规则
            fallback_name = self._generate_fallback_table_name(filename)
            print(f"⚡ 使用回退方案: {filename} -> {fallback_name}")
            return fallback_name

        except Exception as e:
            print(f"⚠️ 表名生成失败，使用回退方案: {e}")
            return self._generate_fallback_table_name(filename)

    def _try_config_mapping(self, filename: str) -> Optional[str]:
        """尝试使用配置文件映射"""
        try:
            if not self.table_name_config:
                return None

            # 清理文件名用于匹配
            clean_filename = filename.replace('.xlsx', '').replace('.xls', '').replace('.csv', '')

            # 精确匹配
            exact_matches = self.table_name_config.get('exact_matches', {})
            if clean_filename in exact_matches:
                return exact_matches[clean_filename]

            # 模式匹配
            pattern_matches = self.table_name_config.get('pattern_matches', {})
            for pattern, table_name in pattern_matches.items():
                if pattern in clean_filename:
                    return table_name

            return None

        except Exception as e:
            print(f"⚠️ 配置文件映射失败: {e}")
            return None

    def _try_llm_table_name_inference(self, filename: str) -> Optional[str]:
        """使用LLM智能推断表名"""
        if not self.api_key:
            return None

        try:
            # 构建LLM提示词
            prompt = f"""
请基于文件名生成一个标准的英文数据库表名。

文件名: {filename}

要求:
1. 表名必须是英文，使用下划线分隔
2. 表名应该反映文件的业务含义
3. 遵循数据库命名规范（小写字母、数字、下划线）
4. 长度不超过50个字符
5. 只返回表名，不要其他文字

示例:
- "客户信息表.xlsx" -> "customer_info"
- "LOAN_CONTRACT_DATA.xlsx" -> "loan_contract_data"
- "存款余额统计.xlsx" -> "deposit_balance"

请为以下文件生成表名: {filename}
"""

            response = self._call_llm_api(prompt, max_retries=2)
            if response:
                # 清理LLM响应，提取表名
                table_name = self._clean_llm_table_name_response(response)
                if table_name and self._validate_table_name(table_name):
                    return table_name

            return None

        except Exception as e:
            print(f"⚠️ LLM表名推断失败: {e}")
            return None

    def _try_pattern_matching(self, filename: str) -> Optional[str]:
        """使用配置文件的模式匹配（仅限配置文件中定义的模式）"""
        if not self.table_name_config:
            return None

        name = filename.replace('.xlsx', '').replace('.xls', '').replace('.csv', '')

        # 只使用配置文件中定义的模式匹配
        pattern_matches = self.table_name_config.get('pattern_matches', {})
        for pattern, table_name in pattern_matches.items():
            if pattern in name:
                return table_name

        return None

    def _generate_fallback_table_name(self, filename: str) -> str:
        """生成简洁的回退表名"""
        import re

        # 移除扩展名
        name = filename.replace('.xlsx', '').replace('.xls', '').replace('.csv', '')

        # 提取英文部分（如果有）
        english_part = ''.join(c for c in name if ord(c) < 128)
        if english_part:
            name = english_part

        # 清理和标准化
        clean_name = re.sub(r'[^\w]', '_', name.lower())
        clean_name = re.sub(r'_+', '_', clean_name).strip('_')

        # 限制长度并确保有效
        if len(clean_name) > 50:
            clean_name = clean_name[:50].rstrip('_')

        return clean_name if clean_name and clean_name[0].isalpha() else 'data_table'

    def _clean_llm_table_name_response(self, response: str) -> Optional[str]:
        """清理LLM响应，提取表名"""
        if not response:
            return None

        # 移除多余的文字，只保留表名
        lines = response.strip().split('\n')
        for line in lines:
            line = line.strip()
            if line and not line.startswith(('请', '表名', '建议', '推荐')):
                # 移除可能的前缀
                line = line.replace('表名:', '').replace('Table name:', '').strip()
                if self._validate_table_name(line):
                    return line

        return None

    def _validate_table_name(self, table_name: str) -> bool:
        """验证表名是否符合规范"""
        import re

        if not table_name:
            return False

        # 检查长度
        if len(table_name) > 50:
            return False

        # 检查字符规范（只允许字母、数字、下划线）
        if not re.match(r'^[a-z][a-z0-9_]*$', table_name):
            return False

        # 检查是否以字母开头
        if not table_name[0].isalpha():
            return False

        return True

    def _extract_chinese_name_from_filename(self, filename: str) -> str:
        """从文件名提取中文名称"""
        name = filename.replace('.xlsx', '').replace('.xls', '').replace('.csv', '')

        # 提取中文部分
        chinese_chars = ''.join(c for c in name if '\u4e00' <= c <= '\u9fff' or c in '（）()，,。.')

        if chinese_chars:
            return chinese_chars.strip('（）(),，。.')
        else:
            return name

    def _step2_analyze_data_dictionaries(self, data_dir: str):
        """步骤2：读取和分析数据字典"""
        print(f"\n📚 步骤2：读取和分析数据字典")

        # 查找数据字典文件
        dict_files = []
        for root, dirs, files in os.walk(data_dir):
            for file in files:
                if file.endswith(('.xlsx', '.xls')) and '数据字典' in file:
                    file_path = os.path.join(root, file)
                    dict_files.append(file_path)

        print(f"📚 发现 {len(dict_files)} 个数据字典文件")

        for dict_file in dict_files:
            file_name = os.path.basename(dict_file)
            print(f"📚 分析数据字典: {file_name}")

            try:
                # 读取数据字典Excel文件
                df = pd.read_excel(dict_file)

                # 提取表名（从文件名）
                table_name = self._extract_table_name_from_dict_file(file_name)

                # 分析字典内容，提取字段信息
                field_info = self._extract_field_info_from_dictionary(df, table_name)

                # 存储数据字典信息
                dict_info = {
                    'file_name': file_name,
                    'file_path': dict_file,
                    'table_name': table_name,
                    'field_info': field_info,
                    'raw_data': df
                }
                self.dictionary_files.append(dict_info)

                print(f"✅ 分析完成: {file_name} - {len(field_info)} 个字段定义")

            except Exception as e:
                print(f"❌ 分析失败: {file_name} - {e}")

        print(f"📚 数据字典分析完成，共分析 {len(self.dictionary_files)} 个字典文件")

    def _extract_table_name_from_dict_file(self, dict_file_name: str) -> str:
        """从数据字典文件名智能提取表名"""
        # 清理文件名
        name = dict_file_name.replace('数据字典-', '').replace('.xlsx', '').replace('.xls', '')

        # 使用已有的智能表名生成方法
        return self._generate_table_name_from_filename(name + '.xlsx')

    def _extract_field_info_from_dictionary(self, df: pd.DataFrame, table_name: str) -> List[Dict]:
        """从数据字典DataFrame中提取字段信息"""
        field_info = []

        # 尝试识别数据字典的结构
        # 常见的列名：字段名、字段中文名、数据类型、长度、是否主键、是否可为空、注释/说明

        for index, row in df.iterrows():
            try:
                # 跳过表头行
                if index == 0 and any(keyword in str(row.iloc[0]) for keyword in ['字段', '名称', '类型']):
                    continue

                # 提取字段信息
                field_data = {}

                # 尝试从不同列提取信息
                for col_idx, value in enumerate(row):
                    if pd.isna(value):
                        continue

                    value_str = str(value).strip()
                    if not value_str:
                        continue

                    # 根据列的位置和内容推断字段信息
                    if col_idx == 0 and value_str:  # 第一列通常是字段名
                        field_data['field_name'] = value_str
                    elif col_idx == 1 and value_str:  # 第二列通常是中文名或说明
                        field_data['chinese_name'] = value_str
                    elif col_idx == 2 and value_str:  # 第三列通常是数据类型
                        field_data['data_type'] = value_str
                    elif col_idx == 3 and value_str:  # 第四列可能是长度
                        field_data['length'] = value_str
                    elif '说明' in str(df.columns[col_idx]) or '注释' in str(df.columns[col_idx]):
                        field_data['description'] = value_str

                # 如果提取到了字段名，则添加到列表
                if 'field_name' in field_data and field_data['field_name']:
                    field_data['table_name'] = table_name
                    field_info.append(field_data)

            except Exception as e:
                print(f"⚠️ 解析字段信息时出错 (行 {index}): {e}")
                continue

        return field_info

    def _step3_generate_business_terms_with_llm(self):
        """步骤3：使用LLM生成业务术语词典"""
        print(f"\n🧠 步骤3：使用LLM生成业务术语词典")

        if not self.api_key:
            print("⚠️ 未配置API密钥，跳过LLM分析")
            self._generate_basic_business_terms()
            return

        if not self.dictionary_files:
            print("⚠️ 没有数据字典文件，跳过业务术语生成")
            return

        # 为每个数据字典生成业务术语
        for dict_info in self.dictionary_files:
            table_name = dict_info['table_name']
            field_info = dict_info['field_info']

            print(f"🧠 为表 {table_name} 生成业务术语...")

            # 构建LLM提示词
            field_summary = ""
            for field in field_info:
                field_name = field.get('field_name', '')
                chinese_name = field.get('chinese_name', '')
                description = field.get('description', '')
                data_type = field.get('data_type', '')

                field_summary += f"- {field_name}: {chinese_name}"
                if description:
                    field_summary += f" ({description})"
                if data_type:
                    field_summary += f" [{data_type}]"
                field_summary += "\n"

            prompt = f"""
请基于以下银行业务数据字典，生成业务术语词典和字段描述。

表名: {table_name}
字段信息:
{field_summary}

请返回JSON格式：
{{
  "business_terms": {{
    "中文业务术语1": "对应的英文字段名或计算规则",
    "中文业务术语2": "对应的英文字段名或计算规则"
  }},
  "field_descriptions": {{
    "字段名1": {{
      "chinese_name": "中文名称",
      "business_meaning": "业务含义说明",
      "calculation_rule": "计算规则（如果适用）",
      "data_source": "数据来源说明"
    }}
  }},
  "query_rules": [
    {{
      "rule_type": "范围限制|数据过滤|业务规则",
      "description": "规则描述",
      "condition": "具体条件"
    }}
  ]
}}

要求：
1. 业务术语要包含银行常用术语（如：对公有效户、不良贷款余额等）
2. 字段描述要详细说明业务含义
3. 查询规则要基于银行业务逻辑
4. 只返回JSON，不要其他文字
"""

            try:
                response = self._call_llm_api(prompt)
                if response:
                    result = json.loads(self._clean_llm_response(response))

                    # 合并业务术语
                    if 'business_terms' in result:
                        self.business_terms.update(result['business_terms'])

                    # 合并字段描述
                    if 'field_descriptions' in result:
                        for field_name, desc in result['field_descriptions'].items():
                            self.field_descriptions[f"{table_name}.{field_name}"] = desc

                    # 合并查询规则
                    if 'query_rules' in result:
                        for rule in result['query_rules']:
                            rule['table_name'] = table_name
                            self.query_scope_rules.append(rule)

                    print(f"✅ 业务术语生成成功: {table_name}")
                else:
                    print(f"❌ 业务术语生成失败: {table_name}")

            except Exception as e:
                print(f"❌ 业务术语生成异常: {table_name} - {e}")

        print(f"🧠 业务术语生成完成")
        print(f"   📚 业务术语: {len(self.business_terms)} 个")
        print(f"   📋 字段描述: {len(self.field_descriptions)} 个")
        print(f"   📏 查询规则: {len(self.query_scope_rules)} 个")

    def _generate_basic_business_terms(self):
        """生成基础业务术语（不使用LLM）"""
        print("🔧 生成基础业务术语（无LLM模式）")

        # 基础银行业务术语映射
        basic_terms = {
            "对公有效户": "客户平均日存款余额 >= 100000",
            "不良贷款余额": "贷款分类为次级、可疑、损失的贷款余额",
            "客户号": "customer_id",
            "客户名称": "customer_name",
            "合同号": "contract_id",
            "贷款余额": "loan_balance",
            "存款余额": "deposit_balance",
            "机构名称": "institution_name",
            "开户日期": "account_date",
            "到期日期": "maturity_date"
        }

        self.business_terms.update(basic_terms)

        # 基础字段描述
        for table_info in self.business_data_files:
            table_name = table_info['table_name']
            for col_name in table_info['column_names']:
                self.field_descriptions[f"{table_name}.{col_name}"] = {
                    "chinese_name": col_name,
                    "business_meaning": f"{table_name}表的{col_name}字段",
                    "calculation_rule": "直接字段值",
                    "data_source": f"来源于{table_info['source_file']}"
                }

    def _step4_generate_context_configuration(self):
        """步骤4：生成完整的数据库上下文配置文件"""
        print(f"\n📄 步骤4：生成数据库上下文配置文件")

        # 生成数据库描述信息
        self._generate_database_description()

        # 构建完整的上下文配置
        context_config = {
            "database_info": {
                "name": os.path.basename(self.output_db_path).replace('.db', ''),
                "path": self.output_db_path,
                "type": "sqlite",
                "created_at": datetime.now().isoformat(),
                "description": "基于数据字典生成的银行业务数据库",
                "source": "dictionary_context_generator"
            },
            "tables": self._generate_tables_config(),
            "business_terms": self.business_terms,
            "field_descriptions": self.field_descriptions,
            "query_scope_rules": self.query_scope_rules,
            "database_description": self.database_description
        }

        # 保存上下文配置文件
        self._save_context_configuration(context_config)

        print(f"📄 上下文配置文件生成完成")

    def _generate_database_description(self):
        """智能生成数据库描述信息"""
        # 基于实际导入的表动态生成业务领域
        key_business_areas = set()

        for table in self.imported_tables:
            category = self._categorize_table(table['table_name'])
            key_business_areas.add(category)

        self.database_description = {
            "purpose": "基于数据字典生成的业务数据分析数据库",
            "domain": "banking",
            "tables_count": len(self.imported_tables),
            "total_rows": sum(table['rows'] for table in self.imported_tables),
            "data_sources": [table['source_file'] for table in self.imported_tables],
            "table_relationships": self._infer_table_relationships(),
            "key_business_areas": list(key_business_areas)
        }

    def _generate_tables_config(self) -> Dict[str, Any]:
        """生成表配置信息"""
        tables_config = {}

        for table_info in self.imported_tables:
            table_name = table_info['table_name']

            # 查找对应的数据字典信息
            dict_info = None
            for dict_file in self.dictionary_files:
                if dict_file['table_name'] == table_name:
                    dict_info = dict_file
                    break

            table_config = {
                "chinese_name": table_info['chinese_name'],
                "description": self._get_table_description(table_name),
                "row_count": table_info['rows'],
                "column_count": table_info['columns'],
                "source_file": table_info['source_file'],
                "columns": table_info['column_names'],
                "primary_keys": self._identify_primary_keys(table_name, table_info['column_names']),
                "business_category": self._categorize_table(table_name),
                "data_dictionary_available": dict_info is not None
            }

            if dict_info:
                table_config["dictionary_fields"] = len(dict_info['field_info'])
                table_config["dictionary_source"] = dict_info['file_name']

            tables_config[table_name] = table_config

        return tables_config

    def _infer_table_relationships(self) -> List[Dict]:
        """使用LLM智能推断表之间的关系"""
        try:
            relationships = []

            if len(self.imported_tables) < 2:
                return relationships

            # 使用LLM分析表关系
            if self.api_key:
                # 构建表信息摘要
                tables_summary = []
                for table in self.imported_tables:
                    summary = {
                        'table_name': table['table_name'],
                        'chinese_name': table.get('chinese_name', ''),
                        'key_fields': [col for col in table['column_names'][:10]
                                     if any(keyword in col.lower() for keyword in ['id', 'no', '号', '编号', 'code'])]
                    }
                    tables_summary.append(summary)

                prompt = f"""
请分析以下银行业务数据表之间的关系：

表信息:
{json.dumps(tables_summary, ensure_ascii=False, indent=2)}

请识别表之间可能的关联关系，返回JSON格式：
{{
  "relationships": [
    {{
      "table1": "表名1",
      "table2": "表名2",
      "relationship_type": "one_to_many|many_to_one|one_to_one",
      "common_field": "关联字段名",
      "description": "关系描述"
    }}
  ]
}}

关系类型说明：
- one_to_many: 一对多关系
- many_to_one: 多对一关系
- one_to_one: 一对一关系

只返回JSON，不要其他文字。
"""

                response = self._call_llm_api(prompt, max_retries=2)
                if response:
                    try:
                        llm_result = self._parse_llm_json_response(response)
                        if llm_result and 'relationships' in llm_result:
                            return llm_result['relationships']
                    except Exception as e:
                        print(f"⚠️ LLM关系分析结果解析失败: {e}")

            # 如果LLM分析失败，返回空关系列表
            return []

        except Exception as e:
            print(f"⚠️ 表关系推断失败: {e}")
            return []

    def _get_table_description(self, table_name: str) -> str:
        """使用LLM智能生成表的业务描述"""
        try:
            # 查找对应的表信息
            table_info = None
            for table in self.imported_tables:
                if table['table_name'] == table_name:
                    table_info = table
                    break

            if not table_info:
                return f'{table_name}业务数据表'

            # 使用LLM生成描述
            if self.api_key:
                prompt = f"""
请为以下数据库表生成简洁的业务描述：

表名: {table_name}
中文名: {table_info.get('chinese_name', '')}
来源文件: {table_info.get('source_file', '')}
字段数量: {table_info.get('columns', 0)}
数据行数: {table_info.get('rows', 0)}

要求:
1. 描述应该简洁明了，不超过50字
2. 突出表的主要业务用途
3. 使用专业的银行业务术语
4. 只返回描述文字，不要其他内容

示例: "存储银行客户的存款余额信息，包括客户基本信息、存款金额、账户状态等"
"""

                response = self._call_llm_api(prompt, max_retries=2)
                if response and response.strip():
                    return response.strip()

            # 如果LLM失败，返回简单描述
            chinese_name = table_info.get('chinese_name', '')
            if chinese_name:
                return f"{chinese_name}数据表"
            else:
                return f"{table_name}数据表"

        except Exception as e:
            print(f"⚠️ 生成表描述失败: {e}")
            return f'{table_name}业务数据表'

    def _identify_primary_keys(self, table_name: str, column_names: List[str]) -> List[str]:
        """智能识别主键字段"""
        try:
            # 使用LLM识别主键
            if self.api_key and len(column_names) > 0:
                columns_text = ', '.join(column_names[:20])  # 限制字段数量避免token过多

                prompt = f"""
请从以下数据库表的字段中识别可能的主键字段：

表名: {table_name}
字段列表: {columns_text}

主键字段通常具有以下特征：
1. 包含ID、编号、号码等标识符
2. 唯一标识每条记录
3. 通常不为空
4. 常见模式：*_ID, *_NO, *编号, *号

请返回最可能的主键字段名，如果有多个请用逗号分隔。如果没有明显的主键字段，返回"无"。
只返回字段名，不要其他文字。
"""

                response = self._call_llm_api(prompt, max_retries=2)
                if response and response.strip() and response.strip() != "无":
                    llm_keys = [key.strip() for key in response.strip().split(',')]
                    # 验证LLM返回的字段是否存在于实际字段列表中
                    valid_keys = [key for key in llm_keys if key in column_names]
                    if valid_keys:
                        return valid_keys

            # 如果LLM失败，返回空列表，让数据库自动处理
            return []

        except Exception as e:
            print(f"⚠️ 主键识别失败: {e}")
            return []

    def _categorize_table(self, table_name: str) -> str:
        """使用LLM智能对表进行业务分类"""
        try:
            # 查找对应的表信息
            table_info = None
            for table in self.imported_tables:
                if table['table_name'] == table_name:
                    table_info = table
                    break

            if not table_info:
                return '数据表'

            # 使用LLM进行开放式分类
            if self.api_key:
                prompt = f"""
请为以下银行业务数据表生成一个合适的业务分类：

表名: {table_name}
中文名: {table_info.get('chinese_name', '')}
来源文件: {table_info.get('source_file', '')}

要求:
1. 分类应该反映表的主要业务用途
2. 使用简洁的中文词汇（2-4个字）
3. 符合银行业务实际情况
4. 只返回分类名称，不要其他文字

示例: "存款业务"、"风险管理"、"客户服务"等
"""

                response = self._call_llm_api(prompt, max_retries=2)
                if response and response.strip():
                    category = response.strip()
                    # 简单验证：确保是合理长度的中文分类
                    if 2 <= len(category) <= 8 and any('\u4e00' <= char <= '\u9fff' for char in category):
                        return category

            # 如果LLM失败，基于表名生成简单分类
            if table_info.get('chinese_name'):
                return f"{table_info['chinese_name'].replace('表', '').replace('信息', '').replace('数据', '')[:4]}业务"
            else:
                return f"{table_name.replace('_', ' ').title()}业务"

        except Exception as e:
            print(f"⚠️ 表分类失败: {e}")
            return '业务数据'

    def _save_context_configuration(self, context_config: Dict[str, Any]):
        """保存上下文配置文件"""
        # 确保配置目录存在
        config_dir = "configs/database_contexts"
        os.makedirs(config_dir, exist_ok=True)

        # 生成配置文件名
        db_name = context_config['database_info']['name']
        config_filename = f"{db_name}_context.json"
        config_path = os.path.join(config_dir, config_filename)

        # 保存配置文件
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(context_config, f, ensure_ascii=False, indent=2)

        print(f"📄 上下文配置已保存: {config_path}")

        # 同时保存一个简化版本用于快速查看
        summary_config = {
            "database_name": db_name,
            "tables": list(context_config['tables'].keys()),
            "business_terms_count": len(context_config['business_terms']),
            "field_descriptions_count": len(context_config['field_descriptions']),
            "query_rules_count": len(context_config['query_scope_rules']),
            "created_at": context_config['database_info']['created_at']
        }

        summary_path = os.path.join(config_dir, f"{db_name}_summary.json")
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary_config, f, ensure_ascii=False, indent=2)

        print(f"📄 配置摘要已保存: {summary_path}")

    def _call_llm_api(self, prompt: str, max_retries: int = 3) -> Optional[str]:
        """调用LLM API，带重试机制"""
        if not self.api_key:
            print("⚠️ 未配置API密钥")
            return None

        for attempt in range(max_retries):
            try:
                print(f"🤖 LLM API调用 (第 {attempt + 1} 次尝试)")

                headers = {
                    'Authorization': f'Bearer {self.api_key}',
                    'Content-Type': 'application/json'
                }

                data = {
                    'model': 'deepseek-chat',
                    'messages': [
                        {'role': 'system', 'content': '你是一个专业的银行业务数据分析专家，擅长生成业务术语词典和数据库上下文配置。'},
                        {'role': 'user', 'content': prompt}
                    ],
                    'max_tokens': 4000,
                    'temperature': 0.1
                }

                response = requests.post(
                    'https://api.deepseek.com/chat/completions',
                    headers=headers,
                    json=data
                )

                if response.status_code == 200:
                    result = response.json()
                    content = result['choices'][0]['message']['content'].strip()
                    print(f"✅ LLM API调用成功 (第 {attempt + 1} 次尝试)")
                    return content
                else:
                    print(f"❌ API调用失败: {response.status_code} - {response.text}")
                    if attempt < max_retries - 1:
                        print(f"⏳ 等待5秒后重试...")
                        import time
                        time.sleep(5)

            except Exception as e:
                print(f"❌ LLM API调用异常 (第 {attempt + 1} 次): {e}")
                if attempt < max_retries - 1:
                    print(f"⏳ 等待5秒后重试...")
                    import time
                    time.sleep(5)

        print(f"❌ LLM API调用最终失败，已尝试 {max_retries} 次")
        return None

    def _clean_llm_response(self, response: str) -> str:
        """清理LLM响应，提取JSON部分"""
        if not response:
            return ""

        response = response.strip()

        # 移除markdown代码块标记
        if response.startswith('```json'):
            response = response[7:]
        if response.startswith('```'):
            response = response[3:]
        if response.endswith('```'):
            response = response[:-3]

        response = response.strip()

        # 查找JSON开始和结束位置
        json_start = response.find('{')
        if json_start == -1:
            return response

        brace_count = 0
        json_end = -1

        for i in range(json_start, len(response)):
            if response[i] == '{':
                brace_count += 1
            elif response[i] == '}':
                brace_count -= 1
                if brace_count == 0:
                    json_end = i + 1
                    break

        if json_end == -1:
            return response[json_start:]

        return response[json_start:json_end]

    def _parse_llm_json_response(self, response: str) -> Optional[Dict[str, Any]]:
        """解析LLM的JSON响应"""
        try:
            # 清理响应内容
            cleaned_response = self._clean_llm_response(response)

            # 尝试解析JSON
            parsed_result = json.loads(cleaned_response)
            return parsed_result

        except json.JSONDecodeError as e:
            print(f"❌ JSON解析失败: {e}")
            return None
        except Exception as e:
            print(f"❌ 响应解析失败: {e}")
            return None

    def _generate_comprehensive_report(self) -> Dict[str, Any]:
        """生成综合报告"""
        return {
            'success': True,
            'timestamp': datetime.now().isoformat(),
            'database_path': self.output_db_path,
            'workflow_steps': {
                'step1_business_data_imported': len(self.imported_tables),
                'step2_dictionaries_analyzed': len(self.dictionary_files),
                'step3_business_terms_generated': len(self.business_terms),
                'step4_context_configuration_created': True
            },
            'tables': self.imported_tables,
            'business_terms_count': len(self.business_terms),
            'field_descriptions_count': len(self.field_descriptions),
            'query_rules_count': len(self.query_scope_rules),
            'total_rows': sum(table['rows'] for table in self.imported_tables),
            'total_columns': sum(table['columns'] for table in self.imported_tables),
            'context_files_generated': True
        }

# CLI接口函数
def generate_database_with_context(data_dir: str, output_db: Optional[str] = None,
                                 api_key: Optional[str] = None) -> Dict[str, Any]:
    """CLI接口函数：生成数据库和上下文配置"""
    if not output_db:
        timestamp = int(datetime.now().timestamp())
        output_db = f"databases/imported/context_generated_db_{timestamp}.db"

    # 确保输出到databases/imported目录
    if not output_db.startswith('databases/'):
        output_db = f"databases/imported/{os.path.basename(output_db)}"

    # 确保目录存在
    os.makedirs(os.path.dirname(output_db), exist_ok=True)

    generator = DictionaryContextGenerator(output_db, api_key)
    return generator.generate_database_with_context(data_dir)

def main():
    """主函数 - 可直接执行"""
    import argparse

    parser = argparse.ArgumentParser(description='数据字典驱动的上下文文件生成器')
    parser.add_argument('data_dir', help='数据目录路径')
    parser.add_argument('-o', '--output', help='输出数据库文件路径')
    parser.add_argument('--api-key', help='LLM API密钥')

    args = parser.parse_args()

    try:
        # 执行数据库和上下文生成
        report = generate_database_with_context(
            args.data_dir,
            args.output,
            args.api_key
        )

        # 保存报告
        timestamp = int(datetime.now().timestamp())
        report_file = f"context_generation_report_{timestamp}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        print(f"\n📋 报告已保存: {report_file}")

        # 显示结果
        print(f"\n📊 数据库和上下文生成结果:")
        print(f"   数据库文件: {report['database_path']}")
        print(f"   总表数: {len(report['tables'])}")
        print(f"   总行数: {report['total_rows']:,}")
        print(f"   业务术语: {report['business_terms_count']} 个")
        print(f"   字段描述: {report['field_descriptions_count']} 个")
        print(f"   查询规则: {report['query_rules_count']} 个")

        for table in report['tables']:
            print(f"   📋 {table['table_name']}: {table['rows']:,}行 ({table['chinese_name']})")

        print(f"\n🎉 数据字典驱动的数据库和上下文生成完成！")
        print(f"💡 上下文配置文件已保存到 configs/database_contexts/ 目录")

    except Exception as e:
        print(f"❌ 生成失败: {e}")
        return 1

    return 0

if __name__ == "__main__":
    exit(main())
