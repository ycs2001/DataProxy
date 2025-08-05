#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DataProxy LLM智能数据导入引擎 v2.0
基于大语言模型的真正智能化数据分析和导入系统

核心特性:
1. LLM驱动的智能文件分析
2. 自然语言理解的字段映射
3. 智能业务术语提取
4. 自动化数据库设计
5. 完整的上下文配置生成
"""

import os
import sqlite3
import pandas as pd
import json
import re
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging
import time

# LLM API 相关导入
from langchain_openai import ChatOpenAI
from langchain.schema.messages import HumanMessage, SystemMessage

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class LLMIntelligentDataImporter:
    """
    基于LLM的智能数据导入引擎 v2.0
    
    使用大语言模型进行真正的智能化数据分析和导入
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """
        初始化智能数据导入引擎
        
        Args:
            api_key: LLM API密钥，如果不提供则从环境变量获取
        """
        # LLM客户端初始化
        self.llm_client = self._init_llm_client(api_key)
        
        # 数据存储
        self.source_files = {}  # 数据源文件信息
        self.dictionary_files = {}  # 数据字典文件信息
        self.llm_analysis_results = {}  # LLM分析结果
        self.database_schema = {}  # 数据库设计方案
        self.import_log = []  # 详细的导入日志
        
        # 配置参数
        self.max_sample_rows = 10  # 发送给LLM的样本数据行数
        self.max_retry_attempts = 3  # LLM API调用重试次数
        
        logger.info("🚀 LLM智能数据导入引擎初始化完成")
    
    def _init_llm_client(self, api_key: Optional[str] = None) -> Optional[ChatOpenAI]:
        """初始化LLM客户端"""
        try:
            if not api_key:
                api_key = os.getenv('DEEPSEEK_API_KEY')
            
            if not api_key:
                logger.warning("⚠️ 未找到LLM API密钥，将使用基础规则模式")
                return None
            
            client = ChatOpenAI(
                model="deepseek-chat",
                openai_api_key=api_key,
                openai_api_base="https://api.deepseek.com",
                temperature=0.1,  # 低温度确保分析的一致性
                max_tokens=4000,  # 足够的token用于详细分析
                timeout=60  # 60秒超时
            )
            
            logger.info("✅ LLM客户端初始化成功")
            return client
            
        except Exception as e:
            logger.error(f"❌ LLM客户端初始化失败: {e}")
            return None
    
    def process_batch_import(self, data_source_dir: str, data_dict_dir: str, output_db_path: str) -> Dict[str, Any]:
        """
        LLM驱动的智能批量导入主流程
        
        Args:
            data_source_dir: 数据源目录路径
            data_dict_dir: 数据字典目录路径
            output_db_path: 输出数据库路径
            
        Returns:
            详细的处理报告
        """
        logger.info("🚀 开始LLM智能数据导入处理")
        start_time = time.time()
        
        try:
            # 第1步: 文件发现和预处理
            self._discover_and_preprocess_files(data_source_dir, data_dict_dir)
            
            # 第2步: LLM智能分析
            self._perform_llm_analysis()
            
            # 第3步: 数据库设计生成
            self._generate_database_design()
            
            # 第4步: 创建数据库结构
            self._create_database_structure(output_db_path)
            
            # 第5步: 执行数据导入
            self._import_data_with_llm_guidance(output_db_path)
            
            # 第6步: 生成上下文配置
            self._generate_context_configuration(output_db_path)
            
            # 第7步: 生成处理报告
            report = self._generate_comprehensive_report(output_db_path, start_time)
            
            logger.info("✅ LLM智能数据导入完成")
            return report
            
        except Exception as e:
            logger.error(f"❌ LLM智能数据导入失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'execution_time': time.time() - start_time
            }
    
    def _discover_and_preprocess_files(self, data_source_dir: str, data_dict_dir: str):
        """
        第1步: 文件发现和预处理
        """
        logger.info("📂 开始文件发现和预处理")
        
        # 发现数据源文件
        self._discover_source_files(data_source_dir)
        
        # 发现数据字典文件
        self._discover_dictionary_files(data_dict_dir)
        
        # 预处理文件内容
        self._preprocess_file_contents()
        
        logger.info(f"📂 文件发现完成: {len(self.source_files)} 个数据源, {len(self.dictionary_files)} 个数据字典")
    
    def _discover_source_files(self, data_source_dir: str):
        """发现数据源文件"""
        if not os.path.exists(data_source_dir):
            logger.warning(f"⚠️ 数据源目录不存在: {data_source_dir}")
            return
        
        for file_name in os.listdir(data_source_dir):
            if file_name.endswith(('.xlsx', '.xls', '.csv')):
                file_path = os.path.join(data_source_dir, file_name)
                
                try:
                    # 读取文件基本信息
                    if file_name.endswith('.csv'):
                        df = pd.read_csv(file_path, nrows=self.max_sample_rows)
                    else:
                        df = pd.read_excel(file_path, nrows=self.max_sample_rows)
                    
                    self.source_files[file_name] = {
                        'file_path': file_path,
                        'file_name': file_name,
                        'columns': list(df.columns),
                        'sample_data': df.to_dict('records'),
                        'total_rows': len(df),
                        'data_types': df.dtypes.to_dict()
                    }
                    
                    logger.info(f"📊 发现数据源文件: {file_name} ({len(df.columns)} 列)")
                    
                except Exception as e:
                    logger.error(f"❌ 读取数据源文件失败 {file_name}: {e}")
    
    def _discover_dictionary_files(self, data_dict_dir: str):
        """发现数据字典文件"""
        if not os.path.exists(data_dict_dir):
            logger.warning(f"⚠️ 数据字典目录不存在: {data_dict_dir}")
            return
        
        for file_name in os.listdir(data_dict_dir):
            if file_name.endswith(('.xlsx', '.xls', '.csv')) and '数据字典' in file_name:
                file_path = os.path.join(data_dict_dir, file_name)
                
                try:
                    # 读取数据字典文件
                    if file_name.endswith('.csv'):
                        df = pd.read_csv(file_path)
                    else:
                        df = pd.read_excel(file_path)
                    
                    self.dictionary_files[file_name] = {
                        'file_path': file_path,
                        'file_name': file_name,
                        'columns': list(df.columns),
                        'content': df.to_dict('records'),
                        'total_rows': len(df)
                    }
                    
                    logger.info(f"📚 发现数据字典文件: {file_name} ({len(df)} 行定义)")
                    
                except Exception as e:
                    logger.error(f"❌ 读取数据字典文件失败 {file_name}: {e}")
    
    def _preprocess_file_contents(self):
        """预处理文件内容，为LLM分析做准备"""
        logger.info("🔄 预处理文件内容")
        
        # 为每个数据源文件准备分析数据
        for file_name, file_info in self.source_files.items():
            # 数据类型转换为字符串
            file_info['data_types_str'] = {k: str(v) for k, v in file_info['data_types'].items()}
            
            # 生成字段统计信息
            file_info['field_stats'] = self._generate_field_statistics(file_info)
    
    def _generate_field_statistics(self, file_info: Dict[str, Any]) -> Dict[str, Any]:
        """生成字段统计信息"""
        stats = {}
        
        try:
            df = pd.DataFrame(file_info['sample_data'])
            
            for column in df.columns:
                col_stats = {
                    'data_type': str(df[column].dtype),
                    'null_count': int(df[column].isnull().sum()),
                    'unique_count': int(df[column].nunique()),
                    'sample_values': df[column].dropna().head(3).tolist()
                }
                
                # 数值类型的额外统计
                if pd.api.types.is_numeric_dtype(df[column]):
                    col_stats.update({
                        'min_value': float(df[column].min()) if not df[column].empty else None,
                        'max_value': float(df[column].max()) if not df[column].empty else None,
                        'mean_value': float(df[column].mean()) if not df[column].empty else None
                    })
                
                stats[column] = col_stats
                
        except Exception as e:
            logger.warning(f"⚠️ 生成字段统计失败: {e}")
        
        return stats

    def _perform_llm_analysis(self):
        """
        第2步: LLM智能分析
        """
        logger.info("🧠 开始LLM智能分析")

        if not self.llm_client:
            logger.warning("⚠️ LLM客户端不可用，跳过智能分析")
            return

        # 分析每个数据源文件
        for file_name, file_info in self.source_files.items():
            logger.info(f"🔍 分析数据源文件: {file_name}")

            # 查找对应的数据字典
            dict_info = self._find_matching_dictionary(file_name)

            # 执行LLM分析
            analysis_result = self._analyze_file_with_llm(file_info, dict_info)

            if analysis_result:
                self.llm_analysis_results[file_name] = analysis_result
                logger.info(f"✅ 完成分析: {file_name}")
            else:
                logger.warning(f"⚠️ 分析失败: {file_name}")

    def _find_matching_dictionary(self, source_file_name: str) -> Optional[Dict[str, Any]]:
        """查找匹配的数据字典文件"""

        # 提取源文件的关键词
        source_keywords = self._extract_keywords_from_filename(source_file_name)

        best_match = None
        best_score = 0

        for dict_name, dict_info in self.dictionary_files.items():
            dict_keywords = self._extract_keywords_from_filename(dict_name)

            # 计算匹配分数
            score = len(set(source_keywords) & set(dict_keywords))

            if score > best_score:
                best_score = score
                best_match = dict_info

        if best_match:
            logger.info(f"📚 找到匹配的数据字典: {best_match['file_name']}")

        return best_match

    def _extract_keywords_from_filename(self, filename: str) -> List[str]:
        """从文件名提取关键词"""
        # 移除扩展名和常见前缀
        name = os.path.splitext(filename)[0]
        name = re.sub(r'^数据字典[-_]?', '', name)

        # 提取中英文关键词
        keywords = []

        # 中文关键词
        chinese_keywords = re.findall(r'[\u4e00-\u9fff]+', name)
        keywords.extend(chinese_keywords)

        # 英文关键词
        english_keywords = re.findall(r'[A-Z_]+', name)
        keywords.extend(english_keywords)

        return keywords

    def _analyze_file_with_llm(self, file_info: Dict[str, Any], dict_info: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """使用LLM分析文件"""

        try:
            # 构建分析提示词
            analysis_prompt = self._build_analysis_prompt(file_info, dict_info)

            # 调用LLM进行分析
            response = self._call_llm_with_retry(analysis_prompt)

            if response:
                # 解析LLM响应
                analysis_result = self._parse_llm_analysis_response(response)
                return analysis_result

        except Exception as e:
            logger.error(f"❌ LLM分析失败: {e}")

        return None

    def _build_analysis_prompt(self, file_info: Dict[str, Any], dict_info: Optional[Dict[str, Any]]) -> str:
        """构建LLM分析提示词"""

        prompt = f"""你是一个专业的数据库设计专家，请分析以下数据文件并提供详细的分析结果。

## 数据源文件信息
文件名: {file_info['file_name']}
字段列表: {', '.join(file_info['columns'])}
数据类型: {json.dumps(file_info['data_types_str'], ensure_ascii=False, indent=2)}
样本数据: {json.dumps(file_info['sample_data'][:3], ensure_ascii=False, indent=2)}
字段统计: {json.dumps(file_info['field_stats'], ensure_ascii=False, indent=2)}
"""

        if dict_info:
            prompt += f"""
## 对应数据字典信息
数据字典文件: {dict_info['file_name']}
字典列名: {', '.join(dict_info['columns'])}
字典内容: {json.dumps(dict_info['content'][:5], ensure_ascii=False, indent=2)}
"""

        prompt += """
## 请提供以下分析结果（以JSON格式返回）:

{
  "table_name": "推荐的标准表名（英文大写，下划线分隔）",
  "table_description": "表的业务含义描述",
  "fields": [
    {
      "original_name": "原始字段名",
      "standard_name": "标准化字段名（英文）",
      "chinese_name": "中文名称",
      "data_type": "推荐的数据类型（如VARCHAR(50), INTEGER, DECIMAL(18,2)等）",
      "is_primary_key": true/false,
      "is_required": true/false,
      "business_meaning": "字段的业务含义",
      "sample_values": ["样本值1", "样本值2"],
      "constraints": "约束条件（如果有）"
    }
  ],
  "business_terms": [
    {
      "term_name": "业务术语名称",
      "definition": "术语定义",
      "sql_condition": "对应的SQL条件",
      "applicable_fields": ["适用字段列表"]
    }
  ],
  "relationships": [
    {
      "type": "外键关系类型",
      "field": "关联字段",
      "reference_table": "引用表名",
      "reference_field": "引用字段",
      "description": "关系描述"
    }
  ],
  "data_quality_issues": [
    "发现的数据质量问题"
  ],
  "recommendations": [
    "数据处理建议"
  ]
}

请基于银行业务领域知识进行分析，特别关注：
1. 客户信息、贷款合同、风险分类等银行核心业务
2. 对公有效户（存款余额≥10万）、不良贷款等重要业务概念
3. 字段的业务含义和数据质量
4. 表间的逻辑关系

只返回JSON格式的分析结果，不要其他解释。
"""

        return prompt

    def _call_llm_with_retry(self, prompt: str) -> Optional[str]:
        """带重试机制的LLM调用"""

        for attempt in range(self.max_retry_attempts):
            try:
                logger.info(f"🤖 调用LLM分析 (尝试 {attempt + 1}/{self.max_retry_attempts})")

                messages = [
                    SystemMessage(content="你是一个专业的数据库设计和数据分析专家，擅长银行业务领域的数据建模。"),
                    HumanMessage(content=prompt)
                ]

                response = self.llm_client.invoke(messages)

                if response and response.content:
                    logger.info("✅ LLM分析完成")
                    return response.content.strip()

            except Exception as e:
                logger.warning(f"⚠️ LLM调用失败 (尝试 {attempt + 1}): {e}")
                if attempt < self.max_retry_attempts - 1:
                    time.sleep(2 ** attempt)  # 指数退避

        logger.error("❌ LLM调用最终失败")
        return None

    def _parse_llm_analysis_response(self, response: str) -> Optional[Dict[str, Any]]:
        """解析LLM分析响应"""

        try:
            # 尝试提取JSON内容
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                analysis_result = json.loads(json_str)

                # 验证必要字段
                required_fields = ['table_name', 'fields']
                if all(field in analysis_result for field in required_fields):
                    logger.info("✅ LLM分析结果解析成功")
                    return analysis_result
                else:
                    logger.warning("⚠️ LLM分析结果缺少必要字段")

        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON解析失败: {e}")
        except Exception as e:
            logger.error(f"❌ 响应解析失败: {e}")

        return None

    def _generate_database_design(self):
        """
        第3步: 数据库设计生成
        """
        logger.info("🏗️ 开始生成数据库设计")

        # 基于LLM分析结果生成数据库设计
        for file_name, analysis in self.llm_analysis_results.items():
            table_name = analysis.get('table_name')
            if table_name:
                self.database_schema[table_name] = {
                    'source_file': file_name,
                    'table_name': table_name,
                    'description': analysis.get('table_description', ''),
                    'fields': analysis.get('fields', []),
                    'business_terms': analysis.get('business_terms', []),
                    'relationships': analysis.get('relationships', []),
                    'create_sql': self._generate_create_table_sql(analysis)
                }

                logger.info(f"✅ 生成表设计: {table_name}")

        logger.info(f"🏗️ 数据库设计完成，共 {len(self.database_schema)} 个表")

    def _generate_create_table_sql(self, analysis: Dict[str, Any]) -> str:
        """生成CREATE TABLE SQL语句"""

        table_name = analysis['table_name']
        fields = analysis.get('fields', [])

        if not fields:
            return ""

        sql_parts = [f"CREATE TABLE {table_name} ("]
        field_definitions = []
        primary_keys = []

        for field in fields:
            field_name = field.get('standard_name', field.get('original_name', ''))
            data_type = field.get('data_type', 'VARCHAR(255)')
            is_required = field.get('is_required', False)
            is_primary_key = field.get('is_primary_key', False)

            # 构建字段定义
            field_def = f"    {field_name} {data_type}"

            if is_required:
                field_def += " NOT NULL"

            field_definitions.append(field_def)

            if is_primary_key:
                primary_keys.append(field_name)

        sql_parts.append(",\n".join(field_definitions))

        # 添加主键约束
        if primary_keys:
            sql_parts.append(f",\n    PRIMARY KEY ({', '.join(primary_keys)})")

        sql_parts.append("\n)")

        return "".join(sql_parts)

    def _create_database_structure(self, output_db_path: str):
        """
        第4步: 创建数据库结构
        """
        logger.info("🗄️ 开始创建数据库结构")

        try:
            # 如果数据库文件已存在，删除它
            if os.path.exists(output_db_path):
                os.remove(output_db_path)

            conn = sqlite3.connect(output_db_path)
            cursor = conn.cursor()

            # 创建所有表
            for table_name, schema in self.database_schema.items():
                create_sql = schema['create_sql']
                if create_sql:
                    logger.info(f"📋 创建表: {table_name}")
                    cursor.execute(create_sql)

                    # 记录创建日志
                    self.import_log.append({
                        'step': 'create_table',
                        'table_name': table_name,
                        'sql': create_sql,
                        'timestamp': datetime.now().isoformat()
                    })

            conn.commit()
            conn.close()

            logger.info("✅ 数据库结构创建完成")

        except Exception as e:
            logger.error(f"❌ 数据库结构创建失败: {e}")
            raise

    def _import_data_with_llm_guidance(self, output_db_path: str):
        """
        第5步: 执行数据导入
        """
        logger.info("📥 开始数据导入")

        try:
            conn = sqlite3.connect(output_db_path)

            for table_name, schema in self.database_schema.items():
                source_file = schema['source_file']
                file_info = self.source_files[source_file]

                logger.info(f"📊 导入数据到表: {table_name}")

                # 读取完整数据文件
                if source_file.endswith('.csv'):
                    df = pd.read_csv(file_info['file_path'])
                else:
                    df = pd.read_excel(file_info['file_path'])

                # 根据LLM分析结果进行字段映射
                df_mapped = self._map_fields_with_llm_guidance(df, schema)

                # 数据清洗
                df_cleaned = self._clean_data_with_llm_guidance(df_mapped, schema)

                # 导入数据
                df_cleaned.to_sql(table_name, conn, if_exists='append', index=False)

                logger.info(f"✅ 成功导入 {len(df_cleaned)} 行数据到 {table_name}")

                # 记录导入日志
                self.import_log.append({
                    'step': 'import_data',
                    'table_name': table_name,
                    'source_file': source_file,
                    'imported_rows': len(df_cleaned),
                    'timestamp': datetime.now().isoformat()
                })

            conn.close()
            logger.info("✅ 数据导入完成")

        except Exception as e:
            logger.error(f"❌ 数据导入失败: {e}")
            raise

    def _map_fields_with_llm_guidance(self, df: pd.DataFrame, schema: Dict[str, Any]) -> pd.DataFrame:
        """根据LLM分析结果进行字段映射"""

        field_mapping = {}
        fields = schema.get('fields', [])

        for field in fields:
            original_name = field.get('original_name')
            standard_name = field.get('standard_name')

            if original_name and standard_name and original_name in df.columns:
                field_mapping[original_name] = standard_name

        # 重命名列
        df_mapped = df.rename(columns=field_mapping)

        # 只保留映射的列
        mapped_columns = list(field_mapping.values())
        existing_columns = [col for col in mapped_columns if col in df_mapped.columns]
        df_mapped = df_mapped[existing_columns]

        return df_mapped

    def _clean_data_with_llm_guidance(self, df: pd.DataFrame, schema: Dict[str, Any]) -> pd.DataFrame:
        """根据LLM分析结果进行数据清洗"""

        fields = schema.get('fields', [])

        for field in fields:
            field_name = field.get('standard_name')
            data_type = field.get('data_type', '')
            is_required = field.get('is_required', False)

            if field_name not in df.columns:
                continue

            # 数据类型转换
            if 'INTEGER' in data_type.upper():
                df[field_name] = pd.to_numeric(df[field_name], errors='coerce')
                df[field_name] = df[field_name].fillna(0).astype(int)

            elif 'DECIMAL' in data_type.upper() or 'REAL' in data_type.upper():
                df[field_name] = pd.to_numeric(df[field_name], errors='coerce')
                df[field_name] = df[field_name].fillna(0.0)

            elif 'VARCHAR' in data_type.upper() or 'TEXT' in data_type.upper():
                df[field_name] = df[field_name].astype(str)
                df[field_name] = df[field_name].replace('nan', '')

                # 截断过长的字符串
                if 'VARCHAR' in data_type.upper():
                    length_match = re.search(r'VARCHAR\((\d+)\)', data_type.upper())
                    if length_match:
                        max_length = int(length_match.group(1))
                        df[field_name] = df[field_name].str[:max_length]

            # 处理必填字段
            if is_required:
                if df[field_name].dtype == 'object':
                    df[field_name] = df[field_name].fillna('')
                else:
                    df[field_name] = df[field_name].fillna(0)

        return df

    def _generate_context_configuration(self, output_db_path: str):
        """
        第6步: 生成上下文配置
        """
        logger.info("⚙️ 开始生成上下文配置")

        try:
            # 生成数据库上下文配置
            context_config = self._build_context_config(output_db_path)

            # 保存配置文件
            config_file_path = self._save_context_config(context_config, output_db_path)

            logger.info(f"✅ 上下文配置已保存: {config_file_path}")

        except Exception as e:
            logger.error(f"❌ 上下文配置生成失败: {e}")

    def _build_context_config(self, output_db_path: str) -> Dict[str, Any]:
        """构建上下文配置"""

        # 收集所有业务术语
        all_business_terms = {}
        all_relationships = {}
        all_field_mappings = {}

        for table_name, schema in self.database_schema.items():
            # 业务术语
            for term in schema.get('business_terms', []):
                term_name = term.get('term_name')
                if term_name:
                    all_business_terms[term_name] = {
                        'name': term_name,
                        'definition': term.get('definition', ''),
                        'sql_conditions': term.get('sql_condition', ''),
                        'applicable_tables': [table_name],
                        'applicable_fields': term.get('applicable_fields', [])
                    }

            # 表关系
            for rel in schema.get('relationships', []):
                rel_name = f"{table_name}_{rel.get('type', 'relation')}"
                all_relationships[rel_name] = rel

            # 字段映射
            for field in schema.get('fields', []):
                field_name = field.get('standard_name')
                if field_name:
                    all_field_mappings[field_name] = {
                        'field_name': field_name,
                        'chinese_name': field.get('chinese_name', ''),
                        'business_meaning': field.get('business_meaning', ''),
                        'data_type': field.get('data_type', ''),
                        'table_name': table_name
                    }

        # 构建完整配置
        context_config = {
            'database_path': output_db_path,
            'database_name': os.path.splitext(os.path.basename(output_db_path))[0],
            'database_type': 'banking',
            'description': 'LLM智能分析生成的银行业务数据库',
            'generated_at': datetime.now().isoformat(),
            'generation_method': 'llm_intelligent_analysis',

            # 表结构信息
            'tables': self._generate_table_info(),

            # 业务术语
            'database_specific_business_terms': all_business_terms,

            # 字段映射
            'database_specific_field_mappings': all_field_mappings,

            # 表关系
            'relationships': all_relationships,

            # LLM分析结果
            'llm_analysis_summary': self._generate_analysis_summary()
        }

        return context_config

    def _generate_table_info(self) -> Dict[str, Any]:
        """生成表信息"""
        tables_info = {}

        for table_name, schema in self.database_schema.items():
            fields = schema.get('fields', [])

            tables_info[table_name] = {
                'name': table_name,
                'description': schema.get('description', ''),
                'columns': [
                    {
                        'name': field.get('standard_name', ''),
                        'type': field.get('data_type', ''),
                        'nullable': not field.get('is_required', False),
                        'is_primary_key': field.get('is_primary_key', False),
                        'chinese_name': field.get('chinese_name', ''),
                        'business_meaning': field.get('business_meaning', '')
                    }
                    for field in fields
                ],
                'primary_keys': [
                    field.get('standard_name', '')
                    for field in fields
                    if field.get('is_primary_key', False)
                ],
                'source_file': schema.get('source_file', '')
            }

        return tables_info

    def _generate_analysis_summary(self) -> Dict[str, Any]:
        """生成LLM分析摘要"""
        summary = {
            'total_files_analyzed': len(self.llm_analysis_results),
            'total_tables_generated': len(self.database_schema),
            'analysis_timestamp': datetime.now().isoformat(),
            'llm_model': 'deepseek-chat',
            'analysis_details': {}
        }

        for file_name, analysis in self.llm_analysis_results.items():
            summary['analysis_details'][file_name] = {
                'table_name': analysis.get('table_name', ''),
                'fields_count': len(analysis.get('fields', [])),
                'business_terms_count': len(analysis.get('business_terms', [])),
                'relationships_count': len(analysis.get('relationships', [])),
                'data_quality_issues': analysis.get('data_quality_issues', []),
                'recommendations': analysis.get('recommendations', [])
            }

        return summary

    def _save_context_config(self, context_config: Dict[str, Any], output_db_path: str) -> str:
        """保存上下文配置文件"""

        # 生成配置文件路径
        db_name = os.path.splitext(os.path.basename(output_db_path))[0]
        db_hash = hashlib.md5(output_db_path.encode()).hexdigest()

        config_dir = "configs/database_contexts"
        os.makedirs(config_dir, exist_ok=True)

        config_file_path = os.path.join(config_dir, f"{db_name}_{db_hash}.json")

        # 保存配置文件
        with open(config_file_path, 'w', encoding='utf-8') as f:
            json.dump(context_config, f, ensure_ascii=False, indent=2)

        return config_file_path

    def _generate_comprehensive_report(self, output_db_path: str, start_time: float) -> Dict[str, Any]:
        """
        第7步: 生成处理报告
        """
        logger.info("📋 生成处理报告")

        execution_time = time.time() - start_time

        # 统计导入数据
        total_imported_rows = sum(
            log['imported_rows'] for log in self.import_log
            if log['step'] == 'import_data'
        )

        # 生成详细报告
        report = {
            'success': True,
            'execution_time': execution_time,
            'output_database': output_db_path,
            'database_name': os.path.splitext(os.path.basename(output_db_path))[0],

            # 处理统计
            'processing_summary': {
                'total_source_files': len(self.source_files),
                'total_dictionary_files': len(self.dictionary_files),
                'total_tables_created': len(self.database_schema),
                'total_imported_rows': total_imported_rows,
                'llm_analysis_success_rate': len(self.llm_analysis_results) / len(self.source_files) if self.source_files else 0
            },

            # 文件处理详情
            'file_processing_details': {
                'source_files': list(self.source_files.keys()),
                'dictionary_files': list(self.dictionary_files.keys()),
                'successfully_analyzed': list(self.llm_analysis_results.keys())
            },

            # 数据库结构
            'database_structure': {
                table_name: {
                    'description': schema.get('description', ''),
                    'fields_count': len(schema.get('fields', [])),
                    'source_file': schema.get('source_file', '')
                }
                for table_name, schema in self.database_schema.items()
            },

            # LLM分析摘要
            'llm_analysis_summary': self._generate_analysis_summary(),

            # 详细日志
            'detailed_log': self.import_log,

            # 生成的业务术语
            'generated_business_terms': self._extract_all_business_terms(),

            # 建议和问题
            'recommendations': self._generate_final_recommendations(),
            'data_quality_issues': self._collect_all_data_quality_issues()
        }

        logger.info("✅ 处理报告生成完成")
        return report

    def _extract_all_business_terms(self) -> List[Dict[str, Any]]:
        """提取所有生成的业务术语"""
        all_terms = []

        for analysis in self.llm_analysis_results.values():
            terms = analysis.get('business_terms', [])
            all_terms.extend(terms)

        return all_terms

    def _generate_final_recommendations(self) -> List[str]:
        """生成最终建议"""
        recommendations = []

        # 收集所有LLM建议
        for analysis in self.llm_analysis_results.values():
            recs = analysis.get('recommendations', [])
            recommendations.extend(recs)

        # 添加通用建议
        recommendations.extend([
            "建议定期验证生成的业务术语定义的准确性",
            "建议根据实际业务需求调整字段映射和数据类型",
            "建议建立数据质量监控机制",
            "建议定期更新数据字典以保持同步"
        ])

        return list(set(recommendations))  # 去重

    def _collect_all_data_quality_issues(self) -> List[str]:
        """收集所有数据质量问题"""
        all_issues = []

        for analysis in self.llm_analysis_results.values():
            issues = analysis.get('data_quality_issues', [])
            all_issues.extend(issues)

        return list(set(all_issues))  # 去重


# 便捷函数
def create_llm_intelligent_importer(api_key: Optional[str] = None) -> LLMIntelligentDataImporter:
    """创建LLM智能数据导入器实例"""
    return LLMIntelligentDataImporter(api_key)


def quick_llm_import(data_source_dir: str, data_dict_dir: str, output_db_path: str, api_key: Optional[str] = None) -> Dict[str, Any]:
    """快速LLM智能导入函数"""
    importer = create_llm_intelligent_importer(api_key)
    return importer.process_batch_import(data_source_dir, data_dict_dir, output_db_path)
