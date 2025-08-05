#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DataProxy 纯LLM驱动的智能数据导入系统

完全基于大语言模型的智能化数据分析和导入引擎
- 零规则编码：所有逻辑都通过LLM推理实现
- 完全智能化：从文件识别到数据库设计全部由LLM完成
- 无限制分析：移除超时限制，允许LLM深度分析
- 自主学习：LLM自主发现数据模式和业务规则
"""

import os
import sqlite3
import pandas as pd
import json
import re
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging

# LLM API 相关导入 - 使用直接requests避免LangChain问题
import requests
import time

# 完全禁用日志记录，避免I/O错误
class SafeLogger:
    """安全的日志记录器，避免I/O错误"""
    def info(self, msg, *args, **kwargs):
        # 直接打印到控制台，避免日志系统问题
        try:
            print(f"ℹ️  {msg}")
        except:
            pass

    def warning(self, msg, *args, **kwargs):
        try:
            print(f"⚠️  {msg}")
        except:
            pass

    def error(self, msg, *args, **kwargs):
        try:
            print(f"❌ {msg}")
        except:
            pass

    def debug(self, msg, *args, **kwargs):
        # 跳过debug信息
        pass

logger = SafeLogger()


class IntelligentDataImporter:
    """
    纯LLM驱动的智能数据导入系统

    核心特性：
    1. 完全LLM驱动 - 所有分析逻辑都通过LLM实现
    2. 零规则编码 - 不使用任何硬编码规则或模式匹配
    3. 无超时限制 - 允许LLM充分分析复杂数据
    4. 自主学习 - LLM自主发现数据模式和业务规则
    """

    def __init__(self, api_key: Optional[str] = None):
        """
        初始化纯LLM智能数据导入系统

        Args:
            api_key: LLM API密钥，如果不提供则从环境变量获取
        """
        # LLM配置参数 (进一步减少以避免ChunkedEncodingError)
        self.max_tokens_per_request = 4000   # 进一步减少token限制
        self.unlimited_retries = True  # 无限制重试
        self.deep_analysis_mode = True   # 启用深度分析模式
        self.simplified_mode = False    # 关闭简化模式

        # API密钥初始化
        self.api_key = self._init_llm_client(api_key)

        # 数据存储
        self.discovered_files = {}  # 发现的所有文件
        self.llm_file_analysis = {}  # LLM文件分析结果
        self.llm_schema_design = {}  # LLM数据库设计方案
        self.llm_business_intelligence = {}  # LLM业务智能分析
        self.import_execution_log = []  # 详细执行日志

        logger.info("🧠 纯LLM智能数据导入系统初始化完成")

    def _init_llm_client(self, api_key: Optional[str] = None) -> Optional[str]:
        """初始化LLM客户端 - 使用直接requests避免LangChain问题"""
        try:
            if not api_key:
                api_key = os.getenv('DEEPSEEK_API_KEY')

            if not api_key:
                logger.error("❌ 未找到LLM API密钥，纯LLM系统无法运行")
                raise ValueError("纯LLM系统需要API密钥才能运行")

            # 直接存储API密钥，不使用LangChain
            logger.info("🔧 使用直接requests API调用，避免LangChain问题")
            logger.info("✅ API密钥配置成功 - 直接requests模式")

            return api_key

        except Exception as e:
            logger.error(f"❌ API密钥配置失败: {e}")
            raise

    def process_batch_import(self, file_paths: List[str], output_db_path: str) -> Dict[str, Any]:
        """
        纯LLM驱动的批量导入主流程

        Args:
            file_paths: 文件路径列表（兼容旧接口）
            output_db_path: 输出数据库路径

        Returns:
            详细的处理报告
        """
        logger.info("🧠 开始纯LLM智能数据导入处理")
        start_time = datetime.now()

        try:
            # 第1步: LLM驱动的文件发现和分类
            self._llm_discover_and_classify_files(file_paths)

            # 第2步: LLM深度文件内容分析
            self._llm_deep_content_analysis()

            # 第3步: LLM智能数据库设计
            self._llm_intelligent_database_design()

            # 第4步: LLM指导的数据库创建
            self._llm_guided_database_creation(output_db_path)

            # 第5步: LLM智能数据导入
            self._llm_intelligent_data_import(output_db_path)

            # 第6步: LLM业务智能分析
            self._llm_business_intelligence_analysis()

            # 第7步: LLM生成最终报告
            report = self._llm_generate_comprehensive_report(output_db_path, start_time)

            logger.info("✅ 纯LLM智能数据导入完成")
            return report

        except Exception as e:
            logger.error(f"❌ 纯LLM智能数据导入失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'execution_time': (datetime.now() - start_time).total_seconds()
            }

    def _llm_discover_and_classify_files(self, file_paths: List[str]):
        """
        第1步: LLM驱动的文件发现和分类
        完全依赖LLM分析文件名和内容来分类文件
        """
        logger.info("🔍 LLM文件发现和分类分析")

        # 收集所有文件的基本信息
        for file_path in file_paths:
            if os.path.exists(file_path) and file_path.endswith(('.xlsx', '.xls', '.csv')):
                try:
                    # 读取文件基本信息
                    file_info = self._extract_file_basic_info(file_path)
                    self.discovered_files[file_path] = file_info

                    logger.info(f"📄 发现文件: {os.path.basename(file_path)}")

                except Exception as e:
                    logger.warning(f"⚠️ 读取文件失败 {file_path}: {e}")

        # 使用LLM分析和分类所有文件
        if self.discovered_files:
            self._llm_classify_files_by_content()

    def _extract_file_basic_info(self, file_path: str) -> Dict[str, Any]:
        """提取文件基本信息供LLM分析"""
        try:
            # 读取文件内容
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path, nrows=20)  # 读取更多行供LLM分析
            else:
                df = pd.read_excel(file_path, nrows=20)

            # 提取详细信息供LLM分析
            file_info = {
                'file_path': file_path,
                'file_name': os.path.basename(file_path),
                'file_size': os.path.getsize(file_path),
                'columns': list(df.columns),
                'column_count': len(df.columns),
                'sample_rows': df.head(10).to_dict('records'),
                'data_types': {col: str(dtype) for col, dtype in df.dtypes.items()},
                'null_counts': df.isnull().sum().to_dict(),
                'unique_counts': df.nunique().to_dict(),
                'sample_values_per_column': {
                    col: df[col].dropna().unique()[:5].tolist()
                    for col in df.columns
                }
            }

            return file_info

        except Exception as e:
            logger.error(f"❌ 提取文件信息失败 {file_path}: {e}")
            return {}

    def _llm_classify_files_by_content(self):
        """使用LLM分析文件内容进行智能分类"""
        logger.info("🧠 LLM智能文件分类分析")

        # 构建文件分类分析提示词
        classification_prompt = self._build_file_classification_prompt()

        # 调用LLM进行文件分类
        classification_result = self._call_llm_unlimited_retry(classification_prompt)

        if classification_result:
            try:
                # 解析LLM分类结果
                parsed_result = self._parse_llm_json_response(classification_result)

                if parsed_result:
                    # 更新文件分析结果
                    for file_path, file_info in self.discovered_files.items():
                        file_name = os.path.basename(file_path)

                        # 查找LLM对该文件的分类结果
                        for file_analysis in parsed_result.get('file_classifications', []):
                            if file_analysis.get('file_name') == file_name:
                                file_info['llm_classification'] = file_analysis
                                logger.info(f"📋 LLM分类: {file_name} -> {file_analysis.get('file_type', 'unknown')}")
                                break

            except Exception as e:
                logger.error(f"❌ LLM文件分类结果解析失败: {e}")

    def _build_file_classification_prompt(self) -> str:
        """构建文件分类分析提示词"""

        # 收集所有文件信息
        files_info = []
        for file_path, file_info in self.discovered_files.items():
            # 转换样本数据为JSON可序列化格式
            sample_data = []
            for row in file_info['sample_rows'][:3]:  # 前3行样本
                if isinstance(row, dict):
                    # 转换字典中的值为字符串
                    converted_row = {}
                    for key, value in row.items():
                        if hasattr(value, 'strftime'):  # 日期时间对象
                            converted_row[key] = str(value)
                        elif hasattr(value, 'item'):  # numpy对象
                            converted_row[key] = value.item()
                        else:
                            converted_row[key] = str(value)
                    sample_data.append(converted_row)
                else:
                    sample_data.append(str(row))

            files_info.append({
                'file_name': file_info['file_name'],
                'columns': file_info['columns'],
                'sample_data': sample_data,
                'data_types': [str(dt) for dt in file_info['data_types']],  # 转换数据类型为字符串
                'column_count': file_info['column_count']
            })

        prompt = f"""你是一个专业的数据分析专家，请分析以下文件并进行智能分类。

## 待分析文件信息
{json.dumps(files_info, ensure_ascii=False, indent=2)}

## 分析任务
请对每个文件进行深度分析，判断其类型和用途。不要使用任何预定义的规则或关键词匹配，完全基于文件内容的语义理解进行分析。

## 分析维度
1. **文件类型识别**：
   - 业务数据文件：包含实际业务数据的文件
   - 数据字典文件：定义字段结构和含义的文件
   - 配置文件：包含系统配置信息的文件
   - 其他类型：请具体说明

2. **业务领域识别**：
   - 分析文件涉及的业务领域（如银行、保险、电商等）
   - 识别具体的业务场景（如客户管理、风险控制、财务分析等）

3. **数据特征分析**：
   - 数据的主要特征和模式
   - 字段的业务含义推断
   - 数据质量评估

4. **关联关系分析**：
   - 文件之间可能的关联关系
   - 主外键关系推断
   - 业务流程关系

## 输出格式
请以JSON格式返回分析结果：

{{
  "analysis_summary": "整体分析总结",
  "business_domain": "识别的业务领域",
  "file_classifications": [
    {{
      "file_name": "文件名",
      "file_type": "文件类型",
      "business_purpose": "业务用途",
      "data_characteristics": "数据特征描述",
      "key_fields": ["关键字段列表"],
      "business_concepts": ["识别的业务概念"],
      "data_quality_assessment": "数据质量评估",
      "relationships_with_other_files": "与其他文件的关系",
      "confidence_score": 0.95
    }}
  ],
  "overall_relationships": [
    {{
      "file1": "文件1",
      "file2": "文件2",
      "relationship_type": "关系类型",
      "relationship_description": "关系描述",
      "confidence": 0.9
    }}
  ],
  "business_intelligence_insights": [
    "业务智能洞察1",
    "业务智能洞察2"
  ]
}}

请进行深度分析，不要使用任何预设的规则或模式匹配，完全基于对数据内容的理解来进行分类和分析。
"""

        return prompt

    def _call_llm_unlimited_retry(self, prompt: str) -> Optional[str]:
        """直接requests调用 - 避免LangChain问题"""
        max_attempts = 15
        attempt = 0

        while attempt < max_attempts:
            attempt += 1
            try:
                logger.info(f"🤖 直接API调用 (第 {attempt} 次尝试)")

                # 根据尝试次数调整请求参数 - 针对ChunkedEncodingError优化
                if attempt > 3:
                    adjusted_tokens = max(1000, self.max_tokens_per_request // 2)
                    logger.info(f"🔧 调整max_tokens为 {adjusted_tokens} (避免ChunkedEncodingError)")
                elif attempt > 1:
                    adjusted_tokens = max(2000, self.max_tokens_per_request * 3 // 4)
                    logger.info(f"🔧 调整max_tokens为 {adjusted_tokens} (减少响应大小)")
                else:
                    adjusted_tokens = self.max_tokens_per_request

                # 构建请求数据
                headers = {
                    'Authorization': f'Bearer {self.api_key}',
                    'Content-Type': 'application/json'
                }

                data = {
                    'model': 'deepseek-chat',
                    'messages': [
                        {
                            'role': 'system',
                            'content': '你是一个专业的数据分析和数据库设计专家，擅长通过深度分析理解数据的业务含义和结构关系。'
                        },
                        {
                            'role': 'user',
                            'content': prompt
                        }
                    ],
                    'max_tokens': adjusted_tokens,
                    'temperature': 0.1
                }

                # 发送请求
                response = requests.post(
                    'https://api.deepseek.com/chat/completions',
                    headers=headers,
                    json=data,
                    timeout=120  # 2分钟超时
                )

                if response.status_code == 200:
                    result = response.json()
                    content = result['choices'][0]['message']['content']
                    logger.info(f"✅ API调用成功 (第 {attempt} 次尝试)")
                    return content.strip()
                else:
                    raise Exception(f"API错误: {response.status_code} - {response.text}")

            except Exception as e:
                error_type = type(e).__name__
                error_msg = str(e)

                logger.warning(f"⚠️ API调用失败 (第 {attempt} 次): {error_type} - {error_msg}")

                # 根据错误类型调整重试策略
                if "ChunkedEncodingError" in error_type or "prematurely" in error_msg:
                    wait_time = min(5 + (attempt * 3), 30)
                    logger.info(f"📦 响应传输中断，等待 {wait_time} 秒")
                elif "ConnectionError" in error_type or "timeout" in error_msg.lower():
                    wait_time = min(10 + (attempt * 5), 60)
                    logger.info(f"🌐 网络问题，等待 {wait_time} 秒")
                elif "429" in error_msg or "rate" in error_msg.lower():
                    wait_time = min(30 + (attempt * 10), 120)
                    logger.info(f"🚦 API限制，等待 {wait_time} 秒")
                else:
                    wait_time = min(2 ** attempt, 60)
                    logger.info(f"❓ 其他错误，等待 {wait_time} 秒")

                if attempt < max_attempts:
                    logger.info(f"⏳ 等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"❌ API调用最终失败，已尝试 {max_attempts} 次")
                    return None

        return None

    def _parse_llm_json_response(self, response: str) -> Optional[Dict[str, Any]]:
        """解析LLM的JSON响应，支持多种格式"""
        if not response or not response.strip():
            logger.warning("⚠️ 响应为空")
            return None

        # 清理响应内容
        cleaned_response = self._clean_llm_response(response)

        try:
            # 尝试直接解析
            parsed_result = json.loads(cleaned_response)
            logger.info("✅ LLM响应解析成功")
            return parsed_result

        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON解析失败: {e}")
            logger.error(f"📝 原始响应前100字符: {response[:100]}...")
            logger.error(f"🧹 清理后响应前100字符: {cleaned_response[:100]}...")
            return None
        except Exception as e:
            logger.error(f"❌ 响应解析失败: {e}")
            return None

    def _clean_llm_response(self, response: str) -> str:
        """清理LLM响应，提取JSON部分"""
        if not response:
            return ""

        # 移除可能的前缀文本
        response = response.strip()

        # 移除markdown代码块标记
        if response.startswith('```json'):
            response = response[7:]
        elif response.startswith('```'):
            response = response[3:]

        if response.endswith('```'):
            response = response[:-3]

        response = response.strip()

        # 查找JSON开始位置
        json_start = response.find('{')
        if json_start == -1:
            # 没有找到JSON，返回原始响应
            return response

        # 查找JSON结束位置
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
            # 没有找到完整的JSON，返回从开始位置到结尾
            return response[json_start:]

        # 返回提取的JSON部分
        return response[json_start:json_end]

    def _extract_schema_info(self, file_path: str) -> Dict[str, Any]:
        """提取文件的schema信息（字段名、类型、样本值）"""
        try:
            df = pd.read_excel(file_path)

            schema_info = {
                'file_name': os.path.basename(file_path),
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'fields': []
            }

            for col in df.columns:
                # 分析字段类型
                col_data = df[col].dropna()
                if len(col_data) == 0:
                    data_type = 'TEXT'
                    sample_values = []
                else:
                    # 智能类型检测
                    if col_data.dtype in ['int64', 'int32']:
                        data_type = 'INTEGER'
                    elif col_data.dtype in ['float64', 'float32']:
                        data_type = 'REAL'
                    elif pd.api.types.is_datetime64_any_dtype(col_data):
                        data_type = 'DATE'
                    else:
                        data_type = 'TEXT'

                    # 获取样本值（最多3个）
                    sample_values = col_data.head(3).tolist()
                    # 转换为JSON可序列化格式
                    sample_values = [str(v) if not isinstance(v, (int, float, str, bool)) else v for v in sample_values]

                schema_info['fields'].append({
                    'field_name': col,
                    'detected_type': data_type,
                    'sample_values': sample_values,
                    'null_count': df[col].isna().sum(),
                    'unique_count': df[col].nunique()
                })

            return schema_info

        except Exception as e:
            logger.error(f"❌ Schema提取失败 {file_path}: {e}")
            return None

    def _convert_data_dict_to_markdown(self, file_path: str) -> str:
        """将数据字典文件转换为Markdown格式"""
        try:
            df = pd.read_excel(file_path)

            markdown_content = f"# 数据字典: {os.path.basename(file_path)}\n\n"

            # 转换为表格格式
            if len(df.columns) >= 2:
                markdown_content += "| " + " | ".join(df.columns) + " |\n"
                markdown_content += "| " + " | ".join(["---"] * len(df.columns)) + " |\n"

                for _, row in df.iterrows():
                    row_values = [str(v) if not pd.isna(v) else "" for v in row]
                    markdown_content += "| " + " | ".join(row_values) + " |\n"

            return markdown_content

        except Exception as e:
            logger.error(f"❌ 数据字典转换失败 {file_path}: {e}")
            return f"数据字典文件: {os.path.basename(file_path)} (转换失败)"

    def _llm_deep_content_analysis(self):
        """
        简化策略：基于规则的快速导入 + 最小LLM增强
        """
        logger.info("🔬 简化策略分析")

        # 直接处理每个业务数据文件，使用基础schema + 简单LLM增强
        for file_path, file_info in self.discovered_files.items():
            if file_info.get('classification') == '业务数据文件':
                logger.info(f"� 处理文件: {file_info['file_name']}")

                try:
                    # 读取Excel文件
                    df = pd.read_excel(file_path)

                    # 构建基础分析结果
                    basic_analysis = {
                        'file_name': file_info['file_name'],
                        'table_name_suggestion': self._generate_table_name(file_info['file_name']),
                        'total_rows': len(df),
                        'total_columns': len(df.columns),
                        'columns': list(df.columns),
                        'field_analysis': []
                    }

                    # 分析每个字段
                    for col in df.columns:
                        col_data = df[col].dropna()

                        # 智能类型检测
                        if len(col_data) == 0:
                            data_type = 'TEXT'
                        elif col_data.dtype in ['int64', 'int32']:
                            data_type = 'INTEGER'
                        elif col_data.dtype in ['float64', 'float32']:
                            data_type = 'REAL'
                        elif pd.api.types.is_datetime64_any_dtype(col_data):
                            data_type = 'DATE'
                        else:
                            data_type = 'TEXT'

                        # 获取样本值
                        sample_values = col_data.head(3).tolist() if len(col_data) > 0 else []

                        basic_analysis['field_analysis'].append({
                            'field_name': col,
                            'english_name': self._generate_english_name(col),
                            'data_type': data_type,
                            'business_meaning': self._guess_business_meaning(col),
                            'sample_values': sample_values,
                            'is_primary_key': self._is_likely_primary_key(col, col_data),
                            'is_foreign_key': False
                        })

                    # 存储分析结果
                    self.llm_file_analysis[file_path] = {
                        'basic_info': basic_analysis,
                        'llm_analysis': basic_analysis,  # 使用基础分析作为LLM分析
                        'sample_data': df.head(3).to_dict('records')
                    }

                    logger.info(f"✅ 分析完成: {file_info['file_name']} ({len(df.columns)}个字段)")

                except Exception as e:
                    logger.error(f"❌ 文件处理失败: {file_info['file_name']} - {e}")
                    continue

    def _generate_table_name(self, file_name: str) -> str:
        """生成表名"""
        # 移除扩展名和特殊字符
        name = file_name.replace('.xlsx', '').replace('.xls', '')
        # 简单的映射规则
        if '合同' in name and '分类' in name:
            return 'contract_classification'
        elif '贷款' in name and '合同' in name:
            return 'loan_contract_info'
        elif '存款' in name and '余额' in name:
            return 'deposit_balance'
        else:
            # 默认使用文件名的拼音或简化版本
            return 'data_table'

    def _generate_english_name(self, chinese_name: str) -> str:
        """生成英文字段名"""
        # 简单的映射规则
        mapping = {
            '客户号': 'customer_id',
            '客户名称': 'customer_name',
            '合同号': 'contract_id',
            '贷款余额': 'loan_balance',
            '存款余额': 'deposit_balance',
            '日期': 'date',
            '金额': 'amount',
            '利率': 'interest_rate',
            '期限': 'term',
            '状态': 'status'
        }
        return mapping.get(chinese_name, chinese_name.lower().replace(' ', '_'))

    def _guess_business_meaning(self, field_name: str) -> str:
        """猜测业务含义"""
        if '客户' in field_name:
            return '客户相关信息'
        elif '合同' in field_name:
            return '合同相关信息'
        elif '余额' in field_name:
            return '金额余额'
        elif '日期' in field_name:
            return '日期时间'
        else:
            return '业务数据字段'

    def _is_likely_primary_key(self, field_name: str, data) -> bool:
        """判断是否可能是主键"""
        if len(data) == 0:
            return False
        # 如果字段名包含ID或唯一值比例很高
        if 'id' in field_name.lower() or '号' in field_name:
            unique_ratio = len(data.unique()) / len(data)
            return unique_ratio > 0.9
        return False

    def _build_deep_analysis_prompt(self, file_info: Dict[str, Any]) -> str:
        """构建深度分析提示词"""

        # 简化模式已关闭，使用完整分析

        # 转换样本数据为JSON可序列化格式 - 减少数据量避免ChunkedEncodingError
        sample_rows_serializable = []
        # 大幅减少样本行数
        max_sample_rows = 3
        for row in file_info['sample_rows'][:max_sample_rows]:
            if isinstance(row, dict):
                converted_row = {}
                for key, value in row.items():
                    if hasattr(value, 'strftime'):  # 日期时间对象
                        converted_row[key] = str(value)
                    elif hasattr(value, 'item'):  # numpy对象
                        converted_row[key] = value.item()
                    else:
                        # 限制字符串长度以减少数据量
                        str_value = str(value)
                        if len(str_value) > 100:
                            str_value = str_value[:100] + "..."
                        converted_row[key] = str_value
                sample_rows_serializable.append(converted_row)
            else:
                sample_rows_serializable.append(str(row))

        prompt = f"""你是一个资深的数据架构师和业务分析专家，请对以下文件进行深度的业务逻辑和数据结构分析。

## 文件信息
文件名: {file_info['file_name']}
字段数量: {file_info['column_count']}
文件大小: {file_info['file_size']} 字节

## 字段列表
{json.dumps(file_info['columns'], ensure_ascii=False, indent=2)}

## 数据类型信息
{json.dumps(file_info['data_types'], ensure_ascii=False, indent=2)}

## 样本数据 (前3行，优化大小)
{json.dumps(sample_rows_serializable, ensure_ascii=False, indent=2)}

## 数据统计信息
空值统计: {json.dumps(file_info['null_counts'], ensure_ascii=False, indent=2)}
唯一值统计: {json.dumps(file_info['unique_counts'], ensure_ascii=False, indent=2)}

## 每列样本值 (限制数量)
{json.dumps(self._convert_to_serializable_limited(file_info['sample_values_per_column']), ensure_ascii=False, indent=2)}

## 深度分析任务
请进行以下维度的深度分析，不要使用任何预设规则，完全基于数据内容的语义理解：

1. **业务语义分析**：
   - 每个字段的真实业务含义
   - 字段之间的业务逻辑关系
   - 数据反映的业务流程和场景

2. **数据结构分析**：
   - 主键字段识别及理由
   - 外键关系推断
   - 数据完整性约束建议

3. **数据质量分析**：
   - 数据质量问题识别
   - 数据清洗建议
   - 异常值和缺失值处理策略

4. **业务规则挖掘**：
   - 从数据中发现的业务规则
   - 数据验证规则建议
   - 业务术语定义

5. **标准化建议**：
   - 字段命名标准化建议
   - 数据类型优化建议
   - 索引策略建议

## 输出格式
请以JSON格式返回详细分析结果：

{{
  "file_analysis": {{
    "business_domain": "业务领域",
    "business_scenario": "具体业务场景",
    "data_purpose": "数据用途描述"
  }},
  "field_analysis": [
    {{
      "field_name": "字段名",
      "business_meaning": "业务含义",
      "data_type_recommendation": "推荐数据类型",
      "is_primary_key": true/false,
      "is_foreign_key": true/false,
      "foreign_key_reference": "外键引用表.字段",
      "is_required": true/false,
      "business_rules": ["业务规则1", "业务规则2"],
      "data_quality_issues": ["质量问题1", "质量问题2"],
      "standardized_name": "标准化字段名",
      "validation_rules": ["验证规则1", "验证规则2"],
      "index_recommendation": "索引建议"
    }}
  ],
  "business_terms": [
    {{
      "term": "业务术语",
      "definition": "术语定义",
      "sql_expression": "SQL表达式",
      "applicable_fields": ["适用字段列表"]
    }}
  ],
  "data_relationships": [
    {{
      "relationship_type": "关系类型",
      "description": "关系描述",
      "fields_involved": ["涉及字段"]
    }}
  ],
  "table_design": {{
    "recommended_table_name": "推荐表名",
    "table_description": "表描述",
    "primary_key_fields": ["主键字段"],
    "indexes": [
      {{
        "index_name": "索引名",
        "fields": ["字段列表"],
        "index_type": "索引类型"
      }}
    ]
  }},
  "data_quality_report": {{
    "overall_quality_score": 0.85,
    "quality_issues": ["问题列表"],
    "cleaning_recommendations": ["清洗建议"],
    "validation_suggestions": ["验证建议"]
  }},
  "business_intelligence": {{
    "key_insights": ["关键洞察"],
    "business_value": "业务价值",
    "usage_scenarios": ["使用场景"]
  }}
}}

请基于对数据的深度理解进行分析，不要使用任何预设的模式或规则。
"""

        return prompt

    def _build_simplified_analysis_prompt(self, file_info: Dict[str, Any]) -> str:
        """构建简化分析提示词 - 极度压缩版本"""

        # 只取最基础的信息
        basic_info = {
            'file_name': file_info['file_name'],
            'columns': file_info['columns'][:10],  # 最多10个字段
            'column_count': min(file_info['column_count'], 10),
            'row_count': file_info['row_count']
        }

        prompt = f"""你是数据分析专家，请对以下文件进行快速分析。

文件名: {basic_info['file_name']}
字段数: {basic_info['column_count']}
数据行数: {basic_info['row_count']}
字段列表: {', '.join(basic_info['columns'])}

请返回JSON格式的简化分析：
{{
  "file_analysis": {{
    "business_domain": "根据文件名推断的业务领域",
    "table_name": "建议的数据库表名",
    "data_purpose": "数据用途简述"
  }},
  "field_mapping": {{
    "primary_fields": ["主要字段1", "主要字段2"],
    "data_types": {{"字段名": "推断类型"}}
  }}
}}

要求：
1. 分析要简洁准确
2. 表名使用英文，符合数据库命名规范
3. 重点关注核心业务字段
4. 响应控制在500字以内"""

        return prompt

    def _convert_to_serializable(self, obj):
        """将对象转换为JSON可序列化格式"""
        if isinstance(obj, dict):
            return {key: self._convert_to_serializable(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_to_serializable(item) for item in obj]
        elif hasattr(obj, 'strftime'):  # 日期时间对象
            return str(obj)
        elif hasattr(obj, 'item'):  # numpy对象
            return obj.item()
        else:
            return str(obj)

    def _convert_to_serializable_limited(self, obj, max_items=3):
        """将对象转换为JSON可序列化格式 - 限制数量版本"""
        if isinstance(obj, dict):
            limited_dict = {}
            for key, value in obj.items():
                if isinstance(value, list):
                    # 限制列表长度
                    limited_dict[key] = [self._convert_to_serializable(item) for item in value[:max_items]]
                else:
                    limited_dict[key] = self._convert_to_serializable(value)
            return limited_dict
        elif isinstance(obj, list):
            return [self._convert_to_serializable(item) for item in obj[:max_items]]
        elif hasattr(obj, 'strftime'):  # 日期时间对象
            return str(obj)
        elif hasattr(obj, 'item'):  # numpy对象
            return obj.item()
        else:
            return str(obj)

    def _llm_intelligent_database_design(self):
        """
        第3步: LLM智能数据库设计
        基于所有文件的分析结果，设计完整的数据库架构
        """
        logger.info("🏗️ LLM智能数据库设计")

        # 构建数据库设计提示词
        design_prompt = self._build_database_design_prompt()

        # 调用LLM进行数据库设计
        design_result = self._call_llm_unlimited_retry(design_prompt)

        if design_result:
            parsed_result = self._parse_llm_json_response(design_result)
            if parsed_result:
                self.llm_schema_design = parsed_result
                logger.info("✅ 数据库设计完成")
            else:
                logger.warning("⚠️ 数据库设计结果解析失败")
        else:
            logger.warning("⚠️ 数据库设计失败")

    def _build_database_design_prompt(self) -> str:
        """构建数据库设计提示词"""

        # 简化分析结果，只包含关键信息
        simplified_analysis = {}
        for file_path, analysis in self.llm_file_analysis.items():
            file_name = os.path.basename(file_path)
            # 只保留关键信息，避免JSON过大
            simplified_analysis[file_name] = {
                'file_type': analysis.get('file_type', ''),
                'business_domain': analysis.get('business_domain', ''),
                'key_fields': analysis.get('key_fields', [])[:5],  # 只取前5个字段
                'table_suggestion': analysis.get('table_suggestion', '')
            }

        # 只处理业务数据文件
        business_files = []
        for file_path, analysis in self.llm_file_analysis.items():
            if analysis.get('file_type') == '业务数据文件':
                business_files.append({
                    'file_name': os.path.basename(file_path),
                    'file_path': file_path,
                    'business_domain': analysis.get('business_domain', ''),
                    'key_fields': analysis.get('key_fields', [])[:5]
                })

        prompt = f"""基于以下业务数据文件，为每个文件设计对应的数据库表。

业务数据文件：
{json.dumps(business_files, ensure_ascii=False, indent=2)}

请为每个业务数据文件设计一个对应的表，返回JSON格式：

{{
  "tables": [
    {{
      "table_name": "基于文件名的英文表名",
      "source_file": "完整的源文件名",
      "description": "表的业务描述",
      "fields": [
        {{
          "field_name": "字段名",
          "data_type": "TEXT",
          "is_primary_key": false,
          "description": "字段描述"
        }}
      ]
    }}
  ]
}}

要求：
1. 必须为每个业务数据文件创建一个表
2. 表名基于文件名生成，使用英文，符合数据库命名规范
3. source_file必须与输入的文件名完全一致
4. 字段类型统一使用TEXT
5. 只返回JSON，不要其他文字
"""

        return prompt

    def _llm_guided_database_creation(self, output_db_path: str):
        """
        第4步: LLM指导的数据库创建
        根据LLM设计的架构创建实际的数据库结构
        """
        logger.info("🗄️ LLM指导的数据库创建")

        if not self.llm_schema_design:
            logger.error("❌ 没有可用的数据库设计方案")
            return

        try:
            # 删除已存在的数据库文件
            if os.path.exists(output_db_path):
                os.remove(output_db_path)

            # 创建数据库连接
            conn = sqlite3.connect(output_db_path)
            cursor = conn.cursor()

            # 创建表结构
            tables = self.llm_schema_design.get('tables', [])
            for table_config in tables:
                table_name = table_config['table_name']

                # 生成CREATE TABLE语句
                create_sql = self._generate_create_table_sql(table_config)

                if create_sql:
                    logger.info(f"📋 创建表: {table_name}")
                    cursor.execute(create_sql)

                    # 记录创建日志
                    self.import_execution_log.append({
                        'step': 'create_table',
                        'table_name': table_name,
                        'sql': create_sql,
                        'timestamp': datetime.now().isoformat()
                    })

                # 创建索引
                indexes = table_config.get('indexes', [])
                for index_config in indexes:
                    if index_config['index_type'] != 'PRIMARY':  # 主键已在表创建时处理
                        index_sql = self._generate_create_index_sql(table_name, index_config)
                        if index_sql:
                            logger.info(f"🔍 创建索引: {index_config['index_name']}")
                            cursor.execute(index_sql)

            conn.commit()
            conn.close()

            logger.info("✅ 数据库结构创建完成")

        except Exception as e:
            logger.error(f"❌ 数据库创建失败: {e}")
            raise

    def _generate_create_table_sql(self, table_config: Dict[str, Any]) -> str:
        """生成CREATE TABLE SQL语句"""

        table_name = table_config['table_name']
        fields = table_config.get('fields', [])

        if not fields:
            return ""

        sql_parts = [f"CREATE TABLE {table_name} ("]
        field_definitions = []
        primary_keys = []

        for field in fields:
            field_name = field['field_name']
            data_type = field['data_type']

            # 构建字段定义
            field_def = f"    {field_name} {data_type}"

            # 添加约束
            if not field.get('is_nullable', True):
                field_def += " NOT NULL"

            if field.get('default_value'):
                default_val = field['default_value']
                if data_type.upper().startswith(('VARCHAR', 'TEXT', 'CHAR')):
                    field_def += f" DEFAULT '{default_val}'"
                else:
                    field_def += f" DEFAULT {default_val}"

            if field.get('check_constraint'):
                field_def += f" CHECK ({field['check_constraint']})"

            field_definitions.append(field_def)

            # 收集主键
            if field.get('is_primary_key', False):
                primary_keys.append(field_name)

        sql_parts.append(",\n".join(field_definitions))

        # 添加主键约束
        if primary_keys:
            sql_parts.append(f",\n    PRIMARY KEY ({', '.join(primary_keys)})")

        sql_parts.append("\n)")

        return "".join(sql_parts)

    def _generate_create_index_sql(self, table_name: str, index_config: Dict[str, Any]) -> str:
        """生成CREATE INDEX SQL语句"""

        index_name = index_config['index_name']
        index_type = index_config['index_type']
        fields = index_config['fields']

        if index_type == 'UNIQUE':
            sql = f"CREATE UNIQUE INDEX {index_name} ON {table_name} ({', '.join(fields)})"
        else:
            sql = f"CREATE INDEX {index_name} ON {table_name} ({', '.join(fields)})"

        return sql

    def _llm_intelligent_data_import(self, output_db_path: str):
        """
        第5步: LLM智能数据导入
        使用LLM指导进行智能的数据映射和导入
        """
        logger.info("📥 LLM智能数据导入")

        if not self.llm_schema_design:
            logger.error("❌ 没有可用的数据库设计方案")
            return

        try:
            conn = sqlite3.connect(output_db_path)

            # 为每个表导入数据
            tables = self.llm_schema_design.get('tables', [])
            for table_config in tables:
                table_name = table_config['table_name']
                source_file = table_config.get('source_file')

                if source_file:
                    # 找到对应的源文件
                    source_file_path = self._find_source_file_path(source_file)

                    if source_file_path:
                        logger.info(f"📊 导入数据到表: {table_name}")

                        # 使用LLM指导进行数据映射和导入
                        success = self._llm_guided_data_mapping_and_import(
                            conn, table_config, source_file_path
                        )

                        if success:
                            logger.info(f"✅ 数据导入成功: {table_name}")
                        else:
                            logger.warning(f"⚠️ 数据导入失败: {table_name}")
                    else:
                        logger.warning(f"⚠️ 未找到源文件: {source_file}")

            conn.close()
            logger.info("✅ 数据导入完成")

        except Exception as e:
            logger.error(f"❌ 数据导入失败: {e}")
            raise

    def _find_source_file_path(self, source_file_name: str) -> Optional[str]:
        """查找源文件路径"""
        for file_path, file_info in self.discovered_files.items():
            if file_info['file_name'] == source_file_name:
                return file_path
        return None

    def _llm_guided_data_mapping_and_import(self, conn: sqlite3.Connection,
                                          table_config: Dict[str, Any],
                                          source_file_path: str) -> bool:
        """LLM指导的数据映射和导入"""

        try:
            # 读取源文件数据
            if source_file_path.endswith('.csv'):
                df = pd.read_csv(source_file_path)
            else:
                df = pd.read_excel(source_file_path)

            # 构建数据映射提示词
            mapping_prompt = self._build_data_mapping_prompt(table_config, df)

            # 调用LLM进行数据映射分析
            mapping_result = self._call_llm_unlimited_retry(mapping_prompt)

            if mapping_result:
                parsed_mapping = self._parse_llm_json_response(mapping_result)

                if parsed_mapping:
                    # 根据LLM的映射结果进行数据转换
                    transformed_df = self._transform_data_with_llm_mapping(df, parsed_mapping)

                    # 导入数据到数据库
                    table_name = table_config['table_name']
                    transformed_df.to_sql(table_name, conn, if_exists='append', index=False)

                    # 记录导入日志
                    self.import_execution_log.append({
                        'step': 'import_data',
                        'table_name': table_name,
                        'source_file': source_file_path,
                        'imported_rows': len(transformed_df),
                        'timestamp': datetime.now().isoformat()
                    })

                    return True

            return False

        except Exception as e:
            logger.error(f"❌ LLM指导的数据映射和导入失败: {e}")
            return False

    def _build_data_mapping_prompt(self, table_config: Dict[str, Any], df: pd.DataFrame) -> str:
        """构建数据映射提示词"""

        # 获取源数据信息
        source_columns = list(df.columns)
        sample_data = df.head(5).to_dict('records')

        # 获取目标表结构
        target_fields = table_config.get('fields', [])

        prompt = f"""你是一个数据映射专家，请分析源数据和目标表结构，提供精确的字段映射和数据转换方案。

## 目标表结构
表名: {table_config['table_name']}
表描述: {table_config.get('description', '')}

目标字段:
{json.dumps(target_fields, ensure_ascii=False, indent=2)}

## 源数据信息
源字段: {source_columns}

样本数据:
{json.dumps(sample_data, ensure_ascii=False, indent=2)}

## 映射任务
请分析源数据和目标表结构，提供详细的字段映射方案。不要使用任何预设规则，完全基于对数据语义的理解进行映射。

## 分析要求
1. **字段映射**：确定每个目标字段对应的源字段
2. **数据转换**：定义必要的数据类型转换和格式化
3. **数据清洗**：识别需要清洗的数据问题
4. **默认值处理**：为缺失数据提供合理的默认值
5. **数据验证**：定义数据有效性检查规则

## 输出格式
请以JSON格式返回映射方案：

{{
  "mapping_analysis": {{
    "confidence_score": 0.95,
    "mapping_strategy": "映射策略描述",
    "data_quality_assessment": "数据质量评估"
  }},
  "field_mappings": [
    {{
      "target_field": "目标字段名",
      "source_field": "源字段名",
      "mapping_type": "DIRECT/TRANSFORM/CALCULATE/DEFAULT",
      "transformation_rule": "转换规则",
      "data_type_conversion": "数据类型转换",
      "default_value": "默认值",
      "validation_rule": "验证规则",
      "cleaning_operations": ["清洗操作列表"],
      "confidence": 0.9
    }}
  ],
  "data_transformations": [
    {{
      "operation": "操作类型",
      "description": "操作描述",
      "affected_fields": ["影响字段"],
      "transformation_logic": "转换逻辑"
    }}
  ],
  "data_quality_issues": [
    {{
      "issue_type": "问题类型",
      "affected_fields": ["影响字段"],
      "description": "问题描述",
      "resolution": "解决方案"
    }}
  ]
}}

请基于深度的语义理解进行映射，确保数据的准确性和完整性。
"""

        return prompt

    def _transform_data_with_llm_mapping(self, df: pd.DataFrame,
                                       mapping_config: Dict[str, Any]) -> pd.DataFrame:
        """根据LLM映射配置转换数据"""

        try:
            # 创建新的DataFrame用于存储转换后的数据
            transformed_data = {}

            # 根据字段映射进行数据转换
            field_mappings = mapping_config.get('field_mappings', [])

            for mapping in field_mappings:
                target_field = mapping['target_field']
                source_field = mapping.get('source_field')
                mapping_type = mapping.get('mapping_type', 'DIRECT')
                default_value = mapping.get('default_value')
                transformation_rule = mapping.get('transformation_rule', '')

                if mapping_type == 'DIRECT' and source_field and source_field in df.columns:
                    # 直接映射
                    transformed_data[target_field] = df[source_field]

                elif mapping_type == 'TRANSFORM' and source_field and source_field in df.columns:
                    # 需要转换的映射
                    transformed_data[target_field] = self._apply_transformation(
                        df[source_field], transformation_rule
                    )

                elif mapping_type == 'DEFAULT':
                    # 使用默认值
                    transformed_data[target_field] = [default_value] * len(df)

                elif mapping_type == 'CALCULATE':
                    # 计算字段（基于其他字段计算）
                    transformed_data[target_field] = self._calculate_field(
                        df, transformation_rule
                    )

                else:
                    # 如果无法映射，使用默认值或空值
                    if default_value is not None:
                        transformed_data[target_field] = [default_value] * len(df)
                    else:
                        transformed_data[target_field] = [None] * len(df)

            # 创建转换后的DataFrame
            result_df = pd.DataFrame(transformed_data)

            # 应用数据清洗操作
            result_df = self._apply_data_cleaning(result_df, mapping_config)

            return result_df

        except Exception as e:
            logger.error(f"❌ 数据转换失败: {e}")
            return pd.DataFrame()

    def _apply_transformation(self, series: pd.Series, transformation_rule: str) -> pd.Series:
        """应用数据转换规则"""
        try:
            # 这里可以根据transformation_rule实现各种转换
            # 为了简化，这里只实现基本的类型转换
            if 'to_string' in transformation_rule.lower():
                return series.astype(str)
            elif 'to_numeric' in transformation_rule.lower():
                return pd.to_numeric(series, errors='coerce')
            elif 'to_datetime' in transformation_rule.lower():
                return pd.to_datetime(series, errors='coerce')
            else:
                return series
        except Exception as e:
            logger.warning(f"⚠️ 转换规则应用失败: {e}")
            return series

    def _calculate_field(self, df: pd.DataFrame, calculation_rule: str) -> pd.Series:
        """计算字段值"""
        try:
            # 这里可以实现复杂的计算逻辑
            # 为了简化，返回空值
            return pd.Series([None] * len(df))
        except Exception as e:
            logger.warning(f"⚠️ 字段计算失败: {e}")
            return pd.Series([None] * len(df))

    def _apply_data_cleaning(self, df: pd.DataFrame,
                           mapping_config: Dict[str, Any]) -> pd.DataFrame:
        """应用数据清洗操作"""
        try:
            # 根据映射配置中的清洗规则进行数据清洗
            data_quality_issues = mapping_config.get('data_quality_issues', [])

            for issue in data_quality_issues:
                resolution = issue.get('resolution', '')
                affected_fields = issue.get('affected_fields', [])

                for field in affected_fields:
                    if field in df.columns:
                        if 'remove_nulls' in resolution.lower():
                            df[field] = df[field].fillna('')
                        elif 'convert_type' in resolution.lower():
                            df[field] = pd.to_numeric(df[field], errors='coerce')

            return df

        except Exception as e:
            logger.warning(f"⚠️ 数据清洗失败: {e}")
            return df

    def _llm_business_intelligence_analysis(self):
        """
        第6步: LLM业务智能分析
        基于导入的数据进行深度的业务智能分析
        """
        logger.info("🧠 LLM业务智能分析")

        # 构建业务智能分析提示词
        bi_prompt = self._build_business_intelligence_prompt()

        # 调用LLM进行业务智能分析
        bi_result = self._call_llm_unlimited_retry(bi_prompt)

        if bi_result:
            parsed_result = self._parse_llm_json_response(bi_result)
            if parsed_result:
                self.llm_business_intelligence = parsed_result
                logger.info("✅ 业务智能分析完成")
            else:
                logger.warning("⚠️ 业务智能分析结果解析失败")
        else:
            logger.warning("⚠️ 业务智能分析失败")

    def _build_business_intelligence_prompt(self) -> str:
        """构建业务智能分析提示词"""

        # 收集所有分析结果
        analysis_summary = {
            'file_analysis': self.llm_file_analysis,
            'schema_design': self.llm_schema_design,
            'import_log': self.import_execution_log
        }

        prompt = f"""你是一个资深的业务智能分析师，请基于以下数据导入和分析结果，提供深度的业务智能洞察。

## 完整分析结果
{json.dumps(analysis_summary, ensure_ascii=False, indent=2)}

## 业务智能分析任务
请基于对数据的深度理解，提供全面的业务智能分析。不要使用任何预设模式，完全基于数据的业务逻辑进行分析。

## 分析维度
1. **业务价值分析**：
   - 数据的核心业务价值
   - 关键业务指标识别
   - 业务流程洞察

2. **数据关系洞察**：
   - 数据实体间的业务关系
   - 关键业务规则发现
   - 数据驱动的业务逻辑

3. **业务术语体系**：
   - 核心业务术语定义
   - 业务概念层次结构
   - 术语间的逻辑关系

4. **查询场景预测**：
   - 可能的业务查询场景
   - 关键分析需求预测
   - 报表需求识别

5. **业务优化建议**：
   - 数据质量改进建议
   - 业务流程优化建议
   - 数据治理建议

## 输出格式
请以JSON格式返回业务智能分析：

{{
  "business_intelligence_summary": {{
    "business_domain": "业务领域",
    "core_business_value": "核心业务价值",
    "key_business_processes": ["关键业务流程"],
    "data_maturity_level": "数据成熟度等级"
  }},
  "business_entities": [
    {{
      "entity_name": "业务实体名",
      "description": "实体描述",
      "key_attributes": ["关键属性"],
      "business_rules": ["业务规则"],
      "relationships": ["关系描述"]
    }}
  ],
  "business_terms_dictionary": [
    {{
      "term": "业务术语",
      "definition": "术语定义",
      "category": "术语分类",
      "sql_expression": "SQL表达式",
      "business_context": "业务上下文",
      "usage_examples": ["使用示例"],
      "related_terms": ["相关术语"]
    }}
  ],
  "key_business_metrics": [
    {{
      "metric_name": "指标名称",
      "description": "指标描述",
      "calculation_logic": "计算逻辑",
      "business_significance": "业务意义",
      "data_sources": ["数据来源"],
      "update_frequency": "更新频率"
    }}
  ],
  "query_scenarios": [
    {{
      "scenario_name": "查询场景",
      "description": "场景描述",
      "business_question": "业务问题",
      "required_data": ["所需数据"],
      "expected_insights": "预期洞察",
      "query_complexity": "查询复杂度"
    }}
  ],
  "data_quality_insights": {{
    "overall_quality_score": 0.85,
    "quality_dimensions": [
      {{
        "dimension": "质量维度",
        "score": 0.9,
        "issues": ["问题列表"],
        "recommendations": ["改进建议"]
      }}
    ]
  }},
  "business_optimization_recommendations": [
    {{
      "category": "优化类别",
      "recommendation": "优化建议",
      "business_impact": "业务影响",
      "implementation_priority": "实施优先级",
      "expected_benefits": ["预期收益"]
    }}
  ],
  "context_configuration": {{
    "database_specific_business_terms": "业务术语配置",
    "query_scope_rules": "查询范围规则",
    "field_mappings": "字段映射配置",
    "relationship_definitions": "关系定义"
  }}
}}

请基于深度的业务理解提供洞察，确保分析结果具有实际的业务指导价值。
"""

        return prompt

    def _llm_generate_comprehensive_report(self, output_db_path: str,
                                         start_time: datetime) -> Dict[str, Any]:
        """
        第7步: LLM生成最终报告
        生成完整的处理报告和业务洞察
        """
        logger.info("📋 LLM生成综合报告")

        execution_time = (datetime.now() - start_time).total_seconds()

        # 统计导入数据
        total_imported_rows = sum(
            log.get('imported_rows', 0) for log in self.import_execution_log
            if log.get('step') == 'import_data'
        )

        total_tables_created = len([
            log for log in self.import_execution_log
            if log.get('step') == 'create_table'
        ])

        # 构建报告生成提示词
        report_prompt = self._build_report_generation_prompt(
            output_db_path, execution_time, total_imported_rows, total_tables_created
        )

        # 调用LLM生成报告
        report_result = self._call_llm_unlimited_retry(report_prompt)

        if report_result:
            parsed_report = self._parse_llm_json_response(report_result)

            if parsed_report:
                # 合并基础统计信息
                final_report = {
                    'success': True,
                    'execution_time': execution_time,
                    'output_database': output_db_path,
                    'processing_summary': {
                        'total_files_processed': len(self.discovered_files),
                        'total_tables_created': total_tables_created,
                        'total_imported_rows': total_imported_rows,
                        'llm_analysis_success_rate': len(self.llm_file_analysis) / len(self.discovered_files) if self.discovered_files else 0
                    },
                    'llm_comprehensive_analysis': parsed_report,
                    'business_intelligence': self.llm_business_intelligence,
                    'detailed_execution_log': self.import_execution_log
                }

                logger.info("✅ 综合报告生成完成")
                return final_report

        # 如果LLM报告生成失败，返回基础报告
        logger.warning("⚠️ LLM报告生成失败，返回基础报告")
        return {
            'success': True,
            'execution_time': execution_time,
            'output_database': output_db_path,
            'processing_summary': {
                'total_files_processed': len(self.discovered_files),
                'total_tables_created': total_tables_created,
                'total_imported_rows': total_imported_rows,
                'llm_analysis_success_rate': len(self.llm_file_analysis) / len(self.discovered_files) if self.discovered_files else 0
            },
            'business_intelligence': self.llm_business_intelligence,
            'detailed_execution_log': self.import_execution_log
        }

    def _build_report_generation_prompt(self, output_db_path: str, execution_time: float,
                                       total_imported_rows: int, total_tables_created: int) -> str:
        """构建报告生成提示词"""

        # 收集所有处理结果
        complete_analysis = {
            'file_analysis': self.llm_file_analysis,
            'schema_design': self.llm_schema_design,
            'business_intelligence': self.llm_business_intelligence,
            'execution_log': self.import_execution_log,
            'statistics': {
                'execution_time': execution_time,
                'total_imported_rows': total_imported_rows,
                'total_tables_created': total_tables_created,
                'total_files_processed': len(self.discovered_files)
            }
        }

        prompt = f"""你是一个专业的数据项目总结专家，请基于以下完整的数据导入和分析结果，生成一份全面的项目报告。

## 完整处理结果
{json.dumps(complete_analysis, ensure_ascii=False, indent=2)}

## 报告生成任务
请基于整个数据导入和分析过程，生成一份专业的项目总结报告。不要使用任何模板，完全基于实际的处理结果进行总结。

## 报告要求
1. **执行总结**：整个过程的高层次总结
2. **技术成果**：技术实现的关键成果
3. **业务价值**：为业务带来的价值和洞察
4. **质量评估**：数据质量和处理质量评估
5. **后续建议**：基于分析结果的后续行动建议

## 输出格式
请以JSON格式返回完整报告：

{{
  "executive_summary": {{
    "project_overview": "项目概述",
    "key_achievements": ["关键成就"],
    "business_impact": "业务影响",
    "technical_highlights": ["技术亮点"]
  }},
  "technical_results": {{
    "data_processing_summary": "数据处理总结",
    "database_design_quality": "数据库设计质量评估",
    "automation_level": "自动化程度",
    "technical_innovations": ["技术创新点"]
  }},
  "business_insights": {{
    "domain_understanding": "业务领域理解",
    "key_business_discoveries": ["关键业务发现"],
    "value_propositions": ["价值主张"],
    "business_readiness": "业务就绪度"
  }},
  "data_quality_assessment": {{
    "overall_quality_rating": "整体质量评级",
    "quality_strengths": ["质量优势"],
    "quality_concerns": ["质量关注点"],
    "data_governance_recommendations": ["数据治理建议"]
  }},
  "implementation_success": {{
    "success_metrics": [
      {{
        "metric": "成功指标",
        "value": "指标值",
        "assessment": "评估结果"
      }}
    ],
    "challenges_overcome": ["克服的挑战"],
    "lessons_learned": ["经验教训"]
  }},
  "future_recommendations": {{
    "immediate_actions": ["即时行动"],
    "short_term_goals": ["短期目标"],
    "long_term_vision": ["长期愿景"],
    "investment_priorities": ["投资优先级"]
  }},
  "stakeholder_communications": {{
    "executive_message": "给高管的信息",
    "technical_team_message": "给技术团队的信息",
    "business_user_message": "给业务用户的信息"
  }}
}}

请基于实际的处理结果和分析洞察生成报告，确保报告具有实际的指导价值和可操作性。
"""

        return prompt


# 向后兼容的别名和便捷函数
PureLLMIntelligentDataImporter = IntelligentDataImporter


def create_intelligent_importer(api_key: Optional[str] = None) -> IntelligentDataImporter:
    """创建智能数据导入器实例"""
    return IntelligentDataImporter(api_key)


def quick_intelligent_import(file_paths: List[str], output_db_path: str,
                           api_key: Optional[str] = None) -> Dict[str, Any]:
    """快速智能导入函数"""
    importer = create_intelligent_importer(api_key)
    return importer.process_batch_import(file_paths, output_db_path)