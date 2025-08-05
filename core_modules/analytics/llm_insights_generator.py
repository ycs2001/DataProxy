#!/usr/bin/env python3
"""
LLM驱动的智能数据洞察生成器
基于实际查询结果数据进行LLM分析，生成真正智能的业务洞察
"""

import json
import os
from typing import Dict, Any, List, Optional
from datetime import datetime

try:
    from langchain_openai import ChatOpenAI
    from langchain.schema.messages import HumanMessage
    LLM_AVAILABLE = True
except ImportError:
    LLM_AVAILABLE = False


class LLMInsightsGenerator:
    """
    LLM驱动的数据洞察生成器
    
    功能：
    1. 基于实际查询结果数据进行LLM分析
    2. 生成针对具体数据特征的业务洞察
    3. 包含数据趋势、异常检测、业务建议等智能分析
    4. 保持API兼容性，维持现有的data_interpretation结构
    """
    
    def __init__(self):
        """初始化LLM洞察生成器"""
        self.llm_client = None
        self._init_llm_client()
    
    def _init_llm_client(self):
        """初始化LLM客户端"""
        if not LLM_AVAILABLE:
            print("[WARNING] LLMInsightsGenerator: LangChain不可用")
            return
            
        try:
            api_key = os.getenv('DEEPSEEK_API_KEY')
            if api_key:
                self.llm_client = ChatOpenAI(
                    api_key=api_key,
                    base_url="https://api.deepseek.com",
                    model="deepseek-chat",
                    temperature=0.3  # 稍微提高创造性，但保持准确性
                )
                print("[DEBUG] LLMInsightsGenerator: DeepSeek API初始化成功")
            else:
                print("[WARNING] LLMInsightsGenerator: 未找到DEEPSEEK_API_KEY")
        except Exception as e:
            print(f"[ERROR] LLMInsightsGenerator: LLM初始化失败: {e}")
    
    def generate_intelligent_insights(self, 
                                    query: str, 
                                    data_tables: Dict[str, List[Dict]], 
                                    sql_query: str = None,
                                    analysis_context: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        生成基于LLM的智能数据洞察
        
        Args:
            query: 用户的原始查询
            data_tables: 查询返回的数据表
            sql_query: 生成的SQL查询语句
            analysis_context: 分析上下文信息
            
        Returns:
            Dict: 包含summary, key_insights, trends, anomalies, recommendations的洞察结果
        """
        try:
            print(f"[DEBUG] LLMInsightsGenerator: 开始生成智能洞察")

            if not self.llm_client:
                raise Exception("LLM客户端未初始化，无法生成智能洞察")

            if not data_tables:
                raise Exception("查询未返回数据，无法生成洞察分析")

            # 构建LLM分析提示词
            prompt = self._build_insights_prompt(query, data_tables, sql_query, analysis_context)

            # 调用LLM生成洞察
            llm_response = self._call_llm(prompt)

            # 解析LLM响应
            insights = self._parse_llm_insights(llm_response)

            # 验证和完善洞察结构
            validated_insights = self._validate_insights_structure(insights, query, data_tables)

            print(f"[DEBUG] LLMInsightsGenerator: 洞察生成完成")
            return validated_insights

        except Exception as e:
            print(f"[ERROR] LLMInsightsGenerator: 洞察生成失败: {e}")
            # 不使用硬编码回退，直接抛出异常
            raise Exception(f"智能洞察生成失败: {e}")
    
    def _build_insights_prompt(self, 
                              query: str, 
                              data_tables: Dict[str, List[Dict]], 
                              sql_query: str = None,
                              analysis_context: Dict[str, Any] = None) -> str:
        """构建LLM分析提示词"""
        
        # 准备数据摘要
        data_summary = self._prepare_data_summary(data_tables)
        
        # 构建上下文信息
        context_info = ""
        if analysis_context:
            context_info = f"\n## 分析上下文\n{json.dumps(analysis_context, ensure_ascii=False, indent=2)}"
        
        sql_info = ""
        if sql_query:
            sql_info = f"\n## 执行的SQL查询\n```sql\n{sql_query}\n```"
        
        prompt = f"""你是一位资深的银行业务数据分析师，请基于以下实际查询结果数据，生成深度的业务洞察分析。

## 用户查询
{query}

## 查询结果数据
{data_summary}
{sql_info}
{context_info}

## 分析任务
请基于实际数据内容进行深度分析，生成真正有价值的业务洞察。不要使用通用模板，要针对具体数据特征进行分析。

## 分析要求
1. **数据驱动**：所有洞察必须基于实际数据内容，不能是泛泛而谈
2. **业务相关**：结合银行业务背景，提供专业的业务解读
3. **具体量化**：尽可能提供具体的数字和比例
4. **实用性强**：提供可操作的业务建议

## 输出格式要求
**重要：必须严格按照以下JSON格式返回，不要添加任何其他文字说明**

```json
{{
  "summary": "基于实际数据的简洁摘要，包含关键数据特征",
  "key_insights": [
    "基于数据的关键发现1",
    "基于数据的关键发现2",
    "基于数据的关键发现3"
  ],
  "trends": [
    "从数据中识别的趋势1",
    "从数据中识别的趋势2"
  ],
  "anomalies": [
    "数据中发现的异常或风险点1",
    "数据中发现的异常或风险点2"
  ],
  "recommendations": [
    "基于数据分析的具体业务建议1",
    "基于数据分析的具体业务建议2",
    "基于数据分析的具体业务建议3"
  ]
}}
```

**输出要求：**
1. 只返回JSON格式，不要任何前缀或后缀说明
2. 确保JSON格式正确，所有字符串用双引号
3. 数组中至少包含1个元素
4. summary字段必须有内容

## 注意事项
- 所有分析内容必须与实际数据高度相关
- 避免使用"建议进一步分析"等模糊表述
- 数值分析要精确，比例计算要准确
- 业务建议要具体可执行
- 如果数据量少，要明确指出并给出相应建议

请开始分析："""

        return prompt
    
    def _prepare_data_summary(self, data_tables: Dict[str, List[Dict]]) -> str:
        """准备数据摘要用于LLM分析"""
        summary_parts = []
        
        for table_name, table_data in data_tables.items():
            if not table_data:
                summary_parts.append(f"### {table_name}\n数据为空")
                continue
            
            record_count = len(table_data)
            columns = list(table_data[0].keys()) if table_data else []
            
            summary_parts.append(f"### {table_name}")
            summary_parts.append(f"记录数: {record_count}")
            summary_parts.append(f"字段: {', '.join(columns)}")
            
            # 添加数据样本（最多3条记录）
            sample_data = table_data[:3]
            summary_parts.append("数据样本:")
            for i, record in enumerate(sample_data, 1):
                summary_parts.append(f"记录{i}: {json.dumps(record, ensure_ascii=False)}")
            
            if record_count > 3:
                summary_parts.append(f"... 还有 {record_count - 3} 条记录")
            
            # 添加数值字段的统计信息
            numeric_stats = self._calculate_numeric_stats(table_data)
            if numeric_stats:
                summary_parts.append("数值字段统计:")
                for field, stats in numeric_stats.items():
                    summary_parts.append(f"  {field}: 最小值={stats['min']}, 最大值={stats['max']}, 平均值={stats['avg']:.2f}")
            
            summary_parts.append("")  # 空行分隔
        
        return "\n".join(summary_parts)
    
    def _calculate_numeric_stats(self, table_data: List[Dict]) -> Dict[str, Dict]:
        """计算数值字段的基础统计信息"""
        if not table_data:
            return {}
        
        numeric_stats = {}
        
        for record in table_data:
            for field, value in record.items():
                if isinstance(value, (int, float)) and value is not None:
                    if field not in numeric_stats:
                        numeric_stats[field] = {'values': []}
                    numeric_stats[field]['values'].append(value)
        
        # 计算统计指标
        for field, data in numeric_stats.items():
            values = data['values']
            if values:
                numeric_stats[field] = {
                    'min': min(values),
                    'max': max(values),
                    'avg': sum(values) / len(values),
                    'count': len(values)
                }
        
        return numeric_stats

    def _call_llm(self, prompt: str) -> str:
        """调用LLM生成洞察"""
        try:
            response = self.llm_client.invoke([HumanMessage(content=prompt)])
            content = response.content
            print(f"[DEBUG] LLMInsightsGenerator: LLM响应长度: {len(content)}")
            print(f"[DEBUG] LLMInsightsGenerator: LLM响应前200字符: {content[:200]}...")
            return content
        except Exception as e:
            print(f"[ERROR] LLMInsightsGenerator: LLM调用失败: {e}")
            raise

    def _parse_llm_insights(self, llm_response: str) -> Dict[str, Any]:
        """解析LLM响应，提取洞察内容 - 增强JSON解析容错"""
        try:
            # 尝试多种JSON提取方法
            insights = self._extract_json_insights(llm_response)
            if insights:
                return insights

            # 如果JSON提取失败，尝试文本解析
            print("[WARNING] LLMInsightsGenerator: JSON提取失败，尝试文本解析")
            insights = self._parse_text_insights(llm_response)
            if insights and insights.get('summary'):
                return insights

            # 如果都失败，抛出异常
            raise Exception("无法从LLM响应中提取有效的洞察内容")

        except Exception as e:
            print(f"[ERROR] LLMInsightsGenerator: 洞察解析失败: {e}")
            raise Exception(f"洞察内容解析失败: {e}")

    def _extract_json_insights(self, llm_response: str) -> Dict[str, Any]:
        """增强的JSON提取方法"""
        # 方法1: 标准JSON提取
        try:
            json_start = llm_response.find('{')
            json_end = llm_response.rfind('}') + 1

            if json_start >= 0 and json_end > json_start:
                json_content = llm_response[json_start:json_end]
                insights = json.loads(json_content)
                if self._validate_insights_basic(insights):
                    return insights
        except:
            pass

        # 方法2: 查找```json代码块
        try:
            json_start = llm_response.find('```json')
            if json_start >= 0:
                json_start = llm_response.find('{', json_start)
                json_end = llm_response.find('```', json_start)
                if json_end > json_start:
                    json_content = llm_response[json_start:json_end]
                    insights = json.loads(json_content)
                    if self._validate_insights_basic(insights):
                        return insights
        except:
            pass

        # 方法3: 逐行解析JSON
        try:
            lines = llm_response.split('\n')
            json_lines = []
            in_json = False

            for line in lines:
                if '{' in line and not in_json:
                    in_json = True
                if in_json:
                    json_lines.append(line)
                if '}' in line and in_json:
                    break

            if json_lines:
                json_content = '\n'.join(json_lines)
                insights = json.loads(json_content)
                if self._validate_insights_basic(insights):
                    return insights
        except:
            pass

        return None

    def _validate_insights_basic(self, insights: Dict[str, Any]) -> bool:
        """基础洞察验证"""
        if not isinstance(insights, dict):
            return False

        required_fields = ['summary', 'key_insights', 'recommendations']
        for field in required_fields:
            if field not in insights:
                return False

        return True

    def _parse_text_insights(self, text_response: str) -> Dict[str, Any]:
        """从文本响应中解析洞察内容（增强版）"""
        print(f"[DEBUG] LLMInsightsGenerator: 开始文本解析，响应长度: {len(text_response)}")

        insights = {
            'summary': '',
            'key_insights': [],
            'trends': [],
            'anomalies': [],
            'recommendations': []
        }

        # 尝试多种解析策略

        # 策略1: 基于关键词的段落解析
        insights = self._parse_by_keywords(text_response, insights)
        if self._validate_insights_basic(insights):
            print(f"[DEBUG] LLMInsightsGenerator: 关键词解析成功")
            return insights

        # 策略2: 基于结构化文本解析
        insights = self._parse_structured_text(text_response, insights)
        if self._validate_insights_basic(insights):
            print(f"[DEBUG] LLMInsightsGenerator: 结构化解析成功")
            return insights

        # 策略3: 智能内容提取
        insights = self._extract_content_intelligently(text_response, insights)
        if self._validate_insights_basic(insights):
            print(f"[DEBUG] LLMInsightsGenerator: 智能提取成功")
            return insights

        print(f"[WARNING] LLMInsightsGenerator: 所有文本解析策略都失败")
        return insights

    def _parse_by_keywords(self, text_response: str, insights: Dict[str, Any]) -> Dict[str, Any]:
        """基于关键词的解析策略"""
        lines = text_response.split('\n')
        current_section = None

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # 识别章节标题
            line_lower = line.lower()
            if any(keyword in line for keyword in ['摘要', 'summary', '总结', '概述']):
                current_section = 'summary'
                # 尝试从同一行提取内容
                content = self._extract_content_from_line(line, ['摘要', 'summary', '总结', '概述'])
                if content:
                    insights['summary'] = content
            elif any(keyword in line for keyword in ['关键洞察', 'key_insights', '洞察', '发现', '关键发现']):
                current_section = 'key_insights'
            elif any(keyword in line for keyword in ['趋势', 'trends', '变化趋势']):
                current_section = 'trends'
            elif any(keyword in line for keyword in ['异常', 'anomalies', '风险', '问题']):
                current_section = 'anomalies'
            elif any(keyword in line for keyword in ['建议', 'recommendations', '措施', '对策']):
                current_section = 'recommendations'
            elif current_section:
                # 处理内容行
                content = self._clean_list_item(line)
                if content and len(content) > 5:
                    if current_section == 'summary' and not insights['summary']:
                        insights['summary'] = content
                    elif current_section != 'summary':
                        insights[current_section].append(content)

        return insights

    def _parse_structured_text(self, text_response: str, insights: Dict[str, Any]) -> Dict[str, Any]:
        """结构化文本解析策略"""
        # 按段落分割
        paragraphs = [p.strip() for p in text_response.split('\n\n') if p.strip()]

        for paragraph in paragraphs:
            lines = [l.strip() for l in paragraph.split('\n') if l.strip()]
            if not lines:
                continue

            first_line = lines[0].lower()

            # 根据首行判断段落类型
            if any(keyword in first_line for keyword in ['摘要', 'summary', '总结']):
                # 提取摘要内容
                content_lines = lines[1:] if len(lines) > 1 else [lines[0]]
                summary_content = ' '.join(content_lines)
                summary_content = self._clean_content(summary_content)
                if summary_content:
                    insights['summary'] = summary_content

            elif any(keyword in first_line for keyword in ['洞察', 'insights', '发现']):
                # 提取洞察列表
                for line in lines[1:]:
                    content = self._clean_list_item(line)
                    if content:
                        insights['key_insights'].append(content)

            elif any(keyword in first_line for keyword in ['建议', 'recommendations']):
                # 提取建议列表
                for line in lines[1:]:
                    content = self._clean_list_item(line)
                    if content:
                        insights['recommendations'].append(content)

        return insights

    def _extract_content_intelligently(self, text_response: str, insights: Dict[str, Any]) -> Dict[str, Any]:
        """智能内容提取策略"""
        # 查找包含银行业务关键词的句子作为洞察
        sentences = []
        for line in text_response.split('\n'):
            line = line.strip()
            if line and len(line) > 20:
                sentences.extend([s.strip() for s in line.split('。') if s.strip()])

        banking_keywords = ['贷款', '客户', '风险', '余额', '银行', '资产', '不良', '支行', '存款']
        actionable_keywords = ['建议', '应该', '需要', '立即', '加强', '优化', '建立']

        # 提取包含银行关键词的句子作为洞察
        for sentence in sentences:
            if any(keyword in sentence for keyword in banking_keywords):
                if len(sentence) > 15 and len(sentence) < 200:
                    if not insights['summary'] and len(sentence) > 30:
                        insights['summary'] = sentence + '。'
                    elif len(insights['key_insights']) < 3:
                        insights['key_insights'].append(sentence + '。')

        # 提取包含行动关键词的句子作为建议
        for sentence in sentences:
            if any(keyword in sentence for keyword in actionable_keywords):
                if len(sentence) > 15 and len(sentence) < 200:
                    insights['recommendations'].append(sentence + '。')

        return insights

    def _extract_content_from_line(self, line: str, keywords: list) -> str:
        """从行中提取内容"""
        for keyword in keywords:
            if keyword in line:
                # 尝试提取冒号后的内容
                if '：' in line:
                    content = line.split('：', 1)[1].strip()
                elif ':' in line:
                    content = line.split(':', 1)[1].strip()
                else:
                    # 移除关键词后的内容
                    content = line.replace(keyword, '').strip()

                if content and len(content) > 5:
                    return content
        return ''

    def _clean_list_item(self, line: str) -> str:
        """清理列表项内容"""
        # 移除列表标记
        line = line.strip()
        prefixes = ['-', '•', '*', '1.', '2.', '3.', '4.', '5.', '①', '②', '③', '④', '⑤']

        for prefix in prefixes:
            if line.startswith(prefix):
                line = line[len(prefix):].strip()
                break

        # 移除多余的标点
        line = line.strip('.,;，。；')

        return line if len(line) > 5 else ''

    def _clean_content(self, content: str) -> str:
        """清理内容"""
        # 移除多余的空格和换行
        content = ' '.join(content.split())

        # 移除特殊字符
        content = content.strip('*-•')

        return content.strip()

    def _validate_insights_structure(self, insights: Dict[str, Any], query: str, data_tables: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """验证洞察结构，确保API兼容性 - 移除所有硬编码默认值"""
        validated = {
            'summary': '',
            'key_insights': [],
            'trends': [],
            'anomalies': [],
            'recommendations': []
        }

        # 严格验证summary - 必须由LLM生成
        if not insights.get('summary'):
            raise Exception("LLM未生成摘要内容")
        validated['summary'] = str(insights['summary'])

        # 严格验证列表字段 - 必须由LLM生成
        for field in ['key_insights', 'trends', 'anomalies', 'recommendations']:
            if insights.get(field) and isinstance(insights[field], list):
                validated[field] = [str(item) for item in insights[field] if item]

            # 对关键字段进行严格要求
            if field in ['key_insights', 'recommendations'] and not validated[field]:
                raise Exception(f"LLM未生成{field}内容")

        return validated


