#!/usr/bin/env python3
"""
简化的可视化工具模块
提供基础的数据可视化功能
"""

import json
import os
import pandas as pd
from typing import Dict, Any, List, Union, Optional

# 导入本地模块
try:
    from .simple_chart_system import SimpleChartSystem
    from .custom_style_manager import CustomStyleManager, CustomChartStyle
    CHART_SYSTEM_AVAILABLE = True
except ImportError as e:
    print(f"[WARNING] 图表系统导入失败: {e}")
    CHART_SYSTEM_AVAILABLE = False

# LLM支持（DeepSeek API）
try:
    import requests
    import re
    LLM_AVAILABLE = True
except ImportError:
    LLM_AVAILABLE = False
    print("[INFO] LLM功能不可用，使用基础可视化功能")


class VisualizationTools:
    """简化的可视化工具类"""

    def __init__(self):
        """初始化可视化工具"""
        if CHART_SYSTEM_AVAILABLE:
            self.chart_system = SimpleChartSystem()
            self.style_manager = CustomStyleManager()
        else:
            self.chart_system = None
            self.style_manager = None

        # 初始化LLM配置
        self.api_key = os.getenv('DEEPSEEK_API_KEY')
        self.base_url = os.getenv('DEEPSEEK_BASE_URL', 'https://api.deepseek.com')
        self.llm_enabled = LLM_AVAILABLE and self.api_key

        if self.llm_enabled:
            print("[INFO] VisualizationTools 初始化完成 (LLM智能功能已启用)")
        else:
            print("[INFO] VisualizationTools 初始化完成 (基础功能模式)")

    def _call_llm_api(self, prompt: str, max_retries: int = 3) -> Optional[str]:
        """调用DeepSeek LLM API"""
        if not self.llm_enabled:
            return None

        for attempt in range(max_retries):
            try:
                headers = {
                    'Authorization': f'Bearer {self.api_key}',
                    'Content-Type': 'application/json'
                }

                data = {
                    'model': 'deepseek-chat',
                    'messages': [
                        {'role': 'user', 'content': prompt}
                    ],
                    'temperature': 0.1,
                    'max_tokens': 2000
                }

                response = requests.post(
                    f'{self.base_url}/v1/chat/completions',
                    headers=headers,
                    json=data,
                    timeout=120  # 增加超时时间到2分钟
                )

                if response.status_code == 200:
                    result = response.json()
                    return result['choices'][0]['message']['content'].strip()
                else:
                    print(f"[WARNING] LLM API调用失败 (状态码: {response.status_code})")

            except Exception as e:
                print(f"[WARNING] LLM API调用异常 (第{attempt+1}次): {e}")
                if attempt < max_retries - 1:
                    import time
                    wait_time = (attempt + 1) * 5  # 递增等待时间：5秒、10秒、15秒
                    print(f"[INFO] 等待{wait_time}秒后重试...")
                    time.sleep(wait_time)

        return None

    def _clean_llm_response(self, response: str) -> str:
        """清理LLM响应，移除代码块标记和多余的文本"""
        # 移除```json和```标记
        cleaned = response.strip()
        if cleaned.startswith('```json'):
            cleaned = cleaned[7:]
        elif cleaned.startswith('```'):
            cleaned = cleaned[3:]

        if cleaned.endswith('```'):
            cleaned = cleaned[:-3]

        # 移除前后空白
        cleaned = cleaned.strip()

        # 如果包含多个JSON对象，只取第一个
        if cleaned.count('{') > 1:
            # 找到第一个完整的JSON对象
            brace_count = 0
            end_pos = 0
            for i, char in enumerate(cleaned):
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        end_pos = i + 1
                        break
            if end_pos > 0:
                cleaned = cleaned[:end_pos]

        return cleaned

    def extract_user_intent(self, user_query: str, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        使用LLM分析用户查询意图

        Args:
            user_query: 用户查询文本
            data: 数据样本

        Returns:
            意图分析结果字典
        """
        if not self.llm_enabled:
            raise Exception("LLM功能未启用，无法进行智能分析")

        if not data:
            raise Exception("无数据输入，无法进行意图分析")

        try:
            # 准备数据信息
            sample_data = data[0] if data else {}
            field_names = list(sample_data.keys())
            field_types = {}
            for key, value in sample_data.items():
                if isinstance(value, (int, float)):
                    field_types[key] = "数值型"
                elif isinstance(value, str):
                    field_types[key] = "文本型"
                else:
                    field_types[key] = "其他"

            # 构建简化的LLM提示词
            prompt = f"""分析用户查询的可视化意图：

查询: "{user_query}"
字段: {field_names}
数据量: {len(data)}条

返回JSON格式：
{{
    "chart_type_explicit": "用户要求的图表类型(饼图/柱状图/折线图/散点图)或null",
    "analysis_type": "分析类型(分布/趋势/对比/排名/统计)",
    "suggested_title": "专业的图表标题",
    "banking_terms": ["银行术语"],
    "chart_type_recommended": "推荐图表类型",
    "confidence": 0.9
}}

只返回JSON。"""

            response = self._call_llm_api(prompt)
            if response:
                # 清理LLM响应，移除代码块标记
                cleaned_response = self._clean_llm_response(response)

                # 尝试解析JSON
                import json
                try:
                    intent_result = json.loads(cleaned_response)
                    intent_result['llm_generated'] = True
                    return intent_result
                except json.JSONDecodeError as e:
                    raise Exception(f"LLM返回的JSON格式无效: {cleaned_response[:100]}...")

            raise Exception("LLM API调用失败，无法获取有效响应")

        except Exception as e:
            print(f"[ERROR] 用户意图分析失败: {e}")
            raise e



    def create_visualization(self, data: List[Dict[str, Any]],
                           chart_type: str = 'bar',
                           title: str = "数据图表",
                           style: str = 'default',
                           user_query: str = None,
                           **kwargs) -> Dict[str, Any]:
        """
        创建可视化图表（支持LLM智能生成）

        Args:
            data: 数据列表
            chart_type: 图表类型 ('bar', 'line', 'pie', 'scatter')
            title: 图表标题
            style: 样式名称
            user_query: 用户原始查询（用于LLM分析）
            **kwargs: 其他参数

        Returns:
            图表结果字典
        """
        # 🚀 纯LLM驱动，不使用任何硬编码回退
        if not CHART_SYSTEM_AVAILABLE or not self.chart_system:
            raise Exception("图表系统不可用")

        if not self.llm_enabled:
            raise Exception("LLM功能未启用，无法进行智能图表生成")

        if not user_query:
            raise Exception("需要用户查询来进行智能分析")

        try:
            # 🧠 LLM智能分析用户意图
            print("[DEBUG] 使用LLM分析用户意图...")
            intent = self.extract_user_intent(user_query, data)

            # 根据用户明确要求调整图表类型
            if intent.get('chart_type_explicit'):
                chart_type = intent['chart_type_explicit']
                print(f"[DEBUG] 用户明确要求: {chart_type}")

            # 使用LLM建议的标题
            if intent.get('suggested_title'):
                title = intent['suggested_title']
                print(f"[DEBUG] LLM建议标题: {title}")

            # 🎨 生成定制化图表代码
            print("[DEBUG] 使用LLM生成定制化图表...")
            custom_result = self.generate_custom_chart_code(data, intent, style)
            if custom_result and custom_result.get('success'):
                custom_result['intent_analysis'] = intent
                print("[DEBUG] LLM定制化生成成功")
                return custom_result
            else:
                raise Exception("LLM图表代码生成失败")

        except Exception as e:
            print(f"[ERROR] 纯LLM可视化创建失败: {e}")
            raise e

    def generate_custom_chart_code(self, data: List[Dict[str, Any]],
                                 intent: Dict[str, Any],
                                 style: str = 'banking') -> Optional[Dict[str, Any]]:
        """
        使用LLM生成定制化的图表代码

        Args:
            data: 数据列表
            intent: 用户意图分析结果
            style: 样式名称

        Returns:
            图表结果字典或None
        """
        if not self.llm_enabled:
            raise Exception("LLM功能未启用，无法生成定制化图表代码")

        if not data:
            raise Exception("无数据输入，无法生成图表代码")

        try:
            # 准备数据信息
            sample_data = data[0]
            field_info = {}
            for key, value in sample_data.items():
                if isinstance(value, (int, float)):
                    field_info[key] = {"type": "numeric", "sample": value}
                elif isinstance(value, str):
                    field_info[key] = {"type": "text", "sample": value}
                else:
                    field_info[key] = {"type": "other", "sample": str(value)}

            # 获取样式配置
            chart_style = self.style_manager.get_style(style) if self.style_manager else None
            style_colors = {
                'primary': '#1f4e79',
                'secondary': '#2e7cb8',
                'accent': '#d4af37',
                'background': '#f8f9fa',
                'text': '#2c3e50'
            }
            if chart_style:
                style_dict = chart_style.to_dict()
                style_colors = style_dict.get('colors', style_colors)

            # 构建简化的LLM提示词
            chart_type = intent.get('chart_type_recommended', 'bar')
            title = intent.get('suggested_title', '数据图表')

            prompt = f"""生成Plotly {chart_type}图表代码：

数据: {len(data)}条记录
字段: {list(field_info.keys())}
标题: {title}
颜色: {style_colors['primary']}

返回JSON:
{{
    "chart_code": "fig = px.{chart_type}(df, x='字段1', y='字段2', title='{title}', color_discrete_sequence=['{style_colors['primary']}'])",
    "chart_config": {{"title": "{title}", "chart_type": "{chart_type}"}},
    "explanation": "生成{chart_type}图表"
}}

使用df作为DataFrame变量名，只返回JSON。"""

            response = self._call_llm_api(prompt)
            if response:
                # 清理LLM响应
                cleaned_response = self._clean_llm_response(response)

                import json
                try:
                    code_result = json.loads(cleaned_response)
                    chart_code = code_result.get('chart_code', '')

                    if chart_code:
                        # 安全执行生成的代码
                        return self._execute_generated_code(chart_code, data, code_result)

                except json.JSONDecodeError as e:
                    raise Exception(f"LLM返回的JSON格式无效: {cleaned_response[:100]}...")
                except Exception as e:
                    raise Exception(f"LLM生成的代码执行失败: {e}")

            raise Exception("LLM API调用失败或返回无效响应")

        except Exception as e:
            print(f"[ERROR] 定制化图表生成失败: {e}")
            raise e



    def _execute_generated_code(self, chart_code: str, data: List[Dict[str, Any]],
                              code_result: Dict[str, Any]) -> Dict[str, Any]:
        """安全执行LLM生成的图表代码"""
        try:
            import pandas as pd

            # 检查代码安全性（基础检查）
            dangerous_keywords = ['import os', 'import sys', 'exec', 'eval', 'open(', 'file(']
            for keyword in dangerous_keywords:
                if keyword in chart_code:
                    print(f"[WARNING] 检测到危险代码关键词: {keyword}")
                    return None

            # 准备执行环境
            df = pd.DataFrame(data)

            # 导入必要的库
            try:
                import plotly.express as px
                import plotly.graph_objects as go
                from plotly.utils import PlotlyJSONEncoder
            except ImportError:
                print("[WARNING] Plotly不可用，无法执行生成的代码")
                return None

            # 创建安全的执行环境
            safe_builtins = {
                'len': len,
                'str': str,
                'int': int,
                'float': float,
                'list': list,
                'dict': dict,
                'range': range,
                'round': round,
                'max': max,
                'min': min,
                'sum': sum
            }

            safe_globals = {
                'df': df,
                'px': px,
                'go': go,
                'pd': pd,
                '__builtins__': safe_builtins
            }

            # 执行代码
            local_vars = {}
            exec(chart_code, safe_globals, local_vars)

            # 查找生成的图表对象
            fig = None
            for var_name, var_value in local_vars.items():
                if hasattr(var_value, 'to_json'):  # Plotly图表对象
                    fig = var_value
                    break

            if fig is None:
                # 尝试从全局变量中查找
                if 'fig' in safe_globals:
                    fig = safe_globals['fig']

            if fig:
                return {
                    'success': True,
                    'chart_type': f"llm_{code_result.get('chart_config', {}).get('chart_type', 'custom')}",
                    'chart_json': fig.to_json(),
                    'title': code_result.get('chart_config', {}).get('title', '智能生成图表'),
                    'data_points': len(data),
                    'llm_generated': True,
                    'generated_code': chart_code,
                    'explanation': code_result.get('explanation', ''),
                    'chart_config': code_result.get('chart_config', {})
                }
            else:
                print("[WARNING] 未找到生成的图表对象")
                return None

        except Exception as e:
            print(f"[ERROR] 代码执行失败: {e}")
            return None



    def get_banking_professional_config(self, banking_terms: List[str],
                                       data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        获取银行业务专业化配置

        Args:
            banking_terms: 识别到的银行术语
            data: 数据样本

        Returns:
            专业化配置字典
        """
        config = {
            'colors': {
                'primary': '#1f4e79',    # 深蓝色（银行主色）
                'secondary': '#2e7cb8',  # 浅蓝色
                'accent': '#d4af37',     # 金色（强调色）
                'success': '#27ae60',    # 绿色（正面指标）
                'warning': '#f39c12',    # 橙色（警示）
                'danger': '#e74c3c',     # 红色（风险）
                'background': '#f8f9fa', # 浅灰背景
                'text': '#2c3e50'        # 深灰文字
            },
            'risk_indicators': [],
            'professional_labels': {},
            'number_formats': {}
        }

        # 风险指标识别和配色
        risk_terms = ['不良贷款', '逾期', '风险', '违约', '损失']
        for term in banking_terms:
            if any(risk_word in term for risk_word in risk_terms):
                config['risk_indicators'].append(term)

        # 专业标签映射
        label_mapping = {
            '对公有效户': '对公有效户数量',
            '不良贷款': '不良贷款余额',
            '存款余额': '存款余额',
            '贷款余额': '贷款余额',
            '支行': '分支机构',
            '客户': '客户',
            'CUST_NAME': '客户名称',
            'BRANCH_NAME': '支行名称',
            'DEPOSIT_BALANCE': '存款余额',
            'LOAN_BALANCE': '贷款余额',
            'EFFECTIVE_CUSTOMERS': '有效户数量'
        }

        # 检查数据字段并应用专业标签
        if data:
            sample = data[0]
            for field_name in sample.keys():
                # 直接匹配
                if field_name in label_mapping:
                    config['professional_labels'][field_name] = label_mapping[field_name]
                else:
                    # 模糊匹配
                    field_lower = field_name.lower()
                    if 'cust' in field_lower and 'name' in field_lower:
                        config['professional_labels'][field_name] = '客户名称'
                    elif 'branch' in field_lower:
                        config['professional_labels'][field_name] = '支行'
                    elif 'deposit' in field_lower and 'bal' in field_lower:
                        config['professional_labels'][field_name] = '存款余额'
                    elif 'loan' in field_lower and 'bal' in field_lower:
                        config['professional_labels'][field_name] = '贷款余额'
                    elif 'effective' in field_lower:
                        config['professional_labels'][field_name] = '有效户数量'

        # 数值格式化配置
        config['number_formats'] = {
            'currency': '¥{:,.0f}',      # 货币格式
            'percentage': '{:.1f}%',      # 百分比格式
            'count': '{:,}',             # 计数格式
            'decimal': '{:,.2f}'         # 小数格式
        }

        return config

    def get_supported_chart_types(self) -> List[str]:
        """获取支持的图表类型"""
        return ['bar', 'line', 'pie', 'scatter']

    def get_available_styles(self) -> List[str]:
        """获取可用的样式"""
        if self.style_manager:
            return self.style_manager.list_available_styles()
        return ['default']

    def analyze_data_for_visualization(self, data: List[Dict[str, Any]],
                                     user_query: str = None) -> Dict[str, Any]:
        """使用LLM分析数据以确定最佳可视化方案"""
        if not self.llm_enabled:
            raise Exception("LLM功能未启用，无法进行智能数据分析")

        if not data:
            raise Exception("无数据输入，无法进行分析")

        if not user_query:
            raise Exception("需要用户查询来进行智能分析")

        # 使用LLM进行智能分析
        try:
            intent = self.extract_user_intent(user_query, data)
            return {
                'recommended_chart': intent.get('chart_type_recommended', 'bar'),
                'reason': f"基于用户查询'{user_query}'的LLM智能分析",
                'data_analysis': {
                    'total_records': len(data),
                    'analysis_type': intent.get('analysis_type'),
                    'banking_terms': intent.get('banking_terms', []),
                    'confidence': intent.get('confidence', 0.0)
                },
                'llm_analysis': intent
            }
        except Exception as e:
            print(f"[ERROR] LLM数据分析失败: {e}")
            raise e

    def create_chart_from_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """根据配置创建图表"""
        data = config.get('data', [])
        chart_type = config.get('chart_type', 'bar')
        title = config.get('title', '数据图表')
        style = config.get('style', 'default')
        
        return self.create_visualization(data, chart_type, title, style)

    def export_chart(self, chart_result: Dict[str, Any], 
                    output_path: str, format: str = 'html') -> bool:
        """导出图表到文件"""
        try:
            if not chart_result.get('success'):
                return False
            
            if self.chart_system:
                return self.chart_system.save_chart(chart_result, output_path)
            else:
                # 简单的文本导出
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(chart_result.get('text_output', '图表数据'))
                return True
                
        except Exception as e:
            print(f"[ERROR] 图表导出失败: {e}")
            return False

    def get_chart_info(self, chart_result: Dict[str, Any]) -> Dict[str, Any]:
        """获取图表信息"""
        return {
            'success': chart_result.get('success', False),
            'chart_type': chart_result.get('chart_type', 'unknown'),
            'title': chart_result.get('title', ''),
            'data_points': chart_result.get('data_points', 0),
            'has_chart_json': 'chart_json' in chart_result,
            'has_text_output': 'text_output' in chart_result,
            'style_applied': chart_result.get('style_applied', 'none'),
            'error': chart_result.get('error')
        }


# 为了向后兼容，保留一些旧的类名
class DataAnalysisTool:
    """数据分析工具（简化版）"""
    
    def __init__(self):
        self.visualization_tools = VisualizationTools()
    
    def analyze_data(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """分析数据"""
        return self.visualization_tools.analyze_data_for_visualization(data)


class ChartGenerationTool:
    """图表生成工具（简化版）"""
    
    def __init__(self):
        self.visualization_tools = VisualizationTools()
    
    def generate_chart(self, data: List[Dict[str, Any]],
                      chart_type: str = 'bar',
                      title: str = "图表",
                      user_query: str = None) -> Dict[str, Any]:
        """生成图表（支持LLM智能功能）"""
        return self.visualization_tools.create_visualization(
            data, chart_type, title, user_query=user_query
        )


class ChartExecutionTool:
    """图表执行工具（简化版）"""
    
    def __init__(self):
        self.visualization_tools = VisualizationTools()
    
    def execute_chart(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """执行图表生成"""
        return self.visualization_tools.create_chart_from_config(config)


# 全局实例
_visualization_tools_instance = None

def get_visualization_tools() -> VisualizationTools:
    """获取全局可视化工具实例"""
    global _visualization_tools_instance
    if _visualization_tools_instance is None:
        _visualization_tools_instance = VisualizationTools()
    return _visualization_tools_instance
