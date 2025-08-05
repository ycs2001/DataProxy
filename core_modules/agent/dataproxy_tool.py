#!/usr/bin/env python3
"""
DataProxy工具 - 智能数据分析查询工具
"""

import os
import time
import json
from typing import Dict, Any, List, Optional

# 导入真正的NL2SQL处理器
try:
    from ..nl2sql.nl2sql_processor import NL2SQLProcessor
    from ..nl2sql.sql_query_engine import SQLQueryEngine
    NL2SQL_AVAILABLE = True
except ImportError as e:
    print(f"[WARNING] NL2SQL模块导入失败: {e}")
    NL2SQL_AVAILABLE = False

# 导入LLM洞察生成器
try:
    from ..analytics.llm_insights_generator import LLMInsightsGenerator
    LLM_INSIGHTS_AVAILABLE = True
except ImportError as e:
    print(f"[WARNING] LLM洞察生成器导入失败: {e}")
    LLM_INSIGHTS_AVAILABLE = False

class DataProxyTool:
    """DataProxy智能分析工具"""
    
    def __init__(self):
        """初始化DataProxy工具"""
        self.name = "DataProxy智能分析工具"
        self.version = "2.0.0"
        self.initialized = False

        # 检查必要的组件
        self.api_key = os.getenv('DEEPSEEK_API_KEY')

        # 确定数据目录路径 - 考虑从flask_backend运行的情况
        default_data_dir = './databases'
        if os.path.basename(os.getcwd()) == 'flask_backend':
            # 如果从flask_backend目录运行，使用相对于项目根目录的路径
            default_data_dir = '../databases'

        self.data_dir = os.getenv('DATAPROXY_DATA_DIR', default_data_dir)

        # 初始化NL2SQL处理器
        self.nl2sql_processor = None
        self.sql_engine = None

        # 初始化LLM洞察生成器
        self.llm_insights_generator = None

        # 初始化组件
        self._initialize_components()
    
    def _initialize_components(self):
        """初始化必要的组件"""
        try:
            # 检查数据目录
            if not os.path.exists(self.data_dir):
                os.makedirs(self.data_dir, exist_ok=True)
            
            # 检查是否有可用的数据库
            self.available_databases = self._scan_databases()
            
            # 检查LLM API
            self.llm_available = bool(self.api_key)

            # 初始化NL2SQL组件
            if NL2SQL_AVAILABLE and self.llm_available:
                try:
                    self.nl2sql_processor = NL2SQLProcessor()
                    self.sql_engine = SQLQueryEngine()
                    print(f"[INFO] NL2SQL处理器: 已初始化")
                except Exception as e:
                    print(f"[WARNING] NL2SQL处理器初始化失败: {e}")
                    self.nl2sql_processor = None
                    self.sql_engine = None
            else:
                print(f"[INFO] NL2SQL处理器: 不可用 (NL2SQL_AVAILABLE={NL2SQL_AVAILABLE}, LLM_AVAILABLE={self.llm_available})")

            # 初始化LLM洞察生成器
            if LLM_INSIGHTS_AVAILABLE and self.llm_available:
                try:
                    self.llm_insights_generator = LLMInsightsGenerator()
                    print(f"[INFO] LLM洞察生成器: 已初始化")
                except Exception as e:
                    print(f"[WARNING] LLM洞察生成器初始化失败: {e}")
                    self.llm_insights_generator = None
            else:
                print(f"[INFO] LLM洞察生成器: 不可用 (LLM_INSIGHTS_AVAILABLE={LLM_INSIGHTS_AVAILABLE}, LLM_AVAILABLE={self.llm_available})")

            self.initialized = True
            print(f"[INFO] DataProxy工具初始化成功")
            print(f"[INFO] 可用数据库: {len(self.available_databases)}个")
            print(f"[INFO] LLM功能: {'可用' if self.llm_available else '不可用'}")
            print(f"[INFO] NL2SQL功能: {'可用' if self.nl2sql_processor else '不可用'}")
            print(f"[INFO] LLM洞察功能: {'可用' if self.llm_insights_generator else '不可用'}")

        except Exception as e:
            print(f"[ERROR] DataProxy工具初始化失败: {e}")
            self.initialized = False
    
    def _scan_databases(self) -> List[str]:
        """扫描可用的数据库文件（包括子目录）"""
        try:
            import glob
            # 扫描当前目录和子目录中的.db文件
            db_files = glob.glob(os.path.join(self.data_dir, '**/*.db'), recursive=True)
            # 也扫描直接在数据目录下的.db文件
            db_files.extend(glob.glob(os.path.join(self.data_dir, '*.db')))

            # 去重并返回相对路径
            unique_files = list(set(db_files))
            print(f"[DEBUG] 扫描数据库目录: {self.data_dir}")
            print(f"[DEBUG] 找到数据库文件: {unique_files}")

            return unique_files
        except Exception as e:
            print(f"[WARNING] 扫描数据库失败: {e}")
            return []

    def _get_database_schema(self, database_path: str) -> Dict[str, Any]:
        """获取数据库schema信息"""
        try:
            import sqlite3

            conn = sqlite3.connect(database_path)
            cursor = conn.cursor()

            # 获取所有表名
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            table_names = [row[0] for row in cursor.fetchall()]

            schema_info = {
                'database_path': database_path,
                'tables': {}
            }

            # 获取每个表的列信息
            for table_name in table_names:
                cursor.execute(f"PRAGMA table_info({table_name});")
                columns = cursor.fetchall()

                schema_info['tables'][table_name] = {
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
            print(f"[DEBUG] 获取schema成功: {len(table_names)}个表")
            return schema_info

        except Exception as e:
            print(f"[ERROR] 获取数据库schema失败: {e}")
            return {}

    def _generate_llm_data_interpretation(self, query: str, data_tables: Dict[str, Any], sql_query: str = None, analysis_context: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        使用LLM生成智能数据洞察

        Args:
            query: 用户原始查询
            data_tables: 查询返回的数据表
            sql_query: 生成的SQL查询
            analysis_context: 分析上下文

        Returns:
            Dict: 包含summary, key_insights, trends, anomalies, recommendations的洞察结果
        """
        try:
            print(f"[DEBUG] DataProxyTool: 开始生成LLM数据洞察")

            # 使用LLM洞察生成器 - 移除所有硬编码回退逻辑
            if not self.llm_insights_generator:
                raise Exception("LLM洞察生成器未初始化，无法生成智能洞察")

            print(f"[DEBUG] DataProxyTool: 使用LLM洞察生成器")
            insights = self.llm_insights_generator.generate_intelligent_insights(
                query=query,
                data_tables=data_tables,
                sql_query=sql_query,
                analysis_context=analysis_context
            )
            print(f"[DEBUG] DataProxyTool: LLM洞察生成完成")
            return insights

        except Exception as e:
            print(f"[ERROR] DataProxyTool: LLM洞察生成失败: {e}")
            # 不使用硬编码回退，直接抛出异常
            raise Exception(f"智能洞察生成失败: {e}")



    def run(self, query: str, analysis_mode: str = "comprehensive",
            enable_statistics: bool = True, **kwargs) -> Dict[str, Any]:
        """执行智能数据分析查询"""
        
        if not self.initialized:
            return {
                'success': False,
                'error': 'DataProxy工具未正确初始化',
                'query': query
            }
        
        # 检查是否有可用数据库
        if not self.available_databases:
            return {
                'success': False,
                'error': '没有找到可用的数据库，请先导入数据',
                'query': query,
                'suggestion': '使用文件上传功能导入Excel数据，或使用CLI命令导入数据'
            }
        
        # 检查LLM功能
        if not self.llm_available:
            return {
                'success': False,
                'error': 'LLM功能不可用，请配置DEEPSEEK_API_KEY环境变量',
                'query': query
            }
        
        # 执行查询分析
        start_time = time.time()
        
        try:
            # 这里应该调用实际的查询处理逻辑
            # 目前返回模拟结果
            result = self._process_query(query, analysis_mode, enable_statistics)
            
            result['execution_time'] = time.time() - start_time
            result['query'] = query
            result['analysis_mode'] = analysis_mode
            
            return result
            
        except Exception as e:
            return {
                'success': False,
                'error': f'查询处理失败: {str(e)}',
                'query': query,
                'execution_time': time.time() - start_time
            }
    
    def _process_query(self, query: str, analysis_mode: str,
                      enable_statistics: bool) -> Dict[str, Any]:
        """处理查询（增强实现）"""

        # 分析查询类型和内容
        query_analysis = self._analyze_query(query)

        # 根据查询类型生成相应的结果
        if query_analysis['type'] == 'banking_business':
            result = self._process_banking_query(query, query_analysis, analysis_mode)
        elif query_analysis['type'] == 'data_analysis':
            result = self._process_data_analysis_query(query, query_analysis, analysis_mode)
        else:
            result = self._process_general_query(query, query_analysis, analysis_mode)

        # 添加统计信息
        if enable_statistics:
            result['statistics'] = {
                'query_length': len(query),
                'processing_mode': analysis_mode,
                'available_databases': self.available_databases,
                'query_type': query_analysis['type'],
                'detected_entities': query_analysis.get('entities', [])
            }

        # 添加可视化建议 - 基于实际查询结果
        if query_analysis.get('needs_visualization', False):
            result['visualizations'] = self._generate_visualizations_from_data(
                query, query_analysis, result.get('data_tables', {})
            )

        return result

    def _analyze_query(self, query: str) -> Dict[str, Any]:
        """分析查询内容和类型"""
        analysis = {
            'type': 'general',
            'entities': [],
            'needs_visualization': False,
            'banking_terms': [],
            'time_period': None
        }

        # 检测银行业务术语
        banking_terms = ['对公有效户', '不良贷款', '存款余额', '贷款余额', '客户', '支行']
        detected_terms = [term for term in banking_terms if term in query]

        if detected_terms:
            analysis['type'] = 'banking_business'
            analysis['banking_terms'] = detected_terms

        # 检测时间期间
        time_indicators = ['2025年3月末', '年末', '季末', '月末']
        for indicator in time_indicators:
            if indicator in query:
                analysis['time_period'] = indicator
                break

        # 检测是否需要可视化
        viz_keywords = ['统计', '排名', '对比', '分析', '图表', '可视化']
        if any(keyword in query for keyword in viz_keywords):
            analysis['needs_visualization'] = True

        # 检测实体
        if '客户名称' in query:
            analysis['entities'].append('customer_name')
        if '余额' in query:
            analysis['entities'].append('balance')

        return analysis

    def _process_banking_query(self, query: str, analysis: Dict[str, Any], mode: str) -> Dict[str, Any]:
        """处理银行业务查询 - 使用真正的NL2SQL"""

        # 如果NL2SQL可用，使用真正的SQL查询
        if self.nl2sql_processor and self.sql_engine:
            try:
                print(f"[DEBUG] 使用NL2SQL处理查询: {query}")

                # 第一步：准备数据库上下文并使用NL2SQL处理器转换查询
                context = {}
                if self.available_databases:
                    database_path = self.available_databases[0]
                    print(f"[DEBUG] 准备数据库上下文: {database_path}")

                    # 获取数据库schema信息
                    schema_info = self._get_database_schema(database_path)
                    if schema_info:
                        context['database_path'] = database_path
                        # 直接传递tables信息，符合SQLQueryEngine的期望格式
                        context['schema_info'] = schema_info.get('tables', {})
                        print(f"[DEBUG] 数据库schema: {len(schema_info.get('tables', {}))}个表")

                        # 显示表名以便调试
                        table_names = list(schema_info.get('tables', {}).keys())
                        print(f"[DEBUG] 可用表名: {table_names}")

                # 直接使用SQLQueryEngine生成SQL（绕过NL2SQLProcessor的上下文问题）
                sql_result = self.sql_engine.generate_sql(query, context)

                # 转换为NL2SQL格式以保持兼容性
                nl2sql_result = {
                    'success': sql_result.success,
                    'sql': sql_result.sql,
                    'data': sql_result.data,
                    'record_count': sql_result.record_count,
                    'execution_time': sql_result.execution_time,
                    'error': sql_result.error if not sql_result.success else None
                }
                print(f"[DEBUG] NL2SQL结果: {nl2sql_result}")

                if nl2sql_result.get('success', False):
                    sql_query = nl2sql_result.get('sql', '')
                    print(f"[DEBUG] 生成的SQL: {sql_query}")

                    # 第二步：设置数据库路径并执行SQL查询
                    if self.available_databases:
                        # 使用第一个可用数据库
                        database_path = self.available_databases[0]
                        print(f"[DEBUG] 设置数据库路径: {database_path}")

                        # 直接传递数据库路径给execute_query方法
                        sql_result = self.sql_engine.execute_query(sql_query, database_path)
                    else:
                        print(f"[ERROR] 没有可用的数据库文件")
                        sql_result = self.sql_engine.execute_query(sql_query)
                    print(f"[DEBUG] SQL执行结果: 成功={sql_result.success}, 记录数={getattr(sql_result, 'record_count', 0)}")

                    if sql_result.success:
                        # 处理查询结果
                        data_tables = {}

                        # 主要数据表
                        if sql_result.data:
                            table_name = self._generate_table_name(query, analysis)
                            data_tables[table_name] = sql_result.data

                            # 如果是统计查询，生成汇总信息
                            if '统计' in query or '汇总' in query:
                                summary = self._generate_summary(sql_result.data, analysis)
                                if summary:
                                    data_tables['统计汇总'] = [summary]

                        # 生成智能回复
                        agent_response = self._generate_agent_response(query, sql_result, analysis)

                        # 生成可视化数据
                        visualizations = []
                        if analysis.get('needs_visualization', False):
                            visualizations = self._generate_visualizations_from_data(query, analysis, data_tables)

                        # 生成智能数据解读 - 使用LLM洞察生成器
                        data_interpretation = self._generate_llm_data_interpretation(query, data_tables, sql_query, analysis)

                        result = {
                            'success': True,
                            'data_tables': data_tables,
                            'agent_response': agent_response,
                            'sql_query': sql_query,
                            'record_count': sql_result.record_count,
                            'execution_time': sql_result.execution_time,
                            'visualizations': visualizations,
                            'data_interpretation': data_interpretation
                        }

                    else:
                        # SQL执行失败
                        result = {
                            'success': False,
                            'error': f'SQL执行失败: {sql_result.error}',
                            'sql_query': sql_query,
                            'agent_response': f'查询执行遇到问题: {sql_result.error}',
                            'data_tables': {},
                            'visualizations': []
                        }
                else:
                    # NL2SQL转换失败
                    result = {
                        'success': False,
                        'error': f'查询理解失败: {nl2sql_result.get("error", "未知错误")}',
                        'agent_response': f'抱歉，我无法理解查询: {query}。请尝试重新表述。',
                        'data_tables': {},
                        'visualizations': []
                    }

            except Exception as e:
                result = {
                    'success': False,
                    'error': f'查询处理异常: {str(e)}',
                    'agent_response': f'查询处理过程中发生错误: {str(e)}',
                    'data_tables': {},
                    'visualizations': []
                }
        else:
            # NL2SQL不可用时的降级处理
            result = {
                'success': False,
                'error': 'NL2SQL功能不可用，请检查配置',
                'agent_response': f'抱歉，当前无法处理查询: {query}。NL2SQL功能未正确初始化。',
                'data_tables': {},
                'visualizations': []
            }

        return result

    def _process_data_analysis_query(self, query: str, analysis: Dict[str, Any], mode: str) -> Dict[str, Any]:
        """处理数据分析查询"""
        result = {
            'success': True,
            'data_tables': {
                '数据分析结果': [{
                    'query': query,
                    'analysis_type': '数据分析',
                    'mode': mode,
                    'timestamp': time.time()
                }]
            },
            'agent_response': f'已执行数据分析查询: {query}',
            'visualizations': []
        }
        return result

    def _process_general_query(self, query: str, analysis: Dict[str, Any], mode: str) -> Dict[str, Any]:
        """处理一般查询"""
        result = {
            'success': True,
            'data_tables': {
                '查询结果': [{
                    'message': f'处理查询: {query}',
                    'mode': mode,
                    'database_count': len(self.available_databases),
                    'timestamp': time.time()
                }]
            },
            'agent_response': f'已处理查询: {query}。当前有{len(self.available_databases)}个可用数据库。',
            'visualizations': []
        }
        return result

    def _generate_visualizations_from_data(self, query: str, analysis: Dict[str, Any], data_tables: Dict[str, Any]) -> List[Dict[str, Any]]:
        """基于实际查询结果生成可视化数据"""
        visualizations = []

        if not data_tables:
            return visualizations

        # 获取第一个数据表作为主要数据源
        table_name = list(data_tables.keys())[0]
        table_data = data_tables[table_name]

        if not table_data or len(table_data) == 0:
            return visualizations

        # 分析数据结构，确定合适的图表类型
        columns = list(table_data[0].keys()) if table_data else []

        # 寻找数值字段和分类字段
        numeric_fields = []
        categorical_fields = []

        for col in columns:
            sample_values = [row.get(col) for row in table_data[:5] if row.get(col) is not None]
            if sample_values:
                # 检查是否为数值字段
                try:
                    numeric_values = [float(str(val).replace(',', '')) for val in sample_values if str(val).replace(',', '').replace('.', '').replace('-', '').isdigit()]
                    if len(numeric_values) >= len(sample_values) * 0.7:  # 70%以上是数值
                        numeric_fields.append(col)
                    else:
                        categorical_fields.append(col)
                except:
                    categorical_fields.append(col)

        # 生成图表
        if len(categorical_fields) >= 1 and len(numeric_fields) >= 1:
            # 柱状图：分类 vs 数值
            x_field = categorical_fields[0]
            y_field = numeric_fields[0]

            # 准备图表数据
            chart_data = []
            for row in table_data:
                if row.get(x_field) is not None and row.get(y_field) is not None:
                    try:
                        y_value = float(str(row[y_field]).replace(',', ''))
                        chart_data.append({
                            'x': str(row[x_field]),
                            'y': y_value
                        })
                    except:
                        continue

            if chart_data:
                visualizations.append({
                    'type': 'bar',
                    'title': f'{y_field} 按 {x_field} 分布',
                    'data_source': table_name,
                    'x_field': x_field,
                    'y_field': y_field,
                    'data': chart_data,
                    'layout': {
                        'xaxis': {'title': x_field},
                        'yaxis': {'title': y_field},
                        'title': f'{y_field} 按 {x_field} 分布'
                    }
                })

        # 如果有多个数值字段，生成饼图（取第一个数值字段的总和分布）
        if len(numeric_fields) >= 1 and len(categorical_fields) >= 1:
            x_field = categorical_fields[0]
            y_field = numeric_fields[0]

            # 聚合数据用于饼图
            pie_data = {}
            for row in table_data:
                if row.get(x_field) is not None and row.get(y_field) is not None:
                    try:
                        category = str(row[x_field])
                        value = float(str(row[y_field]).replace(',', ''))
                        pie_data[category] = pie_data.get(category, 0) + value
                    except:
                        continue

            if pie_data and len(pie_data) > 1:
                pie_chart_data = [
                    {'label': category, 'value': value}
                    for category, value in pie_data.items()
                ]

                visualizations.append({
                    'type': 'pie',
                    'title': f'{y_field} 分布占比',
                    'data_source': table_name,
                    'data': pie_chart_data,
                    'layout': {
                        'title': f'{y_field} 分布占比'
                    }
                })

        return visualizations

















    def _generate_table_name(self, query: str, analysis: Dict[str, Any]) -> str:
        """根据查询内容生成合适的表名"""
        if '对公有效户' in analysis.get('banking_terms', []) and '不良贷款' in analysis.get('banking_terms', []):
            return '对公有效户不良贷款明细'
        elif '客户' in query:
            return '客户信息查询结果'
        elif '存款' in query:
            return '存款信息统计'
        elif '贷款' in query:
            return '贷款信息统计'
        else:
            return '查询结果'

    def _generate_summary(self, data: List[Dict[str, Any]], analysis: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """生成统计汇总信息"""
        if not data:
            return None

        summary = {
            '记录总数': len(data),
            '查询时点': analysis.get('time_period', '当前'),
        }

        # 尝试计算数值字段的统计
        numeric_fields = []
        for record in data[:5]:  # 检查前5条记录
            for key, value in record.items():
                if isinstance(value, (int, float)) and '余额' in key:
                    numeric_fields.append(key)
                    break

        for field in set(numeric_fields):
            try:
                values = [record.get(field, 0) for record in data if isinstance(record.get(field), (int, float))]
                if values:
                    summary[f'{field}_合计'] = sum(values)
                    summary[f'{field}_平均'] = sum(values) / len(values)
            except:
                continue

        return summary

    def _generate_agent_response(self, query: str, sql_result, analysis: Dict[str, Any]) -> str:
        """生成智能回复"""
        record_count = sql_result.record_count

        if record_count == 0:
            return f'根据查询条件"{query}"，未找到符合条件的记录。'

        response_parts = []

        # 基础信息
        time_period = analysis.get('time_period', '')
        if time_period:
            response_parts.append(f'截至{time_period}')

        # 记录数量
        if '对公有效户' in analysis.get('banking_terms', []):
            response_parts.append(f'共找到{record_count}户对公有效户')
        else:
            response_parts.append(f'共找到{record_count}条记录')

        # 业务术语相关
        banking_terms = analysis.get('banking_terms', [])
        if '不良贷款' in banking_terms:
            response_parts.append('的不良贷款信息')
        elif '存款' in banking_terms:
            response_parts.append('的存款信息')
        elif '贷款' in banking_terms:
            response_parts.append('的贷款信息')

        response_parts.append('。详细信息已列示在查询结果中。')

        return ''.join(response_parts)

    def get_status(self) -> Dict[str, Any]:
        """获取工具状态"""
        return {
            'name': self.name,
            'version': self.version,
            'initialized': self.initialized,
            'api_key_configured': bool(self.api_key),
            'data_directory': self.data_dir,
            'available_databases': self.available_databases,
            'database_count': len(self.available_databases),
            'llm_available': self.llm_available,
            'nl2sql_available': bool(self.nl2sql_processor),
            'llm_insights_available': bool(self.llm_insights_generator)
        }
    
    def refresh_databases(self):
        """刷新可用数据库列表"""
        self.available_databases = self._scan_databases()
        print(f"[INFO] 刷新数据库列表: 找到{len(self.available_databases)}个数据库")
        return self.available_databases
