#!/usr/bin/env python3
"""
DataProxy Flask API Backend
基于Streamlit核心逻辑的简化API实现
"""

import os
import sys
import json
import time
import asyncio
import tempfile
import shutil
import signal
import atexit
import numpy as np
import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS
from dotenv import load_dotenv
from werkzeug.utils import secure_filename

# 添加项目路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# 加载环境变量
load_dotenv(os.path.join(project_root, '.env'))

# 🔥 导入简化后的核心模块
try:
    from core_modules.config import get_unified_config, reset_unified_config
    from core_modules.utils import FileConverter

    # 导入核心模块
    try:
        from core_modules.visualization import get_chart_system
        CHART_SYSTEM_AVAILABLE = True
    except ImportError:
        CHART_SYSTEM_AVAILABLE = False

    try:
        from core_modules.agent.consolidated_tool_registry import (
            get_tool_registry,
            create_dataproxy_tools,
            get_system_health
        )
        from core_modules.agent.simplified_dataproxy_tool import SimplifiedDataProxyTool
        UNIFIED_TOOLS_AVAILABLE = True
    except ImportError as e:
        UNIFIED_TOOLS_AVAILABLE = False
        UNIFIED_TOOLS_ERROR = str(e)

    try:
        from core_modules.visualization.enhanced_visualization_langchain_tool import EnhancedVisualizationTool
        ENHANCED_VIZ_AVAILABLE = True
    except ImportError as e:
        ENHANCED_VIZ_AVAILABLE = False
        ENHANCED_VIZ_ERROR = str(e)

    try:
        from core_modules.analytics import smart_stats_analyzer
        SMART_STATS_AVAILABLE = True
    except ImportError as e:
        SMART_STATS_AVAILABLE = False

    try:
        from core_modules.config.metadata_aware_unified_config import MetadataAwareUnifiedConfig
        from core_modules.config.metadata_aware_context_generator import MetadataAwareContextGenerator
        from core_modules.nl2sql.metadata_aware_nl2sql_enhancer import MetadataAwareNL2SQLEnhancer
        METADATA_AWARE_AVAILABLE = True
    except ImportError as e:
        METADATA_AWARE_AVAILABLE = False
        METADATA_AWARE_ERROR = str(e)

    try:
        from core_modules.data_import import IntelligentDataImporter
        INTELLIGENT_IMPORT_AVAILABLE = True
    except ImportError as e:
        INTELLIGENT_IMPORT_AVAILABLE = False
        INTELLIGENT_IMPORT_ERROR = str(e)

    try:
        from core_modules.analytics import table_statistics_analyzer, simple_insights_generator
        NEW_INSIGHTS_AVAILABLE = True
    except ImportError as e:
        NEW_INSIGHTS_AVAILABLE = False

    try:
        from core_modules.analytics import enhanced_insights_generator
        ENHANCED_INSIGHTS_AVAILABLE = True
    except ImportError as e:
        ENHANCED_INSIGHTS_AVAILABLE = False

    # 检查DataProxy工具是否可用
    DATAPROXY_TOOL_AVAILABLE = False
    dataproxy_tool = None

    try:
        from core_modules.agent.dataproxy_tool import DataProxyTool
        dataproxy_tool = DataProxyTool()
        DATAPROXY_TOOL_AVAILABLE = True
        print("✅ DataProxy工具: 已加载")
    except ImportError:
        print("ℹ️ DataProxy工具: 使用简化版本")

except ImportError as e:
    print(f"❌ 核心模块导入失败: {e}")
    sys.exit(1)

# 数据清理函数
def clean_data_for_json(data):
    """清理数据以便JSON序列化"""
    if isinstance(data, dict):
        return {k: clean_data_for_json(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [clean_data_for_json(item) for item in data]
    elif isinstance(data, pd.DataFrame):
        return data.to_dict('records')
    elif isinstance(data, np.ndarray):
        return data.tolist()
    elif isinstance(data, (np.integer, np.floating)):
        return data.item()
    elif isinstance(data, np.bool_):
        return bool(data)
    elif pd.isna(data):
        return None
    else:
        return data

# 创建Flask应用
app = Flask(__name__)
CORS(app)  # 启用CORS支持

# 注册所有API蓝图
try:
    # 系统管理API
    from flask_backend.api.system_endpoints import create_system_blueprint
    system_bp = create_system_blueprint()
    app.register_blueprint(system_bp)
    print("✅ 系统管理API: 已注册")

    # 数据库管理API
    from flask_backend.api.database_endpoints import create_database_blueprint
    database_bp = create_database_blueprint()
    app.register_blueprint(database_bp)
    print("✅ 数据库管理API: 已注册")

    # 文件管理API
    from flask_backend.api.file_endpoints import create_file_blueprint
    file_bp = create_file_blueprint()
    app.register_blueprint(file_bp)
    print("✅ 文件管理API: 已注册")

    # 上下文管理API
    from flask_backend.api.context_endpoints import create_context_blueprint
    context_bp = create_context_blueprint()
    app.register_blueprint(context_bp)
    print("✅ 上下文管理API: 已注册")

    # 可视化API
    from flask_backend.api.visualization_endpoints import create_visualization_blueprint
    viz_bp = create_visualization_blueprint()
    app.register_blueprint(viz_bp)
    print("✅ 可视化API: 已注册")

    # 配置管理API
    from flask_backend.api.config_endpoints import create_config_blueprint
    config_bp = create_config_blueprint()
    app.register_blueprint(config_bp)
    print("✅ 配置管理API: 已注册")

    # 查询API（原有的）
    from flask_backend.api.query_endpoints import create_query_blueprint
    # 总是注册查询API，即使工具不可用也提供基础功能
    query_bp = create_query_blueprint(dataproxy_tool if DATAPROXY_TOOL_AVAILABLE else None)
    app.register_blueprint(query_bp)
    print("✅ 查询API: 已注册")

except ImportError as e:
    print(f"ℹ️ API蓝图导入: {e}")
    print("ℹ️ 使用基础端点")

# 全局状态管理（模仿Streamlit的session_state）
class GlobalState:
    def __init__(self):
        self.initialized = False
        self.unified_config = None
        self.agent = None
        self.available_databases = {}
        self.current_database = None
        self.simple_charts = []
        self.chart_system = None
        self.context_manager = None
        self.file_manager = None
        self.file_converter = None

        # 🔥 新增：元数据感知组件
        self.metadata_aware_config = None
        self.metadata_enhancer = None
        self.metadata_enhanced = False

    def initialize(self):
        """初始化全局状态（模仿Streamlit的初始化逻辑）"""
        if self.initialized:
            return True

        try:
            print("[INFO] 初始化DataProxy全局状态（元数据感知版本）...")

            # 🔥 优先尝试使用元数据感知配置
            if METADATA_AWARE_AVAILABLE:
                try:
                    self.metadata_aware_config = MetadataAwareUnifiedConfig()
                    self.unified_config = self.metadata_aware_config
                    self.metadata_enhanced = True
                    print("✅ 使用元数据感知配置管理器")
                except Exception as e:
                    print(f"⚠️ 元数据感知配置初始化失败，降级到传统模式: {e}")
                    self.unified_config = get_unified_config()
                    self.metadata_enhanced = False
            else:
                # 降级到传统配置
                self.unified_config = get_unified_config()
                self.metadata_enhanced = False
                print("📋 使用传统配置管理器")
            
            # 获取可用数据库
            self.available_databases = self.unified_config.get_available_databases()
            print(f"[INFO] 发现 {len(self.available_databases)} 个可用数据库")
            
            # 🔥 修复：只选择文件存在的数据库
            available_dbs = [
                db_path for db_path, db_info in self.available_databases.items()
                if db_info.get('file_exists', True) and db_info.get('status') == 'available'
            ]

            if available_dbs:
                first_db_path = available_dbs[0]
                print(f"[INFO] 自动选择可用数据库: {first_db_path}")

                success = self.unified_config.switch_database(first_db_path)
                if success:
                    self.current_database = first_db_path
                    print(f"[INFO] 数据库切换成功: {first_db_path}")

                    # 🔥 初始化元数据增强器
                    self._initialize_metadata_enhancer()
                else:
                    print(f"[ERROR] 数据库切换失败: {first_db_path}")
            else:
                print(f"[WARNING] 没有找到可用的数据库文件")
            
            # 🔥 初始化简化的DataProxy工具
            if UNIFIED_TOOLS_AVAILABLE:
                self.dataproxy_tool = SimplifiedDataProxyTool()
                print("✅ 简化DataProxy工具初始化成功")
            else:
                self.dataproxy_tool = None
                print("❌ 统一工具系统不可用")
            
            # 初始化其他组件（简化版本）
            try:
                self.context_manager = self._get_simple_context_manager()
                self.file_manager = self._get_simple_file_manager()
                self.file_converter = self._get_simple_file_converter()
                self.chart_system = self._get_simple_chart_system()
                print("✅ 简化组件初始化成功")
            except Exception as e:
                print(f"[WARNING] 组件初始化部分失败: {e}")
                # 设置默认值
                self.context_manager = None
                self.file_manager = None
                self.file_converter = None
                self.chart_system = None
            
            self.initialized = True
            print("[INFO] 全局状态初始化完成")
            return True

        except Exception as e:
            print(f"[ERROR] 全局状态初始化失败: {e}")
            self.initialized = False
            return False

    def _get_simple_context_manager(self):
        """获取简化的上下文管理器"""
        class SimpleContextManager:
            def get_context(self, database_path):
                return {"database_path": database_path, "tables": {}}

            def update_context(self, database_path, context):
                return True

            def list_contexts(self):
                return []

        return SimpleContextManager()

    def _get_simple_file_manager(self):
        """获取简化的文件管理器"""
        class SimpleFileManager:
            def process_file(self, file_path):
                return {"status": "processed", "file_path": file_path}

        return SimpleFileManager()

    def _get_simple_file_converter(self):
        """获取简化的文件转换器"""
        class SimpleFileConverter:
            def convert_to_database(self, file_path):
                return {"status": "converted", "database_path": file_path.replace('.xlsx', '.db')}

        return SimpleFileConverter()

    def _get_simple_chart_system(self):
        """获取简化的图表系统"""
        try:
            from core_modules.visualization import get_chart_system
            return get_chart_system()
        except ImportError:
            class SimpleChartSystem:
                def create_chart(self, data, chart_type, **kwargs):
                    return {"status": "created", "chart_type": chart_type}

            return SimpleChartSystem()

    def _initialize_metadata_enhancer(self):
        """初始化元数据增强器"""
        if self.metadata_enhanced and METADATA_AWARE_AVAILABLE:
            try:
                self.metadata_enhancer = MetadataAwareNL2SQLEnhancer(self.metadata_aware_config)
                print("✅ 元数据感知NL2SQL增强器初始化成功")
            except Exception as e:
                print(f"⚠️ 元数据增强器初始化失败: {e}")
                self.metadata_enhancer = None

    def process_query_enhanced(self, query: str) -> dict:
        """增强的查询处理方法"""
        try:
            # 🔥 如果有元数据增强器，先进行查询增强
            query_enhancement = {}
            if self.metadata_enhancer:
                query_enhancement = self.metadata_enhancer.enhance_query(query)
                print(f"[DEBUG] 查询增强完成: {len(query_enhancement.get('field_mappings', {}))} 个字段映射")

            # 使用DataProxy工具处理查询
            if not hasattr(self, 'dataproxy_tool') or not self.dataproxy_tool:
                return {"success": False, "error": "DataProxy工具不可用"}

            # 执行查询（使用正确的参数）
            result = self.dataproxy_tool._run(query)

            # 🔥 在响应中添加元数据信息
            if query_enhancement.get('metadata_enhanced'):
                if 'metadata_info' not in result:
                    result['metadata_info'] = {}

                result['metadata_info'].update({
                    'field_mappings': query_enhancement.get('field_mappings', {}),
                    'business_rules': query_enhancement.get('business_rules', {}),
                    'suggestions': query_enhancement.get('suggestions', []),
                    'enhanced_query': query_enhancement.get('enhanced_query', query)
                })

            return result

        except Exception as e:
            print(f"[ERROR] 增强查询处理失败: {e}")
            return {"success": False, "error": str(e)}

# 全局状态实例
global_state = GlobalState()

# Flask Callback Handler（模仿Streamlit的SimpleCallbackHandler）
class FlaskCallbackHandler:
    def __init__(self):
        self.tool_calls = []
        self.current_tool = None
        self.run_inline = True
        self.ignore_chain = False
        self.ignore_agent = False
        self.ignore_llm = False
        self.ignore_retriever = False
        self.ignore_chat_model = False
        self.raise_error = False

        # 可视化数据收集（模仿Streamlit）
        self.charts_to_add = []
        self.pending_viz_tasks = []
        self.session_start_time = time.time()

        # 🔥 新增：查询分解控制参数
        self.enable_decomposition = True  # 默认启用分解

    def on_tool_start(self, serialized, input_str, **kwargs):
        self.current_tool = serialized.get('name', 'unknown') if serialized else 'unknown'

        # 🔥 新增：如果是DataProxy工具，注入分解控制参数
        if self.current_tool == 'dataproxy_analysis':
            print(f"[DEBUG] FlaskCallbackHandler: 拦截DataProxy工具调用，注入分解控制参数")
            print(f"[DEBUG] FlaskCallbackHandler: enable_decomposition = {self.enable_decomposition}")

            # 修改工具输入以包含分解控制参数
            try:
                import json
                if isinstance(input_str, str):
                    # 尝试解析JSON输入
                    try:
                        tool_input = json.loads(input_str)
                        tool_input['enable_decomposition'] = self.enable_decomposition
                        modified_input = json.dumps(tool_input)
                        print(f"[DEBUG] FlaskCallbackHandler: 已注入分解控制参数到工具输入")
                        # 注意：这里我们不能直接修改input_str，但可以记录这个信息
                    except json.JSONDecodeError:
                        print(f"[DEBUG] FlaskCallbackHandler: 工具输入不是JSON格式，无法注入参数")
            except Exception as e:
                print(f"[DEBUG] FlaskCallbackHandler: 注入分解控制参数失败: {e}")

    def on_tool_end(self, output, **kwargs):
        tool_call_data = {
            "tool_name": self.current_tool or 'unknown',
            "input_args": {},
            "output": output,
            "success": True
        }
        self.tool_calls.append(tool_call_data)
        
        # 检查工具输出是否包含可视化数据
        self._check_for_visualization_data(tool_call_data)

    def on_tool_error(self, error, **kwargs):
        self.tool_calls.append({
            "tool_name": self.current_tool or 'unknown',
            "input_args": {},
            "output": None,
            "success": False,
            "error": str(error)
        })
    
    def _check_for_visualization_data(self, tool_call_data):
        """检查工具调用结果中是否包含可视化数据（模仿Streamlit逻辑）"""
        try:
            output = tool_call_data.get('output')
            if not output:
                return
            
            # 如果output是字符串，尝试解析为JSON
            if isinstance(output, str):
                try:
                    output = json.loads(output)
                except:
                    return
            
            if not isinstance(output, dict):
                return
            
            # 检查是否有可视化相关的数据
            has_viz_data = (
                'visualizations' in output or
                'data_tables' in output or
                'data' in output or
                (output.get('success') and len(output.get('data', [])) > 0)
            )
            
            if has_viz_data:
                print(f"[DEBUG] Flask Callback: 发现可视化数据在工具 {tool_call_data['tool_name']}")
                
                # 创建可视化任务
                viz_task = {
                    'tool_result': output,
                    'original_query': getattr(self, 'current_query', '数据分析'),
                    'timestamp': time.time(),
                    'status': 'pending',
                    'tool_name': tool_call_data['tool_name']
                }
                
                self.pending_viz_tasks.append(viz_task)
                print(f"[DEBUG] Flask Callback: 添加可视化任务，当前任务数: {len(self.pending_viz_tasks)}")
                
        except Exception as e:
            print(f"[DEBUG] Flask Callback: 检查可视化数据时出错 - {e}")
    
    def get_pending_viz_tasks(self):
        """获取待处理的可视化任务"""
        return self.pending_viz_tasks
    
    def set_current_query(self, query):
        """设置当前查询"""
        self.current_query = query

    # 添加所有缺失的callback方法
    def on_llm_start(self, serialized, prompts, **kwargs):
        pass

    def on_llm_new_token(self, token, **kwargs):
        pass

    def on_llm_end(self, response, **kwargs):
        pass

    def on_llm_error(self, error, **kwargs):
        pass

    def on_chain_start(self, serialized, inputs, **kwargs):
        pass

    def on_chain_end(self, outputs, **kwargs):
        pass

    def on_chain_error(self, error, **kwargs):
        pass

    def on_agent_action(self, action, **kwargs):
        pass

    def on_agent_finish(self, finish, **kwargs):
        pass

    def on_chat_model_start(self, serialized, messages, **kwargs):
        pass

async def process_pending_enhanced_visualizations_flask(pending_tasks):
    """处理待处理的增强可视化任务 - Flask版本（直接使用Streamlit逻辑）"""
    generated_charts = []
    
    if not pending_tasks:
        print("[DEBUG] Flask可视化：没有待处理的任务")
        return generated_charts
    
    if not ENHANCED_VIZ_AVAILABLE:
        print("[DEBUG] Flask可视化：增强可视化模块不可用")
        return generated_charts
    
    try:
        # 导入增强可视化模块
        from core_modules.visualization.enhanced_visualization_langchain_tool import EnhancedVisualizationTool
        viz_tool = EnhancedVisualizationTool()
        
        for task in pending_tasks:
            if task.get('status') != 'pending':
                continue
                
            print(f"[DEBUG] Flask可视化：处理任务 - {task['original_query']}")
            
            # 标记任务为处理中
            task['status'] = 'processing'
            
            # 提取原始查询字符串
            original_query = task.get('original_query', '数据分析')
            if not isinstance(original_query, str):
                original_query = str(original_query)
            
            # 🔧 修复：正确提取数据表
            tool_result = task['tool_result']
            print(f"[DEBUG] Flask可视化：tool_result类型: {type(tool_result)}")
            print(f"[DEBUG] Flask可视化：tool_result键: {list(tool_result.keys()) if isinstance(tool_result, dict) else 'not dict'}")

            # 尝试多种方式提取数据表
            all_data_tables = {}

            # 方式1：直接从tool_result中提取data_tables
            if isinstance(tool_result, dict) and 'data_tables' in tool_result:
                print(f"[DEBUG] Flask可视化：发现直接的data_tables")
                data_tables = tool_result['data_tables']
                if isinstance(data_tables, dict):
                    for table_name, table_data in data_tables.items():
                        if isinstance(table_data, list) and table_data:
                            all_data_tables[table_name] = table_data
                            print(f"[DEBUG] Flask可视化：✅ 提取表 {table_name}，数据量: {len(table_data)}")

            # 方式2：使用可视化工具的提取方法作为备用
            if not all_data_tables:
                print(f"[DEBUG] Flask可视化：尝试使用可视化工具提取方法")
                all_data_tables = viz_tool._extract_all_visualization_data_from_dataproxy(tool_result)

            # 方式3：如果还是没有，检查是否有visualization_data结构
            if not all_data_tables and isinstance(tool_result, dict):
                if 'visualization_data' in tool_result:
                    viz_data = tool_result['visualization_data']
                    if isinstance(viz_data, dict) and 'data_tables' in viz_data:
                        print(f"[DEBUG] Flask可视化：发现visualization_data.data_tables")
                        data_tables = viz_data['data_tables']
                        if isinstance(data_tables, dict):
                            for table_name, table_data in data_tables.items():
                                if isinstance(table_data, list) and table_data:
                                    all_data_tables[table_name] = table_data
                                    print(f"[DEBUG] Flask可视化：✅ 从visualization_data提取表 {table_name}，数据量: {len(table_data)}")

            # 方式4：🔧 新增：直接从data字段创建数据表（针对NL2SQL结果）
            if not all_data_tables and isinstance(tool_result, dict) and 'data' in tool_result:
                data = tool_result['data']
                if isinstance(data, list) and data:
                    print(f"[DEBUG] Flask可视化：发现直接的data字段，数据量: {len(data)}")
                    # 使用查询内容作为表名
                    table_name = task.get('original_query', '查询结果')
                    if len(table_name) > 20:
                        table_name = table_name[:20] + "..."
                    all_data_tables[table_name] = data
                    print(f"[DEBUG] Flask可视化：✅ 从data字段创建表 {table_name}，数据量: {len(data)}")

            if not all_data_tables:
                print(f"[WARN] Flask可视化：未找到有效数据表")
                print(f"[DEBUG] Flask可视化：tool_result内容预览: {str(tool_result)[:500]}...")
                task['status'] = 'failed'
                continue
            
            print(f"[INFO] Flask可视化：找到 {len(all_data_tables)} 个数据表，将为每个表生成图表")
            
            # 为每个数据表生成独立的图表
            for table_name, table_data in all_data_tables.items():
                try:
                    print(f"[DEBUG] Flask可视化：为表 '{table_name}' 生成图表，数据量: {len(table_data)}")
                    
                    # 🔧 修复：提取用户原始查询，避免系统内部描述
                    # 从original_query中提取真正的用户查询
                    user_query = str(original_query)

                    # 如果包含系统描述，提取用户查询部分
                    if "请使用DataProxy工具分析以下查询" in user_query:
                        # 提取冒号后的用户查询部分
                        parts = user_query.split("：")
                        if len(parts) > 1:
                            user_query = parts[-1].strip()

                    # 清理特殊字符并限制长度
                    safe_query = user_query.replace("'", "").replace('"', '').replace('{', '').replace('}', '')
                    if len(safe_query) > 30:
                        safe_query = "数据分析"

                    table_viz_query = f"基于{table_name}数据表的分析结果，为{safe_query}查询生成专业的可视化图表"
                    
                    # 🔧 修复：正确传递数据给可视化工具
                    print(f"[DEBUG] Flask可视化：准备传递数据给可视化工具，数据类型: {type(table_data)}")
                    print(f"[DEBUG] Flask可视化：数据内容预览: {table_data[:2] if isinstance(table_data, list) and table_data else 'empty'}")

                    # 构造正确的数据格式
                    viz_data = {
                        'data_tables': {
                            table_name: table_data
                        },
                        'query_result': {
                            'data_tables': {
                                table_name: table_data
                            }
                        }
                    }

                    # 🔥 修复：获取并传递上下文信息
                    viz_config = {
                        'height': 500,
                        'color_scheme': 'business',
                        'theme': 'professional'
                    }

                    # 尝试获取当前的统一配置上下文
                    try:
                        from core_modules.config import get_unified_config
                        unified_config = get_unified_config()
                        if unified_config and unified_config.is_valid():
                            context = unified_config.create_context(original_query)
                            viz_config['context'] = context
                            viz_config['original_user_query'] = original_query
                            print(f"[DEBUG] Flask可视化：为表 '{table_name}' 传递上下文，业务术语数量: {len(context.business_terms)}")
                        else:
                            print(f"[DEBUG] Flask可视化：表 '{table_name}' 无法获取有效上下文")
                    except Exception as ctx_error:
                        print(f"[DEBUG] Flask可视化：获取上下文失败: {ctx_error}")

                    # 使用同步方式调用可视化工具
                    viz_result = viz_tool._run(
                        data=viz_data,
                        user_query=table_viz_query,
                        config=viz_config
                    )
                    
                    # 处理可视化结果
                    if viz_result.get('success'):
                        print(f"[INFO] Flask可视化：表 '{table_name}' 图表生成成功")
                        
                        # 提取图表内容
                        figure_html = viz_result.get('figure_html')
                        figure_json = viz_result.get('figure_json')
                        visualizations = viz_result.get('visualizations', [])
                        
                        # 从visualizations中提取HTML和JSON（如果主字段为空）
                        if not figure_html and visualizations:
                            for viz in visualizations:
                                if isinstance(viz, dict):
                                    for field in ['figure_html', 'html', 'chart_html', 'content']:
                                        if viz.get(field):
                                            figure_html = viz[field]
                                            break
                                    if figure_html:
                                        break
                        
                        # 如果有任何可用的图表内容
                        if figure_html or figure_json:
                            chart_info = {
                                'figure_html': figure_html,
                                'figure_json': figure_json,
                                'title': f"{table_name} - {original_query}",
                                'chart_type': 'enhanced_multi',
                                'data_count': len(table_data),
                                'chart_id': f"flask_chart_{table_name}_{int(task['timestamp'])}",
                                'table_name': table_name,
                                'quality_assessment': viz_result.get('quality_assessment', {}),
                                'generated_code': viz_result.get('generated_code', ''),
                                'execution_time': viz_result.get('execution_time', 0),
                                'source': 'flask_enhanced_visualization'
                            }
                            
                            generated_charts.append(chart_info)
                            print(f"[INFO] Flask可视化：表 '{table_name}' 已添加到图表列表")
                        else:
                            print(f"[WARN] Flask可视化：表 '{table_name}' 生成成功但没有HTML或JSON内容")
                    else:
                        print(f"[WARN] Flask可视化：表 '{table_name}' 生成失败 - {viz_result.get('error', '未知错误')}")
                        
                except Exception as e:
                    print(f"[ERROR] Flask可视化：表 '{table_name}' 处理失败 - {e}")
                    continue
            
            task['status'] = 'completed'
        
        print(f"[INFO] Flask可视化：成功生成 {len(generated_charts)} 个图表")
        
    except ImportError as e:
        print(f"[DEBUG] Flask可视化：模块不可用 - {e}")
    except Exception as e:
        print(f"[ERROR] Flask可视化：处理失败 - {e}")
    
    return generated_charts

# API路由定义

@app.route('/api/health', methods=['GET'])
def health_check():
    """健康检查"""
    return jsonify({
        "status": "healthy",
        "service": "DataProxy Flask API",
        "version": "2.0.0",
        "timestamp": time.time(),
        "initialized": global_state.initialized
    })

@app.route('/api/version', methods=['GET'])
def version_info():
    """版本信息端点"""
    return jsonify({
        'version': '2.0.0',
        'build_date': '2024-12-19',
        'features': [
            'LLM驱动的智能查询',
            '智能图表代码生成',
            '银行业务专业化',
            '上下文管理'
        ]
    })

@app.route('/', methods=['GET'])
def root():
    """根路径"""
    return jsonify({
        'message': 'DataProxy API Server',
        'version': '2.0.0',
        'endpoints': {
            'health': '/api/health',
            'version': '/api/version',
            'queries': '/api/v1/queries'
        }
    })

@app.route('/api/v1/queries/agent', methods=['POST'])
def agent_query():
    """
    智能Agent查询（使用Streamlit核心逻辑）

    请求参数:
    - query (str): 用户查询文本
    - use_simple_query (bool, 可选): 是否使用简单查询模式，默认为False
        * False (默认): 复杂查询模式，强制使用DataProxy工具，确保返回结构化数据和图表
        * True: 简单查询模式，让Agent自由选择工具，可能只返回文本回答
    - enable_decomposition (bool, 可选): 是否启用查询分解，默认为True（仅在复杂查询模式下生效）
        * True (默认): 启用分解，将复杂查询分解为多个子查询，获得多维度详细分析
        * False: 禁用分解，将查询作为单一查询处理，提高响应速度

    响应格式:
    - success (bool): 查询是否成功
    - agent_response (str): Agent的文本回答
    - tool_calls (list): 工具调用记录
    - enhanced_insights (dict): 增强洞察（整合insights和statistics）
    - visualizations (list): 可视化图表
    - data_tables (dict): 结构化数据表
    - insights (str): 原始洞察（向后兼容）
    - recommendations (str): 原始建议（向后兼容）
    - statistics (dict): 统计分析结果（向后兼容）
    """
    try:
        # 确保全局状态已初始化
        if not global_state.initialized:
            success = global_state.initialize()
            if not success:
                return jsonify({
                    "success": False,
                    "error": "系统初始化失败"
                }), 500

        # 获取请求数据
        data = request.get_json()
        if not data or 'query' not in data:
            return jsonify({
                "success": False,
                "error": "缺少查询参数"
            }), 400

        query = data['query']
        # 🆕 新增参数：是否使用简单查询模式
        use_simple_query = data.get('use_simple_query', False)  # 默认为False，走复杂查询
        # 🆕 新增参数：是否启用查询分解（仅在复杂查询模式下生效）
        enable_decomposition = data.get('enable_decomposition', True)  # 默认为True，启用分解

        print(f"[INFO] 收到增强Agent查询: {query}")
        print(f"[INFO] 元数据增强状态: {global_state.metadata_enhanced}")
        print(f"[INFO] 查询模式: {'简单查询' if use_simple_query else '复杂查询（默认）'}")
        if not use_simple_query:
            print(f"[INFO] 查询分解控制: {'✅ 启用分解（多维度分析）' if enable_decomposition else '❌ 禁用分解（单一查询模式）'}")

        # 创建callback handler（模仿Streamlit）
        callback_handler = FlaskCallbackHandler()

        # 🔧 根据参数决定查询模式
        if use_simple_query:
            # 简单查询模式：让Agent自由选择工具
            print(f"[DEBUG] 使用简单查询模式")
            callback_handler.set_current_query(query)
            final_query = query
        else:
            # 复杂查询模式（默认）：强制使用DataProxy工具
            print(f"[DEBUG] 使用复杂查询模式（默认）")
            # 🔥 新增：构建包含分解控制参数的查询
            decomposition_instruction = "启用分解进行多维度分析" if enable_decomposition else "禁用分解使用单一查询模式"
            final_query = f"请使用DataProxy工具分析以下查询（{decomposition_instruction}），确保返回结构化数据和可视化图表：{query}"
            callback_handler.set_current_query(final_query)
            # 🔥 新增：设置分解控制参数到callback handler
            callback_handler.enable_decomposition = enable_decomposition

        # 清空之前的图表（模仿Streamlit逻辑）
        global_state.simple_charts = []

        # 🔥 使用增强的查询处理方法
        if not hasattr(global_state, 'dataproxy_tool') or not global_state.dataproxy_tool:
            return jsonify({
                "success": False,
                "error": "DataProxy工具不可用"
            }), 500

        try:
            # 根据查询模式选择分析模式
            if use_simple_query:
                analysis_mode = "simple"
            else:
                analysis_mode = "detailed" if enable_decomposition else "auto"

            print(f"[DEBUG] 使用分析模式: {analysis_mode}")

            # 🔥 使用增强的查询处理方法
            result = global_state.process_query_enhanced(query)

        except Exception as e:
            print(f"[ERROR] DataProxy工具执行失败: {e}")
            return jsonify({
                "success": False,
                "error": f"分析执行失败: {str(e)}"
            }), 500

        # 🔥 简化的响应处理
        if not result.get('success', False):
            return jsonify({
                "success": False,
                "error": result.get('error', '分析失败'),
                "agent_response": "抱歉，分析过程中遇到了问题。",
                "tool_calls": [],
                "data_tables": {},
                "visualizations": [],
                "insights": "",
                "recommendations": "",
                "statistics": {}
            }), 500

        # 提取结果数据
        data_tables = result.get('data_tables', {})
        insights = result.get('insights', [])
        statistics = result.get('statistics', {})
        visualization_data = result.get('visualization_data', {})

        # 🔥 构建简化的响应
        # 生成简单的agent_response
        agent_response = f"已完成对查询「{query}」的分析"
        if insights:
            agent_response += f"，生成了 {len(insights)} 条洞察"
        if data_tables:
            agent_response += f"，包含 {len(data_tables)} 个数据表"

        # 构建工具调用记录（模拟）
        tool_calls = [{
            "tool_name": "simplified_dataproxy",
            "input_args": {"query": query, "analysis_mode": analysis_mode},
            "output": result,
            "success": True
        }]

        # 构建响应数据
        response_data = {
            "success": True,
            "agent_response": agent_response,
            "tool_calls": tool_calls,
            "data_tables": data_tables,
            "visualizations": [],  # 简化版暂不支持可视化
            "insights": "\n".join(insights) if isinstance(insights, list) else str(insights),
            "recommendations": "",  # 简化版暂不支持建议
            "statistics": statistics,
            "conversation_id": None
        }

        return jsonify(response_data)

    except Exception as e:
        print(f"[ERROR] Agent查询失败: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/v1/queries/nl2sql', methods=['POST'])
def nl2sql_query():
    """NL2SQL查询"""
    try:
        # 确保全局状态已初始化
        if not global_state.initialized:
            success = global_state.initialize()
            if not success:
                return jsonify({
                    "success": False,
                    "error": "系统初始化失败"
                }), 500

        # 获取请求数据
        data = request.get_json()
        if not data or 'query' not in data:
            return jsonify({
                "success": False,
                "error": "缺少查询参数"
            }), 400

        query = data['query']
        print(f"[INFO] 收到NL2SQL查询: {query}")

        # 使用Agent的NL2SQL功能
        if hasattr(global_state.agent, 'nl2sql_tool'):
            result = global_state.agent.nl2sql_tool._run(query)
        else:
            # 备用方案：直接使用Agent
            async def run_nl2sql():
                return await global_state.agent.analyze(query, None)

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(run_nl2sql())
            finally:
                loop.close()

        return jsonify({
            "success": True,
            "sql": result.get('sql', ''),
            "data": result.get('data', []),
            "columns": result.get('columns', []),
            "row_count": len(result.get('data', [])),
            "execution_time": result.get('execution_time', 0)
        })

    except Exception as e:
        print(f"[ERROR] NL2SQL查询失败: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/v1/databases', methods=['GET'])
def get_databases():
    """获取可用数据库列表"""
    try:
        # 确保全局状态已初始化
        if not global_state.initialized:
            success = global_state.initialize()
            if not success:
                return jsonify({
                    "success": False,
                    "error": "系统初始化失败"
                }), 500

        databases = []
        for db_path, db_info in global_state.available_databases.items():
            # 🔧 修复：只返回真实存在的数据库文件
            if db_info.get('file_exists', True) and db_info.get('status') == 'available':
                databases.append({
                    "path": db_path,
                    "name": db_info.get('name', os.path.basename(db_path)),
                    "table_count": db_info.get('table_count', 0),
                    "description": db_info.get('description', ''),
                    "size": db_info.get('size', 0)
                })

        return jsonify({
            "success": True,
            "databases": databases,
            "current_database": global_state.current_database,
            "total_count": len(databases)
        })

    except Exception as e:
        print(f"[ERROR] 获取数据库列表失败: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/v1/databases/switch', methods=['POST'])
def switch_database():
    """切换数据库"""
    try:
        # 确保全局状态已初始化
        if not global_state.initialized:
            success = global_state.initialize()
            if not success:
                return jsonify({
                    "success": False,
                    "error": "系统初始化失败"
                }), 500

        # 获取请求数据
        data = request.get_json()
        if not data or 'database_path' not in data:
            return jsonify({
                "success": False,
                "error": "缺少database_path参数"
            }), 400

        database_path = data['database_path']
        print(f"[INFO] 切换数据库到: {database_path}")

        # 执行数据库切换
        success = global_state.unified_config.switch_database(database_path)

        if success:
            global_state.current_database = database_path

            # � 修复：更新全局统一配置
            try:
                from core_modules.config.unified_config import update_global_config_database
                update_global_config_database(database_path)
                print("✅ 全局统一配置已更新")
            except Exception as e:
                print(f"⚠️ 全局统一配置更新失败: {e}")

            # �🔥 重新初始化元数据增强器
            global_state._initialize_metadata_enhancer()

            # 🔥 重新初始化简化的DataProxy工具
            try:
                if UNIFIED_TOOLS_AVAILABLE:
                    global_state.dataproxy_tool = SimplifiedDataProxyTool()
                    print("✅ 简化DataProxy工具重新初始化成功")
                else:
                    global_state.dataproxy_tool = None
                    print("❌ 统一工具系统不可用")

                # 清空图表状态
                global_state.simple_charts = []

                print(f"[INFO] 增强数据库切换成功: {database_path}")

                # 🔥 构建增强的响应
                response = {
                    "success": True,
                    "message": "数据库切换成功",
                    "current_database": database_path,
                    "metadata_enhanced": global_state.metadata_enhanced
                }

                # 如果是元数据增强的，添加额外信息
                if (global_state.metadata_enhanced and
                    hasattr(global_state, 'metadata_aware_config') and
                    global_state.metadata_aware_config and
                    hasattr(global_state.metadata_aware_config, 'is_metadata_enhanced') and
                    global_state.metadata_aware_config.is_metadata_enhanced()):
                    response.update({
                        "metadata_info": {
                            "business_terms_count": len(global_state.metadata_aware_config.business_terms),
                            "field_mappings_count": len(global_state.metadata_aware_config.field_mappings),
                            "enhanced_features": [
                                "字段中文名映射",
                                "业务规则自动应用",
                                "数据字典增强",
                                "查询智能建议"
                            ]
                        }
                    })

                return jsonify(response)
            except Exception as agent_error:
                print(f"[WARNING] Agent重新初始化失败: {agent_error}")
                return jsonify({
                    "success": True,
                    "message": "数据库切换成功，但Agent重新初始化失败",
                    "current_database": database_path,
                    "warning": str(agent_error)
                })
        else:
            return jsonify({
                "success": False,
                "error": "数据库切换失败"
            }), 400

    except Exception as e:
        print(f"[ERROR] 数据库切换失败: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/v1/contexts', methods=['GET'])
def list_contexts():
    """列出所有上下文"""
    try:
        # 确保全局状态已初始化
        if not global_state.initialized:
            success = global_state.initialize()
            if not success:
                return jsonify({
                    "success": False,
                    "error": "系统初始化失败"
                }), 500

        contexts = global_state.file_manager.list_all_contexts()

        return jsonify({
            "success": True,
            "contexts": contexts,
            "total_count": len(contexts)
        })

    except Exception as e:
        print(f"[ERROR] 列出上下文失败: {e}")
        return jsonify({
            "success": False,
            "error": str(e),
            "contexts": [],
            "total_count": 0
        }), 500

@app.route('/api/v1/status', methods=['GET'])
def get_status():
    """获取系统状态"""
    try:
        status = {
            "service": "DataProxy Flask API",
            "version": "1.0.0",
            "initialized": global_state.initialized,
            "current_database": global_state.current_database,
            "available_databases_count": len(global_state.available_databases),
            "enhanced_visualization_available": ENHANCED_VIZ_AVAILABLE,
            "smart_statistics_available": SMART_STATS_AVAILABLE,
            "new_insights_available": NEW_INSIGHTS_AVAILABLE,  # 🆕 新增新洞察分析状态
            "enhanced_insights_available": ENHANCED_INSIGHTS_AVAILABLE,  # 保留备用
            "components": {}
        }

        # 检查各组件状态
        try:
            status["components"]["unified_config"] = "✅ Available" if global_state.unified_config else "❌ Not initialized"
        except:
            status["components"]["unified_config"] = "❌ Error"

        try:
            status["components"]["agent"] = "✅ Available" if global_state.agent else "❌ Not initialized"
        except:
            status["components"]["agent"] = "❌ Error"

        try:
            status["components"]["context_manager"] = "✅ Available" if global_state.context_manager else "❌ Not initialized"
        except:
            status["components"]["context_manager"] = "❌ Error"

        try:
            status["components"]["chart_system"] = "✅ Available" if global_state.chart_system else "❌ Not initialized"
        except:
            status["components"]["chart_system"] = "❌ Error"

        # 🆕 检查智能统计分析模块状态
        try:
            status["components"]["smart_statistics"] = "✅ Available" if SMART_STATS_AVAILABLE else "❌ Not available"
        except:
            status["components"]["smart_statistics"] = "❌ Error"

        # 🆕 检查新洞察分析状态
        try:
            status["components"]["new_insights"] = "✅ Available" if NEW_INSIGHTS_AVAILABLE else "❌ Not available"
        except:
            status["components"]["new_insights"] = "❌ Error"

        # 检查增强洞察生成器状态（备用）
        try:
            status["components"]["enhanced_insights"] = "✅ Available" if ENHANCED_INSIGHTS_AVAILABLE else "❌ Not available"
        except:
            status["components"]["enhanced_insights"] = "❌ Error"

        if not ENHANCED_VIZ_AVAILABLE:
            status["enhanced_visualization_error"] = ENHANCED_VIZ_ERROR

        return jsonify(status)

    except Exception as e:
        return jsonify({
            "service": "DataProxy Flask API",
            "status": "error",
            "error": str(e)
        }), 500

@app.route('/api/v1/files/upload', methods=['POST'])
def upload_file():
    """文件上传接口"""
    try:
        # 确保全局状态已初始化
        if not global_state.initialized:
            success = global_state.initialize()
            if not success:
                return jsonify({
                    "success": False,
                    "error": "系统初始化失败"
                }), 500

        # 检查是否有文件
        if 'file' not in request.files:
            return jsonify({
                "success": False,
                "error": "没有上传文件"
            }), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({
                "success": False,
                "error": "文件名为空"
            }), 400

        # 检查文件类型
        allowed_extensions = {'.db', '.sqlite', '.sqlite3', '.csv', '.xlsx', '.xls'}
        file_ext = os.path.splitext(file.filename)[1].lower()
        if file_ext not in allowed_extensions:
            return jsonify({
                "success": False,
                "error": f"不支持的文件类型: {file_ext}"
            }), 400

        # 获取用户提供的额外信息
        user_description = request.form.get('description', '')
        user_terms = request.form.get('business_terms', '')
        data_dictionary = request.form.get('data_dictionary', '')

        # 保存上传的文件到临时目录
        temp_dir = tempfile.mkdtemp()
        filename = secure_filename(file.filename)
        temp_file_path = os.path.join(temp_dir, filename)
        file.save(temp_file_path)

        print(f"[INFO] 文件已保存到临时路径: {temp_file_path}")

        # 如果是CSV或Excel文件，先转换为SQLite
        if file_ext in {'.csv', '.xlsx', '.xls'}:
            print(f"[INFO] 转换文件格式: {file_ext} -> SQLite")

            # 生成SQLite文件路径
            sqlite_filename = os.path.splitext(filename)[0] + '.db'
            sqlite_path = os.path.join(temp_dir, sqlite_filename)

            # 使用FileConverter转换
            converted_path = global_state.file_converter.convert_to_sqlite(temp_file_path, sqlite_path)

            # 更新文件路径为转换后的SQLite文件
            temp_file_path = converted_path
            filename = sqlite_filename

        # 构建用户提示
        user_hints = {
            'description': user_description,
            'business_terms': user_terms,
            'data_dictionary': data_dictionary
        }

        # 使用上下文管理器处理文件
        context = global_state.context_manager.process_uploaded_file(temp_file_path, user_hints)

        if context:
            # 保存上下文配置
            config_file = global_state.file_manager.save_context(context)

            # 将文件移动到数据库目录（可选）
            databases_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'databases')
            os.makedirs(databases_dir, exist_ok=True)

            final_db_path = os.path.join(databases_dir, filename)
            shutil.move(temp_file_path, final_db_path)

            # 更新上下文中的数据库路径
            context.database_path = final_db_path
            global_state.file_manager.save_context(context)

            # 刷新可用数据库列表
            global_state.available_databases = global_state.unified_config.get_available_databases()

            return jsonify({
                "success": True,
                "message": "文件上传和处理成功",
                "database_path": final_db_path,
                "database_name": context.database_name,
                "database_type": context.database_type,
                "config_file": config_file,
                "context_summary": {
                    "business_terms_count": len(context.business_terms),
                    "field_mappings_count": len(context.field_mappings),
                    "tables_count": len(context.tables)
                }
            })
        else:
            return jsonify({
                "success": False,
                "error": "文件处理失败，无法生成上下文"
            }), 500

    except Exception as e:
        print(f"[ERROR] 文件上传失败: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500
    finally:
        # 清理临时目录
        try:
            if 'temp_dir' in locals():
                shutil.rmtree(temp_dir, ignore_errors=True)
        except:
            pass

@app.route('/api/v1/files/intelligent-import', methods=['POST'])
def intelligent_data_import():
    """智能数据导入接口 - 基于数据字典驱动的批量数据导入"""
    if not INTELLIGENT_IMPORT_AVAILABLE:
        return jsonify({
            "success": False,
            "error": "智能数据导入功能不可用",
            "details": INTELLIGENT_IMPORT_ERROR if 'INTELLIGENT_IMPORT_ERROR' in globals() else "模块未加载"
        }), 503

    try:
        # 确保全局状态已初始化
        if not global_state.initialized:
            success = global_state.initialize()
            if not success:
                return jsonify({
                    "success": False,
                    "error": "系统初始化失败"
                }), 500

        # 检查是否有文件上传
        if 'files' not in request.files:
            return jsonify({
                "success": False,
                "error": "没有上传文件"
            }), 400

        files = request.files.getlist('files')
        if not files or all(file.filename == '' for file in files):
            return jsonify({
                "success": False,
                "error": "没有选择有效文件"
            }), 400

        # 获取输出数据库名称
        output_db_name = request.form.get('output_db_name', 'intelligent_import_result.db')
        if not output_db_name.endswith('.db'):
            output_db_name += '.db'

        # 创建临时目录保存上传的文件
        temp_dir = tempfile.mkdtemp()
        file_paths = []

        try:
            # 保存所有上传的文件
            for file in files:
                if file.filename:
                    filename = secure_filename(file.filename)
                    file_path = os.path.join(temp_dir, filename)
                    file.save(file_path)
                    file_paths.append(file_path)

            if not file_paths:
                return jsonify({
                    "success": False,
                    "error": "没有有效的文件被保存"
                }), 400

            # 设置输出数据库路径
            databases_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'databases')
            os.makedirs(databases_dir, exist_ok=True)
            output_db_path = os.path.join(databases_dir, output_db_name)

            # 创建智能数据导入器
            importer = IntelligentDataImporter()

            # 执行批量导入
            print(f"[INFO] 🚀 开始智能数据导入，处理 {len(file_paths)} 个文件")
            import_report = importer.process_batch_import(file_paths, output_db_path)

            # 构建响应
            response = {
                "success": True,
                "message": "智能数据导入完成",
                "output_database": output_db_path,
                "output_database_name": output_db_name,
                "import_report": import_report,
                "processed_files": [os.path.basename(path) for path in file_paths]
            }

            # 刷新可用数据库列表
            global_state.available_databases = global_state.unified_config.get_available_databases()

            print(f"[INFO] ✅ 智能数据导入成功: {import_report['import_summary']['total_imported_rows']} 条记录")
            return jsonify(response)

        finally:
            # 清理临时目录
            shutil.rmtree(temp_dir, ignore_errors=True)

    except Exception as e:
        print(f"[ERROR] 智能数据导入失败: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }), 500

@app.route('/api/v1/files/llm-intelligent-import', methods=['POST'])
def llm_intelligent_data_import():
    """LLM智能数据导入接口 - 基于大语言模型的真正智能化数据分析和导入"""
    try:
        # 检查LLM智能导入器是否可用
        try:
            from core_modules.data_import import LLMIntelligentDataImporter
        except ImportError as e:
            return jsonify({
                "success": False,
                "error": "LLM智能数据导入功能不可用",
                "details": f"导入失败: {e}",
                "suggestion": "请确保已安装langchain和相关依赖"
            }), 503

        # 确保全局状态已初始化
        if not global_state.initialized:
            success = global_state.initialize()
            if not success:
                return jsonify({
                    "success": False,
                    "error": "系统初始化失败"
                }), 500

        # 获取请求参数
        data_source_dir = request.form.get('data_source_dir')
        data_dict_dir = request.form.get('data_dict_dir')
        output_db_name = request.form.get('output_db_name', 'llm_generated.db')
        api_key = request.form.get('api_key')  # 可选的API密钥

        # 参数验证
        if not data_source_dir or not data_dict_dir:
            return jsonify({
                "success": False,
                "error": "缺少必要参数",
                "required_params": ["data_source_dir", "data_dict_dir"],
                "provided_params": {
                    "data_source_dir": bool(data_source_dir),
                    "data_dict_dir": bool(data_dict_dir)
                }
            }), 400

        # 检查目录是否存在
        project_root = os.path.dirname(os.path.dirname(__file__))
        full_data_source_dir = os.path.join(project_root, data_source_dir)
        full_data_dict_dir = os.path.join(project_root, data_dict_dir)

        if not os.path.exists(full_data_source_dir):
            return jsonify({
                "success": False,
                "error": f"数据源目录不存在: {data_source_dir}",
                "full_path": full_data_source_dir
            }), 400

        if not os.path.exists(full_data_dict_dir):
            return jsonify({
                "success": False,
                "error": f"数据字典目录不存在: {data_dict_dir}",
                "full_path": full_data_dict_dir
            }), 400

        # 设置输出数据库路径
        if not output_db_name.endswith('.db'):
            output_db_name += '.db'

        databases_dir = os.path.join(project_root, 'databases')
        os.makedirs(databases_dir, exist_ok=True)
        output_db_path = os.path.join(databases_dir, output_db_name)

        # 创建LLM智能数据导入器
        print(f"[INFO] 🧠 开始LLM智能数据导入")
        print(f"[INFO] 数据源目录: {full_data_source_dir}")
        print(f"[INFO] 数据字典目录: {full_data_dict_dir}")
        print(f"[INFO] 输出数据库: {output_db_path}")

        importer = LLMIntelligentDataImporter(api_key=api_key)

        # 执行LLM智能导入
        import_report = importer.process_batch_import(
            data_source_dir=full_data_source_dir,
            data_dict_dir=full_data_dict_dir,
            output_db_path=output_db_path
        )

        if import_report.get('success'):
            # 构建成功响应
            response = {
                "success": True,
                "message": "LLM智能数据导入完成",
                "output_database": output_db_path,
                "output_database_name": output_db_name,
                "import_report": import_report,
                "llm_analysis_summary": import_report.get('llm_analysis_summary', {}),
                "processing_summary": import_report.get('processing_summary', {}),
                "generated_business_terms": import_report.get('generated_business_terms', []),
                "recommendations": import_report.get('recommendations', [])
            }

            # 刷新可用数据库列表
            global_state.available_databases = global_state.unified_config.get_available_databases()

            print(f"[INFO] ✅ LLM智能数据导入成功")
            print(f"[INFO] 创建表: {import_report.get('processing_summary', {}).get('total_tables_created', 0)} 个")
            print(f"[INFO] 导入数据: {import_report.get('processing_summary', {}).get('total_imported_rows', 0)} 行")

            return jsonify(response)
        else:
            # 导入失败
            return jsonify({
                "success": False,
                "error": "LLM智能数据导入失败",
                "details": import_report.get('error', 'Unknown error'),
                "execution_time": import_report.get('execution_time', 0)
            }), 500

    except Exception as e:
        print(f"[ERROR] LLM智能数据导入失败: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__,
            "suggestion": "请检查API密钥配置和网络连接"
        }), 500

@app.route('/api/v1/files/pure-llm-import', methods=['POST'])
def pure_llm_intelligent_import():
    """纯LLM驱动的智能数据导入接口 - 完全基于LLM的零规则编码数据分析和导入"""
    try:
        # 检查纯LLM智能导入器是否可用
        try:
            from core_modules.data_import import IntelligentDataImporter
        except ImportError as e:
            return jsonify({
                "success": False,
                "error": "纯LLM智能数据导入功能不可用",
                "details": f"导入失败: {e}",
                "suggestion": "请确保已安装langchain和相关依赖，并配置DEEPSEEK_API_KEY"
            }), 503

        # 确保全局状态已初始化
        if not global_state.initialized:
            success = global_state.initialize()
            if not success:
                return jsonify({
                    "success": False,
                    "error": "系统初始化失败"
                }), 500

        # 获取请求参数
        data_source_dir = request.form.get('data_source_dir')
        data_dict_dir = request.form.get('data_dict_dir')
        output_db_name = request.form.get('output_db_name', 'pure_llm_generated.db')
        api_key = request.form.get('api_key')  # API密钥

        # 检查API密钥
        if not api_key:
            api_key = os.getenv('DEEPSEEK_API_KEY')

        if not api_key:
            return jsonify({
                "success": False,
                "error": "缺少API密钥",
                "details": "纯LLM系统需要DeepSeek API密钥",
                "suggestion": "请在请求中提供api_key参数或设置DEEPSEEK_API_KEY环境变量"
            }), 400

        # 参数验证
        if not data_source_dir or not data_dict_dir:
            return jsonify({
                "success": False,
                "error": "缺少必要参数",
                "required_params": ["data_source_dir", "data_dict_dir"],
                "provided_params": {
                    "data_source_dir": bool(data_source_dir),
                    "data_dict_dir": bool(data_dict_dir)
                }
            }), 400

        # 检查目录是否存在
        project_root = os.path.dirname(os.path.dirname(__file__))
        full_data_source_dir = os.path.join(project_root, data_source_dir)
        full_data_dict_dir = os.path.join(project_root, data_dict_dir)

        if not os.path.exists(full_data_source_dir):
            return jsonify({
                "success": False,
                "error": f"数据源目录不存在: {data_source_dir}",
                "full_path": full_data_source_dir
            }), 400

        if not os.path.exists(full_data_dict_dir):
            return jsonify({
                "success": False,
                "error": f"数据字典目录不存在: {data_dict_dir}",
                "full_path": full_data_dict_dir
            }), 400

        # 收集所有文件路径
        file_paths = []

        # 收集数据源文件
        for file_name in os.listdir(full_data_source_dir):
            if file_name.endswith(('.xlsx', '.xls', '.csv')):
                file_paths.append(os.path.join(full_data_source_dir, file_name))

        # 收集数据字典文件
        for file_name in os.listdir(full_data_dict_dir):
            if file_name.endswith(('.xlsx', '.xls', '.csv')):
                file_paths.append(os.path.join(full_data_dict_dir, file_name))

        if not file_paths:
            return jsonify({
                "success": False,
                "error": "未找到可处理的文件",
                "details": "数据源和数据字典目录中没有找到.xlsx, .xls或.csv文件"
            }), 400

        # 设置输出数据库路径
        if not output_db_name.endswith('.db'):
            output_db_name += '.db'

        databases_dir = os.path.join(project_root, 'databases')
        os.makedirs(databases_dir, exist_ok=True)
        output_db_path = os.path.join(databases_dir, output_db_name)

        # 创建纯LLM智能数据导入器
        print(f"[INFO] 🧠 开始纯LLM智能数据导入")
        print(f"[INFO] 数据源目录: {full_data_source_dir}")
        print(f"[INFO] 数据字典目录: {full_data_dict_dir}")
        print(f"[INFO] 发现文件: {len(file_paths)} 个")
        print(f"[INFO] 输出数据库: {output_db_path}")
        print(f"[INFO] ⏳ 注意: 纯LLM分析可能需要较长时间，请耐心等待...")

        importer = IntelligentDataImporter(api_key=api_key)

        # 执行纯LLM智能导入
        import_report = importer.process_batch_import(file_paths, output_db_path)

        if import_report.get('success'):
            # 构建成功响应
            response = {
                "success": True,
                "message": "纯LLM智能数据导入完成",
                "output_database": output_db_path,
                "output_database_name": output_db_name,
                "import_report": import_report,
                "processing_summary": import_report.get('processing_summary', {}),
                "llm_comprehensive_analysis": import_report.get('llm_comprehensive_analysis', {}),
                "business_intelligence": import_report.get('business_intelligence', {}),
                "execution_time": import_report.get('execution_time', 0),
                "processed_files": [os.path.basename(fp) for fp in file_paths]
            }

            # 刷新可用数据库列表
            global_state.available_databases = global_state.unified_config.get_available_databases()

            print(f"[INFO] ✅ 纯LLM智能数据导入成功")
            processing_summary = import_report.get('processing_summary', {})
            print(f"[INFO] 处理文件: {processing_summary.get('total_files_processed', 0)} 个")
            print(f"[INFO] 创建表: {processing_summary.get('total_tables_created', 0)} 个")
            print(f"[INFO] 导入数据: {processing_summary.get('total_imported_rows', 0)} 行")
            print(f"[INFO] LLM分析成功率: {processing_summary.get('llm_analysis_success_rate', 0):.1%}")

            return jsonify(response)
        else:
            # 导入失败
            return jsonify({
                "success": False,
                "error": "纯LLM智能数据导入失败",
                "details": import_report.get('error', 'Unknown error'),
                "execution_time": import_report.get('execution_time', 0)
            }), 500

    except Exception as e:
        print(f"[ERROR] 纯LLM智能数据导入失败: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__,
            "suggestion": "请检查API密钥配置、网络连接和文件格式"
        }), 500

@app.route('/api/v1/contexts', methods=['POST'])
def get_context_details():
    """获取特定数据库的上下文详情"""
    try:
        # 确保全局状态已初始化
        if not global_state.initialized:
            success = global_state.initialize()
            if not success:
                return jsonify({
                    "success": False,
                    "error": "系统初始化失败"
                }), 500

        # 获取请求数据
        data = request.get_json()
        if not data or 'database_path' not in data:
            return jsonify({
                "success": False,
                "error": "缺少database_path参数"
            }), 400

        database_path = data['database_path']
        print(f"[DEBUG] 获取上下文详情: {database_path}")

        # 加载上下文
        context = global_state.file_manager.load_context(database_path)

        if not context:
            return jsonify({
                "success": False,
                "error": "未找到上下文信息"
            }), 404

        # 转换为字典格式
        context_dict = global_state.file_manager._context_to_dict(context)

        return jsonify({
            "success": True,
            "context": context_dict
        })

    except Exception as e:
        print(f"[ERROR] 获取上下文详情失败: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/v1/contexts', methods=['PUT'])
def update_context():
    """更新上下文配置"""
    try:
        # 确保全局状态已初始化
        if not global_state.initialized:
            success = global_state.initialize()
            if not success:
                return jsonify({
                    "success": False,
                    "error": "系统初始化失败"
                }), 500

        # 获取请求数据
        data = request.get_json()
        if not data or 'database_path' not in data:
            return jsonify({
                "success": False,
                "error": "缺少database_path参数"
            }), 400

        database_path = data['database_path']
        print(f"[DEBUG] 更新上下文: {database_path}")

        # 加载现有上下文
        context = global_state.file_manager.load_context(database_path)
        if not context:
            return jsonify({
                "success": False,
                "error": "未找到上下文信息"
            }), 404

        # 🔧 修复：支持完整的上下文配置更新
        # 基本信息更新
        if 'description' in data:
            context.description = data['description']
        if 'database_type' in data:
            context.database_type = data['database_type']

        # 业务术语更新（通用）
        if 'business_terms' in data:
            context.business_terms = data['business_terms']

        # 字段映射更新（通用）
        if 'field_mappings' in data:
            context.field_mappings = data['field_mappings']

        # 🆕 数据库特定配置更新
        if 'database_specific_business_terms' in data:
            context.database_specific_business_terms = data['database_specific_business_terms']

        if 'database_specific_field_mappings' in data:
            context.database_specific_field_mappings = data['database_specific_field_mappings']

        if 'database_specific_query_scopes' in data:
            context.database_specific_query_scopes = data['database_specific_query_scopes']

        # 表关系更新
        if 'relationships' in data:
            context.relationships = data['relationships']

        # 表结构更新（通常不建议手动修改，但提供接口）
        if 'tables' in data:
            context.tables = data['tables']

        print(f"[DEBUG] 上下文更新项目: {list(data.keys())}")

        # 保存上下文
        success = global_state.file_manager.save_context(context)
        if success:
            return jsonify({
                "success": True,
                "message": "上下文更新成功"
            })
        else:
            return jsonify({
                "success": False,
                "error": "保存上下文失败"
            }), 500

    except Exception as e:
        print(f"[ERROR] 更新上下文失败: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/v1/contexts/<path:database_path>', methods=['DELETE'])
def delete_context(database_path):
    """删除上下文配置"""
    try:
        # 确保全局状态已初始化
        if not global_state.initialized:
            success = global_state.initialize()
            if not success:
                return jsonify({
                    "success": False,
                    "error": "系统初始化失败"
                }), 500

        # 查找配置文件
        config_file = global_state.file_manager._find_config_file(database_path)

        if not config_file:
            return jsonify({
                "success": False,
                "error": "未找到配置文件"
            }), 404

        # 删除配置文件
        os.remove(config_file)

        return jsonify({
            "success": True,
            "message": "上下文配置已删除"
        })

    except Exception as e:
        print(f"[ERROR] 删除上下文失败: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/v1/contexts/business-terms', methods=['GET'])
def get_business_terms():
    """获取业务术语列表"""
    try:
        # 确保全局状态已初始化
        if not global_state.initialized:
            success = global_state.initialize()
            if not success:
                return jsonify({
                    "success": False,
                    "error": "系统初始化失败"
                }), 500

        # 获取请求参数
        database_path = request.args.get('database_path')
        if not database_path:
            return jsonify({
                "success": False,
                "error": "缺少database_path参数"
            }), 400

        print(f"[DEBUG] 获取业务术语: {database_path}")

        # 加载上下文
        context = global_state.file_manager.load_context(database_path)
        if not context:
            return jsonify({
                "success": False,
                "error": "未找到上下文信息"
            }), 404

        # 合并通用业务术语和数据库特定业务术语
        all_terms = {}

        # 添加通用业务术语
        if hasattr(context, 'business_terms') and context.business_terms:
            for term_name, term_data in context.business_terms.items():
                all_terms[term_name] = {
                    **term_data,
                    "term_type": "general"
                }

        # 添加数据库特定业务术语
        if hasattr(context, 'database_specific_business_terms') and context.database_specific_business_terms:
            for term_name, term_data in context.database_specific_business_terms.items():
                all_terms[term_name] = {
                    **term_data,
                    "term_type": "database_specific"
                }

        return jsonify({
            "success": True,
            "business_terms": all_terms,
            "total_count": len(all_terms)
        })

    except Exception as e:
        print(f"[ERROR] 获取业务术语失败: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/v1/contexts/business-terms/<term_name>', methods=['GET'])
def get_business_term(term_name):
    """获取单个业务术语详情"""
    try:
        # 确保全局状态已初始化
        if not global_state.initialized:
            success = global_state.initialize()
            if not success:
                return jsonify({
                    "success": False,
                    "error": "系统初始化失败"
                }), 500

        # 获取请求参数
        database_path = request.args.get('database_path')
        if not database_path:
            return jsonify({
                "success": False,
                "error": "缺少database_path参数"
            }), 400

        print(f"[DEBUG] 获取业务术语详情: {term_name} in {database_path}")

        # 加载上下文
        context = global_state.file_manager.load_context(database_path)
        if not context:
            return jsonify({
                "success": False,
                "error": "未找到上下文信息"
            }), 404

        # 查找术语
        term_data = None
        term_type = None

        # 先在通用业务术语中查找
        if hasattr(context, 'business_terms') and context.business_terms and term_name in context.business_terms:
            term_data = context.business_terms[term_name]
            term_type = "general"

        # 再在数据库特定业务术语中查找
        elif hasattr(context, 'database_specific_business_terms') and context.database_specific_business_terms and term_name in context.database_specific_business_terms:
            term_data = context.database_specific_business_terms[term_name]
            term_type = "database_specific"

        if not term_data:
            return jsonify({
                "success": False,
                "error": f"未找到业务术语: {term_name}"
            }), 404

        return jsonify({
            "success": True,
            "term_name": term_name,
            "term_data": {
                **term_data,
                "term_type": term_type
            }
        })

    except Exception as e:
        print(f"[ERROR] 获取业务术语详情失败: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/v1/contexts/business-terms', methods=['POST'])
def add_business_term():
    """添加新的业务术语"""
    try:
        # 确保全局状态已初始化
        if not global_state.initialized:
            success = global_state.initialize()
            if not success:
                return jsonify({
                    "success": False,
                    "error": "系统初始化失败"
                }), 500

        # 获取请求数据
        data = request.get_json()
        if not data or 'database_path' not in data:
            return jsonify({
                "success": False,
                "error": "缺少database_path参数"
            }), 400

        database_path = data['database_path']
        term_name = data.get('term_name')
        term_data = data.get('term_data')
        term_type = data.get('term_type', 'database_specific')  # 默认为数据库特定术语

        if not term_name or not term_data:
            return jsonify({
                "success": False,
                "error": "缺少term_name或term_data参数"
            }), 400

        print(f"[DEBUG] 添加业务术语: {term_name} ({term_type}) in {database_path}")

        # 加载现有上下文
        context = global_state.file_manager.load_context(database_path)
        if not context:
            return jsonify({
                "success": False,
                "error": "未找到上下文信息"
            }), 404

        # 检查术语是否已存在
        term_exists = False
        if hasattr(context, 'business_terms') and context.business_terms and term_name in context.business_terms:
            term_exists = True
        elif hasattr(context, 'database_specific_business_terms') and context.database_specific_business_terms and term_name in context.database_specific_business_terms:
            term_exists = True

        if term_exists:
            return jsonify({
                "success": False,
                "error": f"业务术语 '{term_name}' 已存在"
            }), 409

        # 添加术语
        if term_type == "general":
            if not hasattr(context, 'business_terms') or context.business_terms is None:
                context.business_terms = {}
            context.business_terms[term_name] = term_data
        else:  # database_specific
            if not hasattr(context, 'database_specific_business_terms') or context.database_specific_business_terms is None:
                context.database_specific_business_terms = {}
            context.database_specific_business_terms[term_name] = term_data

        # 保存上下文
        success = global_state.file_manager.save_context(context)
        if success:
            return jsonify({
                "success": True,
                "message": f"业务术语 '{term_name}' 添加成功",
                "term_name": term_name,
                "term_type": term_type
            })
        else:
            return jsonify({
                "success": False,
                "error": "保存业务术语失败"
            }), 500

    except Exception as e:
        print(f"[ERROR] 添加业务术语失败: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/v1/contexts/business-terms', methods=['PUT'])
def update_business_terms():
    """更新业务术语配置"""
    try:
        # 确保全局状态已初始化
        if not global_state.initialized:
            success = global_state.initialize()
            if not success:
                return jsonify({
                    "success": False,
                    "error": "系统初始化失败"
                }), 500

        # 获取请求数据
        data = request.get_json()
        if not data or 'database_path' not in data:
            return jsonify({
                "success": False,
                "error": "缺少database_path参数"
            }), 400

        database_path = data['database_path']
        print(f"[DEBUG] 更新业务术语: {database_path}")

        # 加载现有上下文
        context = global_state.file_manager.load_context(database_path)
        if not context:
            return jsonify({
                "success": False,
                "error": "未找到上下文信息"
            }), 404

        # 更新业务术语
        if 'business_terms' in data:
            if not hasattr(context, 'database_specific_business_terms') or context.database_specific_business_terms is None:
                context.database_specific_business_terms = {}
            context.database_specific_business_terms.update(data['business_terms'])

        # 保存上下文
        success = global_state.file_manager.save_context(context)
        if success:
            return jsonify({
                "success": True,
                "message": "业务术语更新成功",
                "updated_terms": list(data.get('business_terms', {}).keys())
            })
        else:
            return jsonify({
                "success": False,
                "error": "保存业务术语失败"
            }), 500

    except Exception as e:
        print(f"[ERROR] 更新业务术语失败: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/v1/contexts/business-terms/<term_name>', methods=['PUT'])
def update_business_term(term_name):
    """更新单个业务术语"""
    try:
        # 确保全局状态已初始化
        if not global_state.initialized:
            success = global_state.initialize()
            if not success:
                return jsonify({
                    "success": False,
                    "error": "系统初始化失败"
                }), 500

        # 获取请求数据
        data = request.get_json()
        if not data or 'database_path' not in data:
            return jsonify({
                "success": False,
                "error": "缺少database_path参数"
            }), 400

        database_path = data['database_path']
        term_data = data.get('term_data')
        term_type = data.get('term_type', 'database_specific')

        if not term_data:
            return jsonify({
                "success": False,
                "error": "缺少term_data参数"
            }), 400

        print(f"[DEBUG] 更新业务术语: {term_name} ({term_type}) in {database_path}")

        # 加载现有上下文
        context = global_state.file_manager.load_context(database_path)
        if not context:
            return jsonify({
                "success": False,
                "error": "未找到上下文信息"
            }), 404

        # 查找并更新术语
        term_found = False

        if term_type == "general":
            if hasattr(context, 'business_terms') and context.business_terms and term_name in context.business_terms:
                context.business_terms[term_name] = term_data
                term_found = True
        else:  # database_specific
            if hasattr(context, 'database_specific_business_terms') and context.database_specific_business_terms and term_name in context.database_specific_business_terms:
                context.database_specific_business_terms[term_name] = term_data
                term_found = True

        if not term_found:
            return jsonify({
                "success": False,
                "error": f"未找到业务术语: {term_name}"
            }), 404

        # 保存上下文
        success = global_state.file_manager.save_context(context)
        if success:
            return jsonify({
                "success": True,
                "message": f"业务术语 '{term_name}' 更新成功",
                "term_name": term_name,
                "term_type": term_type
            })
        else:
            return jsonify({
                "success": False,
                "error": "保存业务术语失败"
            }), 500

    except Exception as e:
        print(f"[ERROR] 更新业务术语失败: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/v1/contexts/business-terms/<term_name>', methods=['DELETE'])
def delete_business_term(term_name):
    """删除单个业务术语"""
    try:
        # 确保全局状态已初始化
        if not global_state.initialized:
            success = global_state.initialize()
            if not success:
                return jsonify({
                    "success": False,
                    "error": "系统初始化失败"
                }), 500

        # 获取请求参数
        database_path = request.args.get('database_path')
        if not database_path:
            return jsonify({
                "success": False,
                "error": "缺少database_path参数"
            }), 400

        print(f"[DEBUG] 删除业务术语: {term_name} in {database_path}")

        # 加载现有上下文
        context = global_state.file_manager.load_context(database_path)
        if not context:
            return jsonify({
                "success": False,
                "error": "未找到上下文信息"
            }), 404

        # 查找并删除术语
        term_found = False
        term_type = None

        # 先在通用业务术语中查找
        if hasattr(context, 'business_terms') and context.business_terms and term_name in context.business_terms:
            del context.business_terms[term_name]
            term_found = True
            term_type = "general"

        # 再在数据库特定业务术语中查找
        elif hasattr(context, 'database_specific_business_terms') and context.database_specific_business_terms and term_name in context.database_specific_business_terms:
            del context.database_specific_business_terms[term_name]
            term_found = True
            term_type = "database_specific"

        if not term_found:
            return jsonify({
                "success": False,
                "error": f"未找到业务术语: {term_name}"
            }), 404

        # 保存上下文
        success = global_state.file_manager.save_context(context)
        if success:
            return jsonify({
                "success": True,
                "message": f"业务术语 '{term_name}' 删除成功",
                "term_name": term_name,
                "term_type": term_type
            })
        else:
            return jsonify({
                "success": False,
                "error": "保存上下文失败"
            }), 500

    except Exception as e:
        print(f"[ERROR] 删除业务术语失败: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/v1/contexts/field-mappings', methods=['PUT'])
def update_field_mappings():
    """更新字段映射配置"""
    try:
        # 确保全局状态已初始化
        if not global_state.initialized:
            success = global_state.initialize()
            if not success:
                return jsonify({
                    "success": False,
                    "error": "系统初始化失败"
                }), 500

        # 获取请求数据
        data = request.get_json()
        if not data or 'database_path' not in data:
            return jsonify({
                "success": False,
                "error": "缺少database_path参数"
            }), 400

        database_path = data['database_path']
        print(f"[DEBUG] 更新字段映射: {database_path}")

        # 加载现有上下文
        context = global_state.file_manager.load_context(database_path)
        if not context:
            return jsonify({
                "success": False,
                "error": "未找到上下文信息"
            }), 404

        # 更新字段映射
        if 'field_mappings' in data:
            if not hasattr(context, 'database_specific_field_mappings') or context.database_specific_field_mappings is None:
                context.database_specific_field_mappings = {}
            context.database_specific_field_mappings.update(data['field_mappings'])

        # 保存上下文
        success = global_state.file_manager.save_context(context)
        if success:
            return jsonify({
                "success": True,
                "message": "字段映射更新成功",
                "updated_fields": list(data.get('field_mappings', {}).keys())
            })
        else:
            return jsonify({
                "success": False,
                "error": "保存字段映射失败"
            }), 500

    except Exception as e:
        print(f"[ERROR] 更新字段映射失败: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/v1/contexts/query-scopes', methods=['PUT'])
def update_query_scopes():
    """更新查询范围规则配置"""
    try:
        # 确保全局状态已初始化
        if not global_state.initialized:
            success = global_state.initialize()
            if not success:
                return jsonify({
                    "success": False,
                    "error": "系统初始化失败"
                }), 500

        # 获取请求数据
        data = request.get_json()
        if not data or 'database_path' not in data:
            return jsonify({
                "success": False,
                "error": "缺少database_path参数"
            }), 400

        database_path = data['database_path']
        print(f"[DEBUG] 更新查询范围规则: {database_path}")

        # 加载现有上下文
        context = global_state.file_manager.load_context(database_path)
        if not context:
            return jsonify({
                "success": False,
                "error": "未找到上下文信息"
            }), 404

        # 更新查询范围规则
        if 'query_scopes' in data:
            if not hasattr(context, 'database_specific_query_scopes') or context.database_specific_query_scopes is None:
                context.database_specific_query_scopes = []
            context.database_specific_query_scopes = data['query_scopes']

        # 保存上下文
        success = global_state.file_manager.save_context(context)
        if success:
            return jsonify({
                "success": True,
                "message": "查询范围规则更新成功",
                "updated_scopes_count": len(data.get('query_scopes', []))
            })
        else:
            return jsonify({
                "success": False,
                "error": "保存查询范围规则失败"
            }), 500

    except Exception as e:
        print(f"[ERROR] 更新查询范围规则失败: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

# 🔥 新增：元数据感知API端点
@app.route('/api/v1/metadata/info', methods=['GET'])
def get_metadata_info():
    """获取当前数据库的元数据信息"""
    try:
        if not global_state.initialized:
            return jsonify({
                "success": False,
                "error": "系统未初始化"
            }), 500

        if not global_state.current_database:
            return jsonify({
                "success": False,
                "error": "没有选择数据库"
            }), 400

        response = {
            "success": True,
            "current_database": global_state.current_database,
            "metadata_enhanced": global_state.metadata_enhanced
        }

        # 如果是元数据增强的，提供详细信息
        if (global_state.metadata_enhanced and
            hasattr(global_state, 'metadata_aware_config') and
            global_state.metadata_aware_config and
            hasattr(global_state.metadata_aware_config, 'is_metadata_enhanced') and
            global_state.metadata_aware_config.is_metadata_enhanced()):
            response.update({
                "metadata_details": {
                    "business_terms_count": len(global_state.metadata_aware_config.business_terms),
                    "field_mappings_count": len(global_state.metadata_aware_config.field_mappings),
                    "data_dictionary_tables": len(getattr(global_state.metadata_aware_config, 'data_dictionary_cache', {})),
                    "query_rules_count": len(global_state.metadata_aware_config.query_scope_rules)
                }
            })

        return jsonify(response)

    except Exception as e:
        print(f"[ERROR] 获取元数据信息失败: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/v1/metadata/field-suggestions', methods=['POST'])
def get_field_suggestions():
    """获取字段建议"""
    try:
        if not hasattr(global_state, 'metadata_enhancer') or not global_state.metadata_enhancer:
            return jsonify({
                "success": False,
                "error": "元数据增强器不可用"
            }), 400

        data = request.get_json()
        if not data or 'partial_field_name' not in data:
            return jsonify({
                "success": False,
                "error": "缺少partial_field_name参数"
            }), 400

        partial_field_name = data['partial_field_name']
        suggestions = global_state.metadata_enhancer.get_field_suggestions(partial_field_name)

        return jsonify({
            "success": True,
            "suggestions": suggestions
        })

    except Exception as e:
        print(f"[ERROR] 获取字段建议失败: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

def cleanup_on_exit():
    """优雅关闭处理"""
    print("\n🔄 正在优雅关闭服务器...")
    try:
        if hasattr(global_state, 'cleanup'):
            global_state.cleanup()
        print("✅ 服务器已安全关闭")
    except Exception as e:
        print(f"⚠️ 关闭时出现警告: {e}")

def signal_handler(signum, frame):
    """信号处理器"""
    print(f"\n📡 接收到信号 {signum}，准备关闭服务器...")
    cleanup_on_exit()
    sys.exit(0)

if __name__ == '__main__':
    print("🚀 DataProxy API 服务器启动中...")

    # 简化状态显示
    llm_status = "✅" if os.getenv('DEEPSEEK_API_KEY') else "❌"
    print(f"📊 LLM智能查询: {llm_status} | 可视化: ✅ | 数据分析: ✅")

    # 注册优雅关闭处理
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    atexit.register(cleanup_on_exit)

    # 初始化全局状态
    success = global_state.initialize()
    if success:
        debug_mode = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
        port = int(os.getenv('FLASK_PORT', '8000'))
        print(f"🌐 服务器运行在: http://localhost:{port}")
        print(f"📊 数据库: {global_state.current_database or '未连接'}")

        try:
            app.run(host='0.0.0.0', port=port, debug=debug_mode)
        except OSError as e:
            if "Address already in use" in str(e):
                print(f"❌ 端口 {port} 已被占用，尝试使用端口 {port + 1}")
                try:
                    app.run(host='0.0.0.0', port=port + 1, debug=debug_mode)
                except Exception as e2:
                    print(f"❌ 服务器启动失败: {e2}")
                    sys.exit(1)
            else:
                print(f"❌ 服务器启动失败: {e}")
                sys.exit(1)


