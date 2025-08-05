#!/usr/bin/env python3
"""
Streamlit Agent核心模块
从langchain_streamlit_app.py提取的成熟LangChain Agent实现
"""

import os
import time
from typing import Dict, Any, List
from dataclasses import dataclass
from datetime import datetime

# LangChain核心导入
try:
    from langchain.agents import AgentExecutor, create_openai_tools_agent
    from langchain.tools import BaseTool
    from langchain.schema import AgentAction, AgentFinish
    from langchain.memory import ConversationBufferMemory
    from langchain.callbacks.base import BaseCallbackHandler
    from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
    from langchain_openai import ChatOpenAI
    from langchain.schema.messages import HumanMessage, AIMessage
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False

# 导入模块化的工具
try:
    from ..data_processing.query_analyzer import QueryAnalysisTool
    from .tool_integrations import NL2SQLTool
except ImportError:
    # 如果相对导入失败，尝试绝对导入
    try:
        from core_modules.data_processing.query_analyzer import QueryAnalysisTool
        from core_modules.agent.tool_integrations import NL2SQLTool
    except ImportError:
        # 最后尝试直接导入
        QueryAnalysisTool = None
        NL2SQLTool = None


@dataclass
class ToolCallResult:
    """工具调用结果"""
    tool_name: str
    input_args: Dict[str, Any]
    output: Any
    success: bool
    error: str = None
    execution_time: float = 0.0


class StreamlitCallbackHandler(BaseCallbackHandler):
    """Streamlit专用回调处理器"""
    
    def __init__(self, container=None):
        self.container = container
        self.tool_calls = []
        self.current_tool = None
        self.tool_start_time = None
    
    def on_tool_start(self, serialized: Dict[str, Any], input_str: str, **kwargs) -> None:
        """工具开始时的回调"""
        tool_name = serialized.get('name', 'unknown_tool')
        self.current_tool = {
            'tool_name': tool_name,
            'input_args': {'input': input_str},
            'start_time': time.time()
        }
        
        if self.container:
            with self.container:
                try:
                    import streamlit as st
                    st.info(f"🔧 正在执行工具: {tool_name}")
                except:
                    pass
    
    def on_tool_end(self, output: str, **kwargs) -> None:
        """工具结束时的回调"""
        if self.current_tool:
            self.current_tool['output'] = output
            self.current_tool['execution_time'] = time.time() - self.current_tool['start_time']
            self.tool_calls.append(self.current_tool)
            
            if self.container:
                with self.container:
                    try:
                        import streamlit as st
                        st.success(f"✅ 工具执行完成: {self.current_tool['tool_name']}")
                    except:
                        pass
    
    def on_tool_error(self, error: Exception, **kwargs) -> None:
        """工具错误时的回调"""
        if self.current_tool:
            self.current_tool['output'] = str(error)
            self.current_tool['error'] = str(error)
            self.tool_calls.append(self.current_tool)
            
            if self.container:
                with self.container:
                    try:
                        import streamlit as st
                        st.error(f"❌ 工具执行失败: {self.current_tool['tool_name']} - {error}")
                    except:
                        pass
    
    def on_agent_action(self, action: AgentAction, **kwargs) -> None:
        """Agent执行动作时的回调"""
        print(f"[DEBUG] 回调被触发！工具: {action.tool}, 输入: {action.tool_input}")

        # 收集工具调用信息
        self.tool_calls.append({
            'tool_name': action.tool,
            'input_args': action.tool_input,
            'log': action.log,
            'action': action
        })

        if self.container:
            with self.container:
                try:
                    import streamlit as st
                    st.markdown(f"""
                    <div class="agent-thinking">
                        🤔 <strong>Agent思考:</strong> {action.log}<br>
                        🛠️ <strong>调用工具:</strong> {action.tool}<br>
                        📝 <strong>工具输入:</strong> {action.tool_input}
                    </div>
                    """, unsafe_allow_html=True)
                except:
                    pass
    
    def on_agent_finish(self, finish: AgentFinish, **kwargs) -> None:
        """Agent完成时的回调"""
        if self.container:
            with self.container:
                try:
                    import streamlit as st
                    st.success(f"✅ Agent完成: {finish.return_values.get('output', '')}")
                except:
                    pass


class LangChainDataAnalysisAgent:
    """基于LangChain的数据分析Agent - 重构版"""
    
    def __init__(self):
        self.tools = []
        self.memory = ConversationBufferMemory(
            memory_key="chat_history",
            return_messages=True
        )
        self.agent_executor = None
        self.setup_tools()
        self.setup_agent()
    
    def setup_tools(self):
        """设置工具"""
        print(f"[DEBUG] ===== 开始初始化工具 =====")

        try:
            # 使用模块化的工具
            query_tool = QueryAnalysisTool()
            print(f"[DEBUG] QueryAnalysisTool初始化成功: {type(query_tool)}")

            nl2sql_tool = NL2SQLTool()
            print(f"[DEBUG] NL2SQLTool初始化成功: {type(nl2sql_tool)}")

            self.tools = [query_tool, nl2sql_tool]
            print(f"[DEBUG] 所有工具初始化完成，共{len(self.tools)}个工具")

        except Exception as e:
            print(f"[DEBUG] 工具初始化失败: {e}")
            import traceback
            traceback.print_exc()
    
    def setup_agent(self):
        """设置Agent"""
        if not LANGCHAIN_AVAILABLE:
            return
            
        try:
            # 使用DeepSeek作为LLM
            llm = ChatOpenAI(
                model="deepseek-chat",
                openai_api_key=os.getenv('DEEPSEEK_API_KEY'),
                openai_api_base="https://api.deepseek.com/v1",
                temperature=0.1,
                max_tokens=32000,  # 充分利用DeepSeek的128K上下文
                request_timeout=180,  # 增加超时时间
                max_retries=3,
                timeout=180
            )
            
            # 创建提示模板
            prompt = self._create_agent_prompt()
            
            # 创建Agent
            agent = create_openai_tools_agent(llm, self.tools, prompt)
            
            # 创建Agent执行器
            self.agent_executor = AgentExecutor(
                agent=agent,
                tools=self.tools,
                memory=self.memory,
                verbose=True,
                max_iterations=5,
                handle_parsing_errors=True,
                return_intermediate_steps=True,
                early_stopping_method="force",  # 🔥 修复：使用支持的early_stopping_method
                max_execution_time=180  # 设置3分钟超时
            )
            
        except Exception as e:
            print(f"❌ Agent初始化失败: {e}")
    
    def _create_agent_prompt(self):
        """创建Agent提示模板"""
        return ChatPromptTemplate.from_messages([
            ("system", """你是一个银行数据分析专家。你必须始终执行完整的数据分析流程，为用户提供具体的数据结果和可视化图表。

🚀 阶段1修复：工具完成状态理解
每个工具执行后会返回包含以下字段的结果：
- task_completed: True/False (表示工具是否完成)
- next_action: 建议的下一步操作
- summary: 执行结果摘要

可用工具：
- analyze_query: 分析用户查询，提取分析意图、实体、维度等信息，并制定查询计划
- nl2sql_query: 使用优化的NL2SQL Pipeline，专为agent调用设计，平均响应时间5-15秒
- create_visualization: 调用专门的可视化Agent，智能分析数据特征并自动选择最佳图表类型

🎯 核心要求：
- 对于数据分析查询，必须执行：analyze_query → nl2sql_query → create_visualization
- 每个工具只调用一次！如果工具返回task_completed=True，不要重复调用
- 根据工具返回的next_action字段决定下一步操作

📋 标准工作流程：
1. 调用 analyze_query 工具进行查询分析
   - 如果返回task_completed=True且next_action="nl2sql_query"，进入步骤2
2. 调用 nl2sql_query 工具执行查询
   - 如果返回task_completed=True且有数据，进入步骤3
   - 如果返回task_completed=True但无数据，直接总结结果
3. 调用 create_visualization 工具创建图表
   - 如果返回task_completed=True，完成整个流程

⚠️ 重要执行原则：
- 严格按照task_completed字段判断工具是否完成
- 不要重复调用已经完成的工具
- 如果工具返回success=False但task_completed=True，说明该步骤已完成（可能是预期的失败）
- 每个工具最多调用一次

请严格按照这个流程和完成状态判断逻辑处理用户的查询。"""),
            MessagesPlaceholder(variable_name="chat_history"),
            ("human", "{input}"),
            MessagesPlaceholder(variable_name="agent_scratchpad")
        ])
    
    async def analyze(self, user_query: str, callback_handler=None) -> Dict[str, Any]:
        """执行分析"""
        if not self.agent_executor:
            return {
                'success': False,
                'error': 'Agent未正确初始化',
                'agent_response': '',
                'tool_calls': []
            }
        
        try:
            start_time = time.time()
            
            # 执行Agent
            if callback_handler:
                config = {"callbacks": [callback_handler]}
                result = await self.agent_executor.ainvoke(
                    {"input": user_query},
                    config=config
                )
            else:
                result = await self.agent_executor.ainvoke(
                    {"input": user_query}
                )
            
            execution_time = time.time() - start_time
            
            # 从回调处理器获取工具调用信息
            tool_calls_from_callback = []
            if callback_handler and hasattr(callback_handler, 'tool_calls'):
                for call_info in callback_handler.tool_calls:
                    # 🚀 阶段1修复：改进工具输出解析逻辑
                    output = call_info.get('output', "工具执行完成")

                    if isinstance(output, dict):
                        parsed_output = output
                        # 确保有完成标志
                        if 'task_completed' not in parsed_output:
                            parsed_output['task_completed'] = True
                    elif isinstance(output, str):
                        try:
                            import json
                            parsed_output = json.loads(output)
                            # 确保有完成标志
                            if 'task_completed' not in parsed_output:
                                parsed_output['task_completed'] = True
                        except:
                            # 如果无法解析，创建标准格式
                            parsed_output = {
                                "result": output,
                                "task_completed": True,
                                "summary": f"工具执行完成: {output[:100]}"
                            }
                    else:
                        parsed_output = {
                            "result": str(output),
                            "task_completed": True,
                            "summary": f"工具执行完成: {str(output)[:100]}"
                        }

                    # 🚀 阶段1修复：添加详细的调试日志
                    print(f"[DEBUG] 工具调用完成: {call_info['tool_name']}")
                    print(f"[DEBUG] 任务完成状态: {parsed_output.get('task_completed', 'Unknown')}")
                    print(f"[DEBUG] 建议下一步: {parsed_output.get('next_action', 'Unknown')}")
                    print(f"[DEBUG] 执行摘要: {parsed_output.get('summary', 'No summary')}")

                    tool_calls_from_callback.append(ToolCallResult(
                        tool_name=call_info['tool_name'],
                        input_args=call_info['input_args'],
                        output=parsed_output,
                        success=parsed_output.get('success', True),
                        execution_time=parsed_output.get('execution_time', 0.0)
                    ))

            return {
                'success': True,
                'agent_response': result.get('output', ''),
                'tool_calls': tool_calls_from_callback or self.extract_tool_calls(result),
                'execution_time': execution_time,
                'chat_history': self.memory.chat_memory.messages
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'agent_response': '',
                'tool_calls': []
            }
    
    def extract_tool_calls(self, result: Dict[str, Any]) -> List[ToolCallResult]:
        """从结果中提取工具调用信息"""
        tool_calls = []

        if 'intermediate_steps' in result:
            for step in result['intermediate_steps']:
                if isinstance(step, tuple) and len(step) >= 2:
                    action, observation = step
                    tool_calls.append(ToolCallResult(
                        tool_name=getattr(action, 'tool', 'unknown_tool'),
                        input_args=getattr(action, 'tool_input', {}),
                        output=observation,
                        success=True,
                        execution_time=0.0
                    ))

        return tool_calls
    
    def get_health_status(self) -> Dict[str, Any]:
        """获取Agent健康状态"""
        return {
            'agent_available': self.agent_executor is not None,
            'tools_count': len(self.tools),
            'langchain_available': LANGCHAIN_AVAILABLE,
            'memory_messages': len(self.memory.chat_memory.messages) if self.memory else 0
        }
