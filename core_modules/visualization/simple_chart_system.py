#!/usr/bin/env python3
"""
简化的图表系统
提供基础的图表生成功能，不依赖复杂的外部库
"""

import json
import os
from typing import Dict, Any, List, Optional
import pandas as pd

# 尝试导入Plotly，如果失败则使用基础功能
try:
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.utils import PlotlyJSONEncoder
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False
    print("[INFO] Plotly不可用，使用基础图表功能")


class SimpleChartSystem:
    """简化的图表生成系统"""
    
    def __init__(self):
        self.supported_chart_types = ['bar', 'line', 'pie', 'scatter']
        
    def create_chart(self, data: List[Dict[str, Any]], chart_type: str = 'bar', 
                    title: str = "数据图表", **kwargs) -> Dict[str, Any]:
        """
        创建图表
        
        Args:
            data: 数据列表
            chart_type: 图表类型
            title: 图表标题
            **kwargs: 其他参数
            
        Returns:
            图表结果字典
        """
        try:
            if not PLOTLY_AVAILABLE:
                return self._create_text_chart(data, chart_type, title)
            
            # 转换数据为DataFrame
            df = pd.DataFrame(data)
            if df.empty:
                return self._create_empty_chart(title)
            
            # 根据图表类型创建图表
            if chart_type == 'bar':
                return self._create_bar_chart(df, title, **kwargs)
            elif chart_type == 'line':
                return self._create_line_chart(df, title, **kwargs)
            elif chart_type == 'pie':
                return self._create_pie_chart(df, title, **kwargs)
            elif chart_type == 'scatter':
                return self._create_scatter_chart(df, title, **kwargs)
            else:
                return self._create_bar_chart(df, title, **kwargs)
                
        except Exception as e:
            print(f"[ERROR] 图表创建失败: {e}")
            return self._create_error_chart(str(e))
    
    def _create_bar_chart(self, df: pd.DataFrame, title: str, **kwargs) -> Dict[str, Any]:
        """创建柱状图"""
        try:
            columns = df.columns.tolist()
            if len(columns) < 2:
                return self._create_error_chart("数据列不足")
            
            x_col = kwargs.get('x', columns[0])
            y_col = kwargs.get('y', columns[1])
            
            fig = px.bar(df, x=x_col, y=y_col, title=title)
            fig.update_layout(
                font=dict(size=12),
                height=500,
                showlegend=False
            )
            
            return {
                'success': True,
                'chart_type': 'bar',
                'chart_json': fig.to_json(),
                'title': title,
                'data_points': len(df)
            }
            
        except Exception as e:
            return self._create_error_chart(f"柱状图创建失败: {e}")
    
    def _create_line_chart(self, df: pd.DataFrame, title: str, **kwargs) -> Dict[str, Any]:
        """创建折线图"""
        try:
            columns = df.columns.tolist()
            if len(columns) < 2:
                return self._create_error_chart("数据列不足")
            
            x_col = kwargs.get('x', columns[0])
            y_col = kwargs.get('y', columns[1])
            
            fig = px.line(df, x=x_col, y=y_col, title=title)
            fig.update_layout(
                font=dict(size=12),
                height=500
            )
            
            return {
                'success': True,
                'chart_type': 'line',
                'chart_json': fig.to_json(),
                'title': title,
                'data_points': len(df)
            }
            
        except Exception as e:
            return self._create_error_chart(f"折线图创建失败: {e}")
    
    def _create_pie_chart(self, df: pd.DataFrame, title: str, **kwargs) -> Dict[str, Any]:
        """创建饼图"""
        try:
            columns = df.columns.tolist()
            if len(columns) < 2:
                return self._create_error_chart("数据列不足")
            
            names_col = kwargs.get('names', columns[0])
            values_col = kwargs.get('values', columns[1])
            
            fig = px.pie(df, names=names_col, values=values_col, title=title)
            fig.update_traces(textposition='inside', textinfo='percent+label')
            fig.update_layout(
                font=dict(size=12),
                height=500
            )
            
            return {
                'success': True,
                'chart_type': 'pie',
                'chart_json': fig.to_json(),
                'title': title,
                'data_points': len(df)
            }
            
        except Exception as e:
            return self._create_error_chart(f"饼图创建失败: {e}")
    
    def _create_scatter_chart(self, df: pd.DataFrame, title: str, **kwargs) -> Dict[str, Any]:
        """创建散点图"""
        try:
            columns = df.columns.tolist()
            if len(columns) < 2:
                return self._create_error_chart("数据列不足")
            
            x_col = kwargs.get('x', columns[0])
            y_col = kwargs.get('y', columns[1])
            
            fig = px.scatter(df, x=x_col, y=y_col, title=title)
            fig.update_layout(
                font=dict(size=12),
                height=500
            )
            
            return {
                'success': True,
                'chart_type': 'scatter',
                'chart_json': fig.to_json(),
                'title': title,
                'data_points': len(df)
            }
            
        except Exception as e:
            return self._create_error_chart(f"散点图创建失败: {e}")
    
    def _create_empty_chart(self, title: str) -> Dict[str, Any]:
        """创建空图表"""
        if not PLOTLY_AVAILABLE:
            return self._create_text_chart([], 'empty', title)
        
        try:
            fig = go.Figure()
            fig.add_annotation(
                text="暂无数据",
                x=0.5, y=0.5,
                showarrow=False,
                font=dict(size=16, color="gray")
            )
            fig.update_layout(title=title, width=800, height=400)
            
            return {
                'success': True,
                'chart_type': 'empty',
                'chart_json': fig.to_json(),
                'title': title,
                'data_points': 0
            }
        except Exception as e:
            return self._create_text_chart([], 'empty', title)
    
    def _create_error_chart(self, error_msg: str) -> Dict[str, Any]:
        """创建错误图表"""
        if not PLOTLY_AVAILABLE:
            return {
                'success': False,
                'chart_type': 'error',
                'error': error_msg,
                'text_output': f"图表生成失败: {error_msg}"
            }
        
        try:
            fig = go.Figure()
            fig.add_annotation(
                text=f"图表生成失败\n{error_msg}",
                x=0.5, y=0.5,
                showarrow=False,
                font=dict(size=14, color="red")
            )
            fig.update_layout(title="错误", width=800, height=400)
            
            return {
                'success': False,
                'chart_type': 'error',
                'chart_json': fig.to_json(),
                'error': error_msg
            }
        except:
            return {
                'success': False,
                'chart_type': 'error',
                'error': error_msg,
                'text_output': f"图表生成失败: {error_msg}"
            }
    
    def _create_text_chart(self, data: List[Dict[str, Any]], chart_type: str, title: str) -> Dict[str, Any]:
        """创建文本格式的图表（当Plotly不可用时）"""
        return {
            'success': True,
            'chart_type': f'text_{chart_type}',
            'title': title,
            'text_output': f"文本图表: {title}\n数据点数: {len(data)}",
            'data_points': len(data),
            'note': 'Plotly不可用，使用文本格式显示'
        }
    
    def save_chart(self, chart_result: Dict[str, Any], output_path: str) -> bool:
        """保存图表到文件"""
        try:
            if chart_result.get('chart_json'):
                # 保存为HTML文件
                html_content = f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <title>{chart_result.get('title', '图表')}</title>
                    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
                </head>
                <body>
                    <div id="chart" style="width:100%;height:500px;"></div>
                    <script>
                        var plotlyData = {chart_result['chart_json']};
                        Plotly.newPlot('chart', plotlyData.data, plotlyData.layout);
                    </script>
                </body>
                </html>
                """
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(html_content)
                return True
            else:
                # 保存为文本文件
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(chart_result.get('text_output', '图表数据'))
                return True
                
        except Exception as e:
            print(f"[ERROR] 图表保存失败: {e}")
            return False
