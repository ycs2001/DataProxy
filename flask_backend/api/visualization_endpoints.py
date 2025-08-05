#!/usr/bin/env python3
"""
可视化API端点 - 支持前端数据分析模块
"""

import time
import json
from flask import Blueprint, jsonify, request
from .error_handlers import APIErrorHandler

def create_visualization_blueprint():
    """创建可视化API蓝图"""
    
    viz_bp = Blueprint('visualization', __name__, url_prefix='/api/v1')
    
    # 内存中存储可视化图表（生产环境应使用数据库）
    visualizations_store = {}
    
    @viz_bp.route('/visualize', methods=['POST'])
    def create_chart():
        """创建数据可视化图表"""
        try:
            data = request.get_json()
            
            # 验证必需参数
            required_fields = ['data', 'chart_type']
            for field in required_fields:
                if field not in data:
                    return APIErrorHandler.handle_validation_error(
                        f'{field} is required', field
                    )
            
            chart_data = data['data']
            chart_type = data['chart_type']
            chart_title = data.get('title', 'Untitled Chart')
            chart_config = data.get('config', {})
            
            # 生成图表ID
            chart_id = f"chart_{int(time.time() * 1000)}"
            
            # 创建图表对象
            chart_object = {
                'id': chart_id,
                'title': chart_title,
                'type': chart_type,
                'data': chart_data,
                'config': chart_config,
                'created_time': time.time(),
                'status': 'created'
            }
            
            # 根据图表类型生成相应的配置
            if chart_type == 'bar':
                chart_object['plotly_config'] = {
                    'type': 'bar',
                    'x': chart_config.get('x_field'),
                    'y': chart_config.get('y_field'),
                    'title': chart_title
                }
            elif chart_type == 'pie':
                chart_object['plotly_config'] = {
                    'type': 'pie',
                    'values': chart_config.get('values_field'),
                    'labels': chart_config.get('labels_field'),
                    'title': chart_title
                }
            elif chart_type == 'line':
                chart_object['plotly_config'] = {
                    'type': 'scatter',
                    'mode': 'lines',
                    'x': chart_config.get('x_field'),
                    'y': chart_config.get('y_field'),
                    'title': chart_title
                }
            
            # 存储图表
            visualizations_store[chart_id] = chart_object
            
            return jsonify({
                'success': True,
                'message': 'Chart created successfully',
                'chart': chart_object
            })
            
        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)
    
    @viz_bp.route('/visualizations', methods=['GET'])
    def get_visualizations():
        """获取已创建的可视化图表列表"""
        try:
            # 获取查询参数
            limit = request.args.get('limit', 50, type=int)
            offset = request.args.get('offset', 0, type=int)
            chart_type = request.args.get('type', '')
            
            # 过滤图表
            charts = list(visualizations_store.values())
            
            if chart_type:
                charts = [chart for chart in charts if chart['type'] == chart_type]
            
            # 按创建时间排序
            charts.sort(key=lambda x: x['created_time'], reverse=True)
            
            # 分页
            total_count = len(charts)
            paginated_charts = charts[offset:offset + limit]
            
            return jsonify({
                'success': True,
                'visualizations': paginated_charts,
                'pagination': {
                    'total_count': total_count,
                    'limit': limit,
                    'offset': offset,
                    'has_more': offset + limit < total_count
                }
            })
            
        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)
    
    @viz_bp.route('/visualizations/<chart_id>', methods=['GET'])
    def get_visualization(chart_id):
        """获取指定图表详情"""
        try:
            if chart_id not in visualizations_store:
                return APIErrorHandler.handle_validation_error(
                    'Chart not found', 'chart_id'
                )
            
            chart = visualizations_store[chart_id]
            
            return jsonify({
                'success': True,
                'chart': chart
            })
            
        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)
    
    @viz_bp.route('/visualizations/<chart_id>', methods=['PUT'])
    def update_visualization(chart_id):
        """更新图表配置"""
        try:
            if chart_id not in visualizations_store:
                return APIErrorHandler.handle_validation_error(
                    'Chart not found', 'chart_id'
                )
            
            update_data = request.get_json()
            chart = visualizations_store[chart_id]
            
            # 更新图表属性
            if 'title' in update_data:
                chart['title'] = update_data['title']
            if 'config' in update_data:
                chart['config'].update(update_data['config'])
            if 'data' in update_data:
                chart['data'] = update_data['data']
            
            chart['modified_time'] = time.time()
            
            return jsonify({
                'success': True,
                'message': 'Chart updated successfully',
                'chart': chart
            })
            
        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)
    
    @viz_bp.route('/visualizations/<chart_id>', methods=['DELETE'])
    def delete_visualization(chart_id):
        """删除图表"""
        try:
            if chart_id not in visualizations_store:
                return APIErrorHandler.handle_validation_error(
                    'Chart not found', 'chart_id'
                )
            
            del visualizations_store[chart_id]
            
            return jsonify({
                'success': True,
                'message': 'Chart deleted successfully',
                'chart_id': chart_id
            })
            
        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)
    
    return viz_bp
