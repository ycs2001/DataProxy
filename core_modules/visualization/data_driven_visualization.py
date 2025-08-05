#!/usr/bin/env python3
"""
数据驱动的可视化判断模块

基于实际查询结果数据的特征进行可视化决策，而不是基于查询文本的预判断。
"""

import re
from typing import Dict, Any, List, Optional, Tuple
import pandas as pd


class DataDrivenVisualizationDecision:
    """数据驱动的可视化决策器"""
    
    def __init__(self):
        self.analysis_keywords = [
            '分析', '对比', '统计', '趋势', '分布', '排名', '占比', '比较',
            '汇总', '总计', '合计', '平均', '最大', '最小', '各', '每'
        ]
        
        self.visualization_patterns = [
            r'统计.*分类',
            r'统计.*分布', 
            r'统计.*结果',
            r'分析.*占比',
            r'各.*情况',
            r'.*分类.*结果',
            r'.*分布.*情况',
            r'.*排名.*',
            r'.*对比.*',
            r'.*比较.*'
        ]
    
    def should_create_visualization(self, 
                                  data: List[Dict[str, Any]], 
                                  query: str,
                                  original_query: str = None) -> Tuple[bool, str, Dict[str, Any]]:
        """
        基于数据特征判断是否需要创建可视化
        
        Args:
            data: 查询结果数据
            query: 当前查询文本
            original_query: 原始用户查询（可选）
            
        Returns:
            Tuple[bool, str, Dict]: (是否需要可视化, 判断原因, 可视化配置建议)
        """
        print(f"[DEBUG] 数据驱动可视化判断: 开始分析")
        print(f"  - 数据量: {len(data) if data else 0}")
        print(f"  - 查询: {query[:50]}...")
        
        # 1. 基础数据检查
        if not data or len(data) == 0:
            return False, "无数据返回", {}
        
        # 2. 数据量检查 - 降低阈值，3条及以上考虑可视化
        if len(data) < 3:
            return False, f"数据量较少({len(data)}条)，适合直接查看", {}
        
        # 3. 数据结构分析
        data_analysis = self._analyze_data_structure(data)
        print(f"[DEBUG] 数据结构分析: {data_analysis}")
        
        # 4. 查询语义分析
        semantic_analysis = self._analyze_query_semantics(query, original_query)
        print(f"[DEBUG] 语义分析: {semantic_analysis}")
        
        # 5. 综合判断
        decision_result = self._make_visualization_decision(
            data, data_analysis, semantic_analysis, query
        )
        
        return decision_result
    
    def _analyze_data_structure(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """分析数据结构特征"""
        if not data:
            return {}
        
        sample_record = data[0]
        analysis = {
            'total_rows': len(data),
            'total_columns': len(sample_record) if isinstance(sample_record, dict) else 0,
            'numeric_columns': [],
            'categorical_columns': [],
            'date_columns': [],
            'id_columns': [],
            'has_aggregation_data': False,
            'data_distribution': {}
        }
        
        if isinstance(sample_record, dict):
            for key, value in sample_record.items():
                key_lower = key.lower()
                
                # 识别ID类字段
                if any(id_word in key_lower for id_word in ['id', 'no', '编号', '号码']):
                    analysis['id_columns'].append(key)
                # 识别数值字段
                elif isinstance(value, (int, float)) and key_lower not in ['id', 'no', '编号']:
                    analysis['numeric_columns'].append(key)
                    # 检查是否是聚合数据
                    if any(agg_word in key_lower for agg_word in ['总', '计', '数量', '笔数', '余额', '金额']):
                        analysis['has_aggregation_data'] = True
                # 识别分类字段
                elif isinstance(value, str):
                    analysis['categorical_columns'].append(key)
                    # 检查分类字段的唯一值数量
                    unique_values = set(str(record.get(key, '')) for record in data)
                    analysis['data_distribution'][key] = {
                        'unique_count': len(unique_values),
                        'values': list(unique_values)[:10]  # 只保存前10个值作为示例
                    }
        
        return analysis
    
    def _analyze_query_semantics(self, query: str, original_query: str = None) -> Dict[str, Any]:
        """分析查询语义"""
        # 使用原始查询优先，如果没有则使用当前查询
        target_query = original_query or query
        query_lower = target_query.lower()
        
        analysis = {
            'has_analysis_intent': False,
            'has_statistical_intent': False,
            'has_comparison_intent': False,
            'has_distribution_intent': False,
            'matched_patterns': [],
            'analysis_keywords_found': [],
            'visualization_score': 0
        }
        
        # 检查分析意图关键词
        for keyword in self.analysis_keywords:
            if keyword in query_lower:
                analysis['analysis_keywords_found'].append(keyword)
                analysis['visualization_score'] += 1
        
        # 检查特定模式
        for pattern in self.visualization_patterns:
            if re.search(pattern, target_query):
                analysis['matched_patterns'].append(pattern)
                analysis['visualization_score'] += 2
        
        # 设置意图标志
        analysis['has_analysis_intent'] = len(analysis['analysis_keywords_found']) > 0
        analysis['has_statistical_intent'] = any(kw in query_lower for kw in ['统计', '汇总', '总计', '合计'])
        analysis['has_comparison_intent'] = any(kw in query_lower for kw in ['对比', '比较', '各'])
        analysis['has_distribution_intent'] = any(kw in query_lower for kw in ['分布', '分类', '占比'])
        
        return analysis

    def _make_visualization_decision(self,
                                   data: List[Dict[str, Any]],
                                   data_analysis: Dict[str, Any],
                                   semantic_analysis: Dict[str, Any],
                                   query: str) -> Tuple[bool, str, Dict[str, Any]]:
        """综合判断是否需要可视化"""

        reasons = []
        viz_config = {}
        score = 0

        # 数据量评分
        data_count = len(data)
        if data_count >= 3:
            score += 1
            reasons.append(f"数据量适中({data_count}条)")

        # 数据结构评分
        numeric_cols = data_analysis.get('numeric_columns', [])
        categorical_cols = data_analysis.get('categorical_columns', [])

        if len(numeric_cols) > 0 and len(categorical_cols) > 0:
            score += 2
            reasons.append("包含数值和分类字段，适合图表展示")

            # 建议图表类型
            if data_analysis.get('has_aggregation_data'):
                if len(categorical_cols) == 1 and len(numeric_cols) >= 1:
                    viz_config['suggested_chart_types'] = ['pie', 'bar']
                    viz_config['primary_suggestion'] = 'pie'
                else:
                    viz_config['suggested_chart_types'] = ['bar', 'line']
                    viz_config['primary_suggestion'] = 'bar'

        # 语义分析评分
        if semantic_analysis['has_statistical_intent']:
            score += 2
            reasons.append("查询具有统计分析意图")

        if semantic_analysis['has_distribution_intent']:
            score += 2
            reasons.append("查询涉及分布或分类分析")
            viz_config['primary_suggestion'] = 'pie'

        if semantic_analysis['matched_patterns']:
            score += 1
            reasons.append("匹配可视化模式")

        # 特殊强制可视化场景
        force_viz_patterns = [
            r'统计.*分类.*结果',
            r'统计.*分布',
            r'各.*占比',
            r'.*分类.*统计'
        ]

        for pattern in force_viz_patterns:
            if re.search(pattern, query):
                score += 3
                reasons.append("强制可视化场景")
                break

        # 最终决策
        should_visualize = score >= 3

        if should_visualize:
            reason_text = "，".join(reasons)
            print(f"[DEBUG] 可视化决策: 需要可视化 (评分: {score}) - {reason_text}")
        else:
            reason_text = f"评分不足({score}/3)，数据更适合表格展示"
            print(f"[DEBUG] 可视化决策: 不需要可视化 - {reason_text}")

        return should_visualize, reason_text, viz_config


# 全局实例
_visualization_decision_engine = None

def get_visualization_decision_engine() -> DataDrivenVisualizationDecision:
    """获取全局可视化决策引擎实例"""
    global _visualization_decision_engine
    if _visualization_decision_engine is None:
        _visualization_decision_engine = DataDrivenVisualizationDecision()
    return _visualization_decision_engine
