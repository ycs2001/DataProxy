"""
增强洞察生成器
将统计分析结果与LLM生成的洞察进行深度整合
"""

import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)

class EnhancedInsightsGenerator:
    """增强洞察生成器 - 整合统计分析与业务洞察"""
    
    def __init__(self):
        """初始化增强洞察生成器"""
        self.business_thresholds = {
            '个人贷款': {
                'excellent': 10e8,  # 10亿以上为优秀
                'good': 5e8,        # 5亿以上为良好
                'poor': 1e8         # 1亿以下需关注
            },
            '对公贷款': {
                'excellent': 50e8,
                'good': 20e8,
                'poor': 5e8
            }
        }
        
        self.statistical_significance = {
            'high_variance_cv': 1.0,      # 变异系数>1.0为高方差
            'outlier_threshold': 2.0,     # 超过2倍标准差为异常值
            'concentration_threshold': 0.5 # 前几名占比>50%为高集中度
        }

    def _is_id_field(self, column_name: str) -> bool:
        """
        判断字段是否为ID类字段，不适合进行业务分析

        Args:
            column_name: 字段名称

        Returns:
            bool: True表示是ID类字段，应该跳过分析
        """
        # ID字段识别模式
        id_field_patterns = [
            'id', 'ID', '编号', '代码', '_no', '_code', '_id',
            'statistics_dt', '数据日期', '统计日期',
            'CF_A', 'CF_B', 'CF_C', 'CF_D', 'CF_E', 'CF_F', 'CF_G', 'CF_H', 'CF_I'
        ]

        # 检查字段名是否包含ID类模式
        column_lower = column_name.lower()
        for pattern in id_field_patterns:
            if pattern.lower() in column_lower or column_name == pattern:
                return True

        # 检查是否为纯编码字段（如CF_开头的编码）
        if column_name.startswith('CF_') and len(column_name) <= 4:
            return True

        # 检查是否以常见ID后缀结尾
        id_suffixes = ['_id', '_no', '_code', '_num', '_number']
        for suffix in id_suffixes:
            if column_name.lower().endswith(suffix):
                return True

        return False

    def _get_smart_institution_identifier(self, record: Dict[str, Any]) -> str:
        """
        智能获取机构标识：优先机构名称，其次客户ID，最后使用记录标识

        Args:
            record: 数据记录

        Returns:
            str: 机构标识（机构名称、客户ID或记录标识）
        """
        # 1. 尝试获取机构名称（多种可能的字段名）
        institution_fields = [
            'host_org_name',      # 实际字段名
            '账务机构名称',        # 中文字段名
            'org_name',           # 简化字段名
            'branch_name',        # 分支机构名
            '机构名称',           # 其他可能的中文名
            'institution_name'    # 英文标准名
        ]

        for field in institution_fields:
            if field in record and record[field]:
                value = str(record[field]).strip()
                # 过滤无效值
                if value.lower() not in ['nan', 'null', 'none', '', '未知', 'unknown']:
                    return f"机构:{value}"

        # 2. 如果没有机构名称，尝试使用客户ID作为标识
        customer_id_fields = [
            'CUST_ID',           # 客户ID
            '客户编号',          # 中文客户编号
            'customer_id',       # 英文客户ID
            'cust_no'           # 简化客户号
        ]

        for field in customer_id_fields:
            if field in record and record[field]:
                value = str(record[field]).strip()
                if value.lower() not in ['nan', 'null', 'none', '']:
                    return f"客户ID:{value}"

        # 3. 最后尝试使用其他标识字段
        identifier_fields = [
            'hold_org_id',       # 机构ID
            'statistics_dt',     # 统计日期
            'open_dt'           # 开户日期
        ]

        for field in identifier_fields:
            if field in record and record[field]:
                value = str(record[field]).strip()
                if value.lower() not in ['nan', 'null', 'none', '']:
                    return f"{field}:{value}"

        # 4. 如果所有字段都无效，返回记录摘要
        available_fields = [k for k, v in record.items() if v is not None and str(v).strip()]
        if available_fields:
            return f"记录(含{len(available_fields)}个字段)"
        else:
            return "数据记录"

    def generate_enhanced_insights(self,
                                 agent_response: str,
                                 original_insights: Optional[str],
                                 original_recommendations: Optional[str],
                                 statistics: Dict[str, Any],
                                 data_tables: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """
        生成增强洞察，整合原始洞察和统计分析

        Args:
            agent_response: Agent的原始响应
            original_insights: 原始洞察文本
            original_recommendations: 原始建议文本
            statistics: 统计分析结果
            data_tables: 原始数据表

        Returns:
            Dict: 增强洞察结果
        """
        try:
            logger.info("开始生成增强洞察")
            logger.info(f"输入参数 - statistics有效: {bool(statistics)}, data_tables有效: {bool(data_tables)}")

            # 1. 分析统计数据，生成数据驱动的洞察
            statistical_insights = self._generate_statistical_insights(statistics, data_tables)
            logger.info(f"统计洞察生成完成: {len(statistical_insights)}个")

            # 2. 解析原始洞察，提取关键信息
            parsed_insights = self._parse_original_insights(original_insights, original_recommendations)
            logger.info(f"原始洞察解析完成: {len(parsed_insights)}个")

            # 3. 将统计数据与洞察进行关联
            enhanced_insights = self._merge_insights_with_statistics(
                parsed_insights, statistical_insights, statistics, data_tables
            )
            logger.info(f"洞察合并完成: {len(enhanced_insights)}个")

            # 4. 生成执行摘要
            executive_summary = self._generate_executive_summary(enhanced_insights, statistics)
            logger.info(f"执行摘要生成完成: {bool(executive_summary)}")

            # 5. 生成可操作的建议
            actionable_recommendations = self._generate_actionable_recommendations(
                enhanced_insights, statistics, data_tables
            )
            logger.info(f"可操作建议生成完成: {len(actionable_recommendations)}个")

            # 6. 创建统计摘要
            statistical_summary = self._create_statistical_summary(statistics)

            # 🔧 重新设计更简洁的数据结构
            result = {
                'summary': self._create_business_summary(enhanced_insights, statistics, data_tables),
                'key_insights': self._create_key_insights(enhanced_insights),
                'recommendations': self._create_simple_recommendations(actionable_recommendations),
                'data_overview': self._create_data_overview(statistics, data_tables),
                'metadata': {
                    'generated_at': datetime.now().isoformat(),
                    'total_insights': len(enhanced_insights),
                    'total_recommendations': len(actionable_recommendations),
                    'analysis_timestamp': datetime.now().isoformat()
                }
            }

            logger.info(f"增强洞察生成完成: {len(enhanced_insights)}个洞察, {len(actionable_recommendations)}个建议")
            return result

        except Exception as e:
            logger.error(f"生成增强洞察失败: {e}", exc_info=True)
            return self._create_fallback_insights(original_insights, original_recommendations)
    
    def _generate_statistical_insights(self, statistics: Dict[str, Any],
                                     data_tables: Dict[str, List[Dict]]) -> List[Dict[str, Any]]:
        """基于统计数据生成洞察"""
        insights = []

        logger.info(f"开始生成统计洞察 - statistics: {bool(statistics)}")

        if not statistics or 'statistics' not in statistics:
            logger.warning("统计数据为空或格式不正确")
            return insights

        logger.info(f"统计数据包含 {len(statistics['statistics'])} 个表")

        for table_name, table_stats in statistics['statistics'].items():
            logger.info(f"处理表: {table_name}")

            if 'column_statistics' not in table_stats:
                logger.warning(f"表 {table_name} 缺少 column_statistics")
                continue

            table_data = data_tables.get(table_name, [])
            logger.info(f"表 {table_name} 有 {len(table_data)} 条数据记录")

            # 分析每个数值列
            for column_name, column_stats in table_stats['column_statistics'].items():
                logger.info(f"分析列: {column_name}")

                if 'error' in column_stats:
                    logger.warning(f"列 {column_name} 有错误: {column_stats['error']}")
                    continue

                # 1. 识别最佳表现者
                if 'max_record' in column_stats and column_stats['max_record']:
                    max_insight = self._create_performance_leader_insight(
                        column_name, column_stats, table_name
                    )
                    if max_insight:
                        insights.append(max_insight)
                        logger.info(f"生成业绩领先者洞察: {max_insight['title']}")

                # 2. 识别数据分布特征
                variance_insight = self._analyze_data_distribution(
                    column_name, column_stats, table_name
                )
                if variance_insight:
                    insights.append(variance_insight)
                    logger.info(f"生成分布特征洞察: {variance_insight['title']}")

                # 3. 识别业务风险
                risk_insight = self._identify_business_risks(
                    column_name, column_stats, table_data, table_name
                )
                if risk_insight:
                    insights.append(risk_insight)
                    logger.info(f"生成业务风险洞察: {risk_insight['title']}")

        logger.info(f"统计洞察生成完成，共生成 {len(insights)} 个洞察")
        return insights
    
    def _create_performance_leader_insight(self, column_name: str,
                                         column_stats: Dict[str, Any],
                                         table_name: str) -> Optional[Dict[str, Any]]:
        """创建业绩领先者洞察"""
        try:
            # 🚨 ID字段过滤：排除不适合分析的字段类型
            if self._is_id_field(column_name):
                logger.info(f"跳过ID类字段分析: {column_name}")
                return None

            max_record = column_stats['max_record']
            max_value = column_stats['max']
            mean_value = column_stats['mean']

            # 🔧 智能获取机构标识：优先机构名称，其次客户ID，最后使用记录标识
            institution_name = self._get_smart_institution_identifier(max_record)

            # 计算相对表现
            vs_average_multiple = max_value / mean_value if mean_value > 0 else 0
            
            return {
                'id': f"leader_{column_name}_{hash(institution_name) % 10000}",
                'type': 'performance_leader',
                'priority': 'high',
                'title': f"{institution_name}在{column_name}方面表现最佳",
                'description': f"{institution_name}以{self._format_currency(max_value)}的{column_name}位居第一",
                'supporting_statistics': {
                    'primary_metric': {
                        'value': max_value,
                        'label': column_name,
                        'format': 'currency',
                        'institution': institution_name
                    },
                    'comparative_metrics': [
                        {
                            'label': 'vs 平均值',
                            'value': vs_average_multiple,
                            'format': 'multiplier'
                        }
                    ]
                },
                'evidence': {
                    'data_source': table_name,
                    'record': max_record,
                    'calculation': f"MAX({column_name})"
                },
                'actions': [
                    {
                        'type': 'view_details',
                        'label': '查看详情',
                        'target': f"table_row:{institution_name}"
                    },
                    {
                        'type': 'analyze_factors',
                        'label': '成功因素分析',
                        'target': f"institution:{institution_name}"
                    }
                ]
            }
        except Exception as e:
            logger.error(f"创建业绩领先者洞察失败: {e}")
            return None
    
    def _analyze_data_distribution(self, column_name: str,
                                 column_stats: Dict[str, Any],
                                 table_name: str) -> Optional[Dict[str, Any]]:
        """分析数据分布特征"""
        try:
            # 🚨 ID字段过滤：排除不适合分析的字段类型
            if self._is_id_field(column_name):
                logger.info(f"跳过ID类字段分布分析: {column_name}")
                return None

            mean_value = column_stats['mean']
            std_value = column_stats['std']

            if mean_value <= 0:
                return None
            
            # 计算变异系数
            cv = std_value / mean_value
            
            if cv > self.statistical_significance['high_variance_cv']:
                return {
                    'id': f"variance_{column_name}_{hash(table_name) % 10000}",
                    'type': 'high_variance',
                    'priority': 'medium',
                    'title': f"{column_name}分布不均，存在较大差异",
                    'description': f"{column_name}的变异系数为{cv:.2f}，表明各机构间差异较大",
                    'supporting_statistics': {
                        'primary_metric': {
                            'value': cv,
                            'label': '变异系数',
                            'format': 'decimal'
                        },
                        'comparative_metrics': [
                            {
                                'label': '标准差',
                                'value': std_value,
                                'format': 'currency'
                            },
                            {
                                'label': '均值',
                                'value': mean_value,
                                'format': 'currency'
                            }
                        ]
                    },
                    'evidence': {
                        'data_source': table_name,
                        'calculation': f"STD({column_name}) / MEAN({column_name})"
                    },
                    'actions': [
                        {
                            'type': 'view_distribution',
                            'label': '查看分布图',
                            'target': f"distribution:{column_name}"
                        },
                        {
                            'type': 'analyze_outliers',
                            'label': '异常值分析',
                            'target': f"outliers:{column_name}"
                        }
                    ]
                }
        except Exception as e:
            logger.error(f"分析数据分布失败: {e}")
            return None
    
    def _identify_business_risks(self, column_name: str,
                               column_stats: Dict[str, Any],
                               table_data: List[Dict],
                               table_name: str) -> Optional[Dict[str, Any]]:
        """识别业务风险"""
        try:
            # 🚨 ID字段过滤：排除不适合分析的字段类型
            if self._is_id_field(column_name):
                logger.info(f"跳过ID类字段风险分析: {column_name}")
                return None

            min_value = column_stats['min']
            mean_value = column_stats['mean']

            # 识别业务类型
            business_type = self._identify_business_type(table_name, column_name)
            thresholds = self.business_thresholds.get(business_type, {})
            
            if not thresholds:
                return None
            
            # 统计低于阈值的机构数量
            poor_threshold = thresholds.get('poor', 0)
            poor_performers = [
                record for record in table_data 
                if record.get(column_name, 0) < poor_threshold
            ]
            
            if len(poor_performers) > 0:
                return {
                    'id': f"risk_{column_name}_{hash(table_name) % 10000}",
                    'type': 'business_risk',
                    'priority': 'high',
                    'title': f"{len(poor_performers)}个机构{column_name}低于预警线",
                    'description': f"有{len(poor_performers)}个机构的{column_name}低于{self._format_currency(poor_threshold)}，需要重点关注",
                    'supporting_statistics': {
                        'primary_metric': {
                            'value': len(poor_performers),
                            'label': '风险机构数量',
                            'format': 'integer'
                        },
                        'comparative_metrics': [
                            {
                                'label': '预警阈值',
                                'value': poor_threshold,
                                'format': 'currency'
                            },
                            {
                                'label': '最低值',
                                'value': min_value,
                                'format': 'currency'
                            }
                        ]
                    },
                    'evidence': {
                        'data_source': table_name,
                        'affected_records': [r.get('账务机构名称', '未知') for r in poor_performers[:5]],
                        'calculation': f"COUNT({column_name} < {poor_threshold})"
                    },
                    'actions': [
                        {
                            'type': 'view_list',
                            'label': '查看风险名单',
                            'target': f"filter:{column_name}<{poor_threshold}"
                        },
                        {
                            'type': 'risk_analysis',
                            'label': '风险分析报告',
                            'target': f"risk_report:{column_name}"
                        }
                    ]
                }
        except Exception as e:
            logger.error(f"识别业务风险失败: {e}")
            return None
    
    def _parse_original_insights(self, insights: Optional[str], 
                               recommendations: Optional[str]) -> List[Dict[str, Any]]:
        """解析原始洞察文本"""
        parsed = []
        
        if insights:
            # 简单的文本解析，实际可以用更复杂的NLP
            insight_lines = insights.split('\n')
            for i, line in enumerate(insight_lines):
                if line.strip():
                    parsed.append({
                        'id': f"original_insight_{i}",
                        'type': 'llm_generated',
                        'priority': 'medium',
                        'title': line.strip(),
                        'description': line.strip(),
                        'source': 'llm'
                    })
        
        return parsed
    
    def _merge_insights_with_statistics(self, parsed_insights: List[Dict], 
                                      statistical_insights: List[Dict],
                                      statistics: Dict[str, Any],
                                      data_tables: Dict[str, List[Dict]]) -> List[Dict[str, Any]]:
        """合并洞察与统计数据"""
        # 优先使用统计驱动的洞察，补充LLM生成的洞察
        merged = statistical_insights.copy()
        
        # 添加没有统计支撑的LLM洞察
        for insight in parsed_insights:
            if not self._has_similar_insight(insight, statistical_insights):
                merged.append(insight)
        
        # 按优先级排序
        return sorted(merged, key=lambda x: self._get_priority_score(x['priority']), reverse=True)
    
    def _has_similar_insight(self, insight: Dict, existing_insights: List[Dict]) -> bool:
        """检查是否有相似的洞察"""
        insight_keywords = set(insight['title'].lower().split())
        
        for existing in existing_insights:
            existing_keywords = set(existing['title'].lower().split())
            # 如果有50%以上的关键词重叠，认为是相似洞察
            overlap = len(insight_keywords & existing_keywords)
            if overlap / len(insight_keywords) > 0.5:
                return True
        return False
    
    def _get_priority_score(self, priority: str) -> int:
        """获取优先级分数"""
        scores = {'high': 3, 'medium': 2, 'low': 1}
        return scores.get(priority, 1)
    
    def _generate_executive_summary(self, insights: List[Dict],
                                  statistics: Dict[str, Any]) -> Dict[str, Any]:
        """生成执行摘要"""
        logger.info(f"生成执行摘要 - 洞察数量: {len(insights)}")

        if not insights:
            logger.warning("没有洞察可用于生成执行摘要")
            return {
                'key_finding': '数据分析完成，未发现特殊洞察',
                'supporting_data': {},
                'evidence_links': {},
                'impact_level': 'medium'
            }

        # 找到最重要的洞察
        top_insight = insights[0] if insights else None

        if not top_insight:
            logger.warning("顶级洞察为空")
            return {
                'key_finding': '数据分析完成',
                'supporting_data': {},
                'evidence_links': {},
                'impact_level': 'medium'
            }

        summary = {
            'key_finding': top_insight.get('title', '数据分析完成'),
            'supporting_data': top_insight.get('supporting_statistics', {}),
            'evidence_links': top_insight.get('evidence', {}),
            'impact_level': top_insight.get('priority', 'medium')
        }

        logger.info(f"执行摘要生成完成: {summary['key_finding']}")
        return summary
    
    def _generate_actionable_recommendations(self, insights: List[Dict],
                                           statistics: Dict[str, Any],
                                           data_tables: Dict[str, List[Dict]]) -> List[Dict[str, Any]]:
        """生成可操作的建议"""
        recommendations = []

        logger.info(f"生成可操作建议 - 洞察数量: {len(insights)}")

        # 基于风险洞察生成建议
        risk_insights = [i for i in insights if i.get('type') == 'business_risk']
        logger.info(f"风险洞察数量: {len(risk_insights)}")

        for risk in risk_insights:
            try:
                risk_count = risk.get('supporting_statistics', {}).get('primary_metric', {}).get('value', 0)
                recommendations.append({
                    'id': f"rec_{risk['id']}",
                    'priority': 'high',
                    'title': '制定风险机构提升计划',
                    'description': f"针对{risk_count}个风险机构，建议制定专项提升计划",
                    'supporting_statistics': risk.get('supporting_statistics', {}),
                    'actions': [
                        {
                            'type': 'create_plan',
                            'label': '制定提升计划'
                        },
                        {
                            'type': 'assign_resources',
                            'label': '分配资源支持'
                        }
                    ]
                })
                logger.info(f"生成风险建议: {recommendations[-1]['title']}")
            except Exception as e:
                logger.error(f"生成风险建议失败: {e}")

        # 基于优秀表现生成建议
        leader_insights = [i for i in insights if i.get('type') == 'performance_leader']
        logger.info(f"领先者洞察数量: {len(leader_insights)}")

        for leader in leader_insights:
            try:
                institution = leader.get('supporting_statistics', {}).get('primary_metric', {}).get('institution', '优秀机构')
                recommendations.append({
                    'id': f"rec_{leader['id']}",
                    'priority': 'medium',
                    'title': '推广优秀经验',
                    'description': f"将{institution}的成功经验推广到其他机构",
                    'supporting_statistics': leader.get('supporting_statistics', {}),
                    'actions': [
                        {
                            'type': 'best_practice',
                            'label': '总结最佳实践'
                        },
                        {
                            'type': 'knowledge_sharing',
                            'label': '组织经验分享'
                        }
                    ]
                })
                logger.info(f"生成推广建议: {recommendations[-1]['title']}")
            except Exception as e:
                logger.error(f"生成推广建议失败: {e}")

        # 基于分布特征生成建议
        variance_insights = [i for i in insights if i.get('type') == 'high_variance']
        logger.info(f"分布特征洞察数量: {len(variance_insights)}")

        for variance in variance_insights:
            try:
                recommendations.append({
                    'id': f"rec_{variance['id']}",
                    'priority': 'medium',
                    'title': '优化业务分布',
                    'description': '针对业务分布不均的情况，建议制定均衡发展策略',
                    'supporting_statistics': variance.get('supporting_statistics', {}),
                    'actions': [
                        {
                            'type': 'balance_strategy',
                            'label': '制定均衡策略'
                        },
                        {
                            'type': 'resource_allocation',
                            'label': '优化资源配置'
                        }
                    ]
                })
                logger.info(f"生成分布优化建议: {recommendations[-1]['title']}")
            except Exception as e:
                logger.error(f"生成分布优化建议失败: {e}")

        logger.info(f"可操作建议生成完成，共生成 {len(recommendations)} 个建议")
        return recommendations
    
    def _create_statistical_summary(self, statistics: Dict[str, Any]) -> Dict[str, Any]:
        """创建统计摘要"""
        if not statistics or 'summary' not in statistics:
            return {}
        
        return {
            'tables_analyzed': statistics['summary'].get('analyzed_tables', 0),
            'insights_generated': len(statistics.get('statistics', {})),
            'confidence_score': 0.9,
            'analysis_timestamp': statistics['summary'].get('analyzed_at', '')
        }
    
    def _create_fallback_insights(self, original_insights: Optional[str],
                                original_recommendations: Optional[str]) -> Dict[str, Any]:
        """创建备用洞察（当增强生成失败时）"""
        logger.warning("使用备用洞察模式")

        return {
            'executive_summary': {
                'key_finding': original_insights or '数据分析完成',
                'supporting_data': {},
                'evidence_links': {},
                'impact_level': 'medium'
            },
            'insights': [],
            'recommendations': [],
            'statistical_summary': {},
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'insights_count': 0,
                'recommendations_count': 0,
                'analysis_timestamp': datetime.now().isoformat(),
                'fallback_mode': True
            }
        }
    
    def _identify_business_type(self, table_name: str, column_name: str) -> str:
        """识别业务类型"""
        if '个人贷款' in table_name or '个人' in column_name:
            return '个人贷款'
        elif '对公' in table_name or '企业' in column_name:
            return '对公贷款'
        else:
            return '个人贷款'  # 默认
    
    def _format_currency(self, value: float) -> str:
        """格式化货币"""
        if value >= 1e8:
            return f"{value/1e8:.1f}亿元"
        elif value >= 1e4:
            return f"{value/1e4:.1f}万元"
        else:
            return f"{value:.0f}元"

# 全局实例
enhanced_insights_generator = EnhancedInsightsGenerator()
