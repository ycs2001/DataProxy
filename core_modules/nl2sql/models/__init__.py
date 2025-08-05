#!/usr/bin/env python3
"""
DataProxy NL2SQL 数据模型
定义系统中使用的所有数据结构和模型
"""

from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Union
from enum import Enum
import json
from datetime import datetime


class QueryType(Enum):
    """查询类型枚举"""
    BASIC_QUERY = "basic_query"
    COMPLEX_JOIN = "complex_join"
    AGGREGATION = "aggregation"
    BUSINESS_ANALYSIS = "business_analysis"
    BANKING_DOMAIN = "banking_domain"
    UNKNOWN = "unknown"


class QueryComplexity(Enum):
    """查询复杂度枚举"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    VERY_HIGH = "very_high"


@dataclass
class QueryClassification:
    """查询分类结果"""
    query_type: QueryType = QueryType.UNKNOWN
    complexity: QueryComplexity = QueryComplexity.LOW
    confidence: float = 0.0
    template_name: str = ""
    entities: List[str] = field(default_factory=list)
    business_terms: List[str] = field(default_factory=list)
    tables_needed: List[str] = field(default_factory=list)
    reasoning: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'query_type': self.query_type.value,
            'complexity': self.complexity.value,
            'confidence': self.confidence,
            'template_name': self.template_name,
            'entities': self.entities,
            'business_terms': self.business_terms,
            'tables_needed': self.tables_needed,
            'reasoning': self.reasoning
        }


@dataclass
class SQLResult:
    """SQL 生成结果"""
    sql: str = ""
    success: bool = False
    error: Optional[str] = None
    execution_time: float = 0.0
    record_count: int = 0
    data: List[Dict[str, Any]] = field(default_factory=list)
    
    # 元数据
    classification: Optional[QueryClassification] = None
    prompt_used: str = ""
    template_name: str = ""
    business_context_applied: bool = False
    cache_hit: bool = False
    
    # 验证信息
    validation_passed: bool = False
    validation_warnings: List[str] = field(default_factory=list)
    optimization_suggestions: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        result = {
            'sql': self.sql,
            'success': self.success,
            'error': self.error,
            'execution_time': self.execution_time,
            'record_count': self.record_count,
            'data': self.data,
            'prompt_used': self.prompt_used,
            'template_name': self.template_name,
            'business_context_applied': self.business_context_applied,
            'cache_hit': self.cache_hit,
            'validation_passed': self.validation_passed,
            'validation_warnings': self.validation_warnings,
            'optimization_suggestions': self.optimization_suggestions
        }
        
        if self.classification:
            result['classification'] = self.classification.to_dict()
        
        return result


@dataclass
class QueryContext:
    """查询上下文"""
    user_query: str
    database_path: str = ""
    database_type: str = "unknown"
    business_terms: Dict[str, Any] = field(default_factory=dict)
    schema_info: Dict[str, Any] = field(default_factory=dict)
    field_mappings: Dict[str, Any] = field(default_factory=dict)
    table_relationships: Dict[str, Any] = field(default_factory=dict)
    
    # 查询特定上下文
    classification: Optional[QueryClassification] = None
    template_variables: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        result = {
            'user_query': self.user_query,
            'database_path': self.database_path,
            'database_type': self.database_type,
            'business_terms': self.business_terms,
            'schema_info': self.schema_info,
            'field_mappings': self.field_mappings,
            'table_relationships': self.table_relationships,
            'template_variables': self.template_variables
        }
        
        if self.classification:
            result['classification'] = self.classification.to_dict()
        
        return result


@dataclass
class NL2SQLSchema:
    """NL2SQL Schema 信息"""
    tables: Dict[str, Any] = field(default_factory=dict)
    relationships: List[Dict[str, Any]] = field(default_factory=list)
    business_rules: Dict[str, Any] = field(default_factory=dict)
    field_mappings: Dict[str, Any] = field(default_factory=dict)
    
    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """获取表信息"""
        return self.tables.get(table_name)
    
    def get_related_tables(self, table_name: str) -> List[str]:
        """获取相关表"""
        related = []
        for rel in self.relationships:
            if rel.get('from_table') == table_name:
                related.append(rel.get('to_table'))
            elif rel.get('to_table') == table_name:
                related.append(rel.get('from_table'))
        return related


@dataclass
class BusinessContext:
    """业务上下文"""
    terms: Dict[str, Any] = field(default_factory=dict)
    rules: Dict[str, Any] = field(default_factory=dict)
    domain_knowledge: Dict[str, Any] = field(default_factory=dict)
    
    def get_term_definition(self, term: str) -> Optional[str]:
        """获取业务术语定义"""
        term_info = self.terms.get(term)
        if term_info:
            return term_info.get('definition', '')
        return None
    
    def get_term_condition(self, term: str) -> Optional[str]:
        """获取业务术语对应的SQL条件"""
        term_info = self.terms.get(term)
        if term_info:
            return term_info.get('condition', '')
        return None


@dataclass
class PromptTemplate:
    """提示模板"""
    name: str
    description: str
    template: str
    variables: List[str] = field(default_factory=list)
    query_types: List[QueryType] = field(default_factory=list)
    complexity_level: QueryComplexity = QueryComplexity.LOW
    
    def format(self, **kwargs) -> str:
        """格式化模板"""
        try:
            return self.template.format(**kwargs)
        except KeyError as e:
            raise ValueError(f"模板变量缺失: {e}")


@dataclass
class PromptTemplates:
    """提示模板集合"""
    templates: Dict[str, PromptTemplate] = field(default_factory=dict)
    
    def get_template(self, name: str) -> Optional[PromptTemplate]:
        """获取模板"""
        return self.templates.get(name)
    
    def get_templates_by_type(self, query_type: QueryType) -> List[PromptTemplate]:
        """根据查询类型获取模板"""
        return [
            template for template in self.templates.values()
            if query_type in template.query_types
        ]
    
    def add_template(self, template: PromptTemplate):
        """添加模板"""
        self.templates[template.name] = template


@dataclass
class CacheEntry:
    """缓存条目"""
    query_hash: str
    sql_result: SQLResult
    created_at: datetime
    access_count: int = 0
    last_accessed: Optional[datetime] = None
    
    def is_expired(self, ttl_seconds: int = 3600) -> bool:
        """检查是否过期"""
        if not self.created_at:
            return True
        
        elapsed = (datetime.now() - self.created_at).total_seconds()
        return elapsed > ttl_seconds


# 导出所有模型
__all__ = [
    'QueryType',
    'QueryComplexity', 
    'QueryClassification',
    'SQLResult',
    'QueryContext',
    'NL2SQLSchema',
    'BusinessContext',
    'PromptTemplate',
    'PromptTemplates',
    'CacheEntry'
]
