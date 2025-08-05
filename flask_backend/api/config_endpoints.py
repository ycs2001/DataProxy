#!/usr/bin/env python3
"""
配置管理API端点 - 支持前端数据分析模块
"""

import os
import json
import time
from flask import Blueprint, jsonify, request
from .error_handlers import APIErrorHandler

def create_config_blueprint():
    """创建配置管理API蓝图"""
    
    config_bp = Blueprint('config', __name__, url_prefix='/api/v1/configurations')
    
    def get_config_file_path(config_type):
        """获取配置文件路径"""
        config_dir = os.path.join('flask_backend', 'configs')
        os.makedirs(config_dir, exist_ok=True)
        return os.path.join(config_dir, f"{config_type}.json")
    
    @config_bp.route('/business-terms', methods=['GET'])
    def get_business_terms():
        """获取业务术语配置"""
        try:
            config_file = get_config_file_path('business_terms')

            if os.path.exists(config_file):
                with open(config_file, 'r', encoding='utf-8') as f:
                    business_terms = json.load(f)
            else:
                # 默认业务术语
                business_terms = {
                    "对公有效户": {
                        "definition": "企业客户平均日存款余额≥10万元",
                        "sql_condition": "corp_deposit_y_avg_bal >= 100000",
                        "category": "客户分类"
                    },
                    "不良贷款": {
                        "definition": "五级分类为次级、可疑、损失的贷款",
                        "sql_condition": "CONTRACT_CL_RESULT IN (2, 3, 4)",
                        "category": "风险分类"
                    },
                    "存款余额": {
                        "definition": "客户在银行的存款金额",
                        "field_mapping": "corp_deposit_bal",
                        "category": "财务指标"
                    }
                }
            
            return jsonify({
                'success': True,
                'business_terms': business_terms,
                'total_count': len(business_terms)
            })
            
        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)
    
    @config_bp.route('/business-terms', methods=['POST'])
    def update_business_terms():
        """更新业务术语配置"""
        try:
            data = request.get_json()
            
            if not data or 'business_terms' not in data:
                return APIErrorHandler.handle_validation_error(
                    'business_terms is required', 'business_terms'
                )
            
            business_terms = data['business_terms']
            
            # 验证业务术语格式
            for term, config in business_terms.items():
                if not isinstance(config, dict):
                    return APIErrorHandler.handle_validation_error(
                        f'Invalid configuration for term: {term}', 'business_terms'
                    )
            
            # 保存配置
            config_file = get_config_file_path('business_terms')
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(business_terms, f, ensure_ascii=False, indent=2)
            
            return jsonify({
                'success': True,
                'message': 'Business terms updated successfully',
                'business_terms': business_terms,
                'updated_time': time.time()
            })
            
        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)
    
    @config_bp.route('/field-mappings', methods=['GET'])
    def get_field_mappings():
        """获取字段映射配置"""
        try:
            config_file = get_config_file_path('field_mappings')
            
            if os.path.exists(config_file):
                with open(config_file, 'r', encoding='utf-8') as f:
                    field_mappings = json.load(f)
            else:
                # 默认字段映射
                field_mappings = {
                    "客户名称": "CUST_NAME",
                    "客户ID": "CUST_ID",
                    "存款余额": "corp_deposit_bal",
                    "平均日存款余额": "corp_deposit_y_avg_bal",
                    "贷款余额": "loan_bal_rmb",
                    "合同分类结果": "CONTRACT_CL_RESULT",
                    "支行": "BRANCH_NAME"
                }
            
            return jsonify({
                'success': True,
                'field_mappings': field_mappings,
                'total_count': len(field_mappings)
            })
            
        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)
    
    @config_bp.route('/field-mappings', methods=['POST'])
    def update_field_mappings():
        """更新字段映射配置"""
        try:
            data = request.get_json()
            
            if not data or 'field_mappings' not in data:
                return APIErrorHandler.handle_validation_error(
                    'field_mappings is required', 'field_mappings'
                )
            
            field_mappings = data['field_mappings']
            
            # 验证字段映射格式
            if not isinstance(field_mappings, dict):
                return APIErrorHandler.handle_validation_error(
                    'field_mappings must be a dictionary', 'field_mappings'
                )
            
            # 保存配置
            config_file = get_config_file_path('field_mappings')
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(field_mappings, f, ensure_ascii=False, indent=2)
            
            return jsonify({
                'success': True,
                'message': 'Field mappings updated successfully',
                'field_mappings': field_mappings,
                'updated_time': time.time()
            })
            
        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)
    
    @config_bp.route('/query-scope-rules', methods=['GET'])
    def get_query_scope_rules():
        """获取查询范围规则配置"""
        try:
            config_file = get_config_file_path('query_scope_rules')
            
            if os.path.exists(config_file):
                with open(config_file, 'r', encoding='utf-8') as f:
                    rules = json.load(f)
            else:
                # 默认查询范围规则
                rules = [
                    {
                        "name": "时间范围限制",
                        "description": "限制查询时间范围在最近3年内",
                        "condition": "date_field >= DATE('now', '-3 years')",
                        "enabled": True
                    },
                    {
                        "name": "数据量限制",
                        "description": "单次查询结果不超过10000条",
                        "condition": "LIMIT 10000",
                        "enabled": True
                    }
                ]
            
            return jsonify({
                'success': True,
                'query_scope_rules': rules,
                'total_count': len(rules)
            })
            
        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)
    
    @config_bp.route('/query-scope-rules', methods=['POST'])
    def update_query_scope_rules():
        """更新查询范围规则配置"""
        try:
            data = request.get_json()
            
            if not data or 'query_scope_rules' not in data:
                return APIErrorHandler.handle_validation_error(
                    'query_scope_rules is required', 'query_scope_rules'
                )
            
            rules = data['query_scope_rules']
            
            # 验证规则格式
            if not isinstance(rules, list):
                return APIErrorHandler.handle_validation_error(
                    'query_scope_rules must be a list', 'query_scope_rules'
                )
            
            # 保存配置
            config_file = get_config_file_path('query_scope_rules')
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(rules, f, ensure_ascii=False, indent=2)
            
            return jsonify({
                'success': True,
                'message': 'Query scope rules updated successfully',
                'query_scope_rules': rules,
                'updated_time': time.time()
            })
            
        except Exception as e:
            return APIErrorHandler.handle_unexpected_error(e)
    
    return config_bp
