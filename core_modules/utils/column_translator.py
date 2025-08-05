#!/usr/bin/env python3
"""
简单的列名中文化模块
根据数据字典将英文列名转换为中文，如果没有映射就保持原样
"""

import pandas as pd
import json
import os
import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

class SimpleColumnTranslator:
    """简单的列名转换器"""
    
    def __init__(self):
        self.field_mappings = {}
        self.load_field_mappings()
    
    def load_field_mappings(self):
        """加载字段映射 - 优先从配置文件加载"""
        try:
            # 尝试从学习到的映射文件加载
            mapping_file = "config/auto_generated/enhanced_field_mappings.json"
            if os.path.exists(mapping_file):
                self._load_from_config_file(mapping_file)
                print(f"[INFO] 从配置文件加载字段映射: {mapping_file}")
                return
            
            # 如果没有配置文件，使用空映射（保持原始列名）
            self.field_mappings = {}
            print(f"[INFO] 未找到数据字典，将保持原始列名")
            
        except Exception as e:
            print(f"[WARNING] 加载字段映射失败: {e}")
            self.field_mappings = {}
    
    def _load_from_config_file(self, json_path: str):
        """从配置文件加载映射"""
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            field_mappings_data = data.get('field_mappings', {})
            self.field_mappings = {}
            
            for field_key, field_info in field_mappings_data.items():
                english_name = field_info.get('english_name', '')
                chinese_name = field_info.get('chinese_name', '')
                
                if english_name and chinese_name:
                    # 支持多种格式的键
                    self.field_mappings[english_name.lower()] = chinese_name
                    self.field_mappings[field_key.lower()] = chinese_name
            
            print(f"[INFO] 加载了 {len(self.field_mappings)} 个字段映射")
            
        except Exception as e:
            print(f"[ERROR] 解析配置文件失败: {e}")
            self.field_mappings = {}
    
    def translate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """转换DataFrame的列名 - 有映射就转换，没有就保持原样"""
        if df.empty:
            return df
        
        try:
            # 如果没有任何映射，直接返回原DataFrame
            if not self.field_mappings:
                print(f"[INFO] 无字段映射，保持原始列名: {list(df.columns)}")
                return df
            
            # 创建列名映射
            column_mapping = {}
            translated_count = 0
            
            for col in df.columns:
                chinese_name = self._get_chinese_name(col)
                if chinese_name and chinese_name != col:
                    column_mapping[col] = chinese_name
                    translated_count += 1
                else:
                    # 保持原名
                    column_mapping[col] = col
            
            # 应用映射
            df_translated = df.rename(columns=column_mapping)
            
            if translated_count > 0:
                print(f"[INFO] 成功转换 {translated_count} 个字段名为中文")
                print(f"[INFO] 转换后列名: {list(df_translated.columns)}")
            else:
                print(f"[INFO] 未找到匹配的字段映射，保持原始列名")
            
            return df_translated
            
        except Exception as e:
            print(f"[ERROR] 列名转换失败: {e}")
            return df
    
    def _get_chinese_name(self, field_name: str) -> str:
        """获取字段的中文名称 - 简单匹配策略"""
        if not field_name or not self.field_mappings:
            return field_name

        # 如果已经是中文，直接返回
        if any(ord(c) > 127 for c in field_name):
            return field_name

        # 直接匹配（忽略大小写）
        field_lower = field_name.lower().strip()
        if field_lower in self.field_mappings:
            return self.field_mappings[field_lower]

        # 去除引号后匹配
        field_clean = field_name.strip('"\'').lower()
        if field_clean in self.field_mappings:
            return self.field_mappings[field_clean]

        # 没有找到映射，返回原名
        return field_name
    
    def translate_query_results(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """转换查询结果的字段名"""
        if not results:
            return results
        
        try:
            # 转换为DataFrame
            df = pd.DataFrame(results)
            
            # 转换列名
            df_translated = self.translate_columns(df)
            
            # 转换回字典列表
            return df_translated.to_dict('records')
            
        except Exception as e:
            print(f"[ERROR] 查询结果转换失败: {e}")
            return results
    
    def get_available_mappings(self) -> Dict[str, str]:
        """获取当前可用的映射"""
        return self.field_mappings.copy()

# 全局实例
_global_translator = None

def get_column_translator() -> SimpleColumnTranslator:
    """获取全局列名转换器实例"""
    global _global_translator
    if _global_translator is None:
        _global_translator = SimpleColumnTranslator()
    return _global_translator

def translate_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """便捷函数：转换DataFrame列名"""
    translator = get_column_translator()
    return translator.translate_columns(df)

def translate_query_results(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """便捷函数：转换查询结果字段名"""
    translator = get_column_translator()
    return translator.translate_query_results(results)

def reload_translator():
    """重新加载翻译器（当配置文件更新时使用）"""
    global _global_translator
    _global_translator = None
    return get_column_translator()
