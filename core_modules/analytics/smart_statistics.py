"""
智能统计分析模块
为DataProxy系统提供自动化的数据统计分析功能
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Union
import logging

logger = logging.getLogger(__name__)

class SmartStatisticsAnalyzer:
    """智能统计分析器"""
    
    def __init__(self):
        """初始化统计分析器"""
        self.min_records_threshold = 3  # 触发统计分析的最小记录数
        self.numeric_dtypes = ['int64', 'float64', 'int32', 'float32', 'int16', 'float16']
        
    def should_analyze(self, data: Union[List[Dict], pd.DataFrame]) -> bool:
        """
        判断是否应该进行统计分析
        
        Args:
            data: 数据表（列表或DataFrame格式）
            
        Returns:
            bool: 是否应该进行统计分析
        """
        try:
            if isinstance(data, list):
                return len(data) > self.min_records_threshold
            elif isinstance(data, pd.DataFrame):
                return len(data) > self.min_records_threshold
            else:
                return False
        except Exception as e:
            logger.warning(f"判断是否分析时出错: {e}")
            return False
    
    def detect_numeric_columns(self, df: pd.DataFrame) -> List[str]:
        """
        检测数值型列
        
        Args:
            df: DataFrame数据
            
        Returns:
            List[str]: 数值型列名列表
        """
        try:
            numeric_columns = []
            for col in df.columns:
                # 检查数据类型
                if df[col].dtype in self.numeric_dtypes:
                    numeric_columns.append(col)
                # 尝试转换为数值型（处理字符串数字）
                elif df[col].dtype == 'object':
                    try:
                        # 尝试转换非空值
                        non_null_values = df[col].dropna()
                        if len(non_null_values) > 0:
                            pd.to_numeric(non_null_values, errors='raise')
                            numeric_columns.append(col)
                    except (ValueError, TypeError):
                        # 不是数值型，跳过
                        continue
            
            return numeric_columns
        except Exception as e:
            logger.error(f"检测数值型列时出错: {e}")
            return []
    
    def calculate_basic_statistics(self, df: pd.DataFrame, column: str) -> Dict[str, Any]:
        """
        计算单列的基础统计指标
        
        Args:
            df: DataFrame数据
            column: 列名
            
        Returns:
            Dict: 统计结果
        """
        try:
            # 确保列是数值型
            series = pd.to_numeric(df[column], errors='coerce')
            
            # 移除NaN值
            clean_series = series.dropna()
            
            if len(clean_series) == 0:
                return {
                    "error": "该列没有有效的数值数据"
                }
            
            # 计算基础统计指标
            stats = {
                "count": len(clean_series),
                "mean": float(clean_series.mean()),
                "median": float(clean_series.median()),
                "max": float(clean_series.max()),
                "min": float(clean_series.min()),
                "std": float(clean_series.std()) if len(clean_series) > 1 else 0.0,
                "sum": float(clean_series.sum())
            }
            
            # 找到最大值和最小值对应的记录
            max_idx = series.idxmax()
            min_idx = series.idxmin()
            
            # 获取完整记录
            if pd.notna(max_idx):
                max_record = df.loc[max_idx].to_dict()
                stats["max_record"] = max_record
            
            if pd.notna(min_idx):
                min_record = df.loc[min_idx].to_dict()
                stats["min_record"] = min_record
            
            return stats
            
        except Exception as e:
            logger.error(f"计算统计指标时出错 (列: {column}): {e}")
            return {
                "error": f"计算统计指标失败: {str(e)}"
            }
    
    def analyze_data_table(self, data: Union[List[Dict], pd.DataFrame], table_name: str = "数据表") -> Optional[Dict[str, Any]]:
        """
        分析数据表并生成统计报告
        
        Args:
            data: 数据表（列表或DataFrame格式）
            table_name: 表名
            
        Returns:
            Dict: 统计分析结果，如果不需要分析则返回None
        """
        try:
            # 检查是否需要分析
            if not self.should_analyze(data):
                logger.debug(f"表 '{table_name}' 记录数不足，跳过统计分析")
                return None
            
            # 转换为DataFrame
            if isinstance(data, list):
                if len(data) == 0:
                    return None
                df = pd.DataFrame(data)
            else:
                df = data.copy()
            
            # 检测数值型列
            numeric_columns = self.detect_numeric_columns(df)
            
            if not numeric_columns:
                logger.debug(f"表 '{table_name}' 没有数值型列，跳过统计分析")
                return None
            
            # 生成统计报告
            statistics = {
                "table_name": table_name,
                "total_records": len(df),
                "numeric_columns_count": len(numeric_columns),
                "numeric_columns": numeric_columns,
                "column_statistics": {},
                "summary": {
                    "analyzed_at": pd.Timestamp.now().isoformat(),
                    "analysis_type": "basic_statistics"
                }
            }
            
            # 为每个数值列计算统计指标
            for column in numeric_columns:
                logger.debug(f"分析列: {column}")
                column_stats = self.calculate_basic_statistics(df, column)
                statistics["column_statistics"][column] = column_stats
            
            # 生成整体摘要
            valid_columns = [col for col, stats in statistics["column_statistics"].items() 
                           if "error" not in stats]
            
            if valid_columns:
                statistics["summary"]["successfully_analyzed_columns"] = len(valid_columns)
                statistics["summary"]["failed_columns"] = len(numeric_columns) - len(valid_columns)
                
                # 找出数值最大的列和记录
                max_values = {}
                for col in valid_columns:
                    col_stats = statistics["column_statistics"][col]
                    max_values[col] = col_stats["max"]
                
                if max_values:
                    max_column = max(max_values, key=max_values.get)
                    statistics["summary"]["highest_value_column"] = max_column
                    statistics["summary"]["highest_value"] = max_values[max_column]
            
            logger.info(f"表 '{table_name}' 统计分析完成: {len(valid_columns)}/{len(numeric_columns)} 列成功分析")
            return statistics
            
        except Exception as e:
            logger.error(f"分析数据表时出错 (表: {table_name}): {e}")
            return {
                "table_name": table_name,
                "error": f"统计分析失败: {str(e)}",
                "analyzed_at": pd.Timestamp.now().isoformat()
            }
    
    def analyze_multiple_tables(self, data_tables: Dict[str, Union[List[Dict], pd.DataFrame]]) -> Dict[str, Any]:
        """
        分析多个数据表
        
        Args:
            data_tables: 数据表字典 {表名: 数据}
            
        Returns:
            Dict: 所有表的统计分析结果
        """
        try:
            results = {
                "statistics": {},
                "summary": {
                    "total_tables": len(data_tables),
                    "analyzed_tables": 0,
                    "skipped_tables": 0,
                    "failed_tables": 0,
                    "analyzed_at": pd.Timestamp.now().isoformat()
                }
            }
            
            for table_name, table_data in data_tables.items():
                logger.debug(f"开始分析表: {table_name}")
                
                table_stats = self.analyze_data_table(table_data, table_name)
                
                if table_stats is None:
                    results["summary"]["skipped_tables"] += 1
                    logger.debug(f"表 '{table_name}' 被跳过")
                elif "error" in table_stats:
                    results["summary"]["failed_tables"] += 1
                    results["statistics"][table_name] = table_stats
                    logger.warning(f"表 '{table_name}' 分析失败")
                else:
                    results["summary"]["analyzed_tables"] += 1
                    results["statistics"][table_name] = table_stats
                    logger.info(f"表 '{table_name}' 分析成功")
            
            logger.info(f"多表分析完成: {results['summary']['analyzed_tables']} 成功, "
                       f"{results['summary']['skipped_tables']} 跳过, "
                       f"{results['summary']['failed_tables']} 失败")
            
            return results
            
        except Exception as e:
            logger.error(f"分析多个数据表时出错: {e}")
            return {
                "error": f"多表统计分析失败: {str(e)}",
                "analyzed_at": pd.Timestamp.now().isoformat()
            }

# 全局实例
smart_stats_analyzer = SmartStatisticsAnalyzer()
