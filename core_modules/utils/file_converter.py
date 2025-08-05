"""
文件转换器 - 将 CSV/Excel 文件转换为 SQLite 数据库

功能：
1. CSV 文件转换
2. Excel 文件转换
3. 智能数据类型推断
4. 矩阵型数据处理
5. 使用 LLM 优化转换过程
"""

import pandas as pd
import sqlite3
import os
import json
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
import numpy as np
from openai import OpenAI


class FileConverter:
    """文件转换器"""
    
    def __init__(self):
        self.client = OpenAI(
            api_key=os.getenv("DEEPSEEK_API_KEY"),
            base_url="https://api.deepseek.com"
        )
    
    def convert_to_sqlite(self, file_path: str, output_path: Optional[str] = None) -> str:
        """将文件转换为 SQLite 数据库"""
        file_ext = Path(file_path).suffix.lower()
        
        if file_ext == '.csv':
            return self._convert_csv_to_sqlite(file_path, output_path)
        elif file_ext in ['.xlsx', '.xls']:
            return self._convert_excel_to_sqlite(file_path, output_path)
        else:
            raise ValueError(f"不支持的文件格式: {file_ext}")
    
    def _convert_csv_to_sqlite(self, csv_path: str, output_path: Optional[str] = None) -> str:
        """转换 CSV 文件"""
        print(f"[INFO] 转换 CSV 文件: {csv_path}")
        
        # 读取 CSV 文件
        df = pd.read_csv(csv_path, encoding='utf-8')
        
        # 检查是否是矩阵型数据
        if self._is_matrix_data(df):
            df = self._transform_matrix_data(df)
        
        # 清理数据
        df = self._clean_dataframe(df)
        
        # 生成输出路径
        if not output_path:
            output_path = csv_path.replace('.csv', '.db')
        
        # 转换为 SQLite
        table_name = Path(csv_path).stem
        self._dataframe_to_sqlite(df, output_path, table_name)
        
        return output_path
    
    def _convert_excel_to_sqlite(self, excel_path: str, output_path: Optional[str] = None) -> str:
        """转换 Excel 文件"""
        print(f"[INFO] 转换 Excel 文件: {excel_path}")
        
        # 生成输出路径
        if not output_path:
            output_path = excel_path.replace('.xlsx', '.db').replace('.xls', '.db')
        
        # 读取所有工作表
        excel_file = pd.ExcelFile(excel_path)
        
        # 创建数据库连接
        conn = sqlite3.connect(output_path)
        
        for sheet_name in excel_file.sheet_names:
            print(f"[INFO] 处理工作表: {sheet_name}")
            
            # 读取工作表
            df = pd.read_excel(excel_path, sheet_name=sheet_name)
            
            # 检查是否是矩阵型数据
            if self._is_matrix_data(df):
                df = self._transform_matrix_data(df)
            
            # 清理数据
            df = self._clean_dataframe(df)
            
            # 清理表名
            clean_table_name = self._clean_table_name(sheet_name)
            
            # 保存到数据库
            df.to_sql(clean_table_name, conn, if_exists='replace', index=False)
        
        conn.close()
        return output_path
    
    def _is_matrix_data(self, df: pd.DataFrame) -> bool:
        """检查是否是矩阵型数据"""
        if len(df) < 2:
            return False

        # 检查列名是否大多数是无意义的（如Unnamed__1, Unnamed__2等）
        unnamed_count = sum(1 for col in df.columns if 'Unnamed' in str(col) or str(col).startswith('column'))
        if unnamed_count > len(df.columns) * 0.5:
            print(f"[DEBUG] 检测到 {unnamed_count}/{len(df.columns)} 个无意义列名，判断为矩阵数据")
            return True

        # 检查是否有超长的列名（可能是说明文字）
        long_column_count = sum(1 for col in df.columns if len(str(col)) > 50)
        if long_column_count > 0:
            print(f"[DEBUG] 检测到 {long_column_count} 个超长列名，判断为矩阵数据")
            return True

        # 检查第一行是否包含数值
        first_row = df.iloc[0]
        numeric_count = sum(1 for val in first_row if pd.api.types.is_numeric_dtype(type(val)) or
                           (isinstance(val, str) and str(val).replace('.', '').replace('-', '').isdigit()))

        if numeric_count > len(df.columns) * 0.7:
            print(f"[DEBUG] 第一行有 {numeric_count}/{len(df.columns)} 个数值，判断为矩阵数据")
            return True

        return False
    
    def _transform_matrix_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """转换矩阵型数据为标准表格"""
        print(f"[INFO] 检测到矩阵型数据，使用 LLM 进行智能转换")

        # 获取数据样本（更多行以便更好分析）
        sample_data = df.head(15).to_string()

        # 使用 LLM 分析数据结构
        transformation_plan = self._ask_llm_for_transformation(sample_data)
        print(f"[DEBUG] LLM转换建议: {transformation_plan}")

        # 根据 LLM 建议进行转换
        try:
            # 1. 跳过不需要的行
            if transformation_plan.get('skip_rows'):
                skip_rows = transformation_plan['skip_rows']
                df = df.drop(df.index[skip_rows])
                df = df.reset_index(drop=True)

            # 2. 设置列标题
            if transformation_plan.get('header_row') is not None:
                header_row = transformation_plan['header_row']
                if 0 <= header_row < len(df):
                    # 使用指定行作为列名
                    new_columns = df.iloc[header_row].astype(str).tolist()
                    df.columns = new_columns
                    df = df.drop(df.index[:header_row + 1])
                    df = df.reset_index(drop=True)

            # 3. 转置（如果需要）
            if transformation_plan.get('needs_transpose', False):
                df = df.transpose()
                df = df.reset_index()
                # 第一列通常是原来的列名，可以作为新的标识列
                if len(df.columns) > 0:
                    df.columns = ['indicator'] + [f'value_{i}' for i in range(1, len(df.columns))]

            # 4. 使用建议的列名（如果提供）
            if transformation_plan.get('suggested_columns'):
                suggested_cols = transformation_plan['suggested_columns']
                if len(suggested_cols) == len(df.columns):
                    df.columns = suggested_cols
                elif len(suggested_cols) < len(df.columns):
                    # 部分替换
                    new_columns = list(df.columns)
                    for i, col in enumerate(suggested_cols):
                        if i < len(new_columns):
                            new_columns[i] = col
                    df.columns = new_columns

            print(f"[INFO] 转换完成，新表结构: {len(df)} 行 x {len(df.columns)} 列")
            print(f"[INFO] 列名: {list(df.columns)}")

        except Exception as e:
            print(f"[WARNING] LLM转换失败，使用默认转换: {e}")
            # 回退到简单转换
            df = self._simple_matrix_transform(df)

        return df
    
    def _ask_llm_for_transformation(self, sample_data: str) -> Dict[str, Any]:
        """使用 LLM 分析数据转换需求"""
        prompt = f"""
你是银行数据分析专家，请分析以下Excel数据样本，判断如何转换为标准的数据库表格格式：

{sample_data}

这可能是银行指标数据，常见格式包括：
1. 矩阵型：行是指标名，列是银行名或时间
2. 表格型：每行是一条记录，列是字段
3. 混合型：包含说明行、标题行、数据行

请仔细分析并判断：
1. 数据结构类型（矩阵型/表格型/混合型）
2. 是否需要转置（行列互换）
3. 哪一行应该作为列标题（从0开始计数）
4. 是否需要删除说明行或空行
5. 如何提取有意义的字段名

返回JSON格式：
{{
    "data_type": "matrix/table/mixed",
    "needs_transpose": true/false,
    "header_row": 行号或null,
    "skip_rows": [要跳过的行号列表],
    "suggested_columns": ["建议的列名1", "建议的列名2", ...],
    "description": "详细的转换建议和理由"
}}
"""
        
        try:
            response = self.client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "system", "content": "你是数据分析专家，擅长识别和转换各种数据格式。请用中文回复。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1
            )
            
            content = response.choices[0].message.content
            if content.startswith("```json"):
                content = content.replace("```json", "").replace("```", "").strip()
            
            return json.loads(content)
        except Exception as e:
            print(f"[WARNING] LLM 转换分析失败: {e}")
            return {"data_type": "table", "needs_transpose": False, "header_row": 0}

    def _simple_matrix_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """简单的矩阵转换（回退方案）"""
        print(f"[INFO] 使用简单矩阵转换")

        # 如果第一行看起来像数据，转置
        first_row = df.iloc[0]
        numeric_count = sum(1 for val in first_row if pd.api.types.is_numeric_dtype(type(val)))

        if numeric_count > len(df.columns) * 0.5:
            df = df.transpose()
            df = df.reset_index()
            df.columns = ['indicator'] + [f'value_{i}' for i in range(1, len(df.columns))]

        return df
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """清理数据框"""
        # 删除完全空的行和列
        df = df.dropna(how='all').dropna(axis=1, how='all')
        
        # 清理列名
        df.columns = [self._clean_column_name(str(col)) for col in df.columns]
        
        # 处理重复列名
        df.columns = self._handle_duplicate_columns(df.columns)
        
        # 重置索引
        df = df.reset_index(drop=True)
        
        return df
    
    def _clean_column_name(self, col_name: str) -> str:
        """清理列名"""
        # 移除特殊字符，保留中文、英文、数字、下划线
        import re
        cleaned = re.sub(r'[^\w\u4e00-\u9fff]', '_', col_name)
        
        # 移除开头的数字
        cleaned = re.sub(r'^[0-9]+', '', cleaned)
        
        # 确保不为空
        if not cleaned or cleaned == '_':
            cleaned = 'column'
        
        return cleaned
    
    def _handle_duplicate_columns(self, columns: List[str]) -> List[str]:
        """处理重复的列名"""
        seen = {}
        result = []
        
        for col in columns:
            if col in seen:
                seen[col] += 1
                result.append(f"{col}_{seen[col]}")
            else:
                seen[col] = 0
                result.append(col)
        
        return result
    
    def _clean_table_name(self, table_name: str) -> str:
        """清理表名"""
        import re
        # 移除特殊字符
        cleaned = re.sub(r'[^\w\u4e00-\u9fff]', '_', table_name)
        
        # 移除开头的数字
        cleaned = re.sub(r'^[0-9]+', '', cleaned)
        
        # 确保不为空
        if not cleaned or cleaned == '_':
            cleaned = 'table'
        
        return cleaned
    
    def _dataframe_to_sqlite(self, df: pd.DataFrame, db_path: str, table_name: str):
        """将数据框保存到 SQLite"""
        conn = sqlite3.connect(db_path)
        
        # 优化数据类型
        df = self._optimize_dtypes(df)
        
        # 保存到数据库
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        
        conn.close()
        print(f"[INFO] 数据已保存到: {db_path}, 表名: {table_name}")
    
    def _optimize_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """优化数据类型"""
        for col in df.columns:
            # 尝试转换为数值类型
            if df[col].dtype == 'object':
                # 尝试转换为数值
                numeric_series = pd.to_numeric(df[col], errors='coerce')
                if not numeric_series.isna().all():
                    # 如果大部分都能转换为数值，则使用数值类型
                    if numeric_series.notna().sum() > len(df) * 0.8:
                        df[col] = numeric_series
        
        return df
    
    def get_conversion_summary(self, db_path: str) -> Dict[str, Any]:
        """获取转换摘要"""
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 获取所有表
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        summary = {
            'database_path': db_path,
            'total_tables': len(tables),
            'tables': {}
        }
        
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            row_count = cursor.fetchone()[0]
            
            cursor.execute(f"PRAGMA table_info({table})")
            columns = [row[1] for row in cursor.fetchall()]
            
            summary['tables'][table] = {
                'row_count': row_count,
                'column_count': len(columns),
                'columns': columns
            }
        
        conn.close()
        return summary
