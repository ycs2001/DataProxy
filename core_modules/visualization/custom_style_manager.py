#!/usr/bin/env python3
"""
自定义样式管理器
管理图表的样式配置和主题
"""

from typing import Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class CustomChartStyle:
    """自定义图表样式配置"""
    
    # 颜色配置
    primary_color: str = "#1f4e79"
    secondary_color: str = "#2e7cb8"
    accent_color: str = "#f39c12"
    background_color: str = "#ffffff"
    text_color: str = "#333333"
    
    # 字体配置
    font_family: str = "Arial, sans-serif"
    font_size: int = 12
    title_font_size: int = 16
    
    # 图表尺寸
    width: int = 800
    height: int = 500
    
    # 边距
    margin_top: int = 50
    margin_bottom: int = 50
    margin_left: int = 50
    margin_right: int = 50
    
    # 其他样式
    show_grid: bool = True
    show_legend: bool = True
    border_width: int = 1
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'colors': {
                'primary': self.primary_color,
                'secondary': self.secondary_color,
                'accent': self.accent_color,
                'background': self.background_color,
                'text': self.text_color
            },
            'font': {
                'family': self.font_family,
                'size': self.font_size,
                'title_size': self.title_font_size
            },
            'layout': {
                'width': self.width,
                'height': self.height,
                'margin': {
                    'top': self.margin_top,
                    'bottom': self.margin_bottom,
                    'left': self.margin_left,
                    'right': self.margin_right
                }
            },
            'display': {
                'show_grid': self.show_grid,
                'show_legend': self.show_legend,
                'border_width': self.border_width
            }
        }


class CustomStyleManager:
    """自定义样式管理器"""
    
    def __init__(self):
        self.predefined_styles = self._init_predefined_styles()
        self.current_style = self.predefined_styles['default']
    
    def _init_predefined_styles(self) -> Dict[str, CustomChartStyle]:
        """初始化预定义样式"""
        return {
            'default': CustomChartStyle(),
            
            'banking': CustomChartStyle(
                primary_color="#1f4e79",
                secondary_color="#2e7cb8", 
                accent_color="#d4af37",
                background_color="#f8f9fa",
                text_color="#2c3e50"
            ),
            
            'dark': CustomChartStyle(
                primary_color="#3498db",
                secondary_color="#2980b9",
                accent_color="#e74c3c",
                background_color="#2c3e50",
                text_color="#ecf0f1"
            ),
            
            'minimal': CustomChartStyle(
                primary_color="#34495e",
                secondary_color="#7f8c8d",
                accent_color="#e67e22",
                background_color="#ffffff",
                text_color="#2c3e50",
                show_grid=False,
                border_width=0
            ),
            
            'colorful': CustomChartStyle(
                primary_color="#e74c3c",
                secondary_color="#3498db",
                accent_color="#f39c12",
                background_color="#ecf0f1",
                text_color="#2c3e50"
            )
        }
    
    def get_style(self, style_name: str = 'default') -> CustomChartStyle:
        """获取指定样式"""
        return self.predefined_styles.get(style_name, self.predefined_styles['default'])
    
    def set_current_style(self, style_name: str):
        """设置当前样式"""
        if style_name in self.predefined_styles:
            self.current_style = self.predefined_styles[style_name]
        else:
            print(f"[WARNING] 样式 '{style_name}' 不存在，使用默认样式")
    
    def create_custom_style(self, **kwargs) -> CustomChartStyle:
        """创建自定义样式"""
        return CustomChartStyle(**kwargs)
    
    def apply_style_to_plotly_figure(self, fig, style: Optional[CustomChartStyle] = None):
        """将样式应用到Plotly图表"""
        if style is None:
            style = self.current_style
        
        try:
            # 应用布局样式
            fig.update_layout(
                font=dict(
                    family=style.font_family,
                    size=style.font_size,
                    color=style.text_color
                ),
                title_font_size=style.title_font_size,
                plot_bgcolor=style.background_color,
                paper_bgcolor=style.background_color,
                width=style.width,
                height=style.height,
                margin=dict(
                    t=style.margin_top,
                    b=style.margin_bottom,
                    l=style.margin_left,
                    r=style.margin_right
                ),
                showlegend=style.show_legend
            )
            
            # 应用网格样式
            if hasattr(fig, 'update_xaxes'):
                fig.update_xaxes(showgrid=style.show_grid)
                fig.update_yaxes(showgrid=style.show_grid)
            
            # 应用颜色样式到traces
            if fig.data:
                for i, trace in enumerate(fig.data):
                    if hasattr(trace, 'marker'):
                        if i == 0:
                            trace.marker.color = style.primary_color
                        elif i == 1:
                            trace.marker.color = style.secondary_color
                        else:
                            trace.marker.color = style.accent_color
            
        except Exception as e:
            print(f"[WARNING] 样式应用失败: {e}")
    
    def get_color_palette(self, style_name: str = 'default') -> list:
        """获取颜色调色板"""
        style = self.get_style(style_name)
        return [
            style.primary_color,
            style.secondary_color,
            style.accent_color,
            "#95a5a6",  # 灰色
            "#9b59b6",  # 紫色
            "#1abc9c",  # 青色
            "#f1c40f",  # 黄色
            "#e67e22"   # 橙色
        ]
    
    def list_available_styles(self) -> list:
        """列出可用的样式"""
        return list(self.predefined_styles.keys())
    
    def get_style_preview(self, style_name: str) -> Dict[str, Any]:
        """获取样式预览信息"""
        if style_name not in self.predefined_styles:
            return {}
        
        style = self.predefined_styles[style_name]
        return {
            'name': style_name,
            'colors': {
                'primary': style.primary_color,
                'secondary': style.secondary_color,
                'accent': style.accent_color,
                'background': style.background_color,
                'text': style.text_color
            },
            'description': self._get_style_description(style_name)
        }
    
    def _get_style_description(self, style_name: str) -> str:
        """获取样式描述"""
        descriptions = {
            'default': '默认样式，适合一般用途',
            'banking': '银行业务风格，专业稳重',
            'dark': '深色主题，适合暗色环境',
            'minimal': '极简风格，简洁清爽',
            'colorful': '彩色主题，活泼明亮'
        }
        return descriptions.get(style_name, '自定义样式')
    
    def export_style(self, style_name: str, file_path: str) -> bool:
        """导出样式到文件"""
        try:
            if style_name not in self.predefined_styles:
                return False
            
            style_dict = self.predefined_styles[style_name].to_dict()
            
            import json
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(style_dict, f, indent=2, ensure_ascii=False)
            
            return True
        except Exception as e:
            print(f"[ERROR] 样式导出失败: {e}")
            return False
    
    def import_style(self, style_name: str, file_path: str) -> bool:
        """从文件导入样式"""
        try:
            import json
            with open(file_path, 'r', encoding='utf-8') as f:
                style_dict = json.load(f)
            
            # 解析样式配置
            colors = style_dict.get('colors', {})
            font = style_dict.get('font', {})
            layout = style_dict.get('layout', {})
            margin = layout.get('margin', {})
            display = style_dict.get('display', {})
            
            # 创建样式对象
            custom_style = CustomChartStyle(
                primary_color=colors.get('primary', '#1f4e79'),
                secondary_color=colors.get('secondary', '#2e7cb8'),
                accent_color=colors.get('accent', '#f39c12'),
                background_color=colors.get('background', '#ffffff'),
                text_color=colors.get('text', '#333333'),
                font_family=font.get('family', 'Arial, sans-serif'),
                font_size=font.get('size', 12),
                title_font_size=font.get('title_size', 16),
                width=layout.get('width', 800),
                height=layout.get('height', 500),
                margin_top=margin.get('top', 50),
                margin_bottom=margin.get('bottom', 50),
                margin_left=margin.get('left', 50),
                margin_right=margin.get('right', 50),
                show_grid=display.get('show_grid', True),
                show_legend=display.get('show_legend', True),
                border_width=display.get('border_width', 1)
            )
            
            self.predefined_styles[style_name] = custom_style
            return True
            
        except Exception as e:
            print(f"[ERROR] 样式导入失败: {e}")
            return False
