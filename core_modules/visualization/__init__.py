#!/usr/bin/env python3
"""
Visualization Module - Chart generation and visualization tools
"""

# Import visualization components (non-blocking)
try:
    from .data_driven_visualization import get_visualization_decision_engine

    # 导入可视化工具
    try:
        from .visualization_tools import VisualizationTools, get_visualization_tools
        VISUALIZATION_TOOLS_AVAILABLE = True
        print("✅ Visualization: VisualizationTools 加载成功")
    except ImportError as e:
        VISUALIZATION_TOOLS_AVAILABLE = False
        print(f"⚠️  Visualization: VisualizationTools 导入失败 - {e}")

    __all__ = ['get_visualization_decision_engine']
    if VISUALIZATION_TOOLS_AVAILABLE:
        __all__.extend(['VisualizationTools', 'get_visualization_tools'])

    print("✅ Visualization: 完整功能加载成功")
    print("🎨 Visualization: 支持图表生成和样式管理")

except ImportError as e:
    print(f"❌ Visualization: 模块导入失败 - {e}")
    print("ℹ️  Visualization: 请检查依赖是否正确安装")
    __all__ = []
