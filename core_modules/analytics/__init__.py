#!/usr/bin/env python3
"""
Analytics Module - Statistical analysis and business insights
"""

# Import analytics components
try:
    from .enhanced_insights_generator import EnhancedInsightsGenerator, enhanced_insights_generator
    from .smart_statistics import smart_stats_analyzer


    __all__ = [
        'EnhancedInsightsGenerator',
        'enhanced_insights_generator',
        'smart_stats_analyzer'
    ]

    print("✅ Analytics: 简化后组件加载成功")
    print("ℹ️  Analytics: 复杂的分析组件已被CoreDataEngine替代")
    
except ImportError as e:
    print(f"Warning: Analytics module import failed - {e}")
    __all__ = []
