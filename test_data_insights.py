#!/usr/bin/env python3
"""
测试DataProxy数据洞察功能
验证API是否正确返回数据洞察信息
"""

import requests
import json
import time

def test_data_insights():
    """测试数据洞察功能"""
    
    # API端点
    base_url = "http://localhost:8000"
    endpoint = "/api/v1/queries/agent"
    
    # 测试查询
    test_queries = [
        "统计到2025年3月末，我行对公有效户的不良贷款余额，并输出符合条件的客户名称及对应的不良贷款余额",
        "查询各支行客户数量排名",
        "分析对公存款余额分布情况"
    ]
    
    print("🔍 开始测试DataProxy数据洞察功能...")
    print(f"📡 API地址: {base_url}{endpoint}")
    print("=" * 60)
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n📊 测试查询 {i}: {query}")
        print("-" * 40)
        
        try:
            # 发送请求
            response = requests.post(
                f"{base_url}{endpoint}",
                json={
                    "query": query,
                    "analysis_mode": "auto",
                    "enable_statistics": True
                },
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # 检查响应结构
                if data.get('success'):
                    result_data = data.get('data', {})
                    
                    print(f"✅ 查询成功")
                    print(f"📈 记录数: {result_data.get('record_count', 0)}")
                    print(f"⏱️  执行时间: {result_data.get('execution_time', 0):.2f}秒")
                    
                    # 检查数据洞察
                    data_interpretation = result_data.get('data_interpretation')
                    if data_interpretation:
                        print(f"\n🧠 数据洞察信息:")
                        
                        # 摘要
                        summary = data_interpretation.get('summary', '')
                        if summary:
                            print(f"📝 摘要: {summary}")
                        
                        # 关键洞察
                        key_insights = data_interpretation.get('key_insights', [])
                        if key_insights:
                            print(f"💡 关键洞察 ({len(key_insights)}个):")
                            for j, insight in enumerate(key_insights[:3], 1):  # 只显示前3个
                                print(f"   {j}. {insight}")
                        
                        # 趋势分析
                        trends = data_interpretation.get('trends', [])
                        if trends:
                            print(f"📈 趋势分析 ({len(trends)}个):")
                            for j, trend in enumerate(trends[:2], 1):  # 只显示前2个
                                print(f"   {j}. {trend}")
                        
                        # 建议
                        recommendations = data_interpretation.get('recommendations', [])
                        if recommendations:
                            print(f"💼 建议 ({len(recommendations)}个):")
                            for j, rec in enumerate(recommendations[:2], 1):  # 只显示前2个
                                print(f"   {j}. {rec}")
                        
                        # 异常检测
                        anomalies = data_interpretation.get('anomalies', [])
                        if anomalies:
                            print(f"⚠️  异常检测 ({len(anomalies)}个):")
                            for j, anomaly in enumerate(anomalies[:2], 1):
                                print(f"   {j}. {anomaly}")
                    else:
                        print("❌ 未找到数据洞察信息 (data_interpretation字段缺失)")
                    
                    # 检查其他可能的洞察字段
                    insights = result_data.get('insights')
                    if insights:
                        print(f"\n📊 其他洞察信息:")
                        if isinstance(insights, list):
                            for j, insight in enumerate(insights[:3], 1):
                                print(f"   {j}. {insight}")
                        else:
                            print(f"   {insights}")
                    
                    # 检查统计信息
                    statistics = result_data.get('statistics')
                    if statistics:
                        print(f"\n📈 统计信息: {len(statistics)}个字段")
                        for key in list(statistics.keys())[:3]:  # 只显示前3个键
                            print(f"   - {key}: {statistics[key]}")
                    
                else:
                    print(f"❌ 查询失败: {data.get('error', '未知错误')}")
            
            else:
                print(f"❌ HTTP错误: {response.status_code}")
                print(f"响应: {response.text[:200]}...")
        
        except requests.exceptions.RequestException as e:
            print(f"❌ 网络错误: {e}")
        except Exception as e:
            print(f"❌ 其他错误: {e}")
        
        # 等待一下再进行下一个测试
        if i < len(test_queries):
            time.sleep(2)
    
    print("\n" + "=" * 60)
    print("🏁 测试完成")

def check_api_status():
    """检查API服务状态"""
    try:
        response = requests.get("http://localhost:8000/health", timeout=5)
        if response.status_code == 200:
            print("✅ API服务正常运行")
            return True
        else:
            print(f"❌ API服务异常: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ 无法连接到API服务: {e}")
        return False

if __name__ == "__main__":
    print("🚀 DataProxy数据洞察功能测试")
    print("=" * 60)
    
    # 检查API状态
    if check_api_status():
        test_data_insights()
    else:
        print("\n💡 请确保DataProxy API服务正在运行:")
        print("   cd flask_backend && python3 app.py")
