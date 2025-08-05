# DataProxy API 测试示例

## 🚀 快速测试

### 1. 启动服务
```bash
python3 dataproxy_cli.py start-server --port 8080
```

### 2. 健康检查
```bash
curl http://localhost:8080/api/health
```

预期响应：
```json
{
  "status": "healthy",
  "service": "DataProxy Flask API",
  "version": "2.0.0",
  "timestamp": 1754034758.518235,
  "initialized": false
}
```

### 3. 基本查询测试
```bash
curl -X POST http://localhost:8080/api/v1/queries/agent \
  -H "Content-Type: application/json" \
  -d '{
    "query": "查询所有客户信息"
  }'
```

### 4. 带可视化的查询测试
```bash
curl -X POST http://localhost:8080/api/v1/queries/agent \
  -H "Content-Type: application/json" \
  -d '{
    "query": "查询存款余额前5名的客户，用饼图显示",
    "options": {
      "enable_visualization": true,
      "chart_style": "banking"
    }
  }'
```

## 📊 响应数据说明

### 成功响应结构
```json
{
  "success": true,
  "query": "用户查询",
  "execution_time": 2.34,
  "data_tables": [
    {
      "name": "查询结果",
      "columns": ["列名1", "列名2"],
      "data": [{"列名1": "值1", "列名2": "值2"}],
      "row_count": 1
    }
  ],
  "statistics": {
    "数值字段": {
      "average": 平均值,
      "max": 最大值,
      "min": 最小值,
      "total": 总计
    }
  },
  "visualization": {
    "should_visualize": true,
    "chart_type": "pie",
    "chart_title": "图表标题",
    "generated_code": "Plotly代码",
    "llm_generated": true
  },
  "insights": ["业务洞察文本"]
}
```

### 错误响应结构
```json
{
  "success": false,
  "error": "具体错误信息",
  "query": "用户查询"
}
```

## ⚠️ 常见问题

### 1. 服务未启动
**错误**: `Connection refused`
**解决**: 确保服务已启动在正确端口

### 2. API密钥未设置
**错误**: 查询功能受限
**解决**: 设置 `DEEPSEEK_API_KEY` 环境变量

### 3. 查询超时
**错误**: 请求超时
**解决**: 增加超时时间到60秒，简化查询语句

### 4. 数据库不存在
**错误**: 数据库文件未找到
**解决**: 先使用CLI导入数据或不指定database参数

## 🔧 前端集成要点

1. **异步处理**: 查询需要时间，显示加载状态
2. **错误处理**: 检查success字段和error信息
3. **数据展示**: 使用data_tables中的数据
4. **可视化**: 根据visualization字段决定是否显示图表
5. **统计信息**: 使用statistics字段显示数据摘要
