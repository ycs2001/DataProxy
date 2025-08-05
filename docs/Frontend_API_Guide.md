# DataProxy API 前端开发指导

## 🚀 系统概述

DataProxy 是一个基于LLM驱动的智能数据分析平台，为前端应用提供强大的数据查询和分析能力。

### 🎯 核心价值
- **自然语言查询**: 用户可以用中文直接查询数据，无需编写SQL
- **智能图表生成**: 系统自动判断数据特征，生成合适的可视化图表
- **银行业务专业化**: 内置金融行业术语识别和业务规则
- **LLM驱动**: 基于DeepSeek大语言模型，提供智能分析能力

### 📊 主要功能模块
1. **数据导入**: Excel文件自动解析和数据库生成
2. **智能查询**: 自然语言转SQL，支持复杂多表关联
3. **统计分析**: 自动生成数据统计指标和业务洞察
4. **可视化**: 智能选择图表类型，生成专业图表代码
5. **上下文管理**: 自动维护数据字典和业务规则

## 🌐 API 接口说明

### 服务启动
```bash
# 启动API服务器
python3 dataproxy_cli.py start-server --port 8080

# 服务地址
http://localhost:8080
```

### 基础端点

| 端点 | 方法 | 功能 | 说明 |
|------|------|------|------|
| `/` | GET | 服务信息 | 获取API基本信息和可用端点 |
| `/api/health` | GET | 健康检查 | 检查服务状态和运行时间 |
| `/api/version` | GET | 版本信息 | 获取版本号和功能列表 |
| `/api/v1/queries/agent` | POST | 智能查询 | **核心功能**：自然语言数据查询 |

### 核心查询接口详解

#### `POST /api/v1/queries/agent`
**功能**: 这是系统的核心接口，接收自然语言查询，返回数据结果和可视化建议。

**请求参数**:
```json
{
  "query": "用户的自然语言查询",
  "database": "数据库文件名（可选）",
  "options": {
    "enable_visualization": true,  // 是否启用可视化
    "chart_style": "banking",      // 图表样式
    "max_results": 100            // 最大返回记录数
  }
}
```

**响应数据结构**:
```json
{
  "success": true,                    // 请求是否成功
  "query": "用户原始查询",              // 用户输入的查询
  "sql_generated": "生成的SQL语句",     // 系统生成的SQL（可选）
  "execution_time": 2.34,            // 执行耗时（秒）

  "data_tables": [                   // 查询结果数据
    {
      "name": "表格名称",
      "columns": ["列名1", "列名2"],
      "data": [                      // 实际数据行
        {"列名1": "值1", "列名2": "值2"}
      ],
      "row_count": 5
    }
  ],

  "statistics": {                    // 自动统计分析
    "数值字段名": {
      "average": 平均值,
      "max": 最大值,
      "min": 最小值,
      "total": 总计
    }
  },

  "visualization": {                 // 可视化建议
    "should_visualize": true,        // 是否建议可视化
    "chart_type": "图表类型",         // 建议的图表类型
    "chart_title": "图表标题",       // 智能生成的标题
    "generated_code": "Plotly代码",  // 生成的图表代码（可选）
    "llm_generated": true           // 是否由LLM生成
  },

  "insights": [                      // 业务洞察（可选）
    "自动生成的业务洞察文本"
  ],

  "error": "错误信息"                 // 失败时的错误描述
}
```

## 🔧 API 调用指导

### 基本调用流程

1. **服务检查**: 先调用 `/api/health` 确认服务可用
2. **执行查询**: 调用 `/api/v1/queries/agent` 发送自然语言查询
3. **处理响应**: 根据返回的数据结构处理结果
4. **可视化**: 如果有图表代码，可以用于前端图表渲染

### 查询示例

#### 简单数据查询
```bash
curl -X POST http://localhost:8080/api/v1/queries/agent \
  -H "Content-Type: application/json" \
  -d '{
    "query": "查询所有客户的存款余额"
  }'
```

#### 带可视化的查询
```bash
curl -X POST http://localhost:8080/api/v1/queries/agent \
  -H "Content-Type: application/json" \
  -d '{
    "query": "统计各支行客户数量，用柱状图显示",
    "options": {
      "enable_visualization": true,
      "chart_style": "banking"
    }
  }'
```

#### 指定数据库查询
```bash
curl -X POST http://localhost:8080/api/v1/queries/agent \
  -H "Content-Type: application/json" \
  -d '{
    "query": "查询存款余额前5名的客户",
    "database": "bank_data.db"
  }'
```

### 前端集成要点

#### 1. 异步处理
- 查询可能需要10-30秒，需要显示加载状态
- 建议设置60秒超时时间
- 实现查询取消功能

#### 2. 错误处理
- 检查 `success` 字段判断请求是否成功
- `error` 字段包含具体错误信息
- HTTP状态码可能为500，需要妥善处理

#### 3. 数据展示
- `data_tables` 数组包含所有查询结果表格
- `statistics` 提供自动统计分析
- `insights` 包含业务洞察文本

#### 4. 可视化集成
- `visualization.should_visualize` 判断是否需要图表
- `visualization.chart_type` 提供图表类型建议
- `visualization.generated_code` 包含Plotly图表代码
- 可以直接执行代码或解析参数用于其他图表库

## � 支持的查询类型

### 数据查询类型
- **基础查询**: "查询所有客户信息"
- **条件查询**: "查询存款余额大于1000万的客户"
- **排序查询**: "查询存款余额前10名的客户"
- **统计查询**: "统计各支行的客户数量"
- **时间查询**: "查询2024年的贷款数据"
- **关联查询**: "查询有不良贷款的对公有效户"

### 可视化类型
- **柱状图**: 适合对比不同类别的数据
- **饼图**: 适合显示占比分布
- **折线图**: 适合显示趋势变化
- **散点图**: 适合显示相关性

### 银行业务术语
系统内置银行业务术语识别，支持：
- 对公有效户、不良贷款、存款余额
- 支行、客户、贷款合同
- 五级分类、风险等级

## ⚠️ 重要提醒

### 性能考虑
- **响应时间**: 复杂查询10-30秒，简单查询2-5秒
- **并发限制**: 建议控制并发查询数量
- **数据量**: 大数据集可能影响响应时间

### 错误处理
- **网络超时**: 建议设置60秒超时
- **API限制**: DeepSeek API有调用频率限制
- **数据库错误**: 可能因数据结构问题导致查询失败

### 最佳实践
- **用户体验**: 显示查询进度和加载状态
- **错误提示**: 提供友好的错误信息
- **结果缓存**: 相同查询可以缓存结果
- **查询优化**: 引导用户使用更精确的查询语句

## 🚀 快速开始

1. **启动服务**: `python3 dataproxy_cli.py start-server --port 8080`
2. **健康检查**: `GET http://localhost:8080/api/health`
3. **测试查询**: 发送POST请求到 `/api/v1/queries/agent`
4. **处理响应**: 根据返回的数据结构展示结果

## 📞 技术支持

- 查看启动日志了解系统状态
- 使用健康检查端点监控服务
- 关注响应中的error字段获取错误信息
