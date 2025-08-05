# DataProxy

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://python.org)
[![Flask](https://img.shields.io/badge/flask-2.0%2B-green.svg)](https://flask.palletsprojects.com/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)](https://github.com/your-repo/DataProxy)

**DataProxy** 是一个基于LLM驱动的智能数据分析平台，提供自然语言查询、智能数据洞察和可视化分析功能。专为银行业务数据分析设计，支持中文自然语言查询，无需编写SQL即可获得专业的数据分析结果。

**DataProxy** is an LLM-powered intelligent data analysis platform that provides natural language querying, smart data insights, and visualization analysis. Designed specifically for banking business data analysis, it supports Chinese natural language queries without requiring SQL knowledge.

---

## ✨ 核心特性 | Key Features

### 🧠 智能查询 | Intelligent Querying
- **自然语言转SQL**: 支持中文查询，自动转换为精确的SQL语句
- **业务术语识别**: 内置银行业务术语库，理解专业概念
- **多表关联**: 智能识别表间关系，支持复杂查询

### 📊 智能洞察 | Smart Insights
- **LLM驱动分析**: 基于DeepSeek大语言模型生成深度业务洞察
- **趋势识别**: 自动识别数据趋势和异常模式
- **业务建议**: 提供可操作的业务优化建议

### 📈 数据可视化 | Data Visualization
- **智能图表生成**: 根据数据特征自动选择最适合的图表类型
- **专业样式**: 内置银行业务配色方案和专业图表模板
- **交互式图表**: 基于Plotly的高质量交互式可视化

### 🔧 易于集成 | Easy Integration
- **RESTful API**: 完整的API接口，支持前端集成
- **多数据库支持**: SQLite、MySQL、PostgreSQL等
- **灵活配置**: 支持自定义业务规则和字段映射

---

## 🏗️ 技术架构 | Architecture

### 系统架构图 | System Architecture
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Frontend UI   │    │   Flask API     │    │   Core Engine   │
│                 │◄──►│                 │◄──►│                 │
│ React/Vue/HTML  │    │ RESTful APIs    │    │ DataProxy Core  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
                       ┌─────────────────┐    ┌─────────────────┐
                       │   LLM Service   │    │   Database      │
                       │                 │    │                 │
                       │ DeepSeek API    │    │ SQLite/MySQL    │
                       └─────────────────┘    └─────────────────┘
```

### 核心模块 | Core Modules
```
DataProxy/
├── core_modules/           # 核心功能模块
│   ├── __init__.py        # 统一数据引擎 (CoreDataEngine)
│   ├── nl2sql/            # 自然语言转SQL
│   ├── data_import/       # 智能数据导入
│   ├── visualization/     # 数据可视化
│   ├── analytics/         # 统计分析和LLM洞察
│   ├── config/            # 配置管理
│   └── agent/             # 智能代理工具
├── flask_backend/         # Flask API服务
│   ├── app.py            # 主应用入口
│   └── api/              # API路由模块
├── configs/               # 配置文件
│   └── database_contexts/ # 数据库上下文配置
├── databases/             # 数据库文件目录
│   ├── imported/          # 正式导入的数据库
│   ├── temp/              # 临时和测试数据库
│   └── backup/            # 数据库备份文件
├── data_archive/          # 数据存储
│   ├── 数据源/            # 原始数据文件
│   └── 数据字典/          # 数据字典文件
└── requirements.txt       # 依赖包列表
```

### 技术栈 | Tech Stack
- **后端框架**: Flask 2.0+ + Python 3.8+
- **AI引擎**: LangChain + DeepSeek API
- **数据处理**: Pandas + SQLAlchemy + NumPy
- **可视化**: Plotly + Matplotlib + Seaborn
- **数据库**: SQLite (默认) + MySQL + PostgreSQL
- **API**: RESTful APIs with JSON responses

---

## 🚀 快速开始 | Quick Start

### 环境要求 | Requirements
- **Python**: 3.8 或更高版本
- **内存**: 建议 4GB 以上
- **存储**: 至少 2GB 可用空间
- **API密钥**: DeepSeek API Key (用于LLM功能)

### 1. 安装部署 | Installation

#### 克隆项目 | Clone Repository
```bash
git clone https://github.com/your-username/DataProxy.git
cd DataProxy
```

#### 安装依赖 | Install Dependencies
```bash
# 创建虚拟环境 (推荐)
python -m venv dataproxy-env
source dataproxy-env/bin/activate  # Linux/Mac
# dataproxy-env\Scripts\activate  # Windows

# 安装依赖包
pip install -r requirements.txt
```

#### 配置环境变量 | Environment Configuration
```bash
# 创建 .env 文件
echo "DEEPSEEK_API_KEY=your_deepseek_api_key_here" > .env

# 或者设置环境变量
export DEEPSEEK_API_KEY="your_deepseek_api_key_here"
```

### 2. 启动服务 | Start Services

#### 方式一：Flask API服务 | Flask API Server
```bash
# 启动后端API服务
cd flask_backend
python app.py

# 服务将运行在 http://localhost:8000
```

#### 方式二：快速启动脚本 | Quick Start Script
```bash
# Linux/Mac
chmod +x quick_start.sh
./quick_start.sh

# 或直接运行
bash quick_start.sh
```

#### 方式三：命令行工具 | CLI Tool
```bash
# 交互式命令行
python dataproxy_cli.py interactive

# 直接查询
python dataproxy_cli.py query "查询客户信息"
```

### 3. 验证安装 | Verify Installation

#### 健康检查 | Health Check
```bash
curl http://localhost:8000/api/health
```

#### 测试查询 | Test Query
```bash
curl -X POST "http://localhost:8000/api/v1/queries/agent" \
  -H "Content-Type: application/json" \
  -d '{"query": "统计客户数量", "analysis_mode": "auto"}'
```

### 4. 数据导入 | Data Import

#### Excel文件导入 | Excel Import
```bash
# 使用CLI导入Excel文件
python dataproxy_cli.py import-excel data/sample_data.xlsx

# 或通过API上传
curl -X POST "http://localhost:8000/api/v1/files/upload" \
  -F "file=@data/sample_data.xlsx"
```

---

## 📡 API接口文档 | API Documentation

### 基础信息 | Basic Information
- **Base URL**: `http://localhost:8000`
- **Content-Type**: `application/json`
- **认证方式**: 暂无 (开发环境)

### 核心端点 | Core Endpoints

#### 1. 智能查询 | Intelligent Query
**POST** `/api/v1/queries/agent`

**请求参数 | Request Parameters:**
```json
{
  "query": "统计到2025年3月末，我行对公有效户的不良贷款余额",
  "analysis_mode": "auto",  // auto | simple | detailed
  "enable_statistics": true
}
```

**响应示例 | Response Example:**
```json
{
  "success": true,
  "data": {
    "query": "统计到2025年3月末，我行对公有效户的不良贷款余额",
    "agent_response": "截至2025年3月末共找到6户对公有效户的不良贷款信息",
    "data_tables": {
      "对公有效户不良贷款明细": [
        {"CUST_NAME": "公司A", "bad_loan_balance": 91044.1},
        {"CUST_NAME": "公司B", "bad_loan_balance": 5000000}
      ]
    },
    "data_interpretation": {
      "summary": "截至2025年3月末，我行对公有效户不良贷款余额为5500018.32元",
      "key_insights": [
        "不良贷款全部集中于单一客户，风险集中度极高",
        "单户不良贷款金额达550万元，属于大额风险暴露"
      ],
      "trends": ["单户风险集中度达到100%，需要关注风险分散"],
      "anomalies": ["单户不良贷款金额异常高，需要重点监控"],
      "recommendations": [
        "立即对该客户进行专项风险评估",
        "建立大额客户风险预警机制"
      ]
    },
    "sql_query": "SELECT SUM(loan_bal_rmb) AS bad_loan_balance FROM ...",
    "execution_time": 2.45
  }
}
```

#### 2. 数据导入 | Data Import
**POST** `/api/v1/files/pure-llm-import`

**请求参数 | Request Parameters:**
```http
Content-Type: multipart/form-data

data_source_dir: data_archive/数据源
data_dict_dir: data_archive/数据字典
output_db_name: result.db
```

#### 3. 数据库管理 | Database Management
```http
GET /api/v1/databases              # 列出数据库
GET /api/v1/contexts               # 获取上下文配置
POST /api/v1/contexts              # 创建上下文配置
```

---

## 📖 使用指南 | Usage Guide

### 基础使用 | Basic Usage

#### Python SDK 使用 | Python SDK
```python
from core_modules import CoreDataEngine, quick_query

# 方式一：创建引擎实例
engine = CoreDataEngine("databases/imported/bank_data.db")
result = engine.query("查询对公有效户数量")

# 方式二：快速查询
result = quick_query("databases/imported/bank_data.db", "统计不良贷款余额")

# 处理结果
if result['success']:
    print(f"SQL: {result['sql']}")
    print(f"数据: {result['data']}")
    if 'insights' in result:
        print(f"洞察: {result['insights']}")
```

#### API 调用示例 | API Examples
```javascript
// JavaScript 前端调用示例
async function queryData(query) {
  const response = await fetch('/api/v1/queries/agent', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      query: query,
      analysis_mode: 'auto',
      enable_statistics: true
    })
  });

  const result = await response.json();

  if (result.success) {
    // 处理数据表
    console.log('数据表:', result.data.data_tables);

    // 处理智能洞察
    const insights = result.data.data_interpretation;
    console.log('摘要:', insights.summary);
    console.log('关键洞察:', insights.key_insights);
    console.log('趋势分析:', insights.trends);
    console.log('业务建议:', insights.recommendations);
  }
}

// 调用示例
queryData('统计各支行客户数量排名');
```

### 常见查询示例 | Common Query Examples

#### 银行业务查询 | Banking Queries
```bash
# 客户统计
"统计到2025年3月末，我行对公有效户数量"

# 贷款分析
"查询各支行不良贷款余额排名"

# 存款分析
"分析对公存款余额分布情况"

# 风险评估
"统计单户贷款超过500万的客户信息"

# 趋势分析
"对比2024年和2025年第一季度的贷款增长情况"
```

#### 数据探索查询 | Data Exploration
```bash
# 表结构查询
"显示客户信息表的字段结构"

# 数据质量检查
"检查客户表中的空值情况"

# 关联分析
"分析客户和贷款表的关联关系"
```

### 配置说明 | Configuration

#### 环境变量 | Environment Variables
```bash
# 必需配置
DEEPSEEK_API_KEY=your_api_key_here          # DeepSeek API密钥

# 可选配置
DATAPROXY_DATA_DIR=./databases              # 数据库目录
DATAPROXY_LOG_LEVEL=INFO                    # 日志级别
DATAPROXY_MAX_QUERY_TIME=30                 # 查询超时时间(秒)
DATAPROXY_ENABLE_CACHE=true                 # 启用缓存
```

#### 数据库配置 | Database Configuration
```yaml
# configs/database_contexts/bank_data.yaml
database_info:
  name: "银行业务数据库"
  type: "sqlite"
  description: "包含客户、贷款、存款等核心业务数据"

business_terms:
  - term: "对公有效户"
    definition: "企业客户中日均存款余额≥10万元的客户"
    field_mapping: "corp_deposit_y_avg_bal >= 100000"

  - term: "不良贷款"
    definition: "贷款分类结果为次级、可疑、损失的贷款"
    field_mapping: "CONTRACT_CL_RESULT IN ('2','3','4')"

query_scope_rules:
  - scope_type: "time_filter"
    description: "默认查询最近一年数据"
    filter_conditions: "statistics_dt >= date('now', '-1 year')"
```

---

## 🏗️ 项目结构 | Project Structure

### 详细目录结构 | Detailed Directory Structure
```
DataProxy/
├── 📁 core_modules/                    # 核心功能模块
│   ├── 📄 __init__.py                 # 统一数据引擎入口
│   ├── 📄 core_engine.py              # 核心数据引擎
│   ├── 📁 agent/                      # 智能代理工具
│   │   ├── 📄 dataproxy_tool.py       # DataProxy主工具
│   │   └── 📄 data_query_processor.py # 数据查询处理器
│   ├── 📁 nl2sql/                     # 自然语言转SQL
│   │   ├── 📄 nl2sql_processor.py     # NL2SQL处理器
│   │   ├── 📄 sql_query_engine.py     # SQL查询引擎
│   │   └── 📄 prompt_templates.py     # 提示词模板
│   ├── 📁 analytics/                  # 数据分析和洞察
│   │   ├── 📄 llm_insights_generator.py # LLM洞察生成器
│   │   └── 📄 enhanced_insights_generator.py # 增强洞察生成器
│   ├── 📁 visualization/              # 数据可视化
│   │   ├── 📄 chart_generator.py      # 图表生成器
│   │   └── 📄 visualization_tools.py  # 可视化工具
│   ├── 📁 data_import/                # 数据导入
│   │   ├── 📄 intelligent_data_importer.py # 智能数据导入器
│   │   └── 📄 llm_intelligent_importer.py  # LLM智能导入器
│   ├── 📁 config/                     # 配置管理
│   │   ├── 📄 unified_config.py       # 统一配置管理
│   │   └── 📄 context_manager.py      # 上下文管理器
│   └── 📁 utils/                      # 工具函数
│       ├── 📄 file_converter.py       # 文件转换器
│       └── 📄 database_utils.py       # 数据库工具
├── 📁 flask_backend/                  # Flask API后端
│   ├── 📄 app.py                      # 主应用入口
│   └── 📁 api/                        # API路由模块
│       ├── 📄 query_endpoints.py      # 查询API端点
│       ├── 📄 file_endpoints.py       # 文件管理API
│       ├── 📄 database_endpoints.py   # 数据库管理API
│       └── 📄 visualization_endpoints.py # 可视化API
├── 📁 configs/                        # 配置文件目录
│   └── 📁 database_contexts/          # 数据库上下文配置
│       ├── 📄 bank_data.yaml          # 银行数据配置
│       └── 📄 default.yaml            # 默认配置
├── 📁 databases/                      # 数据库文件目录
│   ├── 📁 imported/                   # 正式导入的数据库
│   ├── 📁 temp/                       # 临时数据库
│   └── 📁 backup/                     # 数据库备份
├── 📁 data_archive/                   # 数据存档
│   ├── 📁 数据源/                     # 原始数据文件
│   └── 📁 数据字典/                   # 数据字典文件
├── 📁 docs/                           # 文档目录
│   ├── 📄 API_Test_Examples.md        # API测试示例
│   ├── 📄 Backend_API_Endpoints.md    # 后端API文档
│   └── 📄 Frontend_API_Guide.md       # 前端API指南
├── 📄 requirements.txt                # Python依赖包
├── 📄 quick_start.sh                  # 快速启动脚本
├── 📄 dataproxy_cli.py               # 命令行工具
├── 📄 .env.example                   # 环境变量示例
└── 📄 README.md                      # 项目说明文档
```

### 主要文件说明 | Key Files Description

#### 核心引擎 | Core Engine
- **`core_modules/__init__.py`**: 统一数据引擎入口，提供`CoreDataEngine`和`quick_query`函数
- **`core_modules/core_engine.py`**: 核心数据引擎实现，整合各个功能模块
- **`core_modules/agent/dataproxy_tool.py`**: DataProxy主工具，实现智能查询和分析

#### API服务 | API Services
- **`flask_backend/app.py`**: Flask主应用，包含所有API端点和全局状态管理
- **`flask_backend/api/query_endpoints.py`**: 查询相关API端点
- **`flask_backend/api/file_endpoints.py`**: 文件上传和管理API

#### 配置文件 | Configuration Files
- **`configs/database_contexts/`**: 数据库上下文配置，包含业务术语和字段映射
- **`.env`**: 环境变量配置文件（需要手动创建）
- **`requirements.txt`**: Python依赖包列表

#### 工具脚本 | Utility Scripts
- **`dataproxy_cli.py`**: 命令行工具，支持交互式查询和数据导入
- **`quick_start.sh`**: 快速启动脚本，一键启动所有服务

---

## 🔧 开发指南 | Development Guide

### 本地开发 | Local Development

#### 开发环境设置 | Development Setup
```bash
# 1. 克隆项目
git clone https://github.com/your-username/DataProxy.git
cd DataProxy

# 2. 创建开发分支
git checkout -b feature/your-feature-name

# 3. 安装开发依赖
pip install -r requirements.txt
pip install -r requirements-dev.txt  # 如果有开发依赖

# 4. 配置开发环境
cp .env.example .env
# 编辑 .env 文件，添加你的API密钥
```

#### 运行测试 | Running Tests
```bash
# 运行单元测试
python -m pytest tests/

# 运行API测试
python -m pytest tests/api/

# 生成测试覆盖率报告
python -m pytest --cov=core_modules tests/
```

#### 代码规范 | Code Standards
```bash
# 代码格式化
black core_modules/ flask_backend/

# 代码检查
flake8 core_modules/ flask_backend/

# 类型检查
mypy core_modules/
```

### 贡献指南 | Contributing

#### 提交代码 | Submitting Code
1. Fork 项目到你的GitHub账户
2. 创建功能分支：`git checkout -b feature/amazing-feature`
3. 提交更改：`git commit -m 'Add some amazing feature'`
4. 推送到分支：`git push origin feature/amazing-feature`
5. 创建Pull Request

#### 代码审查 | Code Review
- 确保所有测试通过
- 遵循项目的代码规范
- 添加必要的文档和注释
- 更新相关的API文档

---

## 📚 文档和资源 | Documentation & Resources

### 官方文档 | Official Documentation
- [API接口文档](docs/Backend_API_Endpoints.md)
- [前端集成指南](docs/Frontend_API_Guide.md)
- [配置说明文档](docs/Configuration_Guide.md)
- [部署指南](docs/Deployment_Guide.md)

### 示例和教程 | Examples & Tutorials
- [API测试示例](docs/API_Test_Examples.md)
- [常见问题解答](docs/FAQ.md)
- [最佳实践指南](docs/Best_Practices.md)

### 社区资源 | Community Resources
- [GitHub Issues](https://github.com/your-username/DataProxy/issues)
- [讨论区](https://github.com/your-username/DataProxy/discussions)
- [更新日志](CHANGELOG.md)

---

## 📄 许可证 | License

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🤝 贡献者 | Contributors

感谢所有为这个项目做出贡献的开发者！

Thanks to all the developers who have contributed to this project!

<!-- 贡献者列表将自动生成 -->

---

## 📞 联系我们 | Contact

- **项目维护者**: [Your Name](mailto:your.email@example.com)
- **GitHub**: [https://github.com/your-username/DataProxy](https://github.com/your-username/DataProxy)
- **问题反馈**: [GitHub Issues](https://github.com/your-username/DataProxy/issues)

---

*最后更新时间: 2025-08-05*
