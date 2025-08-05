# 数据分析智能代理 Makefile

.PHONY: help install test clean run-example lint format docs

# 默认目标
help:
	@echo "数据分析智能代理 - 可用命令:"
	@echo ""
	@echo "  install      - 安装项目依赖"
	@echo "  install-dev  - 安装开发依赖"
	@echo "  test         - 运行测试"
	@echo "  test-basic   - 运行基础测试"
	@echo "  run-example  - 运行快速示例"
	@echo "  run-demo     - 运行完整演示"
	@echo "  lint         - 代码检查"
	@echo "  format       - 代码格式化"
	@echo "  clean        - 清理临时文件"
	@echo "  docs         - 生成文档"
	@echo "  health       - 健康检查"
	@echo ""

# 安装依赖
install:
	@echo "📦 安装项目依赖..."
	pip install -r requirements.txt

install-dev:
	@echo "📦 安装开发依赖..."
	pip install -r requirements.txt
	pip install -e .[dev]

# 运行测试
test:
	@echo "🧪 运行所有测试..."
	pytest tests/ -v

test-basic:
	@echo "🧪 运行基础测试..."
	python tests/test_basic.py

# 运行示例
run-demo:
	@echo "🎬 运行完整演示..."
	python examples/basic_usage.py

# 代码质量
lint:
	@echo "🔍 代码检查..."
	flake8 src/ tests/ --max-line-length=100 --ignore=E203,W503
	mypy src/data_analysis_agent --ignore-missing-imports

format:
	@echo "✨ 代码格式化..."
	black src/ tests/ examples/ --line-length=100
	isort src/ tests/ examples/

# 清理
clean:
	@echo "🧹 清理临时文件..."
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf build/ dist/ .pytest_cache/ .coverage htmlcov/
	rm -rf logs/ reports/

# 文档生成
docs:
	@echo "📚 生成文档..."
	@mkdir -p docs/build
	@echo "文档生成功能待实现"

# 健康检查
health:
	@echo "🏥 执行健康检查..."
	python -c "import sys; sys.path.insert(0, 'src'); from data_analysis_agent.cli import cli_main; import sys; sys.argv = ['cli', 'health']; cli_main()"

# 版本信息
version:
	@echo "📋 显示版本信息..."
	python -c "import sys; sys.path.insert(0, 'src'); from data_analysis_agent.cli import cli_main; import sys; sys.argv = ['cli', 'version']; cli_main()"

# 配置检查
config:
	@echo "⚙️ 检查配置..."
	python -c "import sys; sys.path.insert(0, 'src'); from data_analysis_agent.cli import cli_main; import sys; sys.argv = ['cli', 'config']; cli_main()"

# 创建环境文件
setup-env:
	@echo "🔧 设置环境文件..."
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "已创建 .env 文件，请编辑并填入实际配置值"; \
	else \
		echo ".env 文件已存在"; \
	fi

# 初始化项目
init: setup-env install
	@echo "🎉 项目初始化完成!"
	@echo "请编辑 .env 文件设置必要的环境变量"
	@echo "然后运行 'make run-example' 测试功能"

# 开发环境设置
dev-setup: setup-env install-dev
	@echo "🛠️ 开发环境设置完成!"
	@echo "可以开始开发了"

# 构建分发包
build:
	@echo "📦 构建分发包..."
	python setup.py sdist bdist_wheel

# 安装本地包
install-local:
	@echo "📦 安装本地包..."
	pip install -e .

# 运行特定分析
analyze:
	@echo "🔍 运行分析 (示例)..."
	python -m data_analysis_agent.cli analyze "分析对公有效户信贷余额趋势" --type trend_analysis --format json

# 集成测试
test-integration:
	@echo "🧪 运行集成测试..."
	python test_integration.py

# 启动Streamlit界面
streamlit:
	@echo "🌐 启动Streamlit界面..."
	streamlit run streamlit_app.py

# 安装Streamlit依赖
install-streamlit:
	@echo "📦 安装Streamlit依赖..."
	pip install streamlit plotly

# 完整设置（包含新功能）
setup-full: setup-env install install-streamlit
	@echo "🎉 完整设置完成!"
	@echo "运行 'make streamlit' 启动Web界面"
	@echo "运行 'make test-integration' 测试集成功能"

# 性能测试
perf-test:
	@echo "⚡ 性能测试..."
	python -c "import time; start=time.time(); exec(open('run_example.py').read()); print(f'总耗时: {time.time()-start:.2f}秒')"

# 内存使用检查
memory-check:
	@echo "🧠 内存使用检查..."
	python -m memory_profiler run_example.py

# 生成需求文件
freeze:
	@echo "❄️ 生成当前环境的需求文件..."
	pip freeze > requirements-frozen.txt
	@echo "已生成 requirements-frozen.txt"
