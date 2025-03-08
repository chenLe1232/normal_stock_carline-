# 股票分析服务

这是一个基于 FastAPI 和 Tushare 的股票分析服务，用于分析股票的涨跌概率。

## 功能特点

- 获取过滤后的股票列表（排除北交所股票，市值在 10 亿到 1000 亿之间）
- 分析股票在不同时间维度下的涨跌概率
- 支持多种时间维度：近 1 月、3 月、6 月、1 年、2 年、3 年、4 年、5 年
- 分析不同涨幅区间对应的第二天竞价、1 分钟、5 分钟、15 分钟、30 分钟、1 小时的涨跌概率

## 安装

1. 克隆项目

```bash
git clone https://github.com/yourusername/stock-analysis-service.git
cd stock-analysis-service
```

2. 安装依赖

```bash
pip install -r requirements.txt
```

3. 配置环境变量

```bash
cp .env.example .env
```

然后编辑`.env`文件，填入你的 Tushare API Token。

## 运行

```bash
python app.py
```

服务将在 http://localhost:8000 上运行。

## API 文档

启动服务后，可以访问 http://localhost:8000/docs 查看 API 文档。

### 主要接口

1. 获取过滤后的股票列表

```
GET /api/stocks/list
```

2. 获取股票基本信息

```
GET /api/stocks/{ts_code}
```

3. 获取股票涨跌概率

```
GET /api/stocks/{ts_code}/probability?time_period={time_period}
```

## 数据存储

分析结果会保存在`data`目录下，以 CSV 格式存储，方便后续查询。

## 注意事项

- 需要有效的 Tushare API Token 才能使用本服务
- 首次运行时，数据获取和分析可能需要较长时间
- 建议使用 Python 3.8 或更高版本
