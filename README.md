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

### 获取所有股票的涨跌概率

获取所有过滤后的股票的涨跌概率数据。

**URL:** `/api/stocks/all/probability`

**方法:** `GET`

**查询参数:**

| 参数        | 类型   | 必填 | 描述                                                                     |
| ----------- | ------ | ---- | ------------------------------------------------------------------------ |
| time_period | string | 否   | 时间周期，如 m1(近 1 月), m3(3 月), m6(6 月)等，不指定则返回所有时间周期 |

**成功响应:**

```json
{
  "status": "success",
  "message": "获取所有股票涨跌概率成功",
  "data": {
    "000001.SZ": {
      "name": "平安银行",
      "data": {
        "m1": {
          "period_name": "近1月",
          "categories": {
            "range_1_3p": {
              "category_name": "1-3",
              "time_periods": {
                "auction": {
                  "time_name": "竞价",
                  "up_prob": 60.5,
                  "down_prob": 30.2,
                  "equal_prob": 9.3,
                  "total": 43
                }
                // ... 其他时间段数据
              }
            }
            // ... 其他涨跌幅分类数据
          }
        }
        // ... 其他时间周期数据
      }
    }
    // ... 其他股票数据
  },
  "total": 1000
}
```

**错误响应:**

```json
{
  "detail": "获取股票列表失败"
}
```

**说明:**

- 此接口会遍历所有过滤后的股票，获取每只股票的涨跌概率数据
- 由于需要处理大量数据，接口响应可能需要较长时间
- 数据会被缓存，同一天内的重复请求会直接返回缓存数据，提高响应速度

## 数据存储

分析结果会保存在`data`目录下，以 CSV 格式存储，方便后续查询。

## 注意事项

- 需要有效的 Tushare API Token 才能使用本服务
- 首次运行时，数据获取和分析可能需要较长时间
- 建议使用 Python 3.8 或更高版本
