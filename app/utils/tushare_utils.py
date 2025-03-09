import os
from typing import Dict, Optional, Any
import pandas as pd
import tushare as ts
import datetime
import time
import traceback
import logging
import threading
import glob
import concurrent.futures
from app.utils.logger import setup_logger

# 配置日志
logger = setup_logger(__name__)


# 创建一个请求限制器类
class RequestLimiter:
    """请求限制器，用于限制API请求频率"""
    
    def __init__(self, max_requests_per_minute=500):
        self.max_requests = max_requests_per_minute
        self.request_times = []
        self.lock = threading.Lock()
    
    def wait_if_needed(self):
        """如果需要，等待一段时间以确保不超过请求限制"""
        with self.lock:
            now = time.time()
            # 清理一分钟前的请求记录
            self.request_times = [t for t in self.request_times if now - t < 60]
            
            # 如果当前请求数已经达到最大值，则等待
            if len(self.request_times) >= self.max_requests:
                # 计算需要等待的时间
                oldest_request = self.request_times[0]
                wait_time = 60 - (now - oldest_request)
                if wait_time > 0:
                    # logger.info("已达到每分钟%s次请求限制，等待%s秒", self.max_requests, wait_time)
                    time.sleep(wait_time)
                    # 重新开始计时
                    now = time.time()
                    self.request_times = [t for t in self.request_times if now - t < 60]
            
            # 记录当前请求时间
            self.request_times.append(now)

# 创建请求限制器实例
stk_mins_limiter = RequestLimiter(max_requests_per_minute=500)
stk_auction_limiter = RequestLimiter(max_requests_per_minute=500)

# 获取Tushare Token
TUSHARE_TOKEN = os.getenv('TUSHARE_TOKEN', '')

# 初始化Tushare
try:
    pro = ts.pro_api(TUSHARE_TOKEN)
    logger.info("Tushare API初始化成功")
except Exception as e:
    logger.error("Tushare API初始化失败: %s", e)
    pro = None

# 定义常量
LIST_RANGE_MAP = {
    'range_1_3p': '1-3',
    'range_3_5p': '3-5',
    'range_5_7p': '5-7',
    'range_7_9p': '7-9',
    'range_10_19p': '10-19',
    'limit_up': '涨停'
}

TIME_PERIOD_MAP = {
    # 'm1': '近1月',
    # 'm3': '3月',
    # 'm6': '6月',
    # 'y1': '1年',
    'y2': '2年',
    # 数据量太大，暂时不分析
    # 'y3': '3年',
    # 'y4': '4年',
    # 'y5': '5年'
}

TIME_FREQ_MAP = {
    'auction': '竞价',
    '1min': '1分钟',
    '5min': '5分钟',
    '15min': '15分钟',
    '30min': '30分钟',
    '60min': '1小时',
}

# 涨跌幅分类
PRICE_CHANGE_CATEGORIES = {
    'micro_up': '微涨(涨幅<1%)',
    'small_up': '小涨(涨幅<3%)',
    'medium_up': '中涨(涨幅<5%)',
    'large_up': '大涨(涨幅<7%)',
    'limit_up': '涨停(涨幅>=7%)',
    'flat': '平开(涨幅=0%)',
    'small_down': '小跌(跌幅<-1%)',
    'medium_down': '中跌(跌幅<-3%)',
    'large_down': '大跌(跌幅<-5%)',
    'limit_down': '跌停(跌幅<=-5%)'
}

# 涨跌幅场景描述
SCENARIO_DESCRIPTIONS = {
    'micro_up': '今天微涨(0 ~ 1%)，明天概率情况',
    'small_up': '今天小涨(1% ~ 3%)，明天概率情况',
    'medium_up': '今天中涨(3% ~ 5%)，明天概率情况',
    'large_up': '今天大涨(7% ~ 涨停)，明天概率情况',
    'limit_up': '今天涨停()涨停，明天概率情况',
    'flat': '今天平开(涨幅=0%)  ，明天概率情况',
    'small_down': '今天小跌(-1% ~ 0)，明天概率情况',
    'medium_down': '今天中跌(-3% ~ -1%)，明天概率情况',
    'large_down': '今天大跌(-5% ~ -3%)，明天概率情况',
    'limit_down': '今天跌停(-5% ~ -7%)，明天概率情况',
    'range_1_3p': '今天1-3%，明天概率情况',
    'range_3_5p': '今天3-5%，明天概率情况',
    'range_5_7p': '今天5-7%，明天概率情况',
    'range_7_9p': '今天7-9%，明天概率情况',
    'range_10_19p': '今天10-19%，明天概率情况'
}

def get_stock_list() -> pd.DataFrame:
    """获取所有股票列表"""
    try:
        # 获取所有上市股票
        stocks = pro.stock_basic(exchange='', list_status='L', 
                                fields='ts_code,symbol,name,area,industry,market,list_date')
        logger.info("获取股票列表成功，共%s条记录", len(stocks))
        return stocks
    except Exception as e:
        logger.error(f"获取股票列表失败: {e}")
        return pd.DataFrame()

def filter_stocks(stocks: pd.DataFrame) -> pd.DataFrame:
    """过滤股票列表
    
    过滤条件:
    1. 排除北交所股票(ts_code以.BJ结尾)
    2. 市值在10亿到1000亿之间
    """
    try:
        # 排除北交所股票
        filtered_stocks = stocks[~stocks['ts_code'].str.endswith('.BJ')]
        logger.info("排除北交所股票后，剩余%s条记录", len(filtered_stocks))
        # 排除科创板 ts_code 688开头
        filtered_stocks = filtered_stocks[~filtered_stocks['ts_code'].str.startswith('688')]
        logger.info("排除科创板后，剩余%s条记录", len(filtered_stocks))
        # 排除 st *st *st
        filtered_stocks = filtered_stocks[~filtered_stocks['name'].str.contains('ST')]
        logger.info("排除ST后，剩余%s条记录", len(filtered_stocks))
        
        # 获取最新交易日期
        latest_trade_date = pro.trade_cal(exchange='', start_date=datetime.datetime.now().strftime('%Y%m%d'), 
                                         end_date=datetime.datetime.now().strftime('%Y%m%d'), 
                                         is_open='1')['cal_date'].values
        
        if len(latest_trade_date) == 0:
            # 如果今天不是交易日，获取最近的交易日
            latest_trade_date = pro.trade_cal(exchange='', end_date=datetime.datetime.now().strftime('%Y%m%d'), 
                                             is_open='1')['cal_date'].iloc[-1]
        else:
            latest_trade_date = latest_trade_date[0]

        # !!! 取 20250307
        latest_trade_date = '20250307'
        
        
        try:
            # 检查是否有足够的积分调用daily_basic接口
            # 如果没有足够的积分，我们将使用stock_basic接口的数据
            # 注意：这种情况下我们将无法获取市值数据，但至少可以获取股票列表
            
            # 尝试获取市值数据
            market_values = []
            # 分批获取，避免一次性请求过多
            batch_size = 1000
            for i in range(0, len(filtered_stocks), batch_size):
                batch = filtered_stocks.iloc[i:i+batch_size]
                ts_codes = ','.join(batch['ts_code'].tolist())
                try:
                    logger.info("尝试获取第%s批股票的市值数据，共%s只股票", i//batch_size + 1, len(batch))
                    mv_data = pro.daily_basic(ts_code=ts_codes, trade_date=latest_trade_date)
                    
                    if mv_data.empty:
                        logger.warning("第%s批股票的市值数据为空", i//batch_size + 1)
                    else:
                        logger.info("成功获取第%s批股票的市值数据，共%s条记录", i//batch_size + 1, len(mv_data))
                        market_values.append(mv_data)
                    
                    # 避免频繁请求
                    time.sleep(0.01)
                except Exception as e:
                    logger.error("获取第%s批股票的市值数据失败: %s", i//batch_size + 1, e)
                    continue
            
            if not market_values:
                logger.warning("未获取到市值数据，可能是因为Tushare积分不足（需要至少2000积分）")
                logger.warning("将使用股票基本信息，不包含市值数据")
                # 如果无法获取市值数据，我们将使用原始的股票列表
                result = filtered_stocks
            else:
                # 过滤掉空的DataFrame，避免FutureWarning
                non_empty_market_values = [df for df in market_values if not df.empty]
                if not non_empty_market_values:
                    logger.warning("所有市值数据都为空")
                    result = filtered_stocks
                else:
                    market_value_df = pd.concat(non_empty_market_values, ignore_index=True)
                    
                    # 合并股票信息和市值数据
                    merged_df = pd.merge(filtered_stocks, market_value_df, on='ts_code', how='left')
                    
                    # 保存合并后的数据到CSV文件，方便调试
                    # merged_df.to_csv('data/merged_stocks.csv', index=False, encoding='utf-8-sig')
                    logger.info("已将合并后的数据保存到data/merged_stocks.csv，共%s条记录", len(merged_df))
                    
                    # 过滤市值在10 * 10000到5000 * 10000 之间的股票单位 万元 10亿到 300亿
                    low_mv = 30 * 10000
                    high_mv = 222 * 10000
                    # 注意：total_mv和circ_mv单位为万元，需要转换
                    # 只有在市值数据不为空的情况下才进行过滤
                    if 'total_mv' in merged_df.columns and not merged_df['total_mv'].isna().all():
                        # 保存过滤前的数据
                        # merged_df.to_csv('data/before_filter.csv', index=False, encoding='utf-8-sig')
                        
                        # 过滤市值
                        result = merged_df[(merged_df['total_mv'] >= low_mv) & (merged_df['total_mv'] <= high_mv)]
                        
                        # 保存过滤后的数据
                        # result.to_csv('data/after_filter.csv', index=False, encoding='utf-8-sig')
                        
                        logger.info("过滤市值后，剩余%s条记录", len(result))
                       
                        
                        # 保存被过滤掉的数据
                        filtered_out = merged_df[~merged_df.index.isin(result.index)]
                        filtered_out.to_csv('data/市值小于30亿或者大于222亿之间的股票.csv', index=False, encoding='utf-8-sig')
                        logger.info("被过滤掉的记录已保存到data/市值小于30亿或者大于222亿之间的股票.csv，共%s条记录", len(filtered_out))
                        # 过滤流通性 保留流通市值/总市值 > 0.7 小于的 则过滤掉
                        result = result[result['circ_mv'] / result['total_mv'] > 0.7]
                        logger.info("过滤流通性后，剩余%s条记录", len(result))
                        # 保存过滤后的数据
                        result.to_csv('data/市值30亿_222亿之间流通性大于70%的股票.csv', index=False, encoding='utf-8-sig')
                        logger.info("过滤后数据已保存到data/市值30亿_222亿之间流通性大于70%的股票.csv，共%s条记录", len(result))
                        # 过滤掉特定股票 比如 负面新闻
                        bad_ts_codes = ["600811.SH"]
                        # 存在于 bad_ts_codes 的 则过滤掉
                        result = result[~result['ts_code'].isin(bad_ts_codes)]
                        logger.info("过滤特定股票后，剩余%s条记录", len(result))
                       
                    else:
                        logger.warning("市值数据为空，无法进行市值过滤")
                        result = merged_df
            
            # 如果结果为空，则返回原始的股票列表
            if result.empty:
                logger.warning("过滤后的结果为空，将返回原始的股票列表")
                result = filtered_stocks
            
            logger.info("最终返回%s条股票记录", len(result))
            return result
        except Exception as e:
            logger.error("过滤股票失败: %s", e)
            # 如果发生错误，至少返回原始的股票列表
            return filtered_stocks
    except Exception as e:
        logger.error("过滤股票失败: %s", e)
        return pd.DataFrame()

def get_stock_daily_data(ts_code: str, start_date: str = '20150101', end_date: Optional[str] = None) -> pd.DataFrame:
    """获取股票日线数据"""
    try:
        if end_date is None:
            end_date = datetime.datetime.now().strftime('%Y%m%d')
        
        # 获取每日指标数据
        daily_data = pro.daily_basic(ts_code=ts_code, start_date=start_date, end_date=end_date,
                                    fields='ts_code,trade_date,close,turnover_rate,volume_ratio,pe,pb,total_mv,circ_mv,pct_chg')
        
        # 获取日线行情数据
        daily_price = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date,
                               fields='ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount')
        
        # 合并数据
        result = pd.merge(daily_data, daily_price, on=['ts_code', 'trade_date'], how='left', suffixes=('', '_price'))
        
        logger.info("获取股票%s日线数据成功，共%s条记录", ts_code, len(result))
        return result
    except Exception as e:
        logger.error("获取股票%s日线数据失败: %s", ts_code, e)
        return pd.DataFrame()

def categorize_pct_change(pct_chg: float) -> str:
    """根据涨跌幅分类"""
    if pct_chg >= 9.5:  # 涨停通常为10%，但考虑到一些误差
        return 'limit_up'
    elif 7 <= pct_chg < 9.5:
        return 'range_7_9p'
    elif 5 <= pct_chg < 7:
        return 'range_5_7p'
    elif 3 <= pct_chg < 5:
        return 'range_3_5p'
    elif 1 <= pct_chg < 3:
        return 'range_1_3p'
    elif 0 < pct_chg < 1:
        return 'micro_up'
    elif pct_chg == 0:
        return 'flat'
    elif -1 < pct_chg < 0:
        return 'small_down'
    elif -3 < pct_chg <= -1:
        return 'medium_down'
    elif -5 < pct_chg <= -3:
        return 'limit_down'
    else:  # pct_chg <= -5
        return 'large_down'

def get_auction_data(ts_code: str, trade_date: str) -> pd.DataFrame:
    """获取股票竞价数据"""
    try:
        # 使用请求限制器，确保不超过API限制
        stk_auction_limiter.wait_if_needed()
        
        auction_data = pro.stk_auction_o(ts_code=ts_code, trade_date=trade_date)
        return auction_data
    except Exception as e:
        logger.error("获取股票%s竞价数据失败: %s", ts_code, e)
        return pd.DataFrame()

def get_minutes_data(ts_code: str, trade_date: str, freq: int = 1) -> pd.DataFrame:
    """获取股票分钟行情数据"""
    try:
        # 使用请求限制器，确保不超过API限制
        stk_mins_limiter.wait_if_needed()
        
        # 转换日期格式
        date_obj = datetime.datetime.strptime(trade_date, '%Y%m%d')
        start_time = f"{date_obj.strftime('%Y-%m-%d')} 09:30:00"
        end_time = f"{date_obj.strftime('%Y-%m-%d')} 11:00:00"
        
        # 一次性请求整个交易日的分钟数据
        minute_data = pro.stk_mins(ts_code=ts_code, freq='1min', start_date=start_time, end_date=end_time)
        return minute_data
    except Exception as e:
        logger.error("获取股票%s分钟行情数据失败: %s", ts_code, e)
        return pd.DataFrame()

def process_minutes_data(full_minute_data: pd.DataFrame, freq: str) -> pd.DataFrame:
    """根据不同的时间频率处理分钟数据
    
    Args:
        full_minute_data: 完整的分钟数据
        freq: 时间频率，如'1min', '5min', '15min', '30min', '60min'
    
    Returns:
        处理后的特定时间段的数据
    """
    if full_minute_data.empty:
        return pd.DataFrame()
    
    try:
        # 确保数据按时间排序（从早到晚）
        full_minute_data = full_minute_data.sort_values(by='trade_time', ascending=True)
        
        # 将trade_time列转换为datetime对象
        full_minute_data['trade_time'] = pd.to_datetime(full_minute_data['trade_time'])
        
        # 获取第一条记录的时间
        first_time = full_minute_data['trade_time'].iloc[0]
        
        # 根据不同的时间频率截取数据
        if freq == '1min':
            # 只取第一分钟的数据
            return full_minute_data.head(1)
        elif freq == '5min':
            # 取前5分钟的数据 (09:30:00 - 09:35:00)
            end_time = datetime.datetime(
                first_time.year, first_time.month, first_time.day,
                first_time.hour, 35, 0
            )
            return full_minute_data[full_minute_data['trade_time'] <= end_time]
        elif freq == '15min':
            # 取前15分钟的数据 (09:30:00 - 09:45:00)
            end_time = datetime.datetime(
                first_time.year, first_time.month, first_time.day,
                first_time.hour, 45, 0
            )
            return full_minute_data[full_minute_data['trade_time'] <= end_time]
        elif freq == '30min':
            # 取前30分钟的数据 (09:30:00 - 10:00:00)
            end_time = datetime.datetime(
                first_time.year, first_time.month, first_time.day,
                10, 0, 0
            )
            return full_minute_data[full_minute_data['trade_time'] <= end_time]
        elif freq == '60min':
            # 取前60分钟的数据 (09:30:00 - 10:30:00)
            end_time = datetime.datetime(
                first_time.year, first_time.month, first_time.day,
                10, 30, 0
            )
            return full_minute_data[full_minute_data['trade_time'] <= end_time]
        else:
            logger.warning("不支持的时间频率: %s", freq)
            return pd.DataFrame()
    except Exception as e:
        logger.error("处理%s分钟数据失败: %s", freq, e)
        traceback.print_exc()  # 添加详细的错误跟踪
        return pd.DataFrame()

def calculate_probability(stock_data: pd.DataFrame, time_period: str, circ_mv: float) -> Dict[str, Dict[str, Dict[str, float]]]:
    """计算不同涨幅区间对应的第二天涨跌概率
    
    Args:
        stock_data: 股票数据
        time_period: 时间周期，如'm1', 'm3', 'm6', 'y1'等
    
    Returns:
        概率统计结果
    """
    try:
        # 根据时间周期筛选数据
        end_date = stock_data['trade_date'].max()
        if time_period.startswith('m'):
            months = int(time_period[1:])
            start_date = (datetime.datetime.strptime(end_date, '%Y%m%d') - 
                          datetime.timedelta(days=30*months)).strftime('%Y%m%d')
        elif time_period.startswith('y'):
            years = int(time_period[1:])
            start_date = (datetime.datetime.strptime(end_date, '%Y%m%d') - 
                          datetime.timedelta(days=365*years)).strftime('%Y%m%d')
        else:
            logger.error("不支持的时间周期: %s", time_period)
            return {}
        period_data = stock_data[stock_data['trade_date'] >= start_date].copy()
        
        if period_data.empty:
            logger.warning("时间周期%s内没有数据", time_period)
            return {}
        
        # 按涨跌幅分类
        period_data['pct_chg_category'] = period_data['pct_chg'].apply(categorize_pct_change)
        # 计算第2 个交易日的数据
        period_data['next_trade_date'] = period_data['trade_date'].shift(1)
        
        # 初始化结果字典
        result = {}
        
        # 获取唯一的交易日期和股票代码
        next_trade_dates = period_data['next_trade_date'].dropna().unique()
        ts_code = period_data['ts_code'].iloc[0]  # 假设所有行的ts_code都相同
       
        
        logger.info("预先批量获取竞价和分钟数据，共%s个交易日", len(next_trade_dates))
        
        # 使用线程安全的字典
        auction_cache = {}
        minute_data_cache = {
            '1min': {},
            '5min': {},
            '15min': {},
            '30min': {},
            '60min': {}
        }
        
        # 批量处理，每批100个交易日
        batch_size = 100
        
        # 批量获取竞价数据
        batch_start_time = time.time()
        for i in range(0, len(next_trade_dates), batch_size):
            batch_dates = next_trade_dates[i:i+batch_size]
            logger.info("批量获取竞价数据，批次%s/%s，共%s个交易日", 
                       i//batch_size + 1, (len(next_trade_dates) + batch_size - 1)//batch_size, len(batch_dates))
            
            # 使用多线程并行获取该批次的竞价数据
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(10, len(batch_dates))) as executor:
                future_to_date = {executor.submit(get_auction_data, ts_code, date): date for date in batch_dates}
                for future in concurrent.futures.as_completed(future_to_date):
                    date = future_to_date[future]
                    try:
                        auction_cache[date] = future.result()
                    except Exception as e:
                        logger.error("获取交易日%s的竞价数据失败: %s", date, e)
            
            # 添加延迟，避免请求过快
            time.sleep(1)
        
        logger.info("批量获取竞价数据完成，耗时: %s秒", time.time() - batch_start_time)
        
        # 批量获取分钟数据
        batch_start_time = time.time()
        logger.info("开始批量获取分钟数据")

        for i in range(0, len(next_trade_dates), batch_size):
            batch_dates = next_trade_dates[i:i+batch_size]
            logger.info("批量获取分钟数据，批次%s/%s，共%s个交易日", 
                       i//batch_size + 1, (len(next_trade_dates) + batch_size - 1)//batch_size, len(batch_dates))
            
            # 使用多线程并行获取该批次的分钟数据
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(10, len(batch_dates))) as executor:
                future_to_date = {executor.submit(get_minutes_data, ts_code, date): date for date in batch_dates}
                for future in concurrent.futures.as_completed(future_to_date):
                    date = future_to_date[future]
                    try:
                        # 获取完整的分钟数据
                        full_minute_data = future.result()
                        
                        # 根据不同的时间频率处理数据并存入缓存
                        for time_key in ['1min', '5min', '15min', '30min', '60min']:
                            minute_data_cache[time_key][date] = process_minutes_data(full_minute_data, time_key)
                    except Exception as e:
                        logger.error("获取或处理交易日%s的分钟数据失败: %s", date, e)
            
            # 添加延迟，避免请求过快
            time.sleep(1)

        logger.info("批量获取分钟数据完成，耗时: %s秒", time.time() - batch_start_time)
        
        # 检查数据获取情况
        logger.info("竞价数据获取情况: 共%s/%s个交易日有数据", 
                   sum(1 for d in auction_cache.values() if not d.empty), len(next_trade_dates))
        
        for time_key in minute_data_cache:
            logger.info("%s数据获取情况: 共%s/%s个交易日有数据", 
                       time_key, sum(1 for d in minute_data_cache[time_key].values() if not d.empty), len(next_trade_dates))
        
        # 遍历每个涨跌幅分类
        for category in period_data['pct_chg_category'].unique():
            category_data = period_data[period_data['pct_chg_category'] == category]
            
            # 初始化该分类的结果
            result[category] = {
                'auction': {'up': 0, 'down': 0, 'equal': 0, 'total': 0, 'volume_ratio': 0},
                '1min': {'up': 0, 'down': 0, 'equal': 0, 'total': 0, 'max_pct': 0, 'min_pct': 0, 'close_pct': 0, 'max_pct_sum': 0, 'min_pct_sum': 0, 'close_pct_sum': 0},
                '5min': {'up': 0, 'down': 0, 'equal': 0, 'total': 0, 'max_pct': 0, 'min_pct': 0, 'close_pct': 0, 'max_pct_sum': 0, 'min_pct_sum': 0, 'close_pct_sum': 0},
                '15min': {'up': 0, 'down': 0, 'equal': 0, 'total': 0, 'max_pct': 0, 'min_pct': 0, 'close_pct': 0, 'max_pct_sum': 0, 'min_pct_sum': 0, 'close_pct_sum': 0},
                '30min': {'up': 0, 'down': 0, 'equal': 0, 'total': 0, 'max_pct': 0, 'min_pct': 0, 'close_pct': 0, 'max_pct_sum': 0, 'min_pct_sum': 0, 'close_pct_sum': 0},
                '60min': {'up': 0, 'down': 0, 'equal': 0, 'total': 0, 'max_pct': 0, 'min_pct': 0, 'close_pct': 0, 'max_pct_sum': 0, 'min_pct_sum': 0, 'close_pct_sum': 0}
            }
            
            # 遍历该分类的每一天
            iterrows = category_data.iterrows()
            iterrows_count = 0
            
            for _, row in iterrows:
                iterrows_count += 1
                if pd.isna(row['next_trade_date']):
                    continue
                
                next_trade_date = row['next_trade_date']
                prev_close = row['close']
                
                # 使用缓存的竞价数据
                auction_data = auction_cache.get(next_trade_date, pd.DataFrame())
                
                if not auction_data.empty:
                    # 计算竞价涨跌
                    auction_open = auction_data['open'].iloc[0]
                    
                    if auction_open > prev_close:
                        result[category]['auction']['up'] += 1
                    elif auction_open < prev_close:
                        result[category]['auction']['down'] += 1
                    else:
                        result[category]['auction']['equal'] += 1
                    
                    result[category]['auction']['total'] += 1
                    # 计算集合竞价最大涨幅
                    result[category]['auction']['max_pct'] = round((auction_data['high'].iloc[0] - prev_close) / prev_close * 100, 2)
                    # 计算集合竞价最小涨幅
                    result[category]['auction']['min_pct'] = round((auction_data['low'].iloc[0] - prev_close) / prev_close * 100, 2)
                    # 计算集合竞价收盘涨幅
                    result[category]['auction']['close_pct'] = round((auction_data['close'].iloc[0] - prev_close) / prev_close * 100, 2)
                    # 计算该时间区间内 成交量占据流通市值的百分比 保留 2位小数
                    result[category]['auction']['volume_ratio'] = round(auction_data['amount'].iloc[0] / circ_mv, 2)
                
                # 使用缓存的分钟数据
                for time_key in ['1min', '5min', '15min', '30min', '60min']:
                    minute_data = minute_data_cache[time_key].get(next_trade_date, pd.DataFrame())
                    calculate_minutes_data(minute_data, category, time_key, result, row)
            
            
            
            # 计算概率
            for time_key in result[category]:
                total = result[category][time_key]['total']
                if total > 0:
                    result[category][time_key]['up_prob'] = round(result[category][time_key]['up'] / total * 100, 2)
                    result[category][time_key]['down_prob'] = round(result[category][time_key]['down'] / total * 100, 2)
                    result[category][time_key]['equal_prob'] = round(result[category][time_key]['equal'] / total * 100, 2)
                    
                    # 计算平均涨跌幅（仅对非1min和非auction的数据）
                    if time_key not in ['1min', 'auction'] and 'max_pct_sum' in result[category][time_key]:
                        result[category][time_key]['max_pct'] = round(result[category][time_key]['max_pct_sum'] / total, 2)
                        result[category][time_key]['min_pct'] = round(result[category][time_key]['min_pct_sum'] / total, 2)
                        result[category][time_key]['close_pct'] = round(result[category][time_key]['close_pct_sum'] / total, 2)
        
        return result
    except Exception as e:
        logger.error("计算概率失败: %s", e)
        traceback.print_exc()  # 打印完整的堆栈跟踪
        return {}

def calculate_minutes_data(minute_data: pd.DataFrame, category: str, time_key: str, result: Dict[str, Dict[str, Dict[str, float]]], row: pd.Series) -> pd.DataFrame:
    """计算分钟数据"""
    try:
        if not minute_data.empty:   
            max_price = minute_data['high'].max()
            min_price = minute_data['low'].min()
            
            # 计算1分钟数据的涨跌, 取最后一条数据
            # 如果是1min 则取第一条
            if time_key == '1min':
                minute_close = minute_data['close'].iloc[0]
            else:
                minute_close = minute_data['close'].iloc[-1]
                # 计算最大涨幅、最小涨幅和收盘涨幅
                prev_close = row['close']
                
                # 计算最大涨幅（使用high列的最大值）
                
                max_pct_change = (max_price - prev_close) / prev_close * 100
                
                # 计算最小涨幅（使用low列的最小值）
                
                min_pct_change = (min_price - prev_close) / prev_close * 100
                
                # 计算收盘涨幅
                close_pct_change = (minute_close - prev_close) / prev_close * 100
                # 累加涨跌幅，用于后续计算平均值
                result[category][time_key]['max_pct_sum'] =  max(result[category][time_key]['max_pct_sum'], max_pct_change)
                result[category][time_key]['min_pct_sum'] =  min(result[category][time_key]['min_pct_sum'], min_pct_change)
                result[category][time_key]['close_pct_sum'] = max(result[category][time_key]['close_pct_sum'], close_pct_change)

            prev_close = row['close']
            
            if minute_close > prev_close:
                result[category][time_key]['up'] += 1
               
            elif minute_close < prev_close:
                result[category][time_key]['down'] += 1
            else:
                result[category][time_key]['equal'] += 1
            
            result[category][time_key]['total'] += 1
    except Exception as e:
        logger.error("计算分钟数据失败: %s", e)
        return pd.DataFrame()
    
def save_probability_to_csv(ts_code: str, probability_data: Dict[str, Dict[str, Dict[str, float]]], time_period: str, stock_name: str):
    """将概率数据保存到CSV文件"""
    try:
        # 创建数据目录
        data_dir = os.getenv('DATA_DIR', './data')
        os.makedirs(data_dir, exist_ok=True)
        
        # 创建CSV文件
        file_path = os.path.join(data_dir, f"{ts_code}_{time_period}_probability.csv")
        
        # 准备数据
        rows = []
        for category, time_data in probability_data.items():
            for time_key, prob_data in time_data.items():
             
                row = {
                    '股票代码': ts_code,
                    '股票名称': stock_name,
                    '当日涨幅': LIST_RANGE_MAP.get(category, category),
                    '场景描述': SCENARIO_DESCRIPTIONS.get(category, f"今天{category}，明天概率情况"),
                    '时间段': TIME_FREQ_MAP.get(time_key, time_key),
                    '涨概率': prob_data.get('up_prob', 0),
                    '跌概率': prob_data.get('down_prob', 0),
                    '平概率': prob_data.get('equal_prob', 0),
                    '最大涨幅': prob_data.get('max_pct', 0),
                    '最小涨幅': prob_data.get('min_pct', 0),
                    '收盘涨幅': prob_data.get('close_pct', 0),   
                    # 计算该时间区间内 成交量占据流通市值的百分比
                    '成交量占比': prob_data.get('volume_ratio', 0),
                    '样本数': prob_data.get('total', 0),
                }
                
                rows.append(row)
        
        # 创建DataFrame并保存
        df = pd.DataFrame(rows)
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
        logger.info("概率数据已保存到%s", file_path)
        
        return file_path
    except Exception as e:
        logger.error("保存概率数据失败: %s", e)
        return None

def analyze_stock(ts_code: str, stock_name: str, circ_mv: float) -> Dict[str, Any]:
    """分析股票数据，计算不同时间维度的涨跌概率"""
    try:
        # 检查本地是否已有分析结果
        data_dir = os.getenv('DATA_DIR', './data')
        results = {}
        
        # 获取股票日线数据
        stock_data = get_stock_daily_data(ts_code)
        
        if stock_data.empty:
            return {"error": f"获取股票{ts_code}数据失败"}
        
        # 计算不同时间维度的概率
        for time_period in TIME_PERIOD_MAP.keys():
            # 计算分析耗时
            start_time = time.time()
            # 检查本地是否已有该时间维度的分析结果
            file_path = os.path.join(data_dir, f"{ts_code}_{time_period}_probability.csv")
            
            # if os.path.exists(file_path):
            #     # 如果文件存在且是今天生成的，直接读取
            #     file_time = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
            #     if file_time.date() == datetime.datetime.now().date():
            #         df = pd.read_csv(file_path, encoding='utf-8-sig')
                   
            #         # 转换为字典格式
            #         period_result = {}
            #         for category in df['当日涨幅'].unique():
            #             category_data = df[df['当日涨幅'] == category]
            #             period_result[category] = {}
                        
            #             for _, row in category_data.iterrows():
            #                 time_key = next((k for k, v in TIME_FREQ_MAP.items() if v == row['时间段']), row['时间段'])
            #                 period_result[category][time_key] = {
            #                     'up_prob': row['涨概率'],
            #                     'down_prob': row['跌概率'],
            #                     'equal_prob': row['平概率'],
            #                     'max_pct': row['最大涨幅'],
            #                     'min_pct': row['最小涨幅'],
            #                     'close_pct': row['收盘涨幅'],
            #                     'total': row['样本数']
            #                 }
                    
            #         results[time_period] = period_result
            #         continue
            
            # 计算概率
            probability = calculate_probability(stock_data, time_period, circ_mv)
            
            if probability:
                # 保存到CSV
                save_probability_to_csv(ts_code, probability, time_period, stock_name)
                results[time_period] = probability
            # 计算分析耗时, 猜测加粗打印
            end_time = time.time()
            logger.info("分析股票%s %s 耗时: %s秒", ts_code, time_period, end_time - start_time)
        return results
    except Exception as e:
        # 打印完成错误堆栈
        print(traceback.format_exc())
        logger.error("分析股票%s失败: %s", ts_code, e)
        return {"error": str(e)}

def get_stock_probability_by_pct(ts_code: str, pct_chg: float) -> Dict[str, Dict[str, float]]:
    """
    获取特定股票在特定涨幅范围内的平均概率
    
    Args:
        ts_code: 股票代码
        pct_chg: 涨跌幅百分比，例如4.75
        
    Returns:
        所有时间段的平均概率数据
    """
    try:
        # 根据涨跌幅确定对应的分类
        category = categorize_pct_change(pct_chg)
        
        # 获取对应的涨幅区间显示值
        display_range = LIST_RANGE_MAP.get(category, category)
        
        # 创建数据目录
        data_dir = os.getenv('DATA_DIR', './data')
        
        # 查找所有该股票的概率文件
        probability_files = glob.glob(os.path.join(data_dir, f"{ts_code}_*_probability.csv"))
        
        if not probability_files:
            logger.warning("未找到股票%s的概率数据文件", ts_code)
            return {}
        
        # 初始化结果字典 - 用于存储所有时间段的总和
        total_up_prob = 0
        total_down_prob = 0
        total_equal_prob = 0
        total_samples = 0
        total_time_periods = 0
        max_pct = 0
        min_pct = 0
        close_pct = 0
        
        # 遍历所有概率文件
        for file_path in probability_files:
            try:
                # 读取CSV文件
                df = pd.read_csv(file_path)
                
                # 筛选特定涨幅区间的数据
                filtered_df = df[df['当日涨幅'] == display_range]
                
                if filtered_df.empty:
                    logger.warning("文件%s中没有涨幅为%s的数据", file_path, display_range)
                    continue
                
                # 计算所有时间段的总和
                for _, row in filtered_df.iterrows():
                    total_up_prob += row['涨概率']
                    total_down_prob += row['跌概率']
                    total_equal_prob += row['平概率']
                    total_samples += row['样本数']
                    total_time_periods += 1
                    # 如果列不存在，则设置为0
                    max_pct = max(max_pct, row.get('最大涨幅', 0))
                    min_pct = min(min_pct, row.get('最小涨幅', 0))
                    close_pct = row.get('收盘涨幅', 0)
                    
            except Exception as e:
                logger.error("处理文件%s时出错: %s", file_path, e)
                continue
        
        # 计算平均概率
        result = {}
        if total_time_periods > 0:
            result = {
                'up_prob': round(total_up_prob / total_time_periods, 2),
                'down_prob': round(total_down_prob / total_time_periods, 2),
                'equal_prob': round(total_equal_prob / total_time_periods, 2),
                'avg_total': round(total_samples / total_time_periods, 2),
                'max_pct': max_pct,
                'min_pct': min_pct,
                'close_pct': close_pct
            }
    
        return result
    
    except Exception as e:
        logger.error("获取股票%s在涨幅%s下的平均概率失败: %s", ts_code, pct_chg, e)
        return {} 