import os
import pandas as pd
from typing import Dict, List, Any, Optional
import logging
from app.utils.tushare_utils import (
    get_stock_list, filter_stocks, analyze_stock,
    TIME_PERIOD_MAP, LIST_RANGE_MAP, TIME_FREQ_MAP
)
import datetime

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StockService:
    """股票服务类"""
    
    @staticmethod
    def get_filtered_stocks() -> List[Dict[str, Any]]:
        """获取过滤后的股票列表"""
        try:
            # 检查本地是否已有缓存
            data_dir = os.getenv('DATA_DIR', './data')
            cache_file = os.path.join(data_dir, 'filtered_stocks.csv')
            
            if os.path.exists(cache_file):
                # 读取缓存
                df = pd.read_csv(cache_file, encoding='utf-8-sig')
                return df.to_dict('records')
            
            # 获取所有股票
            stocks = get_stock_list()
            
            if stocks.empty:
                return []
            
            # 过滤股票
            filtered_stocks = filter_stocks(stocks)
            
            if filtered_stocks.empty:
                return []
            
            # 保存到缓存
            os.makedirs(data_dir, exist_ok=True)
            filtered_stocks.to_csv(cache_file, index=False, encoding='utf-8-sig')
            
            return filtered_stocks.to_dict('records')
        except Exception as e:
            logger.error(f"获取过滤后的股票列表失败: {e}")
            return []
    
    @staticmethod
    def get_stock_probability(ts_code: str) -> Dict[str, Any]:
        """获取股票涨跌概率"""
        try:
            # 分析股票
            result = analyze_stock(ts_code)
            
            if "error" in result:
                return {"error": result["error"]}
            
            # 格式化结果
            formatted_result = {}
            
            for time_period, period_data in result.items():
                formatted_result[time_period] = {
                    "period_name": TIME_PERIOD_MAP.get(time_period, time_period),
                    "categories": {}
                }
                
                for category, time_data in period_data.items():
                    category_name = LIST_RANGE_MAP.get(category, category)
                    formatted_result[time_period]["categories"][category] = {
                        "category_name": category_name,
                        "time_periods": {}
                    }
                    
                    for time_key, prob_data in time_data.items():
                        time_name = TIME_FREQ_MAP.get(time_key, time_key)
                        formatted_result[time_period]["categories"][category]["time_periods"][time_key] = {
                            "time_name": time_name,
                            "up_prob": prob_data.get("up_prob", 0),
                            "down_prob": prob_data.get("down_prob", 0),
                            "equal_prob": prob_data.get("equal_prob", 0),
                            "total": prob_data.get("total", 0)
                        }
            
            return formatted_result
        except Exception as e:
            logger.error(f"获取股票{ts_code}涨跌概率失败: {e}")
            return {"error": str(e)}
    
    @staticmethod
    def get_stock_info(ts_code: str) -> Dict[str, Any]:
        """获取股票基本信息"""
        try:
            # 首先尝试从过滤后的股票列表中查找
            stocks = StockService.get_filtered_stocks()
            
            # 查找指定股票
            for stock in stocks:
                if stock['ts_code'] == ts_code:
                    return {
                        "ts_code": stock['ts_code'],
                        "name": stock['name'],
                        "industry": stock.get('industry', ''),
                        "market": stock.get('market', ''),
                        "total_mv": stock.get('total_mv', 0),
                        "circ_mv": stock.get('circ_mv', 0)
                    }
            
            # 如果在过滤后的列表中找不到，直接从Tushare获取
            logger.info(f"在过滤后的列表中未找到股票{ts_code}，尝试直接从Tushare获取")
            try:
                from app.utils.tushare_utils import pro
                
                # 获取股票基本信息
                stock_info = pro.stock_basic(ts_code=ts_code, fields='ts_code,symbol,name,area,industry,market,list_date')
                
                if stock_info.empty:
                    return {"error": f"未找到股票{ts_code}"}
                
                stock = stock_info.iloc[0].to_dict()
                
                # 尝试获取市值信息
                try:
                    # 获取最新交易日期
                    latest_trade_date = pro.trade_cal(exchange='', end_date=datetime.datetime.now().strftime('%Y%m%d'), 
                                                    is_open='1')['cal_date'].iloc[-1]
                    
                    # 获取市值数据
                    mv_data = pro.daily_basic(ts_code=ts_code, trade_date=latest_trade_date, 
                                            fields='ts_code,total_mv,circ_mv')
                    
                    if not mv_data.empty:
                        stock.update(mv_data.iloc[0].to_dict())
                except Exception as e:
                    logger.warning(f"获取股票{ts_code}市值数据失败: {e}")
                
                return {
                    "ts_code": stock['ts_code'],
                    "name": stock['name'],
                    "industry": stock.get('industry', ''),
                    "market": stock.get('market', ''),
                    "total_mv": stock.get('total_mv', 0),
                    "circ_mv": stock.get('circ_mv', 0)
                }
            except Exception as e:
                logger.error(f"从Tushare获取股票{ts_code}信息失败: {e}")
                return {"error": f"未找到股票{ts_code}"}
        except Exception as e:
            logger.error(f"获取股票{ts_code}基本信息失败: {e}")
            return {"error": str(e)} 