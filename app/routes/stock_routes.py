from fastapi import APIRouter, HTTPException, Query
from typing import Dict, List, Any, Optional
from app.services.stock_service import StockService

router = APIRouter()

@router.get("/list")
async def get_stock_list() -> Dict[str, Any]:
    """获取过滤后的股票列表
    
    返回市值在10亿到1000亿之间的非北交所股票
    """
    stocks = StockService.get_filtered_stocks()
    
    if not stocks:
        return {"status": "error", "message": "获取股票列表失败", "data": []}
    
    return {
        "status": "success",
        "message": "获取股票列表成功",
        "data": stocks,
        "total": len(stocks)
    }

@router.get("/{ts_code}")
async def get_stock_info(ts_code: str) -> Dict[str, Any]:
    """获取股票基本信息
    
    Args:
        ts_code: 股票代码，如 000001.SZ
    """
    result = StockService.get_stock_info(ts_code)
    
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    
    return {
        "status": "success",
        "message": "获取股票信息成功",
        "data": result
    }

@router.get("/{ts_code}/probability")
async def get_stock_probability(
    ts_code: str,
    time_period: Optional[str] = Query(None, description="时间周期，如m1, m3, m6, y1等")
) -> Dict[str, Any]:
    """获取股票涨跌概率
    
    Args:
        ts_code: 股票代码，如 000001.SZ
        time_period: 时间周期，如m1, m3, m6, y1等，不指定则返回所有时间周期
    """
    result = StockService.get_stock_probability(ts_code)
    
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    
    # 如果指定了时间周期，只返回该时间周期的数据
    if time_period and time_period in result:
        return {
            "status": "success",
            "message": "获取股票涨跌概率成功",
            "data": {time_period: result[time_period]}
        }
    
    return {
        "status": "success",
        "message": "获取股票涨跌概率成功",
        "data": result
    } 