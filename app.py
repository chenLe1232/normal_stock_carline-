import os
import uvicorn
from fastapi import FastAPI
from app.routes.stock_routes import router as stock_router
from dotenv import load_dotenv
import logging
from contextlib import asynccontextmanager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("app.log")
    ]
)
logger = logging.getLogger(__name__)

# 加载环境变量
load_dotenv()

# 创建数据目录
data_dir = os.getenv('DATA_DIR', './data')
os.makedirs(data_dir, exist_ok=True)

# 定义生命周期事件
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动时执行
    logger.info("股票分析服务启动")
    # 检查Tushare Token是否配置
    tushare_token = os.getenv('TUSHARE_TOKEN')
    if not tushare_token:
        logger.warning("未配置Tushare Token，请在.env文件中设置TUSHARE_TOKEN")
    else:
        logger.info("Tushare Token已配置")
    
    yield
    
    # 关闭时执行
    logger.info("股票分析服务关闭")

# 创建FastAPI应用
app = FastAPI(
    title="股票分析服务",
    description="使用tushare分析股票数据的服务",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# 注册路由
app.include_router(stock_router, prefix="/api/stocks", tags=["stocks"])

@app.get("/", tags=["root"])
async def root():
    """根路由"""
    return {"message": "欢迎使用股票分析服务"}

if __name__ == "__main__":
    # 获取配置
    host = os.getenv('API_HOST', '0.0.0.0')
    port = int(os.getenv('API_PORT', '8000'))
    debug = os.getenv('API_DEBUG', 'false').lower() == 'true'
    
    logger.info(f"启动服务: host={host}, port={port}, debug={debug}")
    
    # 使用__name__:app而不是app:app
    uvicorn.run(
        f"{__name__}:app",
        host=host,
        port=port,
        reload=debug
    ) 