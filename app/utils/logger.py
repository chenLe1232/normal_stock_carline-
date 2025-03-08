import os
import logging

def setup_logger(name):
    # 创建日志目录
    log_dir = os.getenv('LOG_DIR', './logs')
    os.makedirs(log_dir, exist_ok=True)
    
    # 配置日志格式
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # 获取logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # 防止重复添加处理器
    if not logger.handlers:
        # 添加文件处理器
        file_handler = logging.FileHandler(os.path.join(log_dir, 'app.log'))
        file_handler.setFormatter(logging.Formatter(log_format))
        logger.addHandler(file_handler)
        
        # 添加控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(log_format))
        logger.addHandler(console_handler)
    
    # 确保日志不会被重复输出
    logger.propagate = False
    
    return logger 