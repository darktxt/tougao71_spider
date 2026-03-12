"""
日志工具模块
"""

import logging
import os
from datetime import datetime
from config import LOG_DIR, LOG_LEVEL, LOG_FORMAT


def get_logger(name='crawler'):
    """
    获取配置好的日志记录器
    
    Args:
        name: 日志记录器名称
        
    Returns:
        logging.Logger: 配置好的日志记录器
    """
    # 确保日志目录存在
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    
    # 创建日志记录器
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, LOG_LEVEL))
    
    # 避免重复添加处理器
    if logger.handlers:
        return logger
    
    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(LOG_FORMAT)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # 文件处理器
    log_file = os.path.join(LOG_DIR, f'{name}_{datetime.now().strftime("%Y%m%d")}.log')
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(getattr(logging, LOG_LEVEL))
    file_formatter = logging.Formatter(LOG_FORMAT)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    return logger
