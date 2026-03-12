"""
网络请求模块 - 负责发送HTTP请求和获取页面内容
"""

import time
import random
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import HEADERS, REQUEST_TIMEOUT, MAX_RETRIES
from utils.logger import get_logger

logger = get_logger(__name__)


class Fetcher:
    """HTTP请求获取器"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        
        # 配置重试策略
        retry_strategy = Retry(
            total=MAX_RETRIES,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # 标记是否已初始化（访问首页建立会话）
        self._initialized = False
    
    def _init_session(self):
        """初始化会话，先访问首页建立Cookie"""
        if self._initialized:
            return
        try:
            # 先访问首页，建立正确的会话和Cookie
            self.session.get("https://tougao.12371.cn/", timeout=REQUEST_TIMEOUT)
            self._initialized = True
            logger.debug("Fetcher会话已初始化")
        except Exception as e:
            logger.warning(f"会话初始化失败: {e}")
    
    def get(self, url, params=None, **kwargs):
        """
        发送GET请求
        
        Args:
            url: 请求URL
            params: URL参数
            **kwargs: 其他requests参数
            
        Returns:
            Response: 响应对象，失败返回None
        """
        # 确保会话已初始化
        self._init_session()
        
        try:
            # 随机延迟，避免请求过快
            time.sleep(random.uniform(0.5, 1.5))
            
            response = self.session.get(
                url,
                params=params,
                timeout=REQUEST_TIMEOUT,
                **kwargs
            )
            response.raise_for_status()
            response.encoding = response.apparent_encoding or 'utf-8'
            logger.debug(f"成功获取页面: {url}")
            return response
            
        except requests.exceptions.RequestException as e:
            logger.error(f"请求失败 {url}: {str(e)}")
            return None
    
    def close(self):
        """关闭session"""
        self.session.close()
        logger.info("Fetcher session已关闭")
