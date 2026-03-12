"""
爬虫配置文件
"""

import os

# 基础配置
BASE_URL = "https://tougao.12371.cn/liebiao.php?fid=111&typeid=53"
BASE_DOMAIN = "https://tougao.12371.cn"

# 请求配置
REQUEST_TIMEOUT = 30  # 请求超时时间（秒）
MAX_RETRIES = 3       # 最大重试次数

# 请求头
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

# 数据存储配置
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
ARTICLES_FILE = os.path.join(DATA_DIR, 'articles.json')
STATE_FILE = os.path.join(DATA_DIR, 'crawler_state.json')

# 爬取配置
CRAWL_INTERVAL_HOURS = 6  # 爬取间隔（小时）
MAX_PAGES_PER_RUN = 10    # 每次运行最大爬取页数

# 日志配置
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# 确保目录存在
def ensure_dirs():
    """确保必要的目录存在"""
    for dir_path in [DATA_DIR, LOG_DIR]:
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
