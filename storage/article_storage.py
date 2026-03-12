"""
文章存储模块 - 高性能分片存储实现
支持百万级数据的高效存取
"""

import json
import os
from datetime import datetime
from config import ARTICLES_FILE, STATE_FILE, DATA_DIR, ensure_dirs
from utils.logger import get_logger

logger = get_logger(__name__)


class ArticleStorage:
    """
    文章存储管理器 - 分片存储实现
    
    设计特点：
    1. 分片存储：每N篇文章存储为一个独立JSON文件，避免单文件过大
    2. 索引机制：维护ID到分片的映射索引，实现O(1)查找
    3. 延迟加载：只加载需要的分片，而非全量数据
    4. 批量写入：累积一定数量新文章后再写入，减少IO次数
    """
    
    # 每个分片存储的文章数量
    SHARD_SIZE = 1000
    # 批量写入阈值，达到此数量后自动刷入
    BATCH_SIZE = 100
    
    def __init__(self, shard_size=None):
        ensure_dirs()
        self.articles_file = ARTICLES_FILE
        self.state_file = STATE_FILE
        
        # 分片目录
        self.shards_dir = os.path.join(DATA_DIR, 'shards')
        self.index_file = os.path.join(DATA_DIR, 'article_index.json')
        
        os.makedirs(self.shards_dir, exist_ok=True)
        
        self._shard_size = shard_size or self.SHARD_SIZE
        self._batch_size = self.BATCH_SIZE
        
        # 索引缓存: {article_id: shard_id}
        self._index_cache = None
        # 分片缓存: {shard_id: {article_id: article}}
        self._shard_cache = {}
        # 新增文章缓存（待写入）
        self._pending_articles = {}
        # 待刷入计数器
        self._pending_count = 0
        
        # 统计信息缓存
        self._stats_cache = None
        
        # 初始化：从旧格式迁移或创建新索引
        self._init_storage()
    
    def _init_storage(self):
        """初始化存储，处理从旧格式迁移"""
        # 如果存在旧格式文件且没有分片，进行迁移
        if os.path.exists(self.articles_file) and not os.path.exists(self.index_file):
            logger.info("检测到旧格式数据，开始迁移到分片存储...")
            self._migrate_from_legacy()
    
    def _migrate_from_legacy(self):
        """从旧格式迁移到分片存储"""
        try:
            with open(self.articles_file, 'r', encoding='utf-8') as f:
                articles = json.load(f)
            
            # 清空并重建
            self._clear_shards()
            
            # 分批写入
            article_list = list(articles.values())
            for i in range(0, len(article_list), self._shard_size):
                batch = article_list[i:i + self._shard_size]
                shard_id = i // self._shard_size
                shard_data = {a['id']: a for a in batch}
                self._save_shard(shard_id, shard_data)
            
            # 重建索引
            self._rebuild_index()
            
            # 备份旧文件
            backup_file = self.articles_file + '.backup'
            os.rename(self.articles_file, backup_file)
            logger.info(f"迁移完成，原文件已备份到: {backup_file}")
            
        except Exception as e:
            logger.error(f"迁移失败: {str(e)}")
    
    def _clear_shards(self):
        """清空所有分片"""
        if os.path.exists(self.shards_dir):
            for f in os.listdir(self.shards_dir):
                os.remove(os.path.join(self.shards_dir, f))
        if os.path.exists(self.index_file):
            os.remove(self.index_file)
        self._index_cache = None
        self._shard_cache = {}
    
    def _get_shard_path(self, shard_id):
        """获取分片文件路径"""
        return os.path.join(self.shards_dir, f'shard_{shard_id:06d}.json')
    
    def _load_index(self):
        """加载索引"""
        if self._index_cache is None:
            if os.path.exists(self.index_file):
                try:
                    with open(self.index_file, 'r', encoding='utf-8') as f:
                        self._index_cache = json.load(f)
                except Exception as e:
                    logger.error(f"加载索引失败: {str(e)}")
                    self._index_cache = {}
            else:
                self._index_cache = {}
        return self._index_cache
    
    def _save_index(self):
        """保存索引"""
        try:
            with open(self.index_file, 'w', encoding='utf-8') as f:
                json.dump(self._index_cache, f, ensure_ascii=False)
        except Exception as e:
            logger.error(f"保存索引失败: {str(e)}")
    
    def _load_shard(self, shard_id):
        """加载指定分片"""
        if shard_id in self._shard_cache:
            return self._shard_cache[shard_id]
        
        shard_path = self._get_shard_path(shard_id)
        if os.path.exists(shard_path):
            try:
                with open(shard_path, 'r', encoding='utf-8') as f:
                    shard_data = json.load(f)
                self._shard_cache[shard_id] = shard_data
                return shard_data
            except Exception as e:
                logger.error(f"加载分片 {shard_id} 失败: {str(e)}")
                return {}
        return {}
    
    def _save_shard(self, shard_id, shard_data):
        """保存分片"""
        shard_path = self._get_shard_path(shard_id)
        try:
            with open(shard_path, 'w', encoding='utf-8') as f:
                json.dump(shard_data, f, ensure_ascii=False)
            self._shard_cache[shard_id] = shard_data
        except Exception as e:
            logger.error(f"保存分片 {shard_id} 失败: {str(e)}")
    
    def _get_latest_shard_id(self):
        """获取最新的分片ID"""
        index = self._load_index()
        if not index:
            return 0
        return max(index.values())
    
    def _rebuild_index(self):
        """重建索引"""
        self._index_cache = {}
        shard_id = 0
        
        while os.path.exists(self._get_shard_path(shard_id)):
            shard_data = self._load_shard(shard_id)
            for article_id in shard_data.keys():
                self._index_cache[article_id] = shard_id
            shard_id += 1
        
        self._save_index()
        logger.info(f"索引重建完成，共 {len(self._index_cache)} 篇文章")
    
    def _flush_pending(self):
        """将待写入的文章刷入分片"""
        if not self._pending_articles:
            return
        
        index = self._load_index()
        
        # 获取最新分片
        latest_shard_id = self._get_latest_shard_id()
        latest_shard = self._load_shard(latest_shard_id)
        
        for article_id, article in self._pending_articles.items():
            # 如果当前分片已满，创建新分片
            if len(latest_shard) >= self._shard_size:
                self._save_shard(latest_shard_id, latest_shard)
                latest_shard_id += 1
                latest_shard = {}
            
            latest_shard[article_id] = article
            index[article_id] = latest_shard_id
        
        # 保存最后一个分片
        self._save_shard(latest_shard_id, latest_shard)
        
        # 保存索引
        self._index_cache = index
        self._save_index()
        
        # 清空待写入缓存
        count = len(self._pending_articles)
        self._pending_articles = {}
        self._pending_count = 0
        
        logger.info(f"已刷入 {count} 篇文章到分片")
    
    def load_articles(self):
        """
        加载所有文章（返回迭代器模式，避免内存爆炸）
        
        Returns:
            dict: 文章字典（注意：对于大数据量，建议使用迭代器方法）
        """
        # 对于小数据量，直接返回全部
        index = self._load_index()
        total = len(index) + len(self._pending_articles)
        
        if total <= self._shard_size * 2:
            # 小数据量，直接加载
            all_articles = dict(self._pending_articles)
            loaded_shards = set()
            
            for article_id, shard_id in index.items():
                if shard_id not in loaded_shards:
                    shard_data = self._load_shard(shard_id)
                    all_articles.update(shard_data)
                    loaded_shards.add(shard_id)
            
            return all_articles
        else:
            # 大数据量，只返回索引和加载方法
            logger.warning(f"数据量较大({total}篇)，建议使用迭代器方式访问")
            return LazyArticleLoader(self)
    
    def iter_articles(self):
        """
        迭代器方式遍历所有文章（内存友好）
        
        Yields:
            dict: 单篇文章
        """
        # 先yield待写入的文章
        for article in self._pending_articles.values():
            yield article
        
        # 再yield分片中的文章
        index = self._load_index()
        processed_shards = set()
        
        for shard_id in sorted(set(index.values())):
            if shard_id in processed_shards:
                continue
            
            shard_data = self._load_shard(shard_id)
            for article in shard_data.values():
                yield article
            
            processed_shards.add(shard_id)
    
    def save_articles(self, articles):
        """
        批量保存文章（用于初始化或全量更新）
        
        Args:
            articles: 文章字典
        """
        # 清空现有数据
        self._clear_shards()
        self._pending_articles = {}
        
        # 分批写入
        article_list = list(articles.values())
        for i in range(0, len(article_list), self._shard_size):
            batch = article_list[i:i + self._shard_size]
            shard_id = i // self._shard_size
            shard_data = {a['id']: a for a in batch}
            self._save_shard(shard_id, shard_data)
        
        # 重建索引
        self._rebuild_index()
        
        logger.info(f"已保存 {len(articles)} 篇文章")
    
    def add_article(self, article):
        """
        添加或更新单篇文章（批量写入，高性能）
        
        Args:
            article: 文章字典，必须包含'id'字段
            
        Returns:
            bool: 是否是新文章
        """
        article_id = article.get('id')
        
        if not article_id:
            logger.warning("文章缺少ID，无法保存")
            return False
        
        index = self._load_index()
        is_new = article_id not in index and article_id not in self._pending_articles
        
        # 检查是否已存在（在已有分片中）
        if article_id in index:
            # 更新已有文章（立即写入）
            shard_id = index[article_id]
            shard_data = self._load_shard(shard_id)
            shard_data[article_id] = article
            self._save_shard(shard_id, shard_data)
            # 清除统计缓存
            self._stats_cache = None
            return is_new
        
        # 新文章加入待写入缓存（批量写入）
        self._pending_articles[article_id] = article
        self._pending_count += 1
        
        # 达到批量阈值时自动刷入
        if self._pending_count >= self._batch_size:
            self._flush_pending()
        
        # 清除统计缓存
        self._stats_cache = None
        
        return is_new
    
    def article_exists(self, article_id):
        """
        检查文章是否已存在（O(1)性能）
        
        Args:
            article_id: 文章ID
            
        Returns:
            bool: 是否存在
        """
        index = self._load_index()
        return article_id in index or article_id in self._pending_articles
    
    def get_article(self, article_id):
        """
        获取单篇文章（O(1)性能）
        
        Args:
            article_id: 文章ID
            
        Returns:
            dict: 文章信息，不存在返回None
        """
        # 先查待写入缓存
        if article_id in self._pending_articles:
            return self._pending_articles[article_id]
        
        # 再查索引
        index = self._load_index()
        if article_id not in index:
            return None
        
        shard_id = index[article_id]
        shard_data = self._load_shard(shard_id)
        return shard_data.get(article_id)
    
    def get_all_articles(self, sort_by='crawl_time', reverse=True):
        """
        获取所有文章列表（大数据量时性能较低，建议用iter_articles）
        
        Args:
            sort_by: 排序字段
            reverse: 是否倒序
            
        Returns:
            list: 文章列表
        """
        articles = list(self.iter_articles())
        
        if sort_by:
            articles.sort(key=lambda x: x.get(sort_by, ''), reverse=reverse)
        
        return articles
    
    def load_state(self):
        """
        加载爬虫状态
        
        Returns:
            dict: 状态信息
        """
        if not os.path.exists(self.state_file):
            return {}
        try:
            with open(self.state_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"加载状态失败: {str(e)}")
            return {}
    
    def save_state(self, state):
        """
        保存爬虫状态
        
        Args:
            state: 状态字典
        """
        try:
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(state, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存状态失败: {str(e)}")
    
    def update_last_crawl_time(self):
        """更新最后爬取时间"""
        state = self.load_state()
        state['last_crawl_time'] = datetime.now().isoformat()
        state['crawl_count'] = state.get('crawl_count', 0) + 1
        self.save_state(state)
        logger.info(f"更新最后爬取时间: {state['last_crawl_time']}")
    
    def get_stats(self):
        """
        获取统计信息（高性能缓存）
        
        Returns:
            dict: 统计信息
        """
        if self._stats_cache is None:
            index = self._load_index()
            state = self.load_state()
            
            self._stats_cache = {
                'total_articles': len(index) + len(self._pending_articles),
                'last_crawl_time': state.get('last_crawl_time'),
                'total_crawl_count': state.get('crawl_count', 0),
                'shard_count': len(set(index.values())) if index else 0,
                'pending_count': len(self._pending_articles),
            }
        
        return self._stats_cache
    
    def export_to_json(self, filepath=None):
        """
        导出文章到JSON文件（流式写入，内存友好）
        
        Args:
            filepath: 导出文件路径，默认按时间命名
            
        Returns:
            str: 导出文件路径
        """
        if filepath is None:
            filename = f"articles_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            filepath = os.path.join(DATA_DIR, filename)
        
        try:
            # 流式写入JSON数组
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write('[\n')
                first = True
                
                for article in self.iter_articles():
                    if not first:
                        f.write(',\n')
                    first = False
                    
                    json_str = json.dumps(article, ensure_ascii=False)
                    f.write('  ' + json_str)
                
                f.write('\n]')
            
            count = self.get_stats()['total_articles']
            logger.info(f"已导出 {count} 篇文章到: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"导出JSON失败: {str(e)}")
            return None
    
    def export_to_txt(self, filepath=None):
        """
        导出文章到文本文件（流式写入）
        
        Args:
            filepath: 导出文件路径
            
        Returns:
            str: 导出文件路径
        """
        if filepath is None:
            filename = f"articles_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            filepath = os.path.join(DATA_DIR, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                for i, article in enumerate(self.iter_articles(), 1):
                    f.write(f"{'='*60}\n")
                    f.write(f"【{i}】{article.get('title', '无标题')}\n")
                    f.write(f"{'='*60}\n")
                    f.write(f"发布时间: {article.get('publish_date', '未知')}\n")
                    f.write(f"作者: {article.get('author', '未知')}\n")
                    if article.get('author_unit'):
                        f.write(f"作者单位: {article.get('author_unit', '')}\n")
                    f.write(f"链接: {article.get('url', '')}\n")
                    f.write(f"\n{article.get('content', article.get('summary', '无内容'))}\n")
                    f.write(f"\n\n")
            
            count = self.get_stats()['total_articles']
            logger.info(f"已导出 {count} 篇文章到文本文件: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"导出文本文件失败: {str(e)}")
            return None
    
    def flush(self):
        """
        手动刷入待写入的文章到磁盘
        可在爬取过程中主动调用，确保数据不丢失
        """
        if self._pending_articles:
            self._flush_pending()
            logger.info("手动刷入完成")
    
    def close(self):
        """关闭存储，确保所有数据写入磁盘"""
        self._flush_pending()
        logger.info("存储已关闭，所有数据已保存")


class LazyArticleLoader:
    """
    懒加载文章容器
    用于大数据量场景，避免一次性加载所有文章到内存
    """
    
    def __init__(self, storage):
        self._storage = storage
        self._index = storage._load_index()
    
    def __contains__(self, article_id):
        return article_id in self._index or article_id in self._storage._pending_articles
    
    def __getitem__(self, article_id):
        article = self._storage.get_article(article_id)
        if article is None:
            raise KeyError(article_id)
        return article
    
    def get(self, article_id, default=None):
        return self._storage.get_article(article_id) or default
    
    def __len__(self):
        return len(self._index) + len(self._storage._pending_articles)
    
    def __iter__(self):
        return self._storage.iter_articles()
    
    def values(self):
        return self._storage.iter_articles()
    
    def keys(self):
        """返回所有文章ID（生成器）"""
        yield from self._storage._pending_articles.keys()
        yield from self._index.keys()
    
    def items(self):
        """返回所有文章项（生成器）"""
        for article_id in self.keys():
            yield article_id, self._storage.get_article(article_id)
