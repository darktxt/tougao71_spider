"""
爬虫主模块 - 整合请求、解析和存储功能
针对百万级数据优化
"""

from crawler.fetcher import Fetcher
from crawler.parser import ArticleParser
from storage.article_storage import ArticleStorage
from config import MAX_PAGES_PER_RUN
from utils.logger import get_logger

logger = get_logger(__name__)


class ArticleSpider:
    """文章爬虫 - 优化版"""
    
    def __init__(self):
        self.fetcher = Fetcher()
        self.parser = ArticleParser()
        self.storage = ArticleStorage()
        self.new_articles_count = 0
        self.updated_articles_count = 0
        
        # 批量处理缓存
        self._exist_cache = set()
    
    def crawl_list_page(self, page=1):
        """
        爬取文章列表页
        
        Args:
            page: 页码
            
        Returns:
            list: 文章列表
        """
        # 使用tuijian.php进行分页（该页面包含所属板块信息）
        # 格式: https://tougao.12371.cn/tuijian.php?page=N
        if page == 1:
            url = "https://tougao.12371.cn/tuijian.php"
        else:
            url = f"https://tougao.12371.cn/tuijian.php?page={page}"
        
        logger.info(f"正在爬取列表页: {url}")
        
        response = self.fetcher.get(url)
        if not response:
            logger.error(f"获取列表页失败: {url}")
            return []
        
        articles = self.parser.parse_list_page(response.text)
        logger.info(f"列表页解析完成，找到 {len(articles)} 篇文章")
        
        return articles
    
    def crawl_detail_page(self, article):
        """
        爬取文章详情页
        
        Args:
            article: 文章基本信息
            
        Returns:
            dict: 完整的文章信息
        """
        url = article.get('url')
        if not url:
            logger.warning("文章缺少URL，跳过详情页爬取")
            return article
        
        logger.info(f"正在爬取详情页: {url}")
        
        response = self.fetcher.get(url)
        if not response:
            logger.error(f"获取详情页失败: {url}")
            return article
        
        article = self.parser.parse_detail_page(response.text, article)
        return article
    
    def crawl(self, max_pages=None, crawl_detail=True):
        """
        执行爬取任务 - 优化版
        
        Args:
            max_pages: 最大爬取页数
            crawl_detail: 是否爬取详情页
            
        Returns:
            dict: 爬取结果统计
        """
        if max_pages is None:
            max_pages = MAX_PAGES_PER_RUN
        
        self.new_articles_count = 0
        self.updated_articles_count = 0
        
        # 清空缓存
        self._exist_cache.clear()
        
        logger.info(f"开始爬取任务，最大页数: {max_pages}")
        
        try:
            for page in range(1, max_pages + 1):
                logger.info(f"正在处理第 {page}/{max_pages} 页")
                
                # 爬取列表页
                articles = self.crawl_list_page(page)
                
                if not articles:
                    logger.info(f"第 {page} 页没有文章，停止爬取")
                    break
                
                # 批量预检查：先获取本页所有文章的ID
                article_ids = [a.get('id') for a in articles if a.get('id')]
                
                # 批量检查哪些文章已存在
                existing_ids = self._batch_check_exists(article_ids)
                
                # 过滤出需要爬取的新文章
                new_articles = [a for a in articles if a.get('id') not in existing_ids]
                
                # 统计已存在的文章数量
                existing_count = len(articles) - len(new_articles)
                logger.info(f"本页新文章: {len(new_articles)}, 已存在: {existing_count}")
                
                # 处理新文章
                if new_articles:
                    for article in new_articles:
                        self._process_article(article, crawl_detail)
                    logger.info(f"本页处理完成: 新增 {len(new_articles)} 篇")
                else:
                    logger.info("本页没有新文章")
                
                # 继续爬取下一页，除非达到最大页数
                # 注意：不再因为"全是已存在"就停止，让用户决定爬取多少页
                if page >= max_pages:
                    logger.info(f"已达到最大页数限制: {max_pages}")
                    break
            
            # 更新最后爬取时间
            self.storage.update_last_crawl_time()
            
            # 使用get_stats()获取总数，避免全量加载
            stats = self.storage.get_stats()
            
            result = {
                'success': True,
                'new_articles': self.new_articles_count,
                'updated_articles': self.updated_articles_count,
                'total_articles': stats.get('total_articles', 0),
            }
            
            logger.info(f"爬取任务完成: {result}")
            return result
            
        except Exception as e:
            logger.error(f"爬取任务失败: {str(e)}")
            return {
                'success': False,
                'error': str(e),
            }
        finally:
            self.fetcher.close()
    
    def _batch_check_exists(self, article_ids):
        """
        批量检查文章是否存在
        
        Args:
            article_ids: 文章ID列表
            
        Returns:
            set: 已存在的文章ID集合
        """
        existing = set()
        for aid in article_ids:
            # 先检查缓存
            if aid in self._exist_cache:
                existing.add(aid)
            # 再检查存储
            elif self.storage.article_exists(aid):
                existing.add(aid)
                self._exist_cache.add(aid)
        return existing
    
    def _process_article(self, article, crawl_detail):
        """
        处理单篇文章
        
        Args:
            article: 文章信息
            crawl_detail: 是否爬取详情页
        """
        article_id = article.get('id')
        
        # 检查是否已存在（使用缓存）
        if article_id in self._exist_cache:
            logger.debug(f"文章已存在，跳过: {article.get('title', '')}")
            return
        
        # 爬取详情页
        if crawl_detail:
            article = self.crawl_detail_page(article)
        
        # 保存文章
        is_new = self.storage.add_article(article)
        
        if is_new:
            self.new_articles_count += 1
            self._exist_cache.add(article_id)
            logger.info(f"新增文章: {article.get('title', '')}")
        else:
            self.updated_articles_count += 1
    
    def get_stats(self):
        """获取统计信息"""
        return self.storage.get_stats()
