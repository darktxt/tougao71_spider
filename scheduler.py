"""
调度器模块 - 负责定时执行爬取任务
优化版：支持资源管理和错误恢复
"""

import time
import schedule
import gc
from datetime import datetime
from crawler.spider import ArticleSpider
from config import CRAWL_INTERVAL_HOURS
from utils.logger import get_logger

logger = get_logger(__name__)


class CrawlerScheduler:
    """爬虫调度器 - 优化版"""
    
    def __init__(self, interval_hours=None):
        self.interval_hours = interval_hours or CRAWL_INTERVAL_HOURS
        self.spider = None
        self.running = False
        self.job = None
        self.run_count = 0
    
    def _get_spider(self):
        """获取或创建爬虫实例"""
        if self.spider is None:
            self.spider = ArticleSpider()
        return self.spider
    
    def crawl_job(self):
        """爬取任务"""
        logger.info("=" * 60)
        logger.info(f"开始定时爬取任务 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)
        
        spider = self._get_spider()
        
        try:
            result = spider.crawl()
            self.run_count += 1
            
            if result.get('success'):
                logger.info(f"爬取成功: 新增 {result.get('new_articles', 0)} 篇，"
                          f"更新 {result.get('updated_articles', 0)} 篇，"
                          f"总计 {result.get('total_articles', 0)} 篇")
            else:
                logger.error(f"爬取失败: {result.get('error', '未知错误')}")
                
        except Exception as e:
            logger.error(f"爬取任务异常: {str(e)}")
        
        # 定期清理内存（每10次）
        if self.run_count % 10 == 0:
            gc.collect()
            logger.info("内存清理完成")
        
        logger.info(f"下次爬取将在 {self.interval_hours} 小时后执行")
        logger.info("=" * 60)
    
    def start(self, run_immediately=True):
        """
        启动调度器
        
        Args:
            run_immediately: 是否立即执行一次
        """
        logger.info(f"启动爬虫调度器，间隔: {self.interval_hours} 小时")
        
        # 立即执行一次
        if run_immediately:
            self.crawl_job()
        
        # 设置定时任务
        self.job = schedule.every(self.interval_hours).hours.do(self.crawl_job)
        
        self.running = True
        
        # 保持运行
        try:
            while self.running:
                schedule.run_pending()
                time.sleep(60)  # 每分钟检查一次
        except KeyboardInterrupt:
            logger.info("收到停止信号，调度器正在停止...")
            self.stop()
    
    def stop(self):
        """停止调度器"""
        self.running = False
        if self.job:
            schedule.cancel_job(self.job)
        
        # 关闭爬虫
        if self.spider:
            try:
                self.spider.storage.close()
            except:
                pass
            self.spider = None
        
        # 强制垃圾回收
        gc.collect()
        
        logger.info("调度器已停止，资源已释放")
    
    def run_once(self):
        """执行单次爬取"""
        logger.info("=" * 60)
        logger.info(f"执行单次爬取 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)
        
        spider = self._get_spider()
        
        try:
            result = spider.crawl()
            self.run_count += 1
            
            if result.get('success'):
                logger.info(f"爬取成功: 新增 {result.get('new_articles', 0)} 篇，"
                          f"更新 {result.get('updated_articles', 0)} 篇，"
                          f"总计 {result.get('total_articles', 0)} 篇")
            else:
                logger.error(f"爬取失败: {result.get('error', '未知错误')}")
            
            return result
            
        except Exception as e:
            logger.error(f"爬取异常: {str(e)}")
            return {'success': False, 'error': str(e)}
        finally:
            # 释放资源
            if self.spider:
                try:
                    self.spider.storage.close()
                except:
                    pass
                self.spider = None


def run_scheduler(interval_hours=None, run_immediately=True):
    """
    运行调度器的便捷函数
    
    Args:
        interval_hours: 爬取间隔（小时）
        run_immediately: 是否立即执行
    """
    scheduler = CrawlerScheduler(interval_hours)
    scheduler.start(run_immediately)


def run_once():
    """执行单次爬取的便捷函数"""
    scheduler = CrawlerScheduler()
    return scheduler.run_once()
