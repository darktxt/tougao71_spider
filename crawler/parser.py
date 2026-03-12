"""
HTML解析模块 - 负责解析页面内容，提取文章信息
"""

import re
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from config import BASE_DOMAIN
from utils.logger import get_logger

logger = get_logger(__name__)


class ArticleParser:
    """文章解析器"""
    
    def __init__(self):
        self.base_url = BASE_DOMAIN
    
    def parse_list_page(self, html):
        """
        解析文章列表页
        
        Args:
            html: 页面HTML内容
            
        Returns:
            list: 文章信息列表
        """
        soup = BeautifulSoup(html, 'lxml')
        articles = []
        
        try:
            # 针对12371网站的解析逻辑
            # 文章列表通常在 .list 或 .news-list 等容器中
            list_container = soup.select_one('.list, .news-list, .article-list, #list')
            
            if list_container:
                items = list_container.find_all('li')
            else:
                # 备选：直接查找所有li
                items = soup.find_all('li')
            
            for item in items:
                article = self._extract_article_info(item)
                if article:
                    articles.append(article)
            
            logger.info(f"解析到 {len(articles)} 篇文章")
            return articles
            
        except Exception as e:
            logger.error(f"解析列表页失败: {str(e)}")
            return []
    
    def _extract_article_info(self, item):
        """
        从列表项中提取文章信息
        
        Args:
            item: BeautifulSoup元素
            
        Returns:
            dict: 文章基本信息
        """
        try:
            article = {}
            
            # 提取标题和链接
            link_elem = item.find('a')
            if link_elem:
                article['title'] = link_elem.get_text(strip=True)
                href = link_elem.get('href', '')
                article['url'] = urljoin(self.base_url, href)
            else:
                return None
            
            # 过滤掉专题页面链接（非文章）
            url_lower = article['url'].lower()
            if any(keyword in url_lower for keyword in ['special', 'zt', 'zhuanti']):
                return None
            
            # 过滤掉标题为空的条目
            if not article['title'] or len(article['title'].strip()) < 5:
                return None
            
            # 过滤掉标题以"◆"开头的专题条目
            if article['title'].startswith('◆'):
                return None
            
            # 提取各列数据
            # 获取所有span元素
            spans = item.find_all('span')
            
            # 查找并解析各个字段
            for span in spans:
                span_class = span.get('class', [])
                span_text = span.get_text(strip=True)
                
                # 提取发布时间
                if 'fabu_time' in span_class:
                    # 尝试从文本中提取日期
                    date_patterns = [
                        r'(\d{4}-\d{1,2}-\d{1,2}\s+\d{1,2}:\d{2})',  # 2026-3-11 15:38
                        r'(\d{4}-\d{2}-\d{2})',
                        r'(\d{4}/\d{2}/\d{2})',
                        r'(\d{4}年\d{2}月\d{2}日)',
                    ]
                    
                    for pattern in date_patterns:
                        match = re.search(pattern, span_text)
                        if match:
                            date_str = match.group(1)
                            article['publish_date'] = date_str
                            # 从标题中移除日期部分（如果日期不小心被包含在标题中）
                            article['title'] = re.sub(pattern, '', article['title']).strip()
                            break
            
            # 如果没有找到日期，尝试从整个item文本中提取
            if 'publish_date' not in article:
                text = item.get_text(strip=True)
                date_patterns = [
                    r'(\d{4}-\d{1,2}-\d{1,2}\s+\d{1,2}:\d{2})',
                    r'(\d{4}-\d{2}-\d{2})',
                    r'(\d{4}/\d{2}/\d{2})',
                    r'(\d{4}年\d{2}月\d{2}日)',
                ]
                
                for pattern in date_patterns:
                    match = re.search(pattern, text)
                    if match:
                        date_str = match.group(1)
                        article['publish_date'] = date_str
                        article['title'] = re.sub(pattern, '', article['title']).strip()
                        break
            
            # 注意: author 和 author_unit 将从详情页提取，不从列表页提取
            
            # 提取摘要（如果页面中有）
            summary_elem = item.find(class_=re.compile('summary|desc|intro'))
            if summary_elem:
                article['summary'] = summary_elem.get_text(strip=True)
            
            # 生成唯一ID
            article['id'] = self._generate_id(article.get('url', ''))
            # 移除爬取时间字段
            # article['crawl_time'] = datetime.now().isoformat()
            
            return article
            
        except Exception as e:
            logger.warning(f"提取文章信息失败: {str(e)}")
            return None
    
    def parse_detail_page(self, html, article):
        """
        解析文章详情页
        
        Args:
            html: 页面HTML内容
            article: 文章基本信息
            
        Returns:
            dict: 完整的文章信息
        """
        soup = BeautifulSoup(html, 'lxml')
        
        try:
            # 提取作者和作者单位 - 从详情页的small_title标签中提取
            # 格式1: <h2 class="small_title">四川省会理市委组织部  罗川</h2>
            # 格式2: <h2 class="small_title">广西壮族自治区柳州市融安县委组织部  潭头乡人民政府  刘梁春 苏承瑜 蔡智通</h2>
            author_info_elem = soup.find(class_='small_title')
            if author_info_elem:
                author_info_text = author_info_elem.get_text(strip=True)

                # 分析格式：可能是 "单位  作者" 或 "单位1  单位2  作者1 作者2"
                parts = author_info_text.split('  ')

                if len(parts) == 2:
                    # 简单格式: "作者单位  作者名"
                    article['author_unit'] = parts[0].strip()
                    article['author'] = parts[1].strip()
                elif len(parts) >= 3:
                    # 复杂格式: "单位1  单位2  作者1 作者2 ..."
                    # 策略: 第一个以"部"或"委"结尾的是作者单位
                    # 通常"府"、"局"是二级单位，不算主要作者单位
                    primary_unit_keywords = ['部', '委']
                    author_unit = None
                    authors_start = 0

                    for i, part in enumerate(parts):
                        if any(part.strip().endswith(kw) for kw in primary_unit_keywords):
                            author_unit = '  '.join(parts[:i+1]).strip()
                            authors_start = i + 1
                            break

                    if author_unit:
                        article['author_unit'] = author_unit
                        # 剩下的部分都是作者
                        if authors_start < len(parts):
                            article['author'] = '  '.join(parts[authors_start:]).strip()
                    else:
                        # 没有找到明确的作者单位标识，假设第一个是作者单位
                        article['author_unit'] = parts[0].strip()
                        article['author'] = '  '.join(parts[1:]).strip()
                elif len(parts) > 0:
                    # 只有一部分
                    article['author_unit'] = parts[0].strip()

            # 如果没有找到small_title，尝试从内容开头提取
            if not article.get('author_unit') or not article.get('author'):
                content_text = soup.get_text()
                # 匹配格式: "单位名称  作者名" 或 "作者名"
                lines = content_text.split('\n')[:5]  # 看前5行
                for line in lines:
                    # 匹配格式: "四川省会理市委组织部  罗川"
                    match = re.search(r'([\u4e00-\u9fa5]{2,}(?:[\u4e00-\u9fa5]{2,})*[\s]+)([\u4e00-\u9fa5]{2,4})(?:\s|$)', line.strip())
                    if match:
                        article['author_unit'] = match.group(1).strip()
                        article['author'] = match.group(2).strip()
                        break
            
            # 提取正文内容 - 针对12371网站的结构
            content_selectors = [
                '.con',                 # 12371网站正文容器
                '.word',                # 备选
                '.content-detail',
                '.article-content',
                '.content',
                '#content',
                '.detail-content',
                'article',
            ]
            
            content = ''
            for selector in content_selectors:
                elem = soup.select_one(selector)
                if elem:
                    # 清理脚本和样式
                    for script in elem.find_all(['script', 'style']):
                        script.decompose()
                    # 清理附件下载等无关内容
                    for attach in elem.find_all(text=re.compile(r'下载附件|上传')):
                        attach.extract()
                    content = elem.get_text(separator='\n', strip=True)
                    # 清理多余空行
                    content = re.sub(r'\n{3,}', '\n\n', content)
                    break
            
            article['content'] = content
            
            # 提取阅读量
            view_elem = soup.find(text=re.compile(r'浏览|阅读'))
            if view_elem:
                view_match = re.search(r'(\d+)', view_elem)
                if view_match:
                    article['views'] = int(view_match.group(1))
            
            logger.debug(f"成功解析详情页: {article.get('title', '')}, 内容长度: {len(content)}")
            return article
            
        except Exception as e:
            logger.error(f"解析详情页失败: {str(e)}")
            return article
    
    def _generate_id(self, url):
        """根据URL生成唯一ID"""
        import hashlib
        return hashlib.md5(url.encode()).hexdigest()[:16]
