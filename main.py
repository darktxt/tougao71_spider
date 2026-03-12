"""
爬虫主入口 - 优化版

使用方法:
    python main.py              # 启动定时调度器
    python main.py --once       # 执行单次爬取
    python main.py --stats      # 查看统计信息
    python main.py --export     # 导出数据
    python main.py --list       # 查看文章列表
    python main.py --search     # 搜索文章
    python main.py --interval 3 # 设置爬取间隔为3小时
"""

import argparse
import sys
from scheduler import run_scheduler, run_once, CrawlerScheduler
from storage.article_storage import ArticleStorage
from crawler.spider import ArticleSpider
from utils.logger import get_logger

logger = get_logger(__name__)


def show_stats():
    """显示统计信息"""
    storage = ArticleStorage()
    stats = storage.get_stats()
    
    print("\n" + "=" * 50)
    print("文章爬虫统计信息")
    print("=" * 50)
    print(f"总文章数: {stats['total_articles']}")
    print(f"分片数量: {stats.get('shard_count', 'N/A')}")
    print(f"待写入: {stats.get('pending_count', 0)}")
    print(f"总爬取次数: {stats['total_crawl_count']}")
    print(f"最后爬取时间: {stats['last_crawl_time'] or '从未'}")
    print("=" * 50 + "\n")
    
    storage.close()


def list_articles(limit=20, offset=0):
    """
    列出文章 - 优化版，支持分页
    
    Args:
        limit: 每页数量
        offset: 起始位置
    """
    storage = ArticleStorage()
    
    # 使用迭代器，只加载需要的数据
    articles = []
    for i, article in enumerate(storage.iter_articles()):
        if i < offset:
            continue
        if len(articles) >= limit:
            break
        articles.append(article)
    
    total = storage.get_stats()['total_articles']
    
    print(f"\n文章列表 (共 {total} 篇，显示 {offset+1}-{offset+len(articles)}):")
    print("-" * 60)
    
    for i, article in enumerate(articles, offset + 1):
        title = article.get('title', '无标题')[:40]
        date = article.get('publish_date', '未知')
        print(f"{i:4d}. [{date}] {title}")
    
    print("-" * 60)
    print(f"提示: 使用 --offset 参数翻页，如 --offset {offset + limit}\n")
    
    storage.close()


def search_articles(keyword, limit=None, export_format=None, search_type='all'):
    """
    搜索文章 - 支持多种搜索方式，并可导出匹配的文章

    Args:
        keyword: 搜索关键词
        limit: 最多匹配数量，None 表示不限制
        export_format: 导出格式，None 不导出，'json' 或 'txt' 导出
        search_type: 搜索类型 - 'title'=标题, 'content'=正文, 'all'=标题+正文, 'author'=作者, 'author_unit'=作者单位
    """
    storage = ArticleStorage()

    # 显示搜索类型说明
    search_type_names = {
        'title': '标题',
        'content': '正文',
        'all': '标题+正文',
        'author': '作者',
        'author_unit': '作者单位'
    }

    print(f"\n搜索关键词: {keyword}")
    print(f"搜索类型: {search_type_names.get(search_type, search_type)}")
    print("-" * 60)

    # 收集匹配的文章
    matched_articles = []
    count = 0

    for article in storage.iter_articles():
        title = article.get('title', '')
        content = article.get('content', '')
        author = article.get('author', '')
        author_unit = article.get('author_unit', '')

        match = False
        match_location = ""

        # 根据搜索类型进行匹配
        if search_type == 'title':
            # 仅在标题中搜索
            if keyword.lower() in title.lower():
                match = True
                match_location = "标题"
        elif search_type == 'content':
            # 仅在正文中搜索
            if keyword.lower() in content.lower():
                match = True
                match_location = "正文"
        elif search_type == 'all':
            # 在标题和正文中搜索
            if keyword.lower() in title.lower() or keyword.lower() in content.lower():
                match = True
                match_location = "标题" if keyword.lower() in title.lower() else "正文"
        elif search_type == 'author':
            # 在作者字段中搜索
            if keyword.lower() in author.lower():
                match = True
                match_location = "作者"
        elif search_type == 'author_unit':
            # 在作者单位字段中搜索
            if keyword.lower() in author_unit.lower():
                match = True
                match_location = "作者单位"

        if match:
            # 将匹配位置信息添加到文章对象中
            article['_match_location'] = match_location
            matched_articles.append(article)
            count += 1

            # 达到限制后停止搜索
            if limit is not None and count >= limit:
                break
    
    total_matches = count
    
    # 显示结果
    for i, article in enumerate(matched_articles):
        title = article.get('title', '无标题')
        author = article.get('author', '').replace('\xa0', ' ').strip()
        author_unit = article.get('author_unit', '').replace('\xa0', ' ').strip()
        match_location = article.get('_match_location', '')

        print(f"{i+1}. {title}")
        print(f"   {article.get('url', '')}")
        # 显示作者和作者单位信息
        if author:
            print(f"   作者: {author}")
        if author_unit:
            print(f"   单位: {author_unit}")
        # 显示匹配位置
        print(f"   [匹配位置: {match_location}]")
        print()
    
    if limit is not None and len(matched_articles) >= limit:
        print(f"(仅显示前 {limit} 条结果)\n")
        
    print("-" * 60)
    print(f"找到 {total_matches} 篇匹配的文章")
    
    # 导出功能
    if export_format and matched_articles:
        # 创建临时存储保存匹配结果
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        safe_keyword = "".join(c for c in keyword if c.isalnum() or c in (' ', '-', '_')).strip()[:20]
        
        if export_format == 'json':
            filename = f"search_{safe_keyword}_{timestamp}.json"
            filepath = f"data/{filename}"
            
            import json
            try:
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(matched_articles, f, ensure_ascii=False, indent=2)
                print(f"\n已导出 JSON: {filepath}")
            except Exception as e:
                print(f"\n导出失败: {e}")
                
        elif export_format == 'txt':
            filename = f"search_{safe_keyword}_{timestamp}.txt"
            filepath = f"data/{filename}"
            
            try:
                with open(filepath, 'w', encoding='utf-8') as f:
                    for i, article in enumerate(matched_articles, 1):
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
                print(f"\n已导出 TXT: {filepath}")
            except Exception as e:
                print(f"\n导出失败: {e}")
    elif export_format:
        print("\n没有匹配的文章，无法导出")
    
    print()
    storage.close()


def export_data(format_type='json'):
    """
    导出数据 - 优化版，使用流式导出
    
    Args:
        format_type: 导出格式，'json' 或 'txt'
    """
    storage = ArticleStorage()
    stats = storage.get_stats()
    
    print(f"\n正在导出 {stats['total_articles']} 篇文章...")
    
    if format_type == 'json':
        filepath = storage.export_to_json()
    else:
        filepath = storage.export_to_txt()
    
    if filepath:
        print(f"导出完成: {filepath}\n")
    else:
        print("导出失败\n")
    
    storage.close()


def test_crawler(crawl_detail=True):
    """测试爬虫（只爬取1页）"""
    logger.info("运行测试模式...")
    spider = ArticleSpider()
    result = spider.crawl(max_pages=1, crawl_detail=crawl_detail)
    
    print("\n" + "=" * 50)
    print("测试结果")
    print("=" * 50)
    print(f"成功: {result.get('success')}")
    print(f"新增文章: {result.get('new_articles', 0)}")
    print(f"更新文章: {result.get('updated_articles', 0)}")
    print(f"总计文章: {result.get('total_articles', 0)}")
    if result.get('error'):
        print(f"错误: {result['error']}")
    print("=" * 50 + "\n")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='文章爬虫 - 定期爬取网站文章（支持百万级数据）',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python main.py                    # 启动定时调度器（默认6小时）
  python main.py --once             # 执行单次爬取
  python main.py --interval 3       # 设置间隔为3小时
  python main.py --stats            # 查看统计信息
  python main.py --list             # 查看文章列表
  python main.py --list --offset 20 # 查看文章列表（第2页）
  python main.py --search 乡村振兴  # 搜索文章（标题+正文）
  python main.py --search-type title 乡村振兴  # 仅在标题中搜索
  python main.py --search-author 罗川       # 按作者姓名搜索
  python main.py --search-author-unit 组织部 # 按作者单位搜索
  python main.py --export json      # 导出为JSON
  python main.py --export txt       # 导出为文本
  python main.py --test             # 测试模式（爬1页）
        """
    )
    
    parser.add_argument(
        '--once',
        action='store_true',
        help='执行单次爬取后退出'
    )
    
    parser.add_argument(
        '--interval',
        type=int,
        default=None,
        help='设置爬取间隔（小时），默认6小时'
    )
    
    parser.add_argument(
        '--stats',
        action='store_true',
        help='显示统计信息'
    )
    
    parser.add_argument(
        '--list',
        action='store_true',
        help='列出文章'
    )
    
    parser.add_argument(
        '--offset',
        type=int,
        default=0,
        help='文章列表起始位置（用于分页）'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='显示文章数量（用于搜索）'
    )
    
    parser.add_argument(
        '--search',
        type=str,
        nargs='?',
        const='',
        help='搜索文章（按标题和全文）'
    )

    parser.add_argument(
        '--search-author',
        type=str,
        nargs='?',
        const='',
        help='按作者姓名搜索文章'
    )

    parser.add_argument(
        '--search-author-unit',
        type=str,
        nargs='?',
        const='',
        help='按作者单位搜索文章'
    )

    parser.add_argument(
        '--search-type',
        type=str,
        choices=['title', 'content', 'all', 'author', 'author_unit'],
        default='all',
        help='搜索类型: title=标题, content=正文, all=标题+正文, author=作者, author_unit=作者单位'
    )

    parser.add_argument(
        '--search-export',
        choices=['json', 'txt'],
        default='json',
        help='导出搜索结果为json或txt（需配合 --search 使用）'
    )
    
    parser.add_argument(
        '--export',
        choices=['json', 'txt'],
        nargs='?',
        const='json',
        help='导出数据（json或txt格式）'
    )
    
    parser.add_argument(
        '--test',
        action='store_true',
        help='测试模式（只爬取1页）'
    )
    
    parser.add_argument(
        '--no-detail',
        action='store_true',
        help='不爬取详情页内容（仅列表信息）'
    )
    
    args = parser.parse_args()
    
    # 处理各种命令
    if args.stats:
        show_stats()
        return
    
    if args.list:
        list_articles(limit=args.limit, offset=args.offset)
        return
    
    # 处理搜索功能 - 支持多种搜索方式
    if args.search is not None:
        if not args.search:
            print("错误: 请提供搜索关键词，如: --search 乡村振兴")
            sys.exit(1)
        search_articles(args.search, limit=args.limit, export_format=args.search_export, search_type=args.search_type)
        return

    if args.search_author is not None:
        if not args.search_author:
            print("错误: 请提供作者姓名，如: --search-author 罗川")
            sys.exit(1)
        search_articles(args.search_author, limit=args.limit, export_format=args.search_export, search_type='author')
        return

    if args.search_author_unit is not None:
        if not args.search_author_unit:
            print("错误: 请提供作者单位名称，如: --search-author-unit 组织部")
            sys.exit(1)
        search_articles(args.search_author_unit, limit=args.limit, export_format=args.search_export, search_type='author_unit')
        return
    
    if args.export:
        export_data(args.export)
        return
    
    if args.test:
        test_crawler(crawl_detail=not args.no_detail)
        return
    
    if args.once:
        print("\n执行单次爬取...")
        result = run_once()
        
        if result.get('success'):
            print(f"\n爬取完成!")
            print(f"新增文章: {result.get('new_articles', 0)}")
            print(f"更新文章: {result.get('updated_articles', 0)}")
            print(f"总计文章: {result.get('total_articles', 0)}\n")
        else:
            print(f"\n爬取失败: {result.get('error', '未知错误')}\n")
            sys.exit(1)
    else:
        # 启动调度器
        print(f"\n启动爬虫调度器...")
        print(f"爬取间隔: {args.interval or 6} 小时")
        print(f"目标网站: https://tougao.12371.cn/liebiao.php")
        print("按 Ctrl+C 停止\n")
        
        try:
            run_scheduler(interval_hours=args.interval, run_immediately=True)
        except KeyboardInterrupt:
            print("\n爬虫已停止")
            sys.exit(0)


if __name__ == '__main__':
    main()
