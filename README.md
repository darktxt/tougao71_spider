# 文章爬虫

一个Python编写的定期爬虫，用于爬取 `https://tougao.12371.cn` 网站的文章数据。

## 功能特点

- **定期爬取**: 支持定时自动爬取，可配置间隔时间
- **智能去重**: 自动识别已爬取的文章，避免重复
- **高性能存储**: 分片存储架构，支持百万级数据，O(1)查询性能
- **完整内容**: 自动爬取每篇文章详情页，获取完整正文内容
- **批量写入**: 累积一定数量后批量刷入，减少IO次数
- **搜索导出**: 支持全文和标题搜索，导出匹配的文章
- **日志记录**: 完整的日志记录，便于排查问题
- **数据导出**: 支持导出为JSON或TXT格式（包含完整文章内容）

## 项目结构

```
.
├── config.py              # 配置文件
├── main.py                # 主入口程序
├── scheduler.py           # 调度器模块
├── requirements.txt       # 依赖包
├── README.md              # 说明文档
├── crawler/               # 爬虫模块
│   ├── __init__.py
│   ├── fetcher.py         # HTTP请求模块
│   ├── parser.py          # HTML解析模块
│   └── spider.py          # 爬虫主模块
├── storage/               # 存储模块（分片存储）
│   ├── __init__.py
│   └── article_storage.py # 高性能分片存储管理
├── utils/                 # 工具模块
│   ├── __init__.py
│   └── logger.py          # 日志工具
├── data/                  # 数据目录（自动创建）
│   ├── shards/            # 文章分片存储目录
│   ├── article_index.json # 文章索引
│   └── crawler_state.json # 爬虫状态
└── logs/                  # 日志目录（自动创建）
    └── *.log              # 日志文件
```

## 安装依赖

```bash
pip install -r requirements.txt
```

## 使用方法

### 1. 启动定时调度器（默认6小时间隔）

```bash
python main.py
```

### 2. 执行单次爬取

```bash
python main.py --once
```

### 3. 设置自定义爬取间隔

```bash
# 每3小时爬取一次
python main.py --interval 3
```

### 4. 查看统计信息

```bash
python main.py --stats
```

### 5. 查看文章列表

```bash
# 默认显示前20篇
python main.py --list

# 分页显示
python main.py --list --offset 20
```

### 6. 搜索文章

```bash
# 搜索文章（标题+全文）
python main.py --search 关键词

# 显示更多结果
python main.py --search 关键词 --limit 50
```

### 7. 搜索并导出

```bash
# 搜索并导出为JSON
python main.py --search 关键词 --search-export json

# 搜索并导出为TXT
python main.py --search 关键词 --search-export txt
```

### 8. 导出全部数据

```bash
# 导出为JSON
python main.py --export json

# 导出为TXT文本（包含完整文章内容）
python main.py --export txt
```

### 9. 测试模式

```bash
# 测试模式：只爬取1页
python main.py --test

# 测试模式：只爬列表，不爬详情页
python main.py --test --no-detail
```

## 配置说明

编辑 `config.py` 文件可以修改以下配置：

| 配置项 | 说明 | 默认值 |
|:-------|------|--------|
| `BASE_URL` | 目标网站分页URL | liebiao.php |
| `BASE_DOMAIN` | 基础域名 | https://tougao.12371.cn |
| `CRAWL_INTERVAL_HOURS` | 爬取间隔（小时） | 6 |
| `MAX_PAGES_PER_RUN` | 每次运行最大爬取页数 | 10 |
| `REQUEST_TIMEOUT` | 请求超时时间（秒） | 30 |
| `MAX_RETRIES` | 最大重试次数 | 3 |

## 存储架构

存储模块采用**分片存储 + 索引机制**设计：

| 特性 | 说明 |
|------|------|
| 分片大小 | 每1000篇文章一个分片文件 |
| 索引机制 | 内存索引 + 持久化索引，O(1)查询 |
| 延迟加载 | 只加载需要的分片，非全量加载 |
| 批量写入 | 新增文章缓存，累积100篇后批量刷入 |

**性能指标**：
- 写入速度：~5500篇/秒（批量写入）
- 单篇读取：<1ms
- 存在检查：<1ms

## 注意事项

1. **网站结构适配**: 由于目标网站的HTML结构可能变化，如果解析失败，需要根据实际情况调整 `crawler/parser.py` 中的CSS选择器。

2. **反爬策略**: 爬虫已内置随机延迟和重试机制，建议合理设置爬取间隔。

3. **数据安全**: 程序正常退出时会自动保存所有待写入数据，也可手动调用 `flush()` 方法。

## 扩展开发

### 添加新的解析规则

编辑 `crawler/parser.py` 中的 `parse_list_page` 和 `parse_detail_page` 方法，根据目标网站的HTML结构调整选择器。

### 修改存储方式

编辑 `storage/article_storage.py`，可以改为数据库存储（如SQLite、MySQL等）。

## 许可证

MIT License
