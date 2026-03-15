# -*- coding: utf-8 -*-
"""
成语/词语模式提取工具
使用jieba分词，再用正则表达式处理
只需要修改 PATTERN_REGEX 即可自定义提取模式
"""
import sys
import io
import json
import re
import os
import glob
import jieba

# 设置UTF-8输出
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')


# ==================== 在这里修改正则表达式模式 ====================
# 格式: '模式名': r'正则表达式'
# 正则表达式必须是完整的词匹配（^开头，$结尾）
#
# 模式说明：
#   ABAC: 第1=第3字
#   ABCB: 第2=第4字
#   AABC: 前两字相同
#   AABB: 第1=第2字，第3=第4字
# ============================================================
PATTERN_REGEX = {
    # ABAC: 第1、3字相同
    # 如：一心一意、全心全意
    'ABAC': r'^(\S)(\S)\1(\S)$',

    # ABCB: 第2、4字相同
    # 如：念兹在兹、便民利民
    'ABCB': r'^(\S)(\S)(\S)\2$',

    # AABC: 前两字相同
    # 如：欣欣向荣、蒸蒸日上
    'AABC': r'^(\S)\1(\S)(\S)$',

    # AABB: 第1、2字相同，第3、4字相同
    # 如：干干净净
    'AABB': r'^(\S)\1(\S)\2$',
}
# ============================================================


def is_pure_chinese(text):
    """检查文本是否全部为中文字符"""
    if not text:
        return False
    return all('\u4e00' <= char <= '\u9fff' for char in text)


def has_punctuation(text):
    """检查文本是否包含标点符号"""
    if not text:
        return False
    punctuation_pattern = re.compile(r'[，。！？、；：""''（）【】《》…—·]')
    return bool(punctuation_pattern.search(text))


def extract_by_pattern(word, pattern_regex):
    """用正则表达式检测词语是否符合模式"""
    try:
        return bool(re.match(pattern_regex, word))
    except re.error:
        return False


def filter_pattern(words, pattern_name):
    """用正则表达式过滤出符合指定模式的词语"""
    if pattern_name not in PATTERN_REGEX:
        return set()

    pattern = PATTERN_REGEX[pattern_name]
    result = set()

    for word in words:
        # 只处理4字词语（成语）
        if len(word) == 4 and extract_by_pattern(word, pattern):
            result.add(word)

    return result


def extract_pattern_words(text):
    """
    从文本中提取符合模式的词语
    1. 先用jieba分词
    2. 滤除标点符号
    3. 用正则表达式匹配各种模式
    """
    # 1. jieba分词
    words = jieba.lcut(text)

    # 2. 滤除标点和非纯中文词语
    clean_words = []
    for word in words:
        word = word.strip()
        # 长度4，纯中文，不含标点
        if len(word) == 4 and not has_punctuation(word) and is_pure_chinese(word):
            clean_words.append(word)

    # 3. 用正则表达式匹配各种模式
    results = {}
    for pattern_name in PATTERN_REGEX:
        results[pattern_name] = filter_pattern(clean_words, pattern_name)

    return results


def load_all_articles_text(json_files):
    """读取所有JSON文件中的文章内容，合并为一个文本"""
    all_text = ""

    for json_path in json_files:
        print(f"读取文件: {json_path}")

        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if isinstance(data, dict):
            if 'articles' in data:
                articles = data['articles']
            else:
                articles = list(data.values())
        elif isinstance(data, list):
            articles = data
        else:
            continue

        for article in articles:
            content = article.get('content', '')
            if content:
                all_text += content + "\n"

    return all_text


if __name__ == '__main__':
    # 自动识别目标目录下的所有 JSON 文件
    target_dir = 'data/shards'
    json_files = glob.glob(os.path.join(target_dir, '*.json'))

    print(f"自动识别到 {len(json_files)} 个 JSON 文件")
    print(f"当前提取模式: {list(PATTERN_REGEX.keys())}\n")

    # 读取所有文章内容到text中
    print("正在读取所有文章内容...")
    all_text = load_all_articles_text(json_files)
    print(f"已读取 {len(all_text)} 个字符\n")

    # 使用PATTERN_REGEX中的正则表达式模式提取
    print("正在提取词语模式...")
    total_results = extract_pattern_words(all_text)

    # 输出统计结果
    print(f"\n{'#' * 60}")
    print(f"提取结果统计")
    print(f"{'#' * 60}")

    for pattern_name in PATTERN_REGEX:
        words = sorted(total_results[pattern_name])
        print(f"\n{pattern_name} ({len(words)}个):")
        print(', '.join(words))

    # 保存结果
    output_path = 'pattern_words_result.txt'
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("词语模式提取结果\n")
        f.write(f"提取模式: {list(PATTERN_REGEX.keys())}\n")
        f.write("=" * 60 + "\n\n")

        for pattern_name in PATTERN_REGEX:
            words = sorted(total_results[pattern_name])
            f.write(f"{pattern_name} ({len(words)}个):\n")
            f.write(', '.join(words) + "\n\n")

    print(f"\n结果已保存到: {output_path}")
