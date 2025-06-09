import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime, timedelta
import re
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Selenium相关库 (获取初始Cookie) ---
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

# --- 配置区 ---
# 目标板块的fid
FID = 853 

# 要爬取的板块页数
BOARD_PAGES_TO_SCRAPE = 200

# 基础URL
BASE_URL = "https://bbs.nga.cn"

# 并发请求数
MAX_WORKERS = 10

# --- 反爬策略 ---

# 这个好像不是必要的，似乎只有cookie是关键因素
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.3 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
]
USE_PROXY = True

# 本地代理
PROXY_URL = 'http://127.0.0.1:7897' 

# 请求间隔（似乎也不是必要因素）
MIN_DELAY = 1
MAX_DELAY = 3

# 通用的请求头
BASE_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Referer': f'https://bbs.nga.cn/thread.php?fid={FID}',
}

def get_initial_cookies_with_selenium():
    """
    启动Selenium来获取能够通过JS验证的初始Cookie。
    """
    print("正在启动浏览器以获取初始Cookie...")
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument(f'user-agent={random.choice(USER_AGENTS)}')

    if USE_PROXY:
        chrome_options.add_argument(f'--proxy-server={PROXY_URL}')

    driver = None
    try:
        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
        driver.set_page_load_timeout(30)
        driver.get(BASE_URL)

        if '访客不能直接访问' in driver.title:
            print("检测到JS质询，尝试自动点击...")
            wait = WebDriverWait(driver, 10)
            link = wait.until(EC.presence_of_element_located((By.PARTIAL_LINK_TEXT, '如不能自动跳转')))
            link.click()
            wait.until_not(EC.title_contains('访客不能直接访问'))
            print("已通过JS质询。")

        # 获取所有cookie
        cookies = driver.get_cookies()
        print("成功获取浏览器Cookie。")
        return {cookie['name']: cookie['value'] for cookie in cookies}

    except Exception as e:
        print(f"使用Selenium获取Cookie时出错: {e}")
        return None
    finally:
        if driver:
            driver.quit()
            print("浏览器已关闭。")

def fetch_url_with_requests(session, url, retries=3):
    """
    使用配置好的requests session进行高速并发请求。
    """
    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
    headers = BASE_HEADERS.copy()
    headers['User-Agent'] = random.choice(USER_AGENTS)
    
    for i in range(retries):
        try:
            response = session.get(url, headers=headers, timeout=20)
            if response.status_code == 403:
                print(f"请求 {url} 收到403 Forbidden，Cookie可能已失效。")
                # 在未来的版本中，可以触发重新获取cookie的逻辑
                continue # 尝试重试
            response.raise_for_status()
            response.encoding = 'gbk'
            return response.text
        except requests.exceptions.RequestException as e:
            print(f"请求 {url} 失败 (尝试 {i+1}/{retries}): {e}")
            if i < retries - 1:
                time.sleep(2)
    return None

def parse_nga_date(date_str):
    """
    更健壮的日期解析函数，处理时间戳和相对日期。
    """
    now = datetime.now()
    try:
        # 尝试解析为时间戳
        return datetime.fromtimestamp(int(date_str))
    except ValueError:
        # 解析为字符串日期
        if '昨天' in date_str:
            time_part = date_str.split(' ')[-1]
            day = now - timedelta(days=1)
            return datetime.strptime(f'{day.year}-{day.month}-{day.day} {time_part}', '%Y-%m-%d %H:%M')
        elif '-' in date_str:  # YYYY-MM-DD
            return datetime.strptime(date_str, '%Y-%m-%d')
        elif ':' in date_str:  # HH:MM (今天)
            return datetime.strptime(f'{now.year}-{now.month}-{now.day} {date_str}', '%Y-%m-%d %H:%M')
        else:
            return datetime(1970, 1, 1) # 返回一个默认的旧时间

def parse_thread_list(html):
    """
    解析板块列表页的HTML，提取帖子信息。
    """
    soup = BeautifulSoup(html, 'html.parser')
    threads = []
    
    topic_rows_table = soup.find('table', id='topicrows')
    if not topic_rows_table:
        print("警告：在当前页面未找到 'topicrows' 表格。可能是因为Cookie失效或已到最后一页。")
        return []

    for row in topic_rows_table.find_all('tr', class_='topicrow'):
        try:
            replies_tag = row.select_one('td.c1 > a.replies')
            replies = int(replies_tag.text) if replies_tag else 0

            title_tag = row.select_one('td.c2 > a.topic')
            if not title_tag: continue
            
            title = title_tag.text
            href = title_tag['href']
            tid_match = re.search(r'tid=(\d+)', href)
            if not tid_match: continue
            tid = tid_match.group(1)
            
            thread_url = f"{BASE_URL}/read.php?tid={tid}"
            author_tag = row.select_one('td.c3 > a.author')
            author = author_tag.text if author_tag else 'N/A'

            post_date_tag = row.select_one('td.c3 > span.postdate')
            if post_date_tag:
                post_time_dt = parse_nga_date(post_date_tag.text.strip())
                post_time = post_time_dt.strftime('%Y-%m-%d %H:%M:%S')
            else:
                post_time = '1970-01-01 00:00:00'

            threads.append({
                'tid': tid, 'title': title, 'author': author,
                'post_time': post_time, 'replies': replies, 'url': thread_url
            })
        except Exception as e:
            # 增加了对错误行的内容打印，方便调试
            error_row_text = row.get_text(strip=True, separator=' | ')
            print(f"解析某一行时出错: {e} - 行内容: '{error_row_text}'")
            continue
            
    return threads

def get_thread_content(html):
    """
    解析帖子详情页，提取第一页的所有文本内容。
    """
    soup = BeautifulSoup(html, 'html.parser')
    # 使用CSS选择器找到所有class为'postcontent ubbcode'的span标签
    content_spans = soup.select('span.postcontent.ubbcode')
    
    # 将所有楼层的内容合并，用换行符分隔
    full_content = "\n\n--- new post ---\n\n".join(
        [span.get_text(separator='\n', strip=True) for span in content_spans]
    )
    return full_content

def main():
    """
    主执行函数 - 混合模式
    """
    # --- 第一步: 使用Selenium获取初始Cookie ---
    initial_cookies = get_initial_cookies_with_selenium()
    if not initial_cookies:
        print("无法获取初始Cookie，程序退出。")
        return

    # --- 第二步: 配置高速Requests Session ---
    session = requests.Session()
    session.cookies.update(initial_cookies) # 注入Cookie
    if USE_PROXY:
        session.proxies = {'http': PROXY_URL, 'https': PROXY_URL}

    all_threads_info = []
    
    # --- 第三步: 并发爬取板块列表 ---
    print(f"\nCookie已注入，开始高速并发爬取板块列表，共 {BOARD_PAGES_TO_SCRAPE} 页...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_url = {
            executor.submit(fetch_url_with_requests, session, f"{BASE_URL}/thread.php?fid={FID}&page={page_num}"): page_num
            for page_num in range(1, BOARD_PAGES_TO_SCRAPE + 1)
        }
        for future in as_completed(future_to_url):
            page_num = future_to_url[future]
            html = future.result()
            if html:
                threads_on_page = parse_thread_list(html)
                all_threads_info.extend(threads_on_page)
                print(f"板块第 {page_num} 页解析完毕，获得 {len(threads_on_page)} 个帖子。")
            else:
                print(f"未能获取板块第 {page_num} 页的内容。")

    if not all_threads_info:
        print("未能获取任何帖子信息，程序退出。")
        return

    # 按发帖时间排序
    all_threads_info.sort(key=lambda x: x['post_time'], reverse=True)
    print(f"\n帖子列表爬取完成，共获得 {len(all_threads_info)} 个帖子的基本信息。")

    # --- 第四步: 并发爬取每个帖子的内容 ---
    print(f"\n开始高速并发爬取 {len(all_threads_info)} 个帖子的详细内容...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_thread = {
            executor.submit(fetch_url_with_requests, session, thread['url']): thread 
            for thread in all_threads_info
        }
        for i, future in enumerate(as_completed(future_to_thread)):
            thread = future_to_thread[future]
            html = future.result()
            print(f"正在处理帖子 ({i+1}/{len(all_threads_info)}): {thread['title'][:30]}...")
            if html:
                thread['content'] = get_thread_content(html)
            else:
                thread['content'] = "抓取失败"
        
    # --- 第五步: 保存数据到CSV文件 ---
    print("\n所有内容爬取完毕，正在保存到CSV文件...")
    df = pd.DataFrame(all_threads_info)
    output_filename = 'nga_zzz_threads_data.csv'
    df.to_csv(output_filename, index=False, encoding='utf-8-sig')
    
    print(f"数据已成功保存到文件: {output_filename}")
    print("\n爬虫任务完成！")
    print(df.head())

if __name__ == "__main__":
    main()