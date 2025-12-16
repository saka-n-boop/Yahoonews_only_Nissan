import os
import json
import time
import re
import random
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Optional, Set, Dict, Any
import sys
from urllib.parse import urlparse, parse_qs, urlunparse, urlencode

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

# --- Gemini API 関連 (v1.0 SDK) ---
from google import genai
from google.genai import types
from google.api_core.exceptions import ResourceExhausted

# --- コメント収集用モジュールのインポート ---
import comment_scraper
# ------------------------------------

# ====== 設定 ======
SHARED_SPREADSHEET_ID = os.environ.get("SPREADSHEET_KEY")
if not SHARED_SPREADSHEET_ID:
    print("エラー: 環境変数 'SPREADSHEET_KEY' が設定されていません。処理を中断します。")
    sys.exit(1)

KEYWORD_FILE = "keywords.txt"
SOURCE_SPREADSHEET_ID = SHARED_SPREADSHEET_ID
SOURCE_SHEET_NAME = "Yahoo"
DEST_SPREADSHEET_ID = SHARED_SPREADSHEET_ID
MAX_SHEET_ROWS_FOR_REPLACE = 10000
MAX_PAGES = 10 

# ヘッダー
YAHOO_SHEET_HEADERS = ["URL", "タイトル", "投稿日時", "ソース", "本文", "コメント数", "対象企業", "カテゴリ分類", "ポジネガ分類", "日産関連文", "日産ネガ文"]
REQ_HEADERS = {"User-Agent": "Mozilla/5.0"}
TZ_JST = timezone(timedelta(hours=9))

# プロンプトファイル一覧 (コメント分析用を追加)
ALL_PROMPT_FILES = [
    "prompt_gemini_role.txt",
    "prompt_target_company.txt",
    "prompt_category.txt",
    "prompt_posinega.txt",
    "prompt_nissan_mention.txt",
    "prompt_nissan_sentiment.txt",
    "prompt_comment_analysis.txt" # New!
]

# APIキー管理
AVAILABLE_API_KEYS = []
for i in range(1, 6):
    key = os.environ.get(f"GOOGLE_API_KEY_{i}")
    if key: AVAILABLE_API_KEYS.append(key)
if not AVAILABLE_API_KEYS:
    single_key = os.environ.get("GOOGLE_API_KEY")
    if single_key: AVAILABLE_API_KEYS.append(single_key)

if not AVAILABLE_API_KEYS:
    print("警告: APIキーが設定されていません。")
    GEMINI_CLIENT = None
else:
    print(f"APIキーを {len(AVAILABLE_API_KEYS)} 個ロードしました。")

CURRENT_KEY_INDEX = 0
REQUEST_COUNT_PER_KEY = 0
MAX_REQUESTS_BEFORE_ROTATE = 20
NORMAL_WAIT_SECONDS = 15

GEMINI_PROMPT_TEMPLATE = None # 記事分析用
COMMENT_PROMPT_TEMPLATE = None # コメント分析用

# ====== ヘルパー関数群 ======

def get_current_gemini_client() -> Optional[genai.Client]:
    if not AVAILABLE_API_KEYS: return None
    api_key = AVAILABLE_API_KEYS[CURRENT_KEY_INDEX]
    return genai.Client(api_key=api_key)

def rotate_api_key(reason="limit_reached"):
    global CURRENT_KEY_INDEX, REQUEST_COUNT_PER_KEY
    if not AVAILABLE_API_KEYS: return
    old_index = CURRENT_KEY_INDEX
    CURRENT_KEY_INDEX = (CURRENT_KEY_INDEX + 1) % len(AVAILABLE_API_KEYS)
    REQUEST_COUNT_PER_KEY = 0
    print(f"    [Key Rotation] 理由:{reason} | Key#{old_index + 1} -> Key#{CURRENT_KEY_INDEX + 1}")

def increment_request_count():
    global REQUEST_COUNT_PER_KEY
    if not AVAILABLE_API_KEYS: return
    REQUEST_COUNT_PER_KEY += 1
    if REQUEST_COUNT_PER_KEY >= MAX_REQUESTS_BEFORE_ROTATE:
        rotate_api_key(reason="count_limit")

def gspread_util_col_to_letter(col_index: int) -> str:
    if col_index < 1: raise ValueError("Column index must be >= 1")
    return re.sub(r'\d+', '', gspread.utils.rowcol_to_a1(1, col_index))

def jst_now() -> datetime: return datetime.now(TZ_JST)
def format_datetime(dt_obj) -> str: return dt_obj.strftime("%Y/%m/%d %H:%M:%S")

def parse_post_date(raw, today_jst: datetime) -> Optional[datetime]:
    if not raw or not isinstance(raw, str): return None
    s = raw.strip().replace('配信', '')
    s = re.sub(r"\([月火水木金土日]\)$", "", s).strip()
    for fmt in ("%Y/%m/%d %H:%M:%S", "%y/%m/%d %H:%M", "%m/%d %H:%M", "%Y/%m/%d %H:%M"):
        try:
            dt = datetime.strptime(s, fmt)
            if fmt == "%m/%d %H:%M": dt = dt.replace(year=today_jst.year)
            if dt.replace(tzinfo=TZ_JST) > today_jst + timedelta(days=31): dt = dt.replace(year=dt.year - 1)
            return dt.replace(tzinfo=TZ_JST)
        except ValueError: pass
    return None

def build_gspread_client() -> gspread.Client:
    creds_str = os.environ.get("GCP_SERVICE_ACCOUNT_KEY")
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    if creds_str:
        return gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(json.loads(creds_str), scope))
    return gspread.service_account(filename='credentials.json')

def load_keywords(filename: str) -> List[str]:
    try:
        with open(os.path.join(os.path.dirname(__file__), filename), 'r', encoding='utf-8') as f:
            return [l.strip() for l in f if l.strip() and not l.startswith('#')]
    except: return []

def load_merged_prompt() -> str:
    global GEMINI_PROMPT_TEMPLATE
    if GEMINI_PROMPT_TEMPLATE: return GEMINI_PROMPT_TEMPLATE
    combined = []
    try:
        # prompt_comment_analysis.txt 以外を結合
        for fname in ALL_PROMPT_FILES[:-1]:
            with open(os.path.join(os.path.dirname(__file__), fname), 'r', encoding='utf-8') as f:
                combined.append(f.read().strip())
        base = combined[0] + "\n" + "\n".join(combined[1:])
        base += "\n\n【重要】\n該当する情報（特に日産への言及やネガティブ要素）がない場合は、説明文や翻訳を一切書かず、必ず単語で『なし』とだけ出力してください。"
        base += "\n\n記事本文:\n{TEXT_TO_ANALYZE}"
        GEMINI_PROMPT_TEMPLATE = base
        return base
    except Exception as e: print(f"プロンプト読込エラー: {e}"); return ""

def load_comment_prompt() -> str:
    global COMMENT_PROMPT_TEMPLATE
    if COMMENT_PROMPT_TEMPLATE: return COMMENT_PROMPT_TEMPLATE
    try:
        with open(os.path.join(os.path.dirname(__file__), "prompt_comment_analysis.txt"), 'r', encoding='utf-8') as f:
            COMMENT_PROMPT_TEMPLATE = f.read().strip().replace("{COMMENT_TEXT}", "{TEXT_TO_ANALYZE}")
            return COMMENT_PROMPT_TEMPLATE
    except Exception as e: print(f"コメントプロンプト読込エラー: {e}"); return ""

def request_with_retry(url: str, max_retries: int = 3) -> Optional[requests.Response]:
    for attempt in range(max_retries):
        try:
            res = requests.get(url, headers=REQ_HEADERS, timeout=20)
            if res.status_code == 404: return None
            res.raise_for_status()
            return res
        except:
            if attempt < max_retries - 1: time.sleep(2)
            else: return None
    return None

def set_row_height(ws: gspread.Worksheet, row_height_pixels: int):
    try:
        ws.spreadsheet.batch_update({"requests": [{"updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "ROWS", "startIndex": 1, "endIndex": ws.row_count},
            "properties": {"pixelSize": row_height_pixels}, "fields": "pixelSize"}}]})
    except: pass

def update_sheet_with_retry(ws, range_name, values, max_retries=3):
    for attempt in range(max_retries):
        try:
            ws.update(range_name=range_name, values=values, value_input_option='USER_ENTERED')
            return
        except gspread.exceptions.APIError as e:
            if any(c in str(e) for c in ['500', '502', '503']): time.sleep(30 * (attempt + 1))
            else: raise e
        except: time.sleep(10 * (attempt + 1))

# ====== Gemini 共通呼び出し関数 ======
def call_gemini_api(prompt: str, is_batch: bool = False, schema: dict = None) -> Any:
    increment_request_count()
    client = get_current_gemini_client()
    if not client: return None

    for attempt in range(2):
        try:
            response = client.models.generate_content(
                model='gemini-1.5-flash',
                contents=prompt,
                config=types.GenerateContentConfig(response_mime_type="application/json", response_schema=schema),
            )
            return json.loads(response.text.strip())
        except ResourceExhausted:
            rotate_api_key("429_error")
            client = get_current_gemini_client()
            continue
        except Exception as e:
            if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                rotate_api_key("429_in_msg")
                client = get_current_gemini_client()
                continue
            print(f"    ! API Error: {e}")
            return None
    return None

# ====== 記事分析用関数 ======
def analyze_article_batch(texts: List[str]) -> Optional[List[Dict[str, str]]]:
    prompt_template = load_merged_prompt()
    if not prompt_template: return None
    combined_text = "".join([f"\n【記事 {i+1}】\n{txt[:3000]}\n" for i, txt in enumerate(texts)])
    prompt = prompt_template.replace("{TEXT_TO_ANALYZE}", combined_text)
    prompt += f"\n\n※上記の{len(texts)}つの記事それぞれについて分析し、必ず{len(texts)}個のオブジェクトを含むJSONリスト形式で出力してください。"
    
    schema = {"type": "array", "items": {"type": "object", "properties": {
        "company_info": {"type": "string"}, "category": {"type": "string"}, "sentiment": {"type": "string"},
        "nissan_related": {"type": "string"}, "nissan_negative": {"type": "string"}}}}
    
    result = call_gemini_api(prompt, is_batch=True, schema=schema)
    if result and isinstance(result, list):
        default = {"company_info": "N/A", "category": "N/A", "sentiment": "N/A", "nissan_related": "なし", "nissan_negative": "なし"}
        if len(result) < len(texts): result.extend([default] * (len(texts) - len(result)))
        return result[:len(texts)]
    return None

def analyze_article_single(text: str) -> Dict[str, str]:
    default = {"company_info": "N/A", "category": "N/A", "sentiment": "N/A", "nissan_related": "なし", "nissan_negative": "なし"}
    prompt_template = load_merged_prompt()
    if not prompt_template: return default
    prompt = prompt_template.replace("{TEXT_TO_ANALYZE}", text[:15000])
    schema = {"type": "object", "properties": {
        "company_info": {"type": "string"}, "category": {"type": "string"}, "sentiment": {"type": "string"},
        "nissan_related": {"type": "string"}, "nissan_negative": {"type": "string"}}}
    
    result = call_gemini_api(prompt, is_batch=False, schema=schema)
    return result if result else default

# ====== コメント要約用関数 (New!) ======
def analyze_comment_summary(text: str) -> Dict[str, Any]:
    default = {"nissan_product_neg": "なし", "summaries": ["-", "-", "-"], "topic_ranking": ["-", "-", "-", "-", "-"]}
    prompt_template = load_comment_prompt()
    if not prompt_template: return default
    
    # 制限: 最大10万文字程度にカット (トークン超過回避)
    prompt = prompt_template.replace("{TEXT_TO_ANALYZE}", text[:120000])
    
    schema = {"type": "object", "properties": {
        "nissan_product_neg": {"type": "string"},
        "summaries": {"type": "array", "items": {"type": "string"}},
        "topic_ranking": {"type": "array", "items": {"type": "string"}}
    }}
    
    # タイムアウト対策で少し余裕を持たせたいが、標準クライアント設定でいく
    result = call_gemini_api(prompt, is_batch=False, schema=schema)
    return result if result else default

# ====== スクレイピング関数群 ======
def get_yahoo_news_with_selenium(keyword: str) -> list[dict]:
    # (前回と同じため省略なしで記載)
    print(f"  Yahoo!ニュース検索: {keyword}")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument(f"user-agent={REQ_HEADERS['User-Agent']}")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    try: driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    except: return []
    driver.get(f"https://news.yahoo.co.jp/search?p={keyword}&ei=utf-8&categories=domestic,world,business,it,science,life,local")
    try: WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.CSS_SELECTOR, "li[class*='sc-1u4589e-0']")))
    except: pass
    time.sleep(3)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()
    data = []
    today = jst_now()
    for art in soup.find_all("li", class_=re.compile("sc-1u4589e-0")):
        try:
            title = art.find("div", class_=re.compile("sc-3ls169-0")).text.strip()
            link = art.find("a", href=True)["href"]
            if not link.startswith("https://news.yahoo.co.jp/articles/"): continue
            date_str = art.find("time").text.strip() if art.find("time") else ""
            src_div = art.find("div", class_=re.compile("sc-n3vj8g-0"))
            source = ""
            if src_div:
                sub = src_div.find("div", class_=re.compile("sc-110wjhy-8"))
                if sub:
                    cands = [s.text.strip() for s in sub.find_all("span") if not s.find("svg") and not re.match(r'\d{1,2}/\d{1,2}.*\d{2}:\d{2}', s.text.strip())]
                    if cands: source = max(cands, key=len)
            fmt_date = date_str
            try:
                dt = parse_post_date(date_str, today)
                if dt: fmt_date = format_datetime(dt)
                else: fmt_date = re.sub(r"\([月火水木金土日]\)$", "", date_str).strip()
            except: pass
            data.append({"URL": link, "タイトル": title, "投稿日時": fmt_date, "ソース": source})
        except: continue
    print(f"  取得件数: {len(data)}")
    return data

def fetch_article_body_and_comments(base_url: str) -> Tuple[str, int, Optional[str]]:
    aid = re.search(r'/articles/([a-f0-9]+)', base_url)
    if not aid: return "本文取得不可", -1, None
    clean_url = base_url.split('?')[0]
    full_body = []
    cmt_cnt = -1
    ext_date = None
    for page in range(1, MAX_PAGES + 1):
        res = request_with_retry(f"{clean_url}?page={page}")
        if not res: break
        if page > 1 and f"page={page}" not in res.url: break
        soup = BeautifulSoup(res.text, 'html.parser')
        if page == 1:
            btn = soup.find(["button", "a"], attrs={"data-cl-params": re.compile(r"cmtmod")})
            if btn:
                m = re.search(r'(\d+)', btn.get_text(strip=True).replace(",", ""))
                if m: cmt_cnt = int(m.group(1))
            art_div = soup.find('article') or soup.find('div', class_=re.compile(r'article_body|article_detail'))
            if art_div:
                m = re.search(r'(\d{1,2}/\d{1,2})\([月火水木金土日]\)(\s*)(\d{1,2}:\d{2})配信', art_div.get_text()[:500])
                if m: ext_date = f"{m.group(1)} {m.group(3)}"
        content = soup.find('article') or soup.find('div', class_=re.compile(r'article_detail|article_body'))
        p_texts = []
        if content:
            for n in content.find_all(['button', 'a', 'div'], class_=re.compile(r'reaction|rect|module|link|footer|comment')): n.decompose()
            ps = content.find_all('p', class_=re.compile(r'sc-\w+-0\s+\w+.*highLightSearchTarget')) or content.find_all('p')
            for p in ps:
                txt = p.get_text(strip=True)
                if txt and txt not in ["そう思う", "そう思わない", "学びがある", "わかりやすい", "新しい視点", "私もそう思います"]:
                    p_texts.append(txt)
        if not p_texts: 
            if page > 1: break
        page_txt = "\n".join(p_texts)
        if page > 1 and len(full_body) > 0 and page_txt == full_body[0].split('ーーーー\n')[-1]: break
        full_body.append(f"\n{page}ページ目{'ー'*30}\n{page_txt}")
        time.sleep(1)
    return "".join(full_body).strip() or "本文取得不可", cmt_cnt, ext_date

def ensure_source_sheet(gc):
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    try: ws = sh.worksheet(SOURCE_SHEET_NAME)
    except: ws = sh.add_worksheet(SOURCE_SHEET_NAME, MAX_SHEET_ROWS_FOR_REPLACE, len(YAHOO_SHEET_HEADERS))
    if ws.row_values(1) != YAHOO_SHEET_HEADERS:
        ws.update(range_name=f'A1:{gspread_util_col_to_letter(len(YAHOO_SHEET_HEADERS))}1', values=[YAHOO_SHEET_HEADERS])
    return ws

def fetch_details_and_update_sheet(gc: gspread.Client):
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    try: ws = sh.worksheet(SOURCE_SHEET_NAME)
    except: return
    all_values = ws.get_all_values(value_render_option='UNFORMATTED_VALUE')
    if len(all_values) <= 1: return
    data_rows = all_values[1:]
    now_jst = jst_now()
    three_days_ago = (now_jst - timedelta(days=3)).replace(hour=0, minute=0, second=0, microsecond=0)
    
    for idx, data_row in enumerate(data_rows):
        if len(data_row) < len(YAHOO_SHEET_HEADERS): data_row.extend([''] * (len(YAHOO_SHEET_HEADERS) - len(data_row)))
        row_num = idx + 2
        url = str(data_row[0])
        post_date_raw = str(data_row[2])
        body = str(data_row[4])
        comment_count_str = str(data_row[5])
        if not url.strip() or not url.startswith('http'): continue
        
        is_content_fetched = (body.strip() and body != "本文取得不可")
        post_date_dt = parse_post_date(post_date_raw, now_jst)
        is_within_three_days = (post_date_dt and post_date_dt >= three_days_ago)
        if is_content_fetched and not is_within_three_days: continue
        
        needs_full_fetch = not is_content_fetched
        is_comment_only_update = is_content_fetched and is_within_three_days
        
        if needs_full_fetch: print(f"  - 行 {row_num}: 本文/コメント取得...")
        elif is_comment_only_update: print(f"  - 行 {row_num}: コメント数更新...")
        
        fetched_body, fetched_comment_count, extracted_date = fetch_article_body_and_comments(url)
        
        new_body = body
        new_comment_count = comment_count_str
        new_post_date = post_date_raw
        needs_update = False
        
        if needs_full_fetch:
            if fetched_body != "本文取得不可":
                new_body = fetched_body
                needs_update = True
            elif body != "本文取得不可":
                new_body = "本文取得不可"
                needs_update = True
        
        if needs_full_fetch and ("取得不可" in post_date_raw or not post_date_raw.strip()) and extracted_date:
            dt = parse_post_date(extracted_date, now_jst)
            if dt: new_post_date = format_datetime(dt)
            else: new_post_date = re.sub(r"\([月火水木金土日]\)$", "", extracted_date).strip()
            needs_update = True
            
        if fetched_comment_count != -1:
            if str(fetched_comment_count) != comment_count_str:
                new_comment_count = str(fetched_comment_count)
                needs_update = True
        
        if needs_update:
            update_sheet_with_retry(ws, f'C{row_num}:F{row_num}', [[new_post_date, str(data_row[3]), new_body, new_comment_count]])
            time.sleep(1 + random.random() * 0.5)

def analyze_with_gemini_and_update_sheet(gc: gspread.Client):
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    try: ws = sh.worksheet(SOURCE_SHEET_NAME)
    except: return
    data_rows = ws.get_all_values()[1:]
    if not data_rows: return
    print("\n=====   ステップ④ Gemini分析 (バッチ優先) =====")
    target_tasks = []
    for idx, row in enumerate(data_rows):
        row_num = idx + 2
        if len(row) < len(YAHOO_SHEET_HEADERS): row.extend([''] * (len(YAHOO_SHEET_HEADERS) - len(row)))
        body = str(row[4])
        if all(str(v).strip() for v in row[6:11]): continue 
        if not body.strip() or body == "本文取得不可":
            update_sheet_with_retry(ws, f'G{row_num}:K{row_num}', [['N/A(No Body)', 'N/A', 'N/A', 'N/A', 'N/A']])
            continue
        target_tasks.append({"row_num": row_num, "body": body})

    if not target_tasks:
        print("  - 新規分析対象はありません。")
        return

    BATCH_SIZE = 5
    for i in range(0, len(target_tasks), BATCH_SIZE):
        batch = target_tasks[i : i + BATCH_SIZE]
        texts = [t["body"] for t in batch]
        row_nums = [t["row_num"] for t in batch]
        print(f"  - 分析中 (行 {row_nums[0]} ~ {row_nums[-1]}) ...")
        results = analyze_article_batch(texts)
        if results:
            for j, res in enumerate(results):
                n_rel, n_neg = res["nissan_related"], res["nissan_negative"]
                for txt in [n_rel, n_neg]:
                    if any(x in txt for x in ["not mentioned", "no mention", "発見されませんでした", "言及はありません"]):
                        if txt == n_rel: n_rel = "なし"
                        if txt == n_neg: n_neg = "なし"
                    if txt.lower() == "none":
                        if txt == n_rel: n_rel = "なし"
                        if txt == n_neg: n_neg = "なし"
                update_sheet_with_retry(ws, f'G{row_nums[j]}:K{row_nums[j]}', [[res["company_info"], res["category"], res["sentiment"], n_rel, n_neg]])
            print(f"    (Batch OK: {NORMAL_WAIT_SECONDS}s 待機)")
            time.sleep(NORMAL_WAIT_SECONDS)
        else:
            print("    ! バッチ失敗 -> バラ実行")
            for item in batch:
                res = analyze_article_single(item["body"])
                n_rel, n_neg = res["nissan_related"], res["nissan_negative"]
                for txt in [n_rel, n_neg]:
                    if any(x in txt for x in ["not mentioned", "no mention", "発見されませんでした", "言及はありません"]):
                        if txt == n_rel: n_rel = "なし"
                        if txt == n_neg: n_neg = "なし"
                    if txt.lower() == "none":
                        if txt == n_rel: n_rel = "なし"
                        if txt == n_neg: n_neg = "なし"
                update_sheet_with_retry(ws, f'G{item["row_num"]}:K{item["row_num"]}', [[res["company_info"], res["category"], res["sentiment"], n_rel, n_neg]])
                time.sleep(NORMAL_WAIT_SECONDS)
    print(" ? Gemini分析完了。")

def main():
    print("--- 統合スクリプト開始 ---")
    keys = load_keywords(KEYWORD_FILE)
    if not keys: sys.exit(0)
    try: gc = build_gspread_client()
    except Exception as e: print(f"致命的エラー: {e}"); sys.exit(1)
    
    for k in keys:
        print(f"\n===== ① 取得: {k} =====")
        data = get_yahoo_news_with_selenium(k)
        ws = ensure_source_sheet(gc)
        exist = set(str(r[0]) for r in ws.get_all_values()[1:] if len(r)>0 and str(r[0]).startswith("http"))
        new = [[d['URL'], d['タイトル'], d['投稿日時'], d['ソース']] for d in data if d['URL'] not in exist]
        if new: ws.append_rows(new, value_input_option='USER_ENTERED')
        time.sleep(2)

    print("\n===== ② 詳細取得 =====")
    fetch_details_and_update_sheet(gc)
    print("\n===== ③ ソート・整形 =====")
    sort_yahoo_sheet(gc)
    print("\n===== ④ Gemini分析 =====")
    analyze_with_gemini_and_update_sheet(gc)
    
    print("\n===== ⑤ コメント収集・要約 =====")
    # ここで関数オブジェクト(analyze_comment_summary)を渡す
    comment_scraper.run_comment_collection(gc, SHARED_SPREADSHEET_ID, SOURCE_SHEET_NAME, analyze_comment_summary)
    
    print("\n--- 統合スクリプト完了 ---")

if __name__ == '__main__':
    if os.path.dirname(os.path.abspath(__file__)) not in sys.path:
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    main()
