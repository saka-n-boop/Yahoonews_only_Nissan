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

# --- Gemini API 関連のインポート (v1.0 SDK) ---
from google import genai
from google.genai import types
from google.api_core.exceptions import ResourceExhausted, ServiceUnavailable, InternalServerError
# ------------------------------------

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
MAX_SHEET_ROWS_FOR_REPLACE = 10000

# 最大取得ページ数を10に設定
MAX_PAGES = 10 

# ヘッダー (G列にチェック日時、H列以降にGemini分析結果)
YAHOO_SHEET_HEADERS = [
    "URL", "タイトル", "投稿日時", "ソース", "本文", "コメント数", 
    "ニュースチェック日時", # G列
    "対象企業", "カテゴリ分類", "ポジネガ分類", "日産関連文", "日産ネガ文" # H～L列
]
REQ_HEADERS = {"User-Agent": "Mozilla/5.0"}
TZ_JST = timezone(timedelta(hours=9))

# 読み込むプロンプトファイル一覧
ALL_PROMPT_FILES = [
    "prompt_gemini_role.txt",
    "prompt_target_company.txt",
    "prompt_category.txt",
    "prompt_posinega.txt",
    "prompt_nissan_mention.txt",
    "prompt_nissan_sentiment.txt"
]

# ====== APIキー管理設定 ======
API_KEY_ENV_NAMES = ["GOOGLE_API_KEY_1", "GOOGLE_API_KEY_2", "GOOGLE_API_KEY_3"]
AVAILABLE_API_KEYS = []
for env_name in API_KEY_ENV_NAMES:
    key = os.environ.get(env_name)
    if key:
        AVAILABLE_API_KEYS.append(key)

if not AVAILABLE_API_KEYS:
    single_key = os.environ.get("GOOGLE_API_KEY")
    if single_key:
        AVAILABLE_API_KEYS.append(single_key)

if not AVAILABLE_API_KEYS:
    print("警告: APIキー環境変数 (GOOGLE_API_KEY_1～3) が設定されていません。Gemini分析はスキップされます。")

CURRENT_KEY_INDEX = 0
REQUEST_COUNT_PER_KEY = 0
MAX_REQUESTS_BEFORE_ROTATE = 20
BATCH_SIZE = 5
NORMAL_WAIT_SECONDS = 15

GEMINI_PROMPT_TEMPLATE = None

# ====== ヘルパー関数群 ======

def get_current_gemini_client() -> Optional[genai.Client]:
    """ 現在のインデックスに対応するAPIキーでクライアントを作成して返す """
    global CURRENT_KEY_INDEX
    if not AVAILABLE_API_KEYS:
        return None
    api_key = AVAILABLE_API_KEYS[CURRENT_KEY_INDEX]
    return genai.Client(api_key=api_key)

def rotate_api_key_if_needed():
    """ リクエスト回数をチェックし、上限を超えていたら次のキーに切り替える """
    global CURRENT_KEY_INDEX, REQUEST_COUNT_PER_KEY
    if not AVAILABLE_API_KEYS: return
    REQUEST_COUNT_PER_KEY += 1
    if REQUEST_COUNT_PER_KEY >= MAX_REQUESTS_BEFORE_ROTATE:
        print(f"    [Key Info] キー#{CURRENT_KEY_INDEX + 1} の使用回数が {REQUEST_COUNT_PER_KEY}回 に達しました。")
        CURRENT_KEY_INDEX = (CURRENT_KEY_INDEX + 1) % len(AVAILABLE_API_KEYS)
        REQUEST_COUNT_PER_KEY = 0
        print(f"    [Key Info] キー#{CURRENT_KEY_INDEX + 1} に切り替えます。")

def gspread_util_col_to_letter(col_index: int) -> str:
    if col_index < 1: raise ValueError("Column index must be 1 or greater")
    a1_notation = gspread.utils.rowcol_to_a1(1, col_index)
    return re.sub(r'\d+', '', a1_notation)

def jst_now() -> datetime:
    return datetime.now(TZ_JST)

def format_datetime(dt_obj) -> str:
    return dt_obj.strftime("%Y/%m/%d %H:%M:%S")

def parse_post_date(raw, today_jst: datetime) -> Optional[datetime]:
    if raw is None: return None
    if isinstance(raw, str):
        s = raw.strip()
        s = re.sub(r"\([月火水木金土日]\)", "", s).strip() # 曜日削除
        s = s.replace('配信', '').strip()
        
        for fmt in ("%Y/%m/%d %H:%M:%S", "%y/%m/%d %H:%M", "%m/%d %H:%M", "%Y/%m/%d %H:%M"):
            try:
                dt = datetime.strptime(s, fmt)
                if fmt == "%m/%d %H:%M":
                    dt = dt.replace(year=today_jst.year)
                if dt.replace(tzinfo=TZ_JST) > today_jst + timedelta(days=31):
                    dt = dt.replace(year=dt.year - 1)
                return dt.replace(tzinfo=TZ_JST)
            except ValueError:
                pass
        return None

def calculate_check_date_str(dt_obj: datetime) -> str:
    """
    15:00:01 ～ 翌15:00:00 のサイクルで日付を判定
    """
    if not dt_obj:
        return ""
    threshold = dt_obj.replace(hour=14, minute=30, second=0, microsecond=0)
    if dt_obj > threshold:
        return (dt_obj + timedelta(days=1)).strftime("%Y/%m/%d")
    else:
        return dt_obj.strftime("%Y/%m/%d")

def build_gspread_client() -> gspread.Client:
    try:
        creds_str = os.environ.get("GCP_SERVICE_ACCOUNT_KEY")
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        if creds_str:
            info = json.loads(creds_str)
            credentials = ServiceAccountCredentials.from_json_keyfile_dict(info, scope)
            return gspread.authorize(credentials)
        else:
            return gspread.service_account(filename='credentials.json')
    except Exception as e:
        raise RuntimeError(f"Google認証に失敗: {e}")

def load_keywords(filename: str) -> List[str]:
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(script_dir, filename)
        with open(file_path, 'r', encoding='utf-8') as f:
            keywords = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        return keywords
    except FileNotFoundError:
        return []
    except Exception:
        return []

def load_merged_prompt() -> str:
    global GEMINI_PROMPT_TEMPLATE
    if GEMINI_PROMPT_TEMPLATE is not None:
        return GEMINI_PROMPT_TEMPLATE
    combined_instructions = []
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        role_file = ALL_PROMPT_FILES[0]
        file_path = os.path.join(script_dir, role_file)
        with open(file_path, 'r', encoding='utf-8') as f:
            role_instruction = f.read().strip()
        for filename in ALL_PROMPT_FILES[1:]:
            file_path = os.path.join(script_dir, filename)
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
            if content: combined_instructions.append(content)
        base_prompt = role_instruction + "\n" + "\n".join(combined_instructions)
        base_prompt += "\n\n【重要】\n該当する情報（特に日産への言及やネガティブ要素）がない場合は、説明文や翻訳を一切書かず、必ず単語で『なし』とだけ出力してください。"
        base_prompt += "\n\n以下は分析対象の複数の記事データです。JSON配列形式で、各記事IDに対応する分析結果を出力してください。\n\n{TEXT_TO_ANALYZE}"
        GEMINI_PROMPT_TEMPLATE = base_prompt
        return base_prompt
    except Exception as e:
        print(f"致命的エラー: プロンプトファイルの読み込み中にエラー: {e}")
        return ""

def request_with_retry(url: str, max_retries: int = 3) -> Optional[requests.Response]:
    for attempt in range(max_retries):
        try:
            res = requests.get(url, headers=REQ_HEADERS, timeout=20)
            if res.status_code == 404: return None
            res.raise_for_status()
            return res
        except requests.exceptions.RequestException:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt + random.random())
            else:
                return None
    return None

def set_row_height(ws: gspread.Worksheet, row_height_pixels: int):
    try:
        requests = [{
           "updateDimensionProperties": {
                 "range": {"sheetId": ws.id, "dimension": "ROWS", "startIndex": 1, "endIndex": ws.row_count},
                 "properties": {"pixelSize": row_height_pixels}, "fields": "pixelSize"
            }
        }]
        ws.spreadsheet.batch_update({"requests": requests})
    except Exception: pass

def update_sheet_with_retry(ws, range_name, values, max_retries=3):
    for attempt in range(max_retries):
        try:
            ws.update(range_name=range_name, values=values, value_input_option='USER_ENTERED')
            return
        except Exception as e:
            error_str = str(e)
            if any(code in error_str for code in ['500', '502', '503']):
                time.sleep(30 * (attempt + 1))
            else:
                time.sleep(10 * (attempt + 1))
    print(f"  !! 最終リトライ失敗: {range_name} の更新をスキップします。")

# ====== Gemini 分析関数 (バッチ＆単体両対応) ======

def call_gemini_api(prompt: str, is_batch: bool = False) -> Any:
    template = load_merged_prompt()
    if not template: return None
    final_prompt = template.replace("{TEXT_TO_ANALYZE}", prompt)
    
    response_properties = {
        "company_info": {"type": "string", "description": "記事の主題企業名"},
        "category": {"type": "string", "description": "カテゴリ分類"},
        "sentiment": {"type": "string", "description": "ポジティブ、ニュートラル、ネガティブ"},
        "nissan_related": {"type": "string", "description": "日産に関連する言及（なければ『なし』）"},
        "nissan_negative": {"type": "string", "description": "日産に対するネガティブな文脈（なければ『なし』）"}
    }
    
    if is_batch:
        response_properties["id"] = {"type": "integer", "description": "記事ID"}
        schema = {"type": "array", "items": {"type": "object", "properties": response_properties}}
    else:
        schema = {"type": "object", "properties": response_properties}

    max_retries = 2
    for attempt in range(max_retries + 1):
        client = get_current_gemini_client()
        if not client: return None
        try:
            rotate_api_key_if_needed()
            response = client.models.generate_content(
                model='gemini-2.5-flash', 
                contents=final_prompt,
                config=types.GenerateContentConfig(response_mime_type="application/json", response_schema=schema),
            )
            time.sleep(NORMAL_WAIT_SECONDS)
            return json.loads(response.text.strip())
        except (ResourceExhausted, ServiceUnavailable, InternalServerError) as e:
            print(f"    ! Gemini API エラー (Attempt {attempt+1}): {e}")
            if attempt < max_retries:
                print("      -> 30秒待機してリトライします...")
                time.sleep(30)
                continue
            else: return None
        except Exception as e:
            print(f"    ! Gemini 予期せぬエラー: {e}")
            return None
    return None

def analyze_batch(rows_data: List[Dict]) -> Dict[int, Dict]:
    input_text = ""
    for item in rows_data:
        body_short = item['body'][:3000]
        input_text += f"【記事ID: {item['id']}】\n{body_short}\n----------------\n"
    result_map = {}
    print(f"    ... {len(rows_data)}件をまとめてAPIリクエスト中 (Key#{CURRENT_KEY_INDEX+1}) ...")
    api_response = call_gemini_api(input_text, is_batch=True)
    if api_response and isinstance(api_response, list):
        for res_item in api_response:
            r_id = res_item.get("id")
            if r_id is not None: result_map[r_id] = res_item
    return result_map

def analyze_single(body: str) -> Dict[str, str]:
    body_short = body[:10000]
    res = call_gemini_api(body_short, is_batch=False)
    default_res = {"company_info": "N/A", "category": "N/A", "sentiment": "N/A", "nissan_related": "なし", "nissan_negative": "なし"}
    if res and isinstance(res, dict): return {**default_res, **res}
    return default_res

# ====== Selenium関連関数 ======
def get_yahoo_news_with_selenium(keyword: str) -> list[dict]:
    print(f"  Yahoo!ニュース検索開始 (キーワード: {keyword})...")
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f"user-agent={REQ_HEADERS['User-Agent']}")
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    try:
        driver_path = ChromeDriverManager().install()
        service = Service(driver_path)
        driver = webdriver.Chrome(service=service, options=options)
    except Exception as e:
        print(f" WebDriverの初期化に失敗しました: {e}")
        return []
        
    search_url = f"https://news.yahoo.co.jp/search?p={keyword}&ei=utf-8&categories=domestic,world,business,it,science,life,local"
    driver.get(search_url)
    try:
        WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.CSS_SELECTOR, "li[class*='sc-1u4589e-0']")))
        time.sleep(3)
        for i in range(2):
            try:
                more_button = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, "//button[span[contains(text(), 'もっと見る')]]")))
                driver.execute_script("arguments[0].click();", more_button)
                time.sleep(3)
            except Exception: break
    except Exception: pass
    
    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()
    
    articles = soup.find_all("li", class_=re.compile("sc-1u4589e-0"))
    articles_data = []
    today_jst = jst_now()
    
    for article in articles:
        try:
            title_tag = article.find("div", class_=re.compile("sc-3ls169-0"))
            title = title_tag.text.strip() if title_tag else ""
            link_tag = article.find("a", href=True)
            url = link_tag["href"] if link_tag and link_tag["href"].startswith("https://news.yahoo.co.jp/articles/") else ""
            
            date_str = ""
            time_tag = article.find("time")
            if time_tag: date_str = time_tag.text.strip()
            
            # ソース抽出
            source_text = ""
            source_container = article.find("div", class_=re.compile("sc-n3vj8g-0"))
            if source_container:
                time_and_comments = source_container.find("div", class_=re.compile("sc-110wjhy-8"))
                if time_and_comments:
                    source_candidates = [
                        span.text.strip() for span in time_and_comments.find_all("span")
                        if not span.find("svg") and not re.match(r'\d{1,2}/\d{1,2}.*\d{2}:\d{2}', span.text.strip())
                    ]
                    if source_candidates:
                        source_text = max(source_candidates, key=len)
                    if not source_text:
                        for content in time_and_comments.contents:
                            if content.name is None and content.strip() and not re.match(r'\d{1,2}/\d{1,2}.*\d{2}:\d{2}', content.strip()):
                                source_text = content.strip()
                                break
            
            if title and url:
                formatted_date = date_str
                check_date_str = ""
                try:
                    dt_obj = parse_post_date(date_str, today_jst)
                    if dt_obj:
                        formatted_date = format_datetime(dt_obj)
                        check_date_str = calculate_check_date_str(dt_obj)
                    else:
                        formatted_date = re.sub(r"\([月火水木金土日]\)", "", date_str).replace('配信', '').strip()
                except: pass

                articles_data.append({
                    "URL": url, "タイトル": title, 
                    "投稿日時": formatted_date if formatted_date else "取得不可", 
                    "ソース": source_text if source_text else "取得不可",
                    "ニュースチェック日時": check_date_str
                })
        except Exception: continue
    return articles_data

def fetch_article_body_and_comments(base_url: str) -> Tuple[str, int, Optional[str]]:
    comment_count = -1
    extracted_date_str = None
    base_url_clean = base_url.split('?')[0]
    full_body_parts = []
    
    for page_num in range(1, MAX_PAGES + 1):
        target_url = f"{base_url_clean}?page={page_num}"
        response = request_with_retry(target_url)
        if not response: break
        soup = BeautifulSoup(response.text, 'html.parser')
        
        if page_num == 1:
            cmt_btn = soup.find("button", attrs={"data-cl-params": re.compile(r"cmtmod")}) or \
                      soup.find("a", attrs={"data-cl-params": re.compile(r"cmtmod")})
            if cmt_btn:
                m = re.search(r'(\d+)', cmt_btn.get_text(strip=True).replace(",", ""))
                if m: comment_count = int(m.group(1))
            article_temp = soup.find('article') or soup.find('div', class_=re.compile(r'article_body|article_detail'))
            if article_temp:
                temp_text = article_temp.get_text()[:500]
                m = re.search(r'(\d{1,2}/\d{1,2})\([月火水木金土日]\)(\s*)(\d{1,2}:\d{2})配信', temp_text)
                if m: extracted_date_str = f"{m.group(1)} {m.group(3)}"

        article_content = soup.find('article') or soup.find('div', class_='article_body') or soup.find('div', class_=re.compile(r'article_detail|article_body'))
        page_text_blocks = []
        if article_content:
            for noise in article_content.find_all(['button', 'a', 'div'], class_=re.compile(r'reaction|rect|module|link|footer|comment|recommended')):
                noise.decompose()
            paragraphs = article_content.find_all('p', class_=re.compile(r'sc-\w+-0\s+\w+.*highLightSearchTarget')) or article_content.find_all('p')
            for p in paragraphs:
                text = p.get_text(strip=True)
                if text and text not in ["そう思う", "そう思わない", "学びがある", "わかりやすい", "新しい視点", "私もそう思います"]:
                    page_text_blocks.append(text)
        
        if not page_text_blocks:
            if page_num > 1: break
        
        page_body = "\n".join(page_text_blocks)
        if page_num > 1 and len(full_body_parts) > 0 and page_body == full_body_parts[0].split('ーーーー\n')[-1]: break

        full_body_parts.append(f"\n{page_num}ページ目{'ー'*30}\n" + page_body)
        time.sleep(1)

    final_body_text = "".join(full_body_parts).strip()
    return final_body_text if final_body_text else "本文取得不可", comment_count, extracted_date_str

# ====== メイン処理群 ======

def ensure_source_sheet_headers(sh: gspread.Spreadsheet) -> gspread.Worksheet:
    try:
        ws = sh.worksheet(SOURCE_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=SOURCE_SHEET_NAME, rows=str(MAX_SHEET_ROWS_FOR_REPLACE), cols=str(len(YAHOO_SHEET_HEADERS)))
    if ws.row_values(1) != YAHOO_SHEET_HEADERS:
        ws.update(range_name=f'A1:{gspread.utils.rowcol_to_a1(1, len(YAHOO_SHEET_HEADERS))}', values=[YAHOO_SHEET_HEADERS])
    return ws

def write_news_list_to_source(gc: gspread.Client, articles: list[dict]):
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    ws = ensure_source_sheet_headers(sh)
    existing_urls = set(str(row[0]) for row in ws.get_all_values(value_render_option='UNFORMATTED_VALUE')[1:] if len(row) > 0 and str(row[0]).startswith("http"))
    # 【変更】E, F列は空にして、G列にチェック日時を入れる
    new_data = [
        [
            a['URL'], a['タイトル'], a['投稿日時'], a['ソース'], 
            "", "",  # E列:本文, F列:コメント数 は空欄
            a.get('ニュースチェック日時', '') # G列
        ] 
        for a in articles if a['URL'] not in existing_urls
    ]
    
    if new_data:
        ws.append_rows(new_data, value_input_option='USER_ENTERED')
        print(f"  SOURCEシートに {len(new_data)} 件追記しました。")

def fetch_details_and_update_sheet(gc: gspread.Client):
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    try: ws = sh.worksheet(SOURCE_SHEET_NAME)
    except: return
    
    all_values = ws.get_all_values(value_render_option='UNFORMATTED_VALUE')
    if len(all_values) <= 1: return
    
    print("\n=====   ステップ② 記事本文とコメント数の取得・即時反映 (E, F列) =====")
    now_jst = jst_now()
    three_days_ago = (now_jst - timedelta(days=3)).replace(hour=0, minute=0, second=0, microsecond=0)
    update_count = 0
    
    for idx, data_row in enumerate(all_values[1:]):
        if len(data_row) < len(YAHOO_SHEET_HEADERS): data_row.extend([''] * (len(YAHOO_SHEET_HEADERS) - len(data_row)))
        row_num = idx + 2
        url = str(data_row[0])
        post_date_raw = str(data_row[2])
        current_body = str(data_row[4])
        comment_count_str = str(data_row[5])
        
        if not url.startswith('http'): continue
        is_fetched = (current_body.strip() and current_body != "本文取得不可")
        dt_obj = parse_post_date(post_date_raw, now_jst)
        is_recent = (dt_obj and dt_obj >= three_days_ago)
        
        if is_fetched and not is_recent: continue
        
        print(f"  - 行 {row_num}: 詳細取得中...")
        fetched_body, cmt_cnt, ext_date = fetch_article_body_and_comments(url)
        
        final_body = current_body
        if fetched_body != "本文取得不可":
            final_body = fetched_body
        elif current_body == "" or current_body == "本文取得不可":
            final_body = "本文取得不可"
            
        final_date = format_datetime(parse_post_date(ext_date, now_jst)) if ext_date else post_date_raw
        final_cmt = str(cmt_cnt) if cmt_cnt != -1 else comment_count_str
        
        has_change = (final_body != current_body) or (final_cmt != comment_count_str) or (final_date != post_date_raw)
        
        if has_change:
            # C, D, E, F列を更新 (Dはソース。ここでは書き換えないが範囲に含むため現在の値を入れる)
            new_row = [final_date, str(data_row[3]), final_body, final_cmt]
            update_sheet_with_retry(ws, range_name=f'C{row_num}:F{row_num}', values=[new_row])
            update_count += 1
            time.sleep(1)

    print(f" ? {update_count} 行の詳細情報を更新しました。")

def sort_yahoo_sheet(gc: gspread.Client):
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    try: ws = sh.worksheet(SOURCE_SHEET_NAME)
    except: return
    if len(ws.col_values(1)) <= 1: return

    # 【変更】ニュースチェック日時(G列)を一括更新
    print(" ? ニュースチェック日時(G列)の一括更新中...")
    try:
        date_column = ws.col_values(3)[1:] # C列(日付)
        check_date_updates = []
        now_jst = jst_now()
        
        for raw_date in date_column:
            parsed = parse_post_date(raw_date, now_jst)
            if parsed:
                check_val = calculate_check_date_str(parsed)
                check_date_updates.append([check_val])
            else:
                check_date_updates.append([""]) 
        
        # G2から書き込み
        if check_date_updates:
            update_sheet_with_retry(ws, f'G2:G{len(check_date_updates)+1}', check_date_updates)
            
    except Exception as e:
        print(f" ?? ニュースチェック日時更新エラー: {e}")

    try:
        requests = []
        days_of_week = ["月", "火", "水", "木", "金", "土", "日"]
        for day in days_of_week:
            requests.append({"findReplace": {"range": {"sheetId": ws.id, "startColumnIndex": 2, "endColumnIndex": 3}, "find": rf"\({day}\)", "replacement": "", "searchByRegex": True}})
        requests.append({"findReplace": {"range": {"sheetId": ws.id, "startColumnIndex": 2, "endColumnIndex": 3}, "find": r"\s{2,}", "replacement": " ", "searchByRegex": True}})
        
        requests.append({"repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": ws.row_count, "startColumnIndex": 2, "endColumnIndex": 3},
            "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE_TIME", "pattern": "yyyy/mm/dd hh:mm:ss"}}},
            "fields": "userEnteredFormat.numberFormat"
        }})
        
        # 【修正】前回エラーだった箇所: ws.batch_update -> ws.spreadsheet.batch_update
        ws.spreadsheet.batch_update({"requests": requests})
        time.sleep(1)
        
        # G列まで含むが、ソートキーはC列(3列目)のまま
        ws.sort((3, 'des'), range=f'A2:{gspread_util_col_to_letter(len(YAHOO_SHEET_HEADERS))}{ws.row_count}')
        set_row_height(ws, 21)
        print(" ? シートのソート・整形完了")
    except Exception as e:
        print(f" ?? ソートエラー: {e}")

# ====== ステップ4: Gemini分析 メイン ======

def analyze_with_gemini_and_update_sheet(gc: gspread.Client):
    if not AVAILABLE_API_KEYS: return
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    try: ws = sh.worksheet(SOURCE_SHEET_NAME)
    except: return
    all_values = ws.get_all_values(value_render_option='UNFORMATTED_VALUE')
    if len(all_values) <= 1: return
    data_rows = all_values[1:]
    
    pending_rows = []
    print("\n=====   ステップ④ Gemini分析 (バッチ処理 & キーローテーション) =====")

    for idx, data_row in enumerate(data_rows):
        if len(data_row) < len(YAHOO_SHEET_HEADERS): data_row.extend([''] * (len(YAHOO_SHEET_HEADERS) - len(data_row)))
        row_num = idx + 2
        body = str(data_row[4])
        # 【変更】チェック済み判定カラムは H(7)～L(11) になる (全5項目)
        current_vals = data_row[7:12]
        
        if all(str(v).strip() for v in current_vals): continue
        if not body.strip() or body == "本文取得不可":
            # H～L列をN/Aで埋める
            update_sheet_with_retry(ws, f'H{row_num}:L{row_num}', [['N/A(No Body)', 'N/A', 'N/A', 'N/A', 'N/A']])
            continue
            
        pending_rows.append({"id": row_num, "body": body, "title": str(data_row[1])})

    total_processed = 0
    for i in range(0, len(pending_rows), BATCH_SIZE):
        batch = pending_rows[i : i + BATCH_SIZE]
        results_map = analyze_batch(batch)
        
        if results_map:
            for item in batch:
                res = results_map.get(item['id'])
                if res:
                    vals = [res.get("company_info", "N/A"), res.get("category", "N/A"), res.get("sentiment", "N/A"), res.get("nissan_related", "なし"), res.get("nissan_negative", "なし")]
                    for j in [3, 4]:
                        if any(x in vals[j].lower() for x in ["not mentioned", "no mention", "言及はありません", "none"]): vals[j] = "なし"
                    # 【変更】書き込み先は H～L列
                    update_sheet_with_retry(ws, f'H{item["id"]}:L{item["id"]}', [vals])
                    total_processed += 1
                else:
                    print(f"      - 行 {item['id']} の結果がバッチに含まれていません。単体リトライします。")
                    single_res = analyze_single(item['body'])
                    vals = [single_res["company_info"], single_res["category"], single_res["sentiment"], single_res["nissan_related"], single_res["nissan_negative"]]
                    update_sheet_with_retry(ws, f'H{item["id"]}:L{item["id"]}', [vals])
        else:
            print("    ! バッチ処理失敗。個別処理モード(Fallback)でリトライします。")
            for item in batch:
                print(f"      - 個別リトライ: 行 {item['id']}")
                res = analyze_single(item['body'])
                vals = [res["company_info"], res["category"], res["sentiment"], res["nissan_related"], res["nissan_negative"]]
                for j in [3, 4]:
                    if any(x in vals[j].lower() for x in ["not mentioned", "no mention", "言及はありません", "none"]): vals[j] = "なし"
                update_sheet_with_retry(ws, f'H{item["id"]}:L{item["id"]}', [vals])
                total_processed += 1
                time.sleep(NORMAL_WAIT_SECONDS)

    print(f" ? Gemini分析完了: {total_processed} 件処理しました。")

def main():
    print("--- 統合スクリプト開始 ---")
    keywords = load_keywords(KEYWORD_FILE)
    if not keywords: sys.exit(0)
    try: gc = build_gspread_client()
    except Exception as e: sys.exit(1)

    for kw in keywords:
        articles = get_yahoo_news_with_selenium(kw)
        write_news_list_to_source(gc, articles)
        time.sleep(2)

    print("\n=====  本文・コメント取得  =====")
    fetch_details_and_update_sheet(gc)
    print("\n=====   記事データのソートと整形 =====")
    sort_yahoo_sheet(gc)
    print("\n=====   Gemini分析 =====")
    analyze_with_gemini_and_update_sheet(gc)
    print("\n===== ⑤ コメント取得開始 =====")
    comment_scraper.run_comment_collection(gc, SHARED_SPREADSHEET_ID, SOURCE_SHEET_NAME)
    print("\n--- 統合スクリプト完了 ---")

if __name__ == '__main__':
    if os.path.dirname(os.path.abspath(__file__)) not in sys.path:
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    main()
