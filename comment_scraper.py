import time
import re
import requests
from bs4 import BeautifulSoup
import gspread

# 設定
COMMENTS_SHEET_NAME = "Comments"
REQ_HEADERS = {"User-Agent": "Mozilla/5.0"}

def ensure_comments_sheet(sh: gspread.Spreadsheet):
    """ Commentsシートがなければ作成し、ヘッダーを設定する """
    try:
        ws = sh.worksheet(COMMENTS_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        # 列数は多めに確保
        ws = sh.add_worksheet(title=COMMENTS_SHEET_NAME, rows="1000", cols="300")
        
        # ヘッダー作成
        headers = ["URL", "タイトル", "投稿日時", "ソース"]
        # コメント列：1-10 ... 2491-2500
        for i in range(0, 250):
            start = i * 10 + 1
            end = (i + 1) * 10
            headers.append(f"コメント：{start} - {end}")
            
        ws.update(range_name='A1', values=[headers])
        
    return ws

def fetch_comments_from_url(article_url: str) -> list[str]:
    """ 
    記事URLから全コメントを取得し、10件ごとに結合したリストを返す 
    対策: &order=newer で新しい順に取得し、重複を厳密に排除する
    """
    
    # URL調整 (/commentsエンドポイントを作成)
    base_url = article_url.split('?')[0]
    if not base_url.endswith('/comments'):
        if '/comments' in base_url:
             base_url = base_url.split('/comments')[0] + '/comments'
        else:
             base_url = f"{base_url}/comments"

    all_comments_data = [] 
    seen_comments = set() # 重複チェック用
    page = 1
    
    print(f"    - コメント取得開始(新しい順): {base_url}")

    while True:
        # order=newer を付与して新しい順にする
        target_url = f"{base_url}?page={page}&order=newer"
        
        try:
            res = requests.get(target_url, headers=REQ_HEADERS, timeout=10)
            if res.status_code == 404:
                break 
            res.raise_for_status()
        except Exception as e:
            print(f"      ! 通信エラー(p{page}): {e}")
            break

        soup = BeautifulSoup(res.text, 'html.parser')
        
        # 記事タグ構造から取得
        articles = soup.find_all('article')
        
        if not articles:
            break 

        new_comments_in_this_page = 0
        
        for art in articles:
            # ユーザー名
            user_tag = art.find('h2')
            user_name = user_tag.get_text(strip=True) if user_tag else "匿名"
            
            # 本文
            p_tags = art.find_all('p')
            comment_body = ""
            if p_tags:
                comment_body = max([p.get_text(strip=True) for p in p_tags], key=len)
            
            if comment_body:
                full_text = f"【投稿者: {user_name}】\n{comment_body}"
                
                # 重複チェック
                if full_text in seen_comments:
                    continue
                
                seen_comments.add(full_text)
                all_comments_data.append(full_text)
                new_comments_in_this_page += 1
        
        # 新しいコメントがなければ終了
        if new_comments_in_this_page == 0:
            break 

        page += 1
        time.sleep(1) 

    # 10件ごとに結合
    merged_columns = []
    chunk_size = 10
    for i in range(0, len(all_comments_data), chunk_size):
        chunk = all_comments_data[i : i + chunk_size]
        merged_text = "\n\n".join(chunk)
        merged_columns.append(merged_text)
        
    print(f"    - 取得完了: 全{len(all_comments_data)}件")
    return merged_columns

def set_row_height(ws, pixels):
    try:
        requests = [{
           "updateDimensionProperties": {
                 "range": {"sheetId": ws.id, "dimension": "ROWS", "startIndex": 1, "endIndex": ws.row_count},
                 "properties": {"pixelSize": pixels}, "fields": "pixelSize"
            }
        }]
        ws.spreadsheet.batch_update({"requests": requests})
    except: pass

def run_comment_collection(gc: gspread.Client, source_sheet_id: str, source_sheet_name: str):
    print("\n=====   ステップ⑤ 条件付きコメント収集・保存 =====")
    
    sh = gc.open_by_key(source_sheet_id)
    try:
        source_ws = sh.worksheet(source_sheet_name)
    except:
        print("  ! Sourceシートが見つかりません。")
        return

    dest_ws = ensure_comments_sheet(sh)
    
    # 既存データの読み込み
    dest_rows = dest_ws.get_all_values()
    existing_urls = set()
    if len(dest_rows) > 1:
        existing_urls = set(row[0] for row in dest_rows[1:] if row)

    # ソースデータの読み込み
    source_rows = source_ws.get_all_values()
    if len(source_rows) < 2: return
    data_rows = source_rows[1:]
    
    process_count = 0

    for i, row in enumerate(data_rows):
        if len(row) < 11: continue
        
        url = row[0]
        title = row[1]
        post_date = row[2]
        source = row[3]
        comment_count_str = row[5]
        nissan_neg_text = row[11] # K列: 日産ネガ文
        
        # 重複チェック
        if url in existing_urls:
            continue

        # --- 条件判定 (変更後) ---
        is_target = False
        
        # 条件①: コメント数が100件以上 (企業問わず)
        try:
            cnt = int(re.sub(r'\D', '', comment_count_str))
            if cnt >= 100:
                is_target = True
        except:
            pass
            
        # 条件②: 日産ネガ文に記載がある ("なし" 以外)
        if not is_target:
            val = str(nissan_neg_text).strip()
            # 「なし」「N/A」「-」以外の記述があれば対象
            if val and val not in ["なし", "N/A", "N/A(No Body)", "-"]:
                is_target = True
        
        if is_target:
            print(f"  - 対象記事発見(行{i+2}): {title[:20]}...")
            comment_columns = fetch_comments_from_url(url)
            
            if comment_columns:
                row_data = [url, title, post_date, source] + comment_columns
                dest_ws.append_rows([row_data], value_input_option='USER_ENTERED')
                process_count += 1
                time.sleep(2)

    # --- 最後にソート処理 ---
    if process_count > 0:
        print("  - Commentsシートを投稿日時順（新しい順）に並び替えます...")
        try:
            last_row = len(dest_ws.col_values(1))
            if last_row > 1:
                dest_ws.sort((3, 'des'), range=f'A2:Z{last_row}') 
        except Exception as e:
            print(f"  ! ソートエラー: {e}")
            
        set_row_height(dest_ws, 21)

    print(f" ? コメント収集完了: 新たに {process_count} 件の記事からコメントを保存しました。")