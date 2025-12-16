import time
import re
import json
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
        # 列数は多めに確保 (300列 = KN列まで)
        ws = sh.add_worksheet(title=COMMENTS_SHEET_NAME, rows="1000", cols="300")
        
        # ヘッダー作成 (新仕様: 要約とランキングを各1列に統合)
        # 1:URL, 2:タイトル, 3:日時, 4:ソース, 5:コメント数, 6:製品批判, 7:要約, 8:ランキング, 9~:コメント
        headers = [
            "URL", "タイトル", "投稿日時", "ソース", 
            "コメント数", "製品批判有無", 
            "コメント要約(全体)", "話題ランキング(TOP5)"
        ]
        
        # コメント本文列：1-10 ... (9列目から開始)
        for i in range(0, 240): 
            start = i * 10 + 1
            end = (i + 1) * 10
            headers.append(f"コメント：{start} - {end}")
            
        ws.update(range_name='A1', values=[headers])
        return ws
        
    return ws

def fetch_comments_from_url(article_url: str) -> list[str]:
    """ 記事URLから全コメントを取得し、10件ごとに結合したリストを返す """
    
    base_url = article_url.split('?')[0]
    if not base_url.endswith('/comments'):
        if '/comments' in base_url:
             base_url = base_url.split('/comments')[0] + '/comments'
        else:
             base_url = f"{base_url}/comments"

    all_comments_data = [] 
    seen_comments = set()
    page = 1
    
    print(f"    - コメント取得開始: {base_url}")

    while True:
        target_url = f"{base_url}?page={page}&order=newer"
        try:
            res = requests.get(target_url, headers=REQ_HEADERS, timeout=10)
            if res.status_code == 404: break 
            res.raise_for_status()
        except Exception: break

        soup = BeautifulSoup(res.text, 'html.parser')
        articles = soup.find_all('article')
        if not articles: break 

        new_cnt = 0
        for art in articles:
            user_tag = art.find('h2')
            user_name = user_tag.get_text(strip=True) if user_tag else "匿名"
            
            p_tags = art.find_all('p')
            comment_body = ""
            if p_tags:
                comment_body = max([p.get_text(strip=True) for p in p_tags], key=len)
            
            if comment_body:
                # ノイズ除去
                ignore = ["このコメントを削除しますか", "コメントを削除しました", "違反報告する", "非表示・報告", "投稿を受け付けました"]
                if any(x in comment_body for x in ignore): continue

                full_text = f"【投稿者: {user_name}】\n{comment_body}"
                if full_text in seen_comments: continue
                
                seen_comments.add(full_text)
                all_comments_data.append(full_text)
                new_cnt += 1
        
        if new_cnt == 0: break 
        page += 1
        time.sleep(1) 

    # 10件ごとに結合
    merged_columns = []
    chunk_size = 10
    for i in range(0, len(all_comments_data), chunk_size):
        chunk = all_comments_data[i : i + chunk_size]
        merged_text = "\n\n".join(chunk)
        merged_columns.append(merged_text)
    
    full_text_for_ai = "\n".join(all_comments_data)
    
    print(f"    - 取得完了: 全{len(all_comments_data)}件")
    return merged_columns, full_text_for_ai

def set_row_height(ws, pixels):
    try:
        requests = [{"updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "ROWS", "startIndex": 1, "endIndex": ws.row_count},
            "properties": {"pixelSize": pixels}, "fields": "pixelSize"}}]
        ws.spreadsheet.batch_update({"requests": requests})
    except: pass

def run_comment_collection(gc: gspread.Client, source_sheet_id: str, source_sheet_name: str, summarizer_func):
    """ 
    summarizer_func: main.pyから渡されるGemini分析用関数 
    """
    print("\n=====   ステップ⑤ 条件付きコメント収集・要約・保存 =====")
    
    sh = gc.open_by_key(source_sheet_id)
    try: source_ws = sh.worksheet(source_sheet_name)
    except: return

    dest_ws = ensure_comments_sheet(sh)
    
    # 既存チェック
    dest_rows = dest_ws.get_all_values()
    existing_urls = set()
    if len(dest_rows) > 1:
        existing_urls = set(row[0] for row in dest_rows[1:] if row)

    source_rows = source_ws.get_all_values()
    if len(source_rows) < 2: return
    
    # 生データ（ヘッダー除く）
    raw_data_rows = source_rows[1:]
    
    # コメント数順にソートするためのリスト作成
    sorted_target_rows = []
    
    for i, row in enumerate(raw_data_rows):
        if len(row) < 11: continue
        
        # コメント数を数値化
        cnt = 0
        try:
            cnt = int(re.sub(r'\D', '', str(row[5])))
        except:
            cnt = 0
            
        sorted_target_rows.append({
            "original_index": i,
            "count": cnt,
            "data": row
        })
    
    # コメント数が多い順に並び替え
    sorted_target_rows.sort(key=lambda x: x['count'], reverse=True)
    
    print(f"  - 分析順序: コメント数が多い順に {len(sorted_target_rows)} 件をスキャンします。")

    process_count = 0

    for item in sorted_target_rows:
        row = item['data']
        i = item['original_index']
        comment_cnt = item['count'] # 数値化したコメント数
        
        url = row[0]
        title = row[1]
        post_date = row[2]
        source = row[3]
        comment_count_str = row[5]
        target_company = row[7] # G列
        category = row[8]       # H列
        nissan_neg_text = row[11] # K列
        
        if url in existing_urls: continue

        # --- 条件判定 (修正版) ---
        is_target = False
        
        # 1. 共通の前提条件:
        #    - 対象企業が「日産」で始まる
        #    - カテゴリが「その他」を含まない
        #    - 【追加】コメント数が1以上であること (0件は除外)
        if target_company.startswith("日産") and "その他" not in category and comment_cnt > 0:
            
            # 条件①: コメント数が100件以上
            if comment_cnt >= 100:
                is_target = True
            
            # 条件②: 日産ネガ文に記載がある ("なし" 以外)
            if not is_target:
                val = str(nissan_neg_text).strip()
                if val and val not in ["なし", "N/A", "N/A(No Body)", "-"]:
                    is_target = True
        
        if is_target:
            print(f"  - 対象記事発見(元行{i+2}, コメ数{comment_cnt}): {title[:20]}...")
            
            # コメント取得
            comment_cols, full_text = fetch_comments_from_url(url)
            
            if comment_cols:
                # --- Gemini要約実行 ---
                print("    > Geminiでコメント要約中...")
                summary_data = summarizer_func(full_text)
                
                # 結果の展開
                prod_neg = summary_data.get("nissan_product_neg", "N/A")
                
                summaries_list = summary_data.get("summaries", [])
                summary_combined = "\n\n".join(summaries_list) if summaries_list else "-"
                
                rankings_list = summary_data.get("topic_ranking", [])
                ranking_combined = "\n".join(rankings_list) if rankings_list else "-"

                # データ構築
                row_data = [
                    url, title, post_date, source, 
                    comment_count_str, 
                    prod_neg,
                    summary_combined, 
                    ranking_combined
                ] + comment_cols
                
                dest_ws.append_rows([row_data], value_input_option='USER_ENTERED')
                process_count += 1
                
                print("    (Gemini実行完了: 60秒待機...)")
                time.sleep(60) 

    # 最後にソート (日時順)
    if process_count > 0:
        print("  - Commentsシートを日時順にソート中...")
        try:
            last_row = len(dest_ws.col_values(1))
            if last_row > 1:
                # KN列(300列) まで指定
                dest_ws.sort((3, 'des'), range=f'A2:KN{last_row}') 
        except Exception as e: print(f"  ! ソートエラー: {e}")
        set_row_height(dest_ws, 21)

    print(f" ? コメント収集・要約完了: 新たに {process_count} 件処理しました。")
