#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
欠損distance_mの原因調査スクリプト

診断情報で特定されたレースのbinファイルを読み込み、
data_intro内の実際のHTMLテキストを表示する。
"""

import re
from pathlib import Path
from bs4 import BeautifulSoup

# 欠損が発生しているrace_id（診断情報より）
# 最新の実行結果（52.09%欠損）から
MISSING_RACE_IDS = [
    '202305040301',  # 2歳未勝利
    '202305040306',  # 2歳新馬
    '202305040307',  # 3歳以上1勝クラス
    '202305040309',  # 昇仙峡特別(2勝)
    '202305040311',  # グリーンチャンネルC(L)
    '202305040312',  # 3歳以上1勝クラス
    '202308020302',  # 2歳未勝利
    '202308020306',  # 3歳以上1勝クラス
    '202308020307',  # 3歳以上1勝クラス
    '202308020308',  # 3歳以上1勝クラス
    '202308020310',  # 大山崎ステークス(3勝)
    '202308020312',  # 3歳以上2勝クラス
]

# 前回の欠損（14.47%）- 今回は修正できた可能性
OLD_MISSING_RACE_IDS = [
    '202305040304',  # 障害3歳以上未勝利
    '202308020303',  # 2歳未勝利
    '202308020309',  # りんどう賞(1勝)
    '202308020311',  # 第58回京都大賞典(GII)
]

def analyze_race_html(race_id: str, bin_dir: Path):
    """race_idのbinファイルを分析"""
    bin_file = bin_dir / f"{race_id}.bin"

    if not bin_file.exists():
        print(f"\n[!] {race_id}: binファイルが見つかりません")
        return

    print(f"\n{'='*70}")
    print(f"race_id: {race_id}")
    print(f"{'='*70}")

    # HTMLを読み込み
    with open(bin_file, 'rb') as f:
        html_bytes = f.read()

    try:
        html_text = html_bytes.decode('euc_jp', errors='replace')
    except:
        html_text = html_bytes.decode('utf-8', errors='replace')

    soup = BeautifulSoup(html_text, 'html.parser')

    # data_introを探す
    data_intro = soup.find('div', class_='data_intro')

    if not data_intro:
        print("[!] data_intro が見つかりません")
        return

    print("[✓] data_intro が見つかりました\n")

    # テキスト全体を取得
    text = data_intro.get_text(strip=True)
    print(f"【data_intro 全テキスト】 (最初の500文字)")
    print("-" * 70)
    print(text[:500])
    print("-" * 70)

    # 現在の正規表現でマッチするか確認
    print(f"\n【正規表現マッチテスト】")
    print("-" * 70)

    # パターン1: 全体テキストから
    distance_match = re.search(r'(芝|ダ|障)\s*(?:右|左|直|外|内)?\s*(\d+)\s*m?', text, re.IGNORECASE)
    if distance_match:
        print(f"✓ パターン1マッチ: {distance_match.group(0)}")
        print(f"  馬場: {distance_match.group(1)}, 距離: {distance_match.group(2)}m")
    else:
        print(f"✗ パターン1: マッチせず")

    # パターン2: spanタグ個別検索
    spans = data_intro.find_all('span')
    print(f"\n【spanタグ一覧】 ({len(spans)}個)")
    print("-" * 70)

    for i, span in enumerate(spans[:20]):  # 最初の20個のみ
        span_text = span.get_text(strip=True)
        if span_text:
            print(f"span[{i:2d}]: {span_text}")

            # 距離パターンをチェック
            distance_match = re.search(r'(芝|ダ|障)\s*(?:右|左|直|外|内)?\s*(\d+)', span_text)
            if distance_match:
                print(f"         → ✓ マッチ: {distance_match.group(0)}")

    # HTMLタグ構造も表示
    print(f"\n【data_intro のHTML構造】 (最初の1000文字)")
    print("-" * 70)
    print(str(data_intro)[:1000])
    print("-" * 70)

    # 他のパターンを試す
    print(f"\n【追加パターンテスト】")
    print("-" * 70)

    # 全角数字対応
    if re.search(r'[０-９]', text):
        print(f"✓ 全角数字が含まれています")

    # 改行・スペースを削除したテキスト
    text_compact = re.sub(r'\s+', '', text)
    distance_match_compact = re.search(r'(芝|ダ|障)(右|左|直|外|内)?(\d+)', text_compact)
    if distance_match_compact:
        print(f"✓ スペース削除後マッチ: {distance_match_compact.group(0)}")

    # レース名も表示
    race_title = soup.find('h1')
    if race_title:
        print(f"\nレース名: {race_title.get_text(strip=True)}")


def main():
    """メイン処理"""
    print("="*70)
    print("distance_m 欠損原因調査ツール")
    print("="*70)

    # binディレクトリ
    bin_dir = Path('data/raw/html/race')

    if not bin_dir.exists():
        print(f"\n[!] エラー: {bin_dir} が存在しません")
        print(f"    ローカル環境で実行してください。")
        return

    # 各レースを分析
    for race_id in MISSING_RACE_IDS:
        analyze_race_html(race_id, bin_dir)

    print("\n" + "="*70)
    print("分析完了")
    print("="*70)
    print("\n次のステップ:")
    print("1. 上記の出力から、距離情報がどのように記載されているか確認")
    print("2. 正規表現パターンを調整")
    print("3. debug_scraping_and_parsing.py を修正")


if __name__ == '__main__':
    main()
