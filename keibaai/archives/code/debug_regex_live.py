#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
実際のbinファイルに対して正規表現をライブテスト

新しく欠損したレースと、修正できたレースの両方を分析して、
何が問題かを特定する。
"""

import re
from pathlib import Path
from bs4 import BeautifulSoup

def test_regex_on_actual_html(race_id: str, bin_dir: Path):
    """実際のHTMLに対して正規表現をテスト"""
    bin_file = bin_dir / f"{race_id}.bin"

    if not bin_file.exists():
        print(f"[!] {race_id}: binファイルが見つかりません\n")
        return

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
        print(f"[!] {race_id}: data_intro が見つかりません\n")
        return

    # テキスト全体を取得
    text = data_intro.get_text(strip=True)

    print(f"race_id: {race_id}")
    print(f"テキスト (最初の200文字): {text[:200]}")
    print(f"'障' in text: {'障' in text}")
    print()

    # 新しいパターンをテスト
    surface = None
    distance = None

    # パターン1: 障害レース（「障」があれば障害レース扱い）
    if '障' in text:
        print("  → 障害レースとして処理")
        distance_match = re.search(r'障(?:[^0-9])*(\d+)\s*m?', text)
        if distance_match:
            surface = '障害'
            distance = int(distance_match.group(1))
            print(f"  → パターン1マッチ: 障害 {distance}m")
        else:
            print(f"  → パターン1マッチせず")
    else:
        # パターン2: 通常レース（方向情報を複数許容: "芝右 外1800m"）
        print("  → 通常レースとして処理")
        distance_match = re.search(r'(芝|ダート?)\s*(?:右|左|直|外|内|\s)*(\d+)\s*m?', text, re.IGNORECASE)
        if distance_match:
            surface_map = {'芝': '芝', 'ダ': 'ダート', 'ダート': 'ダート'}
            surface = surface_map.get(distance_match.group(1))
            distance = int(distance_match.group(2))
            print(f"  → パターン2マッチ: {surface} {distance}m")
            print(f"     マッチした文字列: '{distance_match.group(0)}'")
        else:
            print(f"  → パターン2マッチせず")

    print(f"最終結果: 馬場={surface}, 距離={distance}m")
    print("-" * 70)
    print()


def main():
    print("="*70)
    print("正規表現ライブテスト（実際のbinファイル）")
    print("="*70)
    print()

    bin_dir = Path('data/raw/html/race')

    if not bin_dir.exists():
        print(f"[!] エラー: {bin_dir} が存在しません")
        print(f"    ローカル環境で実行してください。")
        return

    # 新しく欠損したレース（問題）
    print("【新しく欠損したレース（52.09%欠損の原因）】")
    print("="*70)
    new_missing = [
        '202305040301',  # 2歳未勝利
        '202305040306',  # 2歳新馬
        '202305040307',  # 3歳以上1勝クラス
    ]

    for race_id in new_missing:
        test_regex_on_actual_html(race_id, bin_dir)

    # 前回欠損していたが、今回は修正できたレース（成功）
    print("\n【前回欠損していたレース（14.47%欠損）- 今回は修正できた？】")
    print("="*70)
    old_missing = [
        '202305040304',  # 障害3歳以上未勝利
        '202308020303',  # 2歳未勝利
        '202308020309',  # りんどう賞(1勝)
    ]

    for race_id in old_missing:
        test_regex_on_actual_html(race_id, bin_dir)

    print("\n" + "="*70)
    print("分析完了")
    print("="*70)


if __name__ == '__main__':
    main()
