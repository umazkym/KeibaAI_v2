#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
正規表現パターンのテストスクリプト

修正した正規表現パターンが、診断ツールで見つかった4つのレースで
正しく動作するかテストする。
"""

import re

# テストケース（実際のHTMLから抽出）
test_cases = [
    {
        'race_id': '202305040304',
        'race_name': '障害3歳以上未勝利',
        'text': '障芝 ダート3000m / \n天候 : 雨 / \n芝 : 稍重  / \n発走 : 11:35',
        'expected_surface': '障害',
        'expected_distance': 3000
    },
    {
        'race_id': '202308020303',
        'race_name': '2歳未勝利',
        'text': '芝右 外1800m / \n天候 : 曇 / \n芝 : 重  / \n発走 : 10:50',
        'expected_surface': '芝',
        'expected_distance': 1800
    },
    {
        'race_id': '202308020309',
        'race_name': 'りんどう賞(1勝)',
        'text': '芝右 外1400m / \n天候 : 曇 / \n芝 : 重  / \n発走 : 14:25',
        'expected_surface': '芝',
        'expected_distance': 1400
    },
    {
        'race_id': '202308020311',
        'race_name': '第58回京都大賞典(GII)',
        'text': '芝右 外2400m / \n天候 : 曇 / \n芝 : 重  / \n発走 : 15:35',
        'expected_surface': '芝',
        'expected_distance': 2400
    }
]

def test_old_pattern(text):
    """旧パターンをテスト"""
    distance_match = re.search(r'(芝|ダ|障)\s*(?:右|左|直|外|内)?\s*(\d+)\s*m?', text, re.IGNORECASE)
    if distance_match:
        surface_map = {'芝': '芝', 'ダ': 'ダート', '障': '障害'}
        return {
            'surface': surface_map.get(distance_match.group(1)),
            'distance': int(distance_match.group(2))
        }
    return None

def test_new_pattern(text):
    """新パターンをテスト（最新版 - 非貪欲マッチ）"""
    surface = None
    distance = None

    # パターン1: 障害レース（「障」があれば障害レース扱い）
    # 非貪欲マッチ *? で最初の数字を優先的にマッチ
    if '障' in text:
        distance_match = re.search(r'障[^0-9]*?(\d+)\s*m?', text)
        if distance_match:
            surface = '障害'
            distance = int(distance_match.group(1))

    # パターン2: 通常レース
    # 「芝」「ダート」の後、数字以外の文字を非貪欲でマッチ
    if distance is None:
        distance_match = re.search(r'(芝|ダート?)[^0-9]*?(\d+)\s*m?', text, re.IGNORECASE)
        if distance_match:
            surface_map = {'芝': '芝', 'ダ': 'ダート', 'ダート': 'ダート'}
            surface = surface_map.get(distance_match.group(1))
            distance = int(distance_match.group(2))

    if surface and distance:
        return {'surface': surface, 'distance': distance}
    return None

def main():
    print("="*70)
    print("正規表現パターン修正テスト")
    print("="*70)

    total_tests = len(test_cases)
    old_passed = 0
    new_passed = 0

    for i, test_case in enumerate(test_cases, 1):
        print(f"\n{i}. {test_case['race_id']}: {test_case['race_name']}")
        print("-"*70)
        print(f"テキスト: {test_case['text'][:60]}...")
        print(f"期待値: 馬場={test_case['expected_surface']}, 距離={test_case['expected_distance']}m")

        # 旧パターンテスト
        old_result = test_old_pattern(test_case['text'])
        if old_result:
            old_match = (old_result['surface'] == test_case['expected_surface'] and
                        old_result['distance'] == test_case['expected_distance'])
            old_status = "✓" if old_match else "✗"
            print(f"\n旧パターン: {old_status}")
            print(f"  結果: 馬場={old_result['surface']}, 距離={old_result['distance']}m")
            if old_match:
                old_passed += 1
        else:
            print(f"\n旧パターン: ✗ マッチせず")

        # 新パターンテスト
        new_result = test_new_pattern(test_case['text'])
        if new_result:
            new_match = (new_result['surface'] == test_case['expected_surface'] and
                        new_result['distance'] == test_case['expected_distance'])
            new_status = "✓" if new_match else "✗"
            print(f"\n新パターン: {new_status}")
            print(f"  結果: 馬場={new_result['surface']}, 距離={new_result['distance']}m")
            if new_match:
                new_passed += 1
        else:
            print(f"\n新パターン: ✗ マッチせず")

    # サマリー
    print("\n" + "="*70)
    print("テスト結果サマリー")
    print("="*70)
    print(f"旧パターン: {old_passed}/{total_tests} 合格 ({old_passed/total_tests*100:.1f}%)")
    print(f"新パターン: {new_passed}/{total_tests} 合格 ({new_passed/total_tests*100:.1f}%)")

    if new_passed == total_tests:
        print("\n✓ すべてのテストに合格！")
    else:
        print("\n✗ 一部のテストが失敗しました")

    print("\n次のステップ:")
    if new_passed == total_tests:
        print("1. debug_full_pipeline_by_date.py --parse-only で再テスト")
        print("2. distance_m 欠損が 0% になることを確認")
    else:
        print("1. 失敗したテストケースを確認")
        print("2. 正規表現パターンをさらに調整")

if __name__ == '__main__':
    main()
