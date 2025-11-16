#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
距離パーサーのテストスクリプト

複数の修正案を比較テストします。
"""

import re

def test_distance_patterns():
    """距離抽出のテストケース"""

    # 修正案1: 桁数指定版（3-4桁）
    pattern_v1 = r'(芝|ダート?)[^0-9]*?(\d{3,4})\s*m?'

    # 修正案2: 通常版（デバッグ用）
    pattern_v2 = r'(芝|ダート?)[^0-9]*?(\d+)\s*m?'

    # 修正案3: 前処理版
    def preprocess(text):
        """HTMLテキストの前処理"""
        text = re.sub(r'\s+', ' ', text)  # 複数の空白を1つに
        text = re.sub(r'(\D)(\s+)(\d)', r'\1\3', text)  # 文字と数字の間の空白を削除
        text = re.sub(r'(\d)(\s+)(\d)', r'\1\3', text)  # 数字間の空白を削除
        text = re.sub(r'(\d)(\s+)(m)', r'\1\3', text)   # 数字とmの間の空白を削除
        return text

    test_cases = [
        ("ダ1000m / 天候 : 雨", "ダート", 1000),
        ("芝1600m / 天候 : 曇", "芝", 1600),
        ("ダ 1 0 0 0 m / 天候 : 雨", "ダート", 1000),  # 空白あり（問題のケース）
        ("芝右 外1800m", "芝", 1800),
        ("障害芝3000m", "障害", 3000),
        ("ダート2400m", "ダート", 2400),
        ("ダ10m", "ダート", 10),  # 異常ケース
    ]

    print("=" * 80)
    print("距離抽出パーサー - テスト結果")
    print("=" * 80)

    print("\n【修正案1: 桁数指定版 (\\d{3,4})】")
    print("-" * 80)
    passed_v1 = 0
    for text, expected_surface, expected_distance in test_cases:
        match = re.search(pattern_v1, text, re.IGNORECASE)
        if match:
            surface = match.group(1)
            surface_map = {'芝': '芝', 'ダ': 'ダート', 'ダート': 'ダート'}
            surface_normalized = surface_map.get(surface, surface)
            distance = int(match.group(2))

            is_correct = (distance == expected_distance)
            status = "✓" if is_correct else "✗"

            print(f"{status} '{text[:40]:<40}' → {surface_normalized:6s} {distance:4d}m", end="")
            if not is_correct:
                print(f" (期待値: {expected_distance}m)", end="")
            if is_correct and expected_distance >= 800:  # 正常範囲
                passed_v1 += 1
            print()
        else:
            print(f"✗ '{text[:40]:<40}' → マッチせず")
    print(f"\n合格: {passed_v1} / {len([t for t in test_cases if t[2] >= 800])} (正常範囲のみ)")

    print("\n【修正案2: 通常版 (\\d+) - デバッグ用】")
    print("-" * 80)
    passed_v2 = 0
    for text, expected_surface, expected_distance in test_cases:
        match = re.search(pattern_v2, text, re.IGNORECASE)
        if match:
            surface = match.group(1)
            surface_map = {'芝': '芝', 'ダ': 'ダート', 'ダート': 'ダート'}
            surface_normalized = surface_map.get(surface, surface)
            distance = int(match.group(2))

            is_correct = (distance == expected_distance)
            status = "✓" if is_correct else "✗"

            print(f"{status} '{text[:40]:<40}' → {surface_normalized:6s} {distance:4d}m", end="")
            if not is_correct:
                print(f" (期待値: {expected_distance}m, マッチ: '{match.group(0)}')", end="")
            if is_correct:
                passed_v2 += 1
            print()
        else:
            print(f"✗ '{text[:40]:<40}' → マッチせず")
    print(f"\n合格: {passed_v2} / {len(test_cases)}")

    print("\n【修正案3: 前処理 + 通常パターン】")
    print("-" * 80)
    passed_v3 = 0
    for text, expected_surface, expected_distance in test_cases:
        processed_text = preprocess(text)
        match = re.search(pattern_v2, processed_text, re.IGNORECASE)
        if match:
            surface = match.group(1)
            surface_map = {'芝': '芝', 'ダ': 'ダート', 'ダート': 'ダート'}
            surface_normalized = surface_map.get(surface, surface)
            distance = int(match.group(2))

            is_correct = (distance == expected_distance)
            status = "✓" if is_correct else "✗"

            print(f"{status} '{text[:40]:<40}' → {surface_normalized:6s} {distance:4d}m", end="")
            if text != processed_text:
                print(f" (前処理: '{processed_text[:30]}')", end="")
            if not is_correct:
                print(f" (期待値: {expected_distance}m)", end="")
            if is_correct:
                passed_v3 += 1
            print()
        else:
            print(f"✗ '{text[:40]:<40}' (前処理: '{processed_text[:30]}') → マッチせず")
    print(f"\n合格: {passed_v3} / {len(test_cases)}")

    # 総合評価
    print("\n" + "=" * 80)
    print("総合評価")
    print("=" * 80)
    print(f"修正案1 (桁数指定):  {passed_v1}/{len([t for t in test_cases if t[2] >= 800])} ← 異常値(10m)を除外")
    print(f"修正案2 (通常版):    {passed_v2}/{len(test_cases)} ← デバッグ用")
    print(f"修正案3 (前処理):    {passed_v3}/{len(test_cases)} ← すべて対応")

    print("\n【推奨】")
    if passed_v3 == len(test_cases):
        print("✓ 修正案3 (前処理) - すべてのケースに対応")
    elif passed_v1 >= len([t for t in test_cases if t[2] >= 800]):
        print("✓ 修正案1 (桁数指定) - 正常範囲のみ対応（異常値を除外）")
    else:
        print("⚠ さらなる調査が必要")

if __name__ == '__main__':
    test_distance_patterns()
