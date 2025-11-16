#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
距離パーサーのテスト（修正版）

正規表現パターンの問題を修正してテストします。
"""

import re

def test_distance_patterns_v2():
    """距離抽出のテストケース（修正版）"""

    # === 各パターンの定義 ===

    # 現在のパターン（問題あり）
    pattern_current = r'(芝|ダート?)[^0-9]*?(\d+)\s*m?'

    # 修正案A: ダー?ト? で "ダ", "ダー", "ダート" すべてにマッチ
    pattern_fix_a = r'(芝|ダー?ト?)[^0-9]*?(\d+)\s*m?'

    # 修正案B: より明示的に
    pattern_fix_b = r'(芝|ダート|ダー|ダ)[^0-9]*?(\d+)\s*m?'

    # 修正案C: 非キャプチャグループを使用
    pattern_fix_c = r'(芝|ダ(?:ート)?)[^0-9]*?(\d+)\s*m?'

    test_cases = [
        ("ダ1000m", "ダート", 1000),          # 最も一般的なケース
        ("ダート1200m", "ダート", 1200),      # フル表記
        ("ダー1400m", "ダート", 1400),        # "ダー" 表記
        ("芝1600m", "芝", 1600),              # 芝
        ("芝右 外1800m", "芝", 1800),         # 方向あり
        ("障害芝3000m", "障害", 3000),        # 障害（別処理）
    ]

    print("=" * 80)
    print("距離抽出パーサー - 修正版テスト")
    print("=" * 80)

    patterns = {
        "現在のパターン (ダート?)": pattern_current,
        "修正案A (ダー?ト?)": pattern_fix_a,
        "修正案B (明示的)": pattern_fix_b,
        "修正案C (非キャプチャ)": pattern_fix_c,
    }

    for pattern_name, pattern in patterns.items():
        print(f"\n【{pattern_name}】")
        print(f"パターン: {pattern}")
        print("-" * 80)

        passed = 0
        for text, expected_surface, expected_distance in test_cases:
            # 障害レースは別パターンで処理するため、ここではスキップ
            if "障害" in text:
                continue

            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                raw_surface = match.group(1)
                surface_map = {
                    '芝': '芝',
                    'ダ': 'ダート',
                    'ダー': 'ダート',
                    'ダート': 'ダート'
                }
                surface_normalized = surface_map.get(raw_surface, raw_surface)
                distance = int(match.group(2))

                is_correct = (distance == expected_distance and surface_normalized == expected_surface)
                status = "✓" if is_correct else "✗"

                print(f"{status} '{text:25s}' → {surface_normalized:6s} {distance:4d}m (マッチ: '{match.group(0)}')")

                if is_correct:
                    passed += 1
            else:
                print(f"✗ '{text:25s}' → マッチせず")

        print(f"\n合格: {passed} / {len([t for t in test_cases if '障害' not in t[0]])}")

    # 推奨パターンの提示
    print("\n" + "=" * 80)
    print("推奨される修正")
    print("=" * 80)
    print("""
修正前:
    distance_match = re.search(r'(芝|ダート?)[^0-9]*?(\\d+)\\s*m?', text, re.IGNORECASE)
                                     ^^^^^^
                                     問題: "ダ" にマッチしない

修正後（推奨案A）:
    distance_match = re.search(r'(芝|ダー?ト?)[^0-9]*?(\\d+)\\s*m?', text, re.IGNORECASE)
                                     ^^^^^^^
                                     "ダ", "ダー", "ダート" すべてにマッチ

修正後（推奨案C - よりクリーン）:
    distance_match = re.search(r'(芝|ダ(?:ート)?)[^0-9]*?(\\d+)\\s*m?', text, re.IGNORECASE)
                                     ^^^^^^^^^^
                                     非キャプチャグループで "ダ" または "ダート" にマッチ
    """)

if __name__ == '__main__':
    test_distance_patterns_v2()
