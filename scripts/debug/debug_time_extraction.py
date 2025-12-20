#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""タイム取得のデバッグスクリプト - どこでタイムが失われるか確認"""

import sys
sys.path.insert(0, '.')

from generate_race_report import NetkeibaAnalyzer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# テスト用の馬ID（1Rの馬）
TEST_HORSE_IDS = [
    "2022104939",  # ウィンターベル
    "2023104927",  # ホワイトナイル
]

def debug_time_extraction():
    print("=" * 60)
    print("タイム取得デバッグ")
    print("=" * 60)
    
    analyzer = NetkeibaAnalyzer("20251214", "中山")
    
    for horse_id in TEST_HORSE_IDS:
        print(f"\n--- 馬ID: {horse_id} ---")
        
        # 1. HTMLを取得
        history = analyzer.get_horse_history(horse_id)
        
        if not history:
            print("  ⚠️ 履歴データなし")
            continue
        
        print(f"  履歴件数: {len(history)}件")
        
        # 2. 最初の3レースのタイムを確認
        for i, race in enumerate(history[:3]):
            date = race.get('日付', 'N/A')
            time_val = race.get('ﾀｲﾑ', 'N/A')
            l3f = race.get('上り', 'N/A')
            time_idx = race.get('タイム指数', 'N/A')
            
            print(f"  [{i+1}] 日付:{date} | ﾀｲﾑ:{time_val} | 上り:{l3f} | 指数:{time_idx}")
        
        # 3. calculate_metricsを実行後のデータを確認
        print("  --- calculate_metrics実行後 ---")
        history_with_metrics = analyzer.calculate_metrics(history)
        
        for i, race in enumerate(history_with_metrics[:3]):
            date = race.get('日付', 'N/A')
            time_val = race.get('ﾀｲﾑ', 'N/A')
            l3f = race.get('上り', 'N/A')
            time_idx = race.get('タイム指数', 'N/A')
            
            print(f"  [{i+1}] 日付:{date} | ﾀｲﾑ:{time_val} | 上り:{l3f} | 指数:{time_idx}")

if __name__ == "__main__":
    debug_time_extraction()
