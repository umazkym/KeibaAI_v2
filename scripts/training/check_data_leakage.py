#!/usr/bin/env python3
"""
μv3.3 データリークチェックスクリプト

Phase 1実装後の特徴量リストをスキャンし、データリークの可能性がある特徴量を検出します。
"""

import json
import sys
from pathlib import Path

# 禁止特徴量リスト（データリーク源）
FORBIDDEN_FEATURES = [
    # レース結果（未来情報）
    'finish_position',  # 着順
    'finish_time_seconds',  # 完走タイム
    'margin_seconds',  # 着差
    'prize_money',  # 実際の獲得賞金
    
    # オッズ・人気（確定情報）
    'win_odds',  # 確定オッズ（評価のみ）
    'popularity',  # 確定人気（評価のみ）
    'place_odds',
    
    # レース展開（未来情報）
    'passing_order_1', 'passing_order_2', 'passing_order_3', 'passing_order_4',
    'last_3f_time', 'time_except_last3f', 'pace_index',
    'final_corner_to_finish', 'position_change_1_2', 'position_change_2_3',
    'position_change_3_4', 'last3f_rank', 'running_style', 'time_deviation',
    
    # 派生された未来情報
    'win_probability',  # 確定オッズから計算
    'relative_odds',  # 確定オッズから計算
    'popularity_finish_diff',  # finish_positionを含む
]

# 条件付きOK特徴量（過去データから計算されていればOK）
CONDITIONAL_OK_PREFIXES = [
    'past_',  # 過去N走のデータ
    'avg_',  # 過去データの平均
    'gap_',  # 過去データとの差分（実力ランク vs 人気など）
    'time_deviation_score_avg_', # タイム指数の過去平均
]

def check_data_leakage(feature_list: list) -> tuple:
    """
    特徴量リストからデータリークをチェック
    
    Returns:
        (leaked_features, warnings): リーク特徴量のリストと警告リスト
    """
    leaked = []
    warnings = []
    
    for feature in feature_list:
        # 条件付きOK特徴量はスキップ
        if any(feature.startswith(prefix) for prefix in CONDITIONAL_OK_PREFIXES):
            continue
        
        # 禁止ワードチェック
        for forbidden in FORBIDDEN_FEATURES:
            if forbidden in feature:
                if forbidden == 'popularity' and 'gap_' in feature:
                    # gap_ability_popularity などは差分なのでOK
                    warnings.append(f"⚠️  要確認: '{feature}' は過去データとの差分ですか？")
                else:
                    leaked.append(feature)
                break
    
    return leaked, warnings

def main():
    """メイン処理"""
    # v3.3の特徴量リストを読み込み
    feature_file = Path('keibaai/models/mu_v3_3/feature_names.json')
    
    if not feature_file.exists():
        print(f"⚠️  特徴量リストが見つかりません: {feature_file}")
        print("   学習を実行してください: python scripts/training/train_mu_v3_0_ranker.py")
        return 1
    
    with open(feature_file) as f:
        features = json.load(f)
    
    print("=" * 60)
    print("μv3.3 データリークチェック")
    print("=" * 60)
    print(f"特徴量数: {len(features)}個\n")
    
    leaked, warnings = check_data_leakage(features)
    
    # 結果表示
    if leaked:
        print("❌ データリーク検出！")
        print(f"\n🚨 以下の{len(leaked)}個の特徴量がリーク源の可能性があります:")
        for f in leaked:
            print(f"  - {f}")
        print("\n⚠️  これらの特徴量を除外してください。")
        result = 1
    else:
        print("✅ リーク特徴量は検出されませんでした")
        result = 0
    
    if warnings:
        print(f"\n⚠️  以下の{len(warnings)}個の特徴量は要確認:")
        for w in warnings:
            print(f"  {w}")
    
    print("\n" + "=" * 60)
    print("チェック完了")
    print("=" * 60)
    
    return result

if __name__ == "__main__":
    sys.exit(main())
