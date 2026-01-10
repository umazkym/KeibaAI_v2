# -*- coding: utf-8 -*-
"""
脚質の多角的・定量的分析スクリプト

人間の分類（逃げ/先行/差し/追込）に頼らず、
C1-C4すべてのコーナーデータから馬の脚質を定量化
"""
import pandas as pd
import numpy as np
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent.parent


def load_data():
    """データを読み込む"""
    print("データ読み込み中...")
    data_dir = project_root / "keibaai/data/parsed/parquet"
    
    races = pd.read_parquet(data_dir / "races/races.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    return races, corners


def analyze_corners(corners):
    """C1-C4全てのコーナーデータを分析"""
    
    print("\n" + "=" * 80)
    print("【全コーナーデータ分析】")
    print("=" * 80)
    
    print(f"\n総レコード数: {len(corners):,}")
    
    print("\n【コーナー別件数】")
    print(corners['corner'].value_counts().sort_index())
    
    # C1-C4すべてが存在するレース数
    race_corner_counts = corners.groupby('race_id')['corner'].nunique()
    print(f"\n【コーナー数別レース数】")
    print(f"  4コーナー全てあり: {(race_corner_counts == 4).sum():,}レース")
    print(f"  3コーナー以上: {(race_corner_counts >= 3).sum():,}レース")
    print(f"  2コーナー以上: {(race_corner_counts >= 2).sum():,}レース")
    print(f"  1コーナーのみ: {(race_corner_counts == 1).sum():,}レース")
    
    print("\n【各コーナーのposition統計】")
    for c in [1, 2, 3, 4]:
        subset = corners[corners['corner'] == c]
        if len(subset) > 0:
            print(f"  C{c}: position平均={subset['position'].mean():.1f}, σ={subset['position'].std():.1f}, 件数={len(subset):,}")
    
    print("\n【各コーナーのgap_from_leader統計】")
    for c in [1, 2, 3, 4]:
        subset = corners[corners['corner'] == c]
        if len(subset) > 0:
            print(f"  C{c}: gap平均={subset['gap_from_leader'].mean():.1f}馬身, σ={subset['gap_from_leader'].std():.1f}")
    
    return corners


def quantify_running_style(races, corners):
    """脚質を多角的・定量的に分析"""
    
    print("\n" + "=" * 80)
    print("【脚質の定量化分析】")
    print("=" * 80)
    
    # 各レースの出走頭数を取得
    field_size = races.groupby('race_id')['horse_number'].count().reset_index()
    field_size.columns = ['race_id', 'field_size']
    
    # コーナーデータをピボット（C1-C4を横に展開）
    corner_pivot = corners.pivot_table(
        index=['race_id', 'horse_number'],
        columns='corner',
        values=['position', 'gap_from_leader']
    ).reset_index()
    
    # カラム名をフラット化
    corner_pivot.columns = [f'{col[0]}_c{col[1]}' if col[1] else col[0] for col in corner_pivot.columns]
    
    # 出走頭数をマージ
    corner_pivot = corner_pivot.merge(field_size, on='race_id', how='left')
    
    # 相対位置を計算（各コーナー）
    for c in [1, 2, 3, 4]:
        pos_col = f'position_c{c}'
        if pos_col in corner_pivot.columns:
            corner_pivot[f'relative_c{c}'] = corner_pivot[pos_col] / corner_pivot['field_size']
    
    print("\n【相対コーナー位置の相関】")
    rel_cols = [f'relative_c{c}' for c in [1, 2, 3, 4] if f'relative_c{c}' in corner_pivot.columns]
    if len(rel_cols) >= 2:
        corr = corner_pivot[rel_cols].corr()
        print(corr.round(2))
    
    # 脚質スコア1: C1-C4の平均相対位置
    corner_pivot['style_score_avg'] = corner_pivot[rel_cols].mean(axis=1)
    
    # 脚質スコア2: C1からC4への位置変動（マイナス=追い上げ、プラス=後退）
    if 'relative_c1' in corner_pivot.columns and 'relative_c4' in corner_pivot.columns:
        corner_pivot['position_change'] = corner_pivot['relative_c4'] - corner_pivot['relative_c1']
    
    # 脚質スコア3: 馬身差の推移
    for c in [1, 2, 3, 4]:
        gap_col = f'gap_from_leader_c{c}'
        if gap_col in corner_pivot.columns:
            corner_pivot[f'gap_c{c}'] = corner_pivot[gap_col]
    
    gap_cols = [f'gap_c{c}' for c in [1, 2, 3, 4] if f'gap_c{c}' in corner_pivot.columns]
    if len(gap_cols) >= 2:
        corner_pivot['gap_avg'] = corner_pivot[gap_cols].mean(axis=1)
    
    print("\n【脚質スコアの分布】")
    print(f"  style_score_avg (0=先頭, 1=最後尾):")
    print(f"    平均: {corner_pivot['style_score_avg'].mean():.3f}")
    print(f"    σ: {corner_pivot['style_score_avg'].std():.3f}")
    print(f"    25%: {corner_pivot['style_score_avg'].quantile(0.25):.3f}")
    print(f"    50%: {corner_pivot['style_score_avg'].quantile(0.50):.3f}")
    print(f"    75%: {corner_pivot['style_score_avg'].quantile(0.75):.3f}")
    
    if 'position_change' in corner_pivot.columns:
        print(f"\n  position_change (C1→C4の変動, -=追上げ, +=後退):")
        print(f"    平均: {corner_pivot['position_change'].mean():.3f}")
        print(f"    σ: {corner_pivot['position_change'].std():.3f}")
    
    # レース結果とマージして勝率分析
    races_subset = races[['race_id', 'horse_number', 'finish_position', 'win_odds']].copy()
    corner_with_result = corner_pivot.merge(races_subset, on=['race_id', 'horse_number'], how='left')
    
    print("\n" + "=" * 80)
    print("【脚質スコアと勝率の関係】")
    print("=" * 80)
    
    # style_score_avgを10分位に分割
    corner_with_result['style_decile'] = pd.qcut(
        corner_with_result['style_score_avg'], 10, 
        labels=['D1(先)', 'D2', 'D3', 'D4', 'D5', 'D6', 'D7', 'D8', 'D9', 'D10(後)'],
        duplicates='drop'
    )
    
    print("\n【相対コーナー位置10分位別の勝率】")
    print("-" * 60)
    for decile in ['D1(先)', 'D2', 'D3', 'D4', 'D5', 'D6', 'D7', 'D8', 'D9', 'D10(後)']:
        subset = corner_with_result[corner_with_result['style_decile'] == decile]
        wins = subset[subset['finish_position'] == 1]
        if len(subset) > 1000:
            win_rate = len(wins) / len(subset) * 100
            roi = wins['win_odds'].sum() / len(subset) * 100
            print(f"  {decile}: 勝率 {win_rate:.1f}%, ROI {roi:.1f}% ({len(subset):,}件)")
    
    # position_changeと勝率
    if 'position_change' in corner_with_result.columns:
        print("\n【C1→C4位置変動と勝率】")
        print("-" * 60)
        corner_with_result['change_category'] = pd.cut(
            corner_with_result['position_change'],
            bins=[-1, -0.2, -0.1, 0.1, 0.2, 1],
            labels=['大追上げ', '小追上げ', '維持', '小後退', '大後退']
        )
        
        for cat in ['大追上げ', '小追上げ', '維持', '小後退', '大後退']:
            subset = corner_with_result[corner_with_result['change_category'] == cat]
            wins = subset[subset['finish_position'] == 1]
            if len(subset) > 1000:
                win_rate = len(wins) / len(subset) * 100
                roi = wins['win_odds'].sum() / len(subset) * 100
                print(f"  {cat}: 勝率 {win_rate:.1f}%, ROI {roi:.1f}% ({len(subset):,}件)")
    
    # 各コーナー位置と勝率
    print("\n【各コーナー位置と勝率の関係】")
    print("-" * 60)
    
    for c in [1, 2, 3, 4]:
        rel_col = f'relative_c{c}'
        if rel_col in corner_with_result.columns:
            # 相関係数
            corr = corner_with_result[[rel_col, 'finish_position']].corr().iloc[0,1]
            print(f"  C{c}相対位置 vs 着順: 相関 {corr:.3f}")
    
    return corner_with_result


def main():
    races, corners = load_data()
    print(f"\n総データ: レース{len(races):,}件, コーナー{len(corners):,}件")
    
    corners = analyze_corners(corners)
    result = quantify_running_style(races, corners)
    
    print("\n" + "=" * 80)
    print("【V31への特徴量設計示唆】")
    print("=" * 80)
    print("""
■ 従来の人間定義（逃げ/先行/差し/追込）ではなく、データから導出:

1. style_score_avg: C1-C4平均相対位置（連続値）
   → 離散的な4分類ではなく、連続スケールで脚質を表現

2. position_change: C1→C4の位置変動
   → 「追い上げ型」「維持型」「後退型」をデータから検出

3. 各コーナー個別の相対位置 (relative_c1-c4)
   → C4だけでなくC1-C3も活用

4. gap_progression: 馬身差の推移パターン
   → 先頭との距離がどう変化するか

5. style_consistency: 脚質安定性（C1-C4位置のσ）
   → 毎回同じ位置取りか、レースごとに異なるか
""")


if __name__ == "__main__":
    main()
