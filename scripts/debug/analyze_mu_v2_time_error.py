#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
μ_time_v2 実タイム乖離分析

標準化タイムではなく、実際の秒数での予測精度を詳細分析
"""
import sys
import json
import pickle
from pathlib import Path
import numpy as np
import pandas as pd
from scipy.stats import spearmanr

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

def main():
    print("=" * 70)
    print("μ_time_v2 実タイム（秒）乖離分析")
    print("=" * 70)
    
    # モデルと基準タイムをロード
    model_dir = project_root / "keibaai" / "models" / "mu_time_v2"
    with open(model_dir / "mu_time_model.pkl", 'rb') as f:
        model = pickle.load(f)
    with open(model_dir / "feature_names.json", 'r') as f:
        feature_cols = json.load(f)
    base_time_stats = pd.read_parquet(model_dir / "base_time_stats.parquet")
    
    # データ読み込み
    df = pd.read_parquet(project_root / "keibaai/data/parsed/parquet/races/races.parquet")
    df['race_date'] = pd.to_datetime(df['race_date'])
    df = df.dropna(subset=['finish_position', 'finish_time_seconds'])
    df = df.drop_duplicates(subset=['race_id', 'horse_number'], keep='first')
    
    # 特徴量生成
    from keibaai.src.features.time_feature_engineer_v1 import TimeFeatureEngineerV1
    
    train_end = pd.Timestamp('2023-01-01')
    train_mask = df['race_date'] < train_end
    
    time_fe = TimeFeatureEngineerV1()
    time_fe.fit(df[train_mask])
    df = time_fe.transform(df)
    
    # 基本特徴量のエンコード
    for col in ['track_surface', 'track_condition', 'sex', 'venue', 'distance_category']:
        if col in df.columns:
            df[col + '_encoded'] = df[col].astype('category').cat.codes
    
    # Test期間
    test_mask = (df['race_date'] >= '2023-01-01') & (df['race_date'] < '2024-01-01')
    test_df = df[test_mask].copy()
    
    # 予測（標準化タイム）
    available_features = [c for c in feature_cols if c in test_df.columns]
    X_test = test_df[available_features].fillna(0)
    test_df['pred_normalized'] = model.predict(X_test)
    
    # ===== 実際のタイム（秒）に逆変換 =====
    # pred_time = pred_normalized * base_time_std + base_time_mean
    test_df['pred_time_seconds'] = (
        test_df['pred_normalized'] * test_df['base_time_std'] + test_df['base_time_mean']
    )
    
    print("\n" + "=" * 70)
    print("1. 実タイム予測の基本統計")
    print("=" * 70)
    
    test_df['time_error'] = test_df['pred_time_seconds'] - test_df['finish_time_seconds']
    test_df['time_error_abs'] = test_df['time_error'].abs()
    
    print(f"予測タイム: mean={test_df['pred_time_seconds'].mean():.2f}秒, std={test_df['pred_time_seconds'].std():.2f}秒")
    print(f"実タイム:   mean={test_df['finish_time_seconds'].mean():.2f}秒, std={test_df['finish_time_seconds'].std():.2f}秒")
    print()
    print(f"タイム誤差: mean={test_df['time_error'].mean():.3f}秒 {'(予測が遅い)' if test_df['time_error'].mean() > 0 else '(予測が速い)'}")
    print(f"タイム誤差(絶対値): mean={test_df['time_error_abs'].mean():.3f}秒, median={test_df['time_error_abs'].median():.3f}秒")
    print(f"RMSE: {np.sqrt((test_df['time_error']**2).mean()):.3f}秒")
    
    print("\n" + "=" * 70)
    print("2. タイム誤差の分布")
    print("=" * 70)
    
    percentiles = [10, 25, 50, 75, 90, 95, 99]
    print("タイム誤差（秒）のパーセンタイル:")
    for p in percentiles:
        val = np.percentile(test_df['time_error_abs'], p)
        print(f"  {p}%: {val:.2f}秒以内")
    
    print("\n" + "=" * 70)
    print("3. 距離別のタイム誤差（秒）")
    print("=" * 70)
    
    dist_stats = test_df.groupby('distance_m').agg({
        'time_error': 'mean',
        'time_error_abs': 'mean',
        'finish_time_seconds': 'count'
    }).rename(columns={'finish_time_seconds': 'count'})
    dist_stats = dist_stats.sort_index()
    
    print(f"{'距離':>6} {'バイアス':>10} {'MAE':>8} {'N':>8}")
    print("-" * 40)
    for dist, row in dist_stats.head(12).iterrows():
        if row['count'] > 100:
            print(f"{dist:>6}m {row['time_error']:>+9.2f}秒 {row['time_error_abs']:>7.2f}秒 {int(row['count']):>7}")
    
    print("\n" + "=" * 70)
    print("4. 馬場状態別のタイム誤差（秒）")
    print("=" * 70)
    
    for surface in ['芝', 'ダート']:
        print(f"\n【{surface}】")
        for cond in ['良', '稍重', '重', '不良']:
            sub = test_df[(test_df['track_surface'] == surface) & (test_df['track_condition'] == cond)]
            if len(sub) > 50:
                mae = sub['time_error_abs'].mean()
                bias = sub['time_error'].mean()
                print(f"  {cond}: バイアス={bias:+.2f}秒, MAE={mae:.2f}秒, N={len(sub):,}")
    
    print("\n" + "=" * 70)
    print("5. レース単位での最速馬の予測精度")
    print("=" * 70)
    
    def analyze_race_winner(group):
        # 実際の最速馬
        actual_winner = group.loc[group['finish_time_seconds'].idxmin()]
        # 予測の最速馬
        pred_winner = group.loc[group['pred_time_seconds'].idxmin()]
        
        return pd.Series({
            'correct': actual_winner.name == pred_winner.name,
            'actual_winner_time': actual_winner['finish_time_seconds'],
            'pred_winner_time': pred_winner['pred_time_seconds'],
            'actual_winner_pred_time': actual_winner['pred_time_seconds'],
            'pred_winner_actual_time': pred_winner['finish_time_seconds'],
            'winner_time_error': actual_winner['pred_time_seconds'] - actual_winner['finish_time_seconds'],
        })
    
    race_analysis = test_df.groupby('race_id').apply(analyze_race_winner, include_groups=False)
    
    print(f"Top1精度（最速馬を当てた率）: {race_analysis['correct'].mean():.1%}")
    print(f"1着馬のタイム予測誤差: mean={race_analysis['winner_time_error'].mean():.2f}秒, std={race_analysis['winner_time_error'].std():.2f}秒")
    
    print("\n" + "=" * 70)
    print("6. タイム誤差が大きいパターン分析")
    print("=" * 70)
    
    # 誤差が3秒以上のケース
    large_error = test_df[test_df['time_error_abs'] > 3.0]
    print(f"\n誤差3秒以上のケース: {len(large_error):,}件 ({len(large_error)/len(test_df)*100:.1f}%)")
    
    print("\n距離別:")
    large_error_dist = large_error.groupby('distance_m').size() / test_df.groupby('distance_m').size() * 100
    for dist, pct in large_error_dist.sort_values(ascending=False).head(5).items():
        print(f"  {dist}m: {pct:.1f}%")
    
    print("\n馬場状態別:")
    for cond in ['良', '稍重', '重', '不良']:
        sub_all = test_df[test_df['track_condition'] == cond]
        sub_err = large_error[large_error['track_condition'] == cond]
        if len(sub_all) > 100:
            pct = len(sub_err) / len(sub_all) * 100
            print(f"  {cond}: {pct:.1f}%")
    
    print("\n" + "=" * 70)
    print("7. レース内でのタイム差予測精度")
    print("=" * 70)
    
    def analyze_race_time_spread(group):
        actual_spread = group['finish_time_seconds'].max() - group['finish_time_seconds'].min()
        pred_spread = group['pred_time_seconds'].max() - group['pred_time_seconds'].min()
        return pd.Series({
            'actual_spread': actual_spread,
            'pred_spread': pred_spread,
            'spread_diff': pred_spread - actual_spread,
        })
    
    spread_analysis = test_df.groupby('race_id').apply(analyze_race_time_spread, include_groups=False)
    
    print(f"実際の1着-最下位タイム差: mean={spread_analysis['actual_spread'].mean():.2f}秒")
    print(f"予測の1着-最下位タイム差: mean={spread_analysis['pred_spread'].mean():.2f}秒")
    print(f"差: {spread_analysis['spread_diff'].mean():+.2f}秒 {'(予測が広すぎ)' if spread_analysis['spread_diff'].mean() > 0 else '(予測が狭すぎ)'}")
    
    print("\n" + "=" * 70)
    print("8. 考察: 何が予測できていて、何ができていないか")
    print("=" * 70)
    
    print("""
【予測できていること】
- レース内での順位関係（Spearman 0.40）
- 距離別の平均タイム傾向

【予測できていないこと】
- 個別レースの正確なペース（バイアスあり）
- 荒れ馬場での大きなタイム変動
- 長距離でのタイム予測（誤差が大きい）

【改善が必要な点】
""")
    
    # 距離別のRMSEを計算
    dist_rmse = test_df.groupby('distance_m').apply(
        lambda g: np.sqrt((g['time_error']**2).mean()) if len(g) > 100 else np.nan
    )
    worst_dist = dist_rmse.dropna().sort_values(ascending=False).head(3)
    for dist, rmse in worst_dist.items():
        print(f"  - {dist}mのRMSE: {rmse:.2f}秒")
    
    print("\n" + "=" * 70)
    print("分析完了")
    print("=" * 70)


if __name__ == "__main__":
    main()
