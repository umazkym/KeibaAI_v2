#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
μ_time_v2 予測詳細分析

現状のモデルの強み・弱みを深く分析し、改善点を特定する
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
    print("μ_time_v2 予測詳細分析")
    print("=" * 70)
    
    # モデルと基準タイムをロード
    model_dir = project_root / "keibaai" / "models" / "mu_time_v2"
    with open(model_dir / "mu_time_model.pkl", 'rb') as f:
        model = pickle.load(f)
    with open(model_dir / "feature_names.json", 'r') as f:
        feature_cols = json.load(f)
    base_time_stats = pd.read_parquet(model_dir / "base_time_stats.parquet")
    
    print(f"\n特徴量数: {len(feature_cols)}")
    print(f"特徴量: {feature_cols}")
    
    # データ読み込み
    df = pd.read_parquet(project_root / "keibaai/data/parsed/parquet/races/races.parquet")
    df['race_date'] = pd.to_datetime(df['race_date'])
    df = df.dropna(subset=['finish_position', 'finish_time_seconds'])
    df = df.drop_duplicates(subset=['race_id', 'horse_number'], keep='first')
    
    # 特徴量生成（シンプルに主要なものだけ）
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
    
    # Test期間のデータ
    test_mask = (df['race_date'] >= '2023-01-01') & (df['race_date'] < '2024-01-01')
    test_df = df[test_mask].copy()
    
    # 予測
    available_features = [c for c in feature_cols if c in test_df.columns]
    X_test = test_df[available_features].fillna(0)
    test_df['pred_normalized'] = model.predict(X_test)
    
    print("\n" + "=" * 70)
    print("1. 特徴量重要度（上位15）")
    print("=" * 70)
    importance = pd.DataFrame({
        'feature': available_features,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    print(importance.head(15).to_string(index=False))
    
    print("\n" + "=" * 70)
    print("2. 予測値の分布")
    print("=" * 70)
    print(f"予測 normalized_time: mean={test_df['pred_normalized'].mean():.3f}, std={test_df['pred_normalized'].std():.3f}")
    print(f"実際 normalized_time: mean={test_df['normalized_time'].mean():.3f}, std={test_df['normalized_time'].std():.3f}")
    
    print("\n" + "=" * 70)
    print("3. 残差分析")
    print("=" * 70)
    test_df['residual'] = test_df['normalized_time'] - test_df['pred_normalized']
    print(f"残差: mean={test_df['residual'].mean():.3f}, std={test_df['residual'].std():.3f}")
    print(f"残差絶対値: mean={test_df['residual'].abs().mean():.3f}")
    
    # バイアスがあるかチェック
    print(f"\n【バイアス確認】")
    print(f"  予測が系統的に{'+' if test_df['residual'].mean() > 0 else '-'}方向にずれている: {test_df['residual'].mean():.3f}")
    
    print("\n" + "=" * 70)
    print("4. 距離別のSpearman相関")
    print("=" * 70)
    
    def calc_spearman(group):
        if len(group) < 3:
            return np.nan
        corr, _ = spearmanr(group['normalized_time'].rank(), group['pred_normalized'].rank())
        return corr
    
    # 距離別
    test_df['distance_cat'] = pd.cut(test_df['distance_m'], bins=[0, 1400, 1800, 5000], labels=['Sprint', 'Mile', 'Long'])
    for dist_cat in ['Sprint', 'Mile', 'Long']:
        sub = test_df[test_df['distance_cat'] == dist_cat]
        race_corrs = sub.groupby('race_id').apply(calc_spearman)
        print(f"  {dist_cat}: Spearman={race_corrs.mean():.3f}, N={len(race_corrs):,}レース")
    
    print("\n" + "=" * 70)
    print("5. 馬場状態別のSpearman相関")
    print("=" * 70)
    for cond in ['良', '稍重', '重', '不良']:
        sub = test_df[test_df['track_condition'] == cond]
        if len(sub) < 100:
            continue
        race_corrs = sub.groupby('race_id').apply(calc_spearman)
        print(f"  {cond}: Spearman={race_corrs.mean():.3f}, N={len(race_corrs):,}レース")
    
    print("\n" + "=" * 70)
    print("6. Top1精度の詳細分析")
    print("=" * 70)
    
    def calc_top1(group):
        pred_top = group.loc[group['pred_normalized'].idxmin()]
        actual_top = group.loc[group['normalized_time'].idxmin()]
        return pred_top.name == actual_top.name
    
    results = test_df.groupby('race_id').apply(calc_top1)
    print(f"  全体 Top1 精度: {results.mean():.1%}")
    
    # 出走頭数別
    head_counts = test_df.groupby('race_id').size()
    for min_heads, max_heads in [(8, 10), (11, 14), (15, 18)]:
        race_ids = head_counts[(head_counts >= min_heads) & (head_counts <= max_heads)].index
        sub_results = results[race_ids]
        print(f"  {min_heads}-{max_heads}頭立て: {sub_results.mean():.1%} (N={len(sub_results):,})")
    
    print("\n" + "=" * 70)
    print("7. 予測が外れやすいパターン")
    print("=" * 70)
    
    # 実際の1着が予測でどこにいたか
    def get_winner_pred_rank(group):
        winner = group[group['finish_position'] == 1]
        if len(winner) == 0:
            return np.nan
        group_sorted = group.sort_values('pred_normalized')
        pred_rank = (group_sorted.index == winner.index[0]).argmax() + 1
        return pred_rank
    
    winner_pred_ranks = test_df.groupby('race_id').apply(get_winner_pred_rank)
    print(f"  実際の1着馬の予測順位: mean={winner_pred_ranks.mean():.2f}, median={winner_pred_ranks.median():.1f}")
    print(f"  1着馬を1位予測: {(winner_pred_ranks == 1).mean():.1%}")
    print(f"  1着馬を3位以内予測: {(winner_pred_ranks <= 3).mean():.1%}")
    print(f"  1着馬を5位以内予測: {(winner_pred_ranks <= 5).mean():.1%}")
    
    print("\n" + "=" * 70)
    print("8. 改善ポイントの考察")
    print("=" * 70)
    
    # 特徴量の欠損率
    print("\n【特徴量の欠損率（高い順）】")
    missing = test_df[available_features].isnull().mean().sort_values(ascending=False)
    high_missing = missing[missing > 0.1]
    if len(high_missing) > 0:
        print(high_missing.to_string())
    else:
        print("  10%以上の欠損がある特徴量はありません")
    
    # 相関が低い特徴量
    print("\n【normalized_timeとの相関（低い順）】")
    correlations = {}
    for col in available_features:
        valid_mask = test_df[col].notna() & test_df['normalized_time'].notna()
        if valid_mask.sum() > 100:
            corr = test_df.loc[valid_mask, col].corr(test_df.loc[valid_mask, 'normalized_time'])
            correlations[col] = corr
    corr_series = pd.Series(correlations).sort_values()
    print(corr_series.head(10).to_string())
    
    print("\n" + "=" * 70)
    print("分析完了")
    print("=" * 70)


if __name__ == "__main__":
    main()
