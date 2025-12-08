"""
ベット戦略の検証と最適化

問題: EVフィルタリングでほぼ全馬にベットしている（約50,000件/61,376件）
目標: より厳選したベット戦略でROI向上

検証項目:
1. EVフィルタを高くする（EV >= 1.5, 2.0など）
2. 予測確率で上位Nのみベット（レース内Top1のみ等）
3. 人気馬を外す（低オッズを除外）
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from keibaai.src.features.leak_free_feature_engineer import LeakFreeFeatureEngineer
from keibaai.src.modules.models.ability_estimator import Layer1_AbilityEstimator
from keibaai.src.modules.models.probability_calibrator import Layer2_ProbabilityCalibrator


def main():
    print("=" * 70)
    print("ベット戦略の検証と最適化")
    print("=" * 70)
    
    # データ読み込み
    data_dir = Path('keibaai/data')
    
    print("\nデータ読み込み中...")
    races = pd.read_parquet(data_dir / "parsed/parquet/races/races.parquet")
    races['race_date'] = pd.to_datetime(races['race_date'])
    races = races.dropna(subset=['finish_position', 'win_odds'])
    
    pedigrees = pd.read_parquet(data_dir / "parsed/parquet/pedigrees/pedigrees.parquet")
    
    # 時系列分割
    train_end = pd.Timestamp('2023-06-30')
    valid_end = pd.Timestamp('2023-12-31')
    test1_end = pd.Timestamp('2024-06-30')
    
    train_df = races[races['race_date'] <= train_end].copy()
    valid_df = races[(races['race_date'] > train_end) & (races['race_date'] <= valid_end)].copy()
    test2_df = races[races['race_date'] > test1_end].copy()
    
    # 特徴量エンジニア
    print("\n特徴量生成中...")
    fe = LeakFreeFeatureEngineer()
    fe.fit(train_df, pedigrees_df=pedigrees)
    
    train_feat = fe.transform(train_df)
    valid_feat = fe.transform(valid_df)
    test2_feat = fe.transform(test2_df)
    
    # 特徴量
    feature_cols = fe.get_feature_columns()
    feature_cols = [c for c in feature_cols if c in train_feat.columns]
    
    # NaN処理
    for col in feature_cols:
        median_val = train_feat[col].median()
        median_val = median_val if pd.notna(median_val) else 0
        for df in [train_feat, valid_feat, test2_feat]:
            df[col] = df[col].fillna(median_val)
    
    # モデル訓練
    print("\nモデル訓練中...")
    X_train = train_feat[feature_cols]
    y_train = train_feat['finish_position']
    groups_train = train_feat['race_id']
    X_valid = valid_feat[feature_cols]
    y_valid = valid_feat['finish_position']
    groups_valid = valid_feat['race_id']
    
    config = {'name': 'default', 'n_estimators': 500, 'learning_rate': 0.05, 'max_depth': 4, 'min_child_samples': 50}
    layer1 = Layer1_AbilityEstimator(config=config)
    layer1.train(X_train, y_train, groups_train, X_valid, y_valid, groups_valid)
    
    train_pred = layer1.predict(X_train, groups_train)
    train_scores = train_pred['ability_score'].values
    layer2 = Layer2_ProbabilityCalibrator(method='isotonic_softmax')
    layer2.fit(train_scores, y_train.values, groups_train.values)
    
    # Test2予測
    X_test2 = test2_feat[feature_cols]
    groups_test2 = test2_feat['race_id']
    test2_pred = layer1.predict(X_test2, groups_test2)
    test2_scores = test2_pred['ability_score'].values
    test2_probs_df = layer2.transform(test2_scores, groups_test2.values)
    test2_probs = test2_probs_df['win_prob'].values
    
    test2_feat = test2_feat.copy()
    test2_feat['pred_prob'] = test2_probs
    test2_feat['ev'] = test2_feat['pred_prob'] * test2_feat['win_odds'] * 0.8
    
    # ランダムベースライン
    winners = test2_feat[test2_feat['finish_position'] == 1]
    random_roi = (winners['win_odds'] * 100).sum() / (len(test2_feat) * 100) * 100
    print(f"\nランダムROI: {random_roi:.1f}%")
    
    # ========================================
    # 戦略1: EVフィルタを高くする
    # ========================================
    print("\n" + "=" * 70)
    print("戦略1: EVフィルタを高くする")
    print("=" * 70)
    
    for ev_th in [1.0, 1.5, 2.0, 2.5, 3.0]:
        bet_mask = test2_feat['ev'] >= ev_th
        bet_df = test2_feat[bet_mask]
        if len(bet_df) == 0:
            continue
        hits = bet_df[bet_df['finish_position'] == 1]
        roi = (hits['win_odds'] * 100).sum() / (len(bet_df) * 100) * 100
        print(f"  EV >= {ev_th:.1f}: ROI={roi:.1f}%, ベット={len(bet_df):,}, 的中={len(hits)}")
    
    # ========================================
    # 戦略2: レース内Top1のみベット
    # ========================================
    print("\n" + "=" * 70)
    print("戦略2: レース内上位のみベット")
    print("=" * 70)
    
    test2_feat['rank_in_race'] = test2_feat.groupby('race_id')['pred_prob'].rank(ascending=False, method='first')
    
    for top_n in [1, 2, 3]:
        bet_mask = test2_feat['rank_in_race'] <= top_n
        bet_df = test2_feat[bet_mask]
        hits = bet_df[bet_df['finish_position'] == 1]
        roi = (hits['win_odds'] * 100).sum() / (len(bet_df) * 100) * 100
        hit_rate = len(hits) / len(bet_df) * 100
        print(f"  Top{top_n}: ROI={roi:.1f}%, 的中率={hit_rate:.2f}%, ベット={len(bet_df):,}")
    
    # ========================================
    # 戦略3: Top1 + オッズ範囲
    # ========================================
    print("\n" + "=" * 70)
    print("戦略3: Top1 + オッズ範囲でフィルタ")
    print("=" * 70)
    
    bet_mask = test2_feat['rank_in_race'] == 1
    bet_df = test2_feat[bet_mask]
    
    for low, high in [(1, 5), (5, 10), (10, 20), (20, 50), (50, 100)]:
        subset = bet_df[(bet_df['win_odds'] >= low) & (bet_df['win_odds'] < high)]
        if len(subset) == 0:
            continue
        hits = subset[subset['finish_position'] == 1]
        roi = (hits['win_odds'] * 100).sum() / (len(subset) * 100) * 100
        hit_rate = len(hits) / len(subset) * 100
        print(f"  オッズ{low}-{high}倍: ROI={roi:.1f}%, 的中率={hit_rate:.2f}%, ベット={len(subset):,}")
    
    # ========================================
    # 戦略4: Top1 + EV >= 1.0
    # ========================================
    print("\n" + "=" * 70)
    print("戦略4: Top1 + EV >= 1.0")
    print("=" * 70)
    
    bet_mask = (test2_feat['rank_in_race'] == 1) & (test2_feat['ev'] >= 1.0)
    bet_df = test2_feat[bet_mask]
    hits = bet_df[bet_df['finish_position'] == 1]
    roi = (hits['win_odds'] * 100).sum() / (len(bet_df) * 100) * 100
    hit_rate = len(hits) / len(bet_df) * 100
    print(f"  Top1+EV>=1.0: ROI={roi:.1f}%, 的中率={hit_rate:.2f}%, ベット={len(bet_df):,}")
    
    # ========================================
    # 戦略5: 人気馬を外す（オッズ5倍以上）
    # ========================================
    print("\n" + "=" * 70)
    print("戦略5: 人気馬を外す（オッズ >= 5）")
    print("=" * 70)
    
    for top_n in [1, 2, 3]:
        bet_mask = (test2_feat['rank_in_race'] <= top_n) & (test2_feat['win_odds'] >= 5.0)
        bet_df = test2_feat[bet_mask]
        if len(bet_df) == 0:
            continue
        hits = bet_df[bet_df['finish_position'] == 1]
        roi = (hits['win_odds'] * 100).sum() / (len(bet_df) * 100) * 100
        hit_rate = len(hits) / len(bet_df) * 100
        print(f"  Top{top_n}+オッズ>=5: ROI={roi:.1f}%, 的中率={hit_rate:.2f}%, ベット={len(bet_df):,}")
    
    # ========================================
    # 最終比較
    # ========================================
    print("\n" + "=" * 70)
    print("最終比較")
    print("=" * 70)
    print(f"ランダム: {random_roi:.1f}%")
    print(f"EVフィルタのみ: 約62-63%")


if __name__ == '__main__':
    main()
