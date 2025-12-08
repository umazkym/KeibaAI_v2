"""
拡張特徴量エンジニアV2を使用した訓練スクリプト

約90個の特徴量を使用してモデルを訓練し、ROIを検証
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np
from typing import Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from keibaai.src.features.leak_free_feature_engineer_v2 import LeakFreeFeatureEngineerV2
from keibaai.src.modules.models.ability_estimator import Layer1_AbilityEstimator
from keibaai.src.modules.models.probability_calibrator import Layer2_ProbabilityCalibrator


def calculate_roi(test_df: pd.DataFrame, pred_probs: np.ndarray, ev_threshold: float = 1.0) -> Dict:
    """均等ベットでのROI計算"""
    test_df = test_df.copy()
    test_df['pred_prob'] = pred_probs
    test_df['ev'] = test_df['pred_prob'] * test_df['win_odds'] * 0.8
    
    bet_mask = test_df['ev'] >= ev_threshold
    bet_df = test_df[bet_mask]
    
    if len(bet_df) == 0:
        return {'n_bets': 0, 'n_hits': 0, 'roi': 0, 'hit_rate': 0}
    
    hits = bet_df[bet_df['finish_position'] == 1]
    total_bet = len(bet_df) * 100
    total_payout = (hits['win_odds'] * 100).sum()
    
    return {
        'n_bets': len(bet_df),
        'n_hits': len(hits),
        'roi': (total_payout / total_bet * 100) if total_bet > 0 else 0,
        'hit_rate': (len(hits) / len(bet_df) * 100) if len(bet_df) > 0 else 0
    }


def main():
    print("=" * 70)
    print("拡張特徴量エンジニアV2による訓練")
    print("=" * 70)
    
    # データ読み込み
    data_dir = Path('keibaai/data')
    
    print("\n1. データ読み込み...")
    races = pd.read_parquet(data_dir / "parsed/parquet/races/races.parquet")
    races['race_date'] = pd.to_datetime(races['race_date'])
    races = races.dropna(subset=['finish_position', 'win_odds'])
    
    pedigrees = pd.read_parquet(data_dir / "parsed/parquet/pedigrees/pedigrees.parquet")
    
    print(f"   全レコード数: {len(races):,}")
    
    # 時系列分割
    train_end = pd.Timestamp('2023-06-30')
    valid_end = pd.Timestamp('2023-12-31')
    test1_end = pd.Timestamp('2024-06-30')
    
    train_df = races[races['race_date'] <= train_end].copy()
    valid_df = races[(races['race_date'] > train_end) & (races['race_date'] <= valid_end)].copy()
    test1_df = races[(races['race_date'] > valid_end) & (races['race_date'] <= test1_end)].copy()
    test2_df = races[races['race_date'] > test1_end].copy()
    
    print(f"\n2. 時系列分割")
    print(f"   Train: ~{train_end.date()} ({len(train_df):,})")
    print(f"   Valid: ~{valid_end.date()} ({len(valid_df):,})")
    print(f"   Test1: ~{test1_end.date()} ({len(test1_df):,})")
    print(f"   Test2: {test1_end.date()}~ ({len(test2_df):,})")
    
    # 特徴量エンジニア
    print("\n3. 特徴量エンジニアをfit...")
    fe = LeakFreeFeatureEngineerV2()
    fe.fit(train_df, pedigrees_df=pedigrees)
    
    # 各データセットにtransform
    print("\n4. 特徴量を生成...")
    train_feat = fe.transform(train_df)
    valid_feat = fe.transform(valid_df)
    test1_feat = fe.transform(test1_df)
    test2_feat = fe.transform(test2_df)
    
    # 特徴量カラムを取得
    feature_cols = fe.get_feature_columns()
    feature_cols = [c for c in feature_cols if c in train_feat.columns]
    print(f"\n   使用特徴量数: {len(feature_cols)}")
    
    # NaN処理
    print("\n5. NaN処理...")
    for col in feature_cols:
        median_val = train_feat[col].median()
        median_val = median_val if pd.notna(median_val) else 0
        for df in [train_feat, valid_feat, test1_feat, test2_feat]:
            df[col] = df[col].fillna(median_val)
    
    # モデル訓練
    print("\n" + "=" * 70)
    print("6. モデル訓練")
    print("=" * 70)
    
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
    
    # Test2で予測
    print("\n7. 予測とROI計算...")
    X_test2 = test2_feat[feature_cols]
    groups_test2 = test2_feat['race_id']
    test2_pred = layer1.predict(X_test2, groups_test2)
    test2_scores = test2_pred['ability_score'].values
    test2_probs_df = layer2.transform(test2_scores, groups_test2.values)
    test2_probs = test2_probs_df['win_prob'].values
    
    test2_feat = test2_feat.copy()
    test2_feat['pred_prob'] = test2_probs
    test2_feat['rank_in_race'] = test2_feat.groupby('race_id')['pred_prob'].rank(ascending=False, method='first')
    
    # ========================================
    # 結果
    # ========================================
    print("\n" + "=" * 70)
    print("8. 結果")
    print("=" * 70)
    
    # ランダムベースライン
    winners = test2_feat[test2_feat['finish_position'] == 1]
    random_roi = (winners['win_odds'] * 100).sum() / (len(test2_feat) * 100) * 100
    print(f"\nランダムROI: {random_roi:.1f}%")
    
    # EVフィルタ
    print("\n--- EVフィルタ ---")
    for ev_th in [1.0, 1.5, 2.0]:
        roi_result = calculate_roi(test2_feat, test2_probs, ev_threshold=ev_th)
        print(f"  EV >= {ev_th}: ROI={roi_result['roi']:.1f}%, ベット={roi_result['n_bets']:,}")
    
    # Top戦略
    print("\n--- Top戦略 ---")
    for top_n in [1, 2, 3]:
        bet_mask = test2_feat['rank_in_race'] <= top_n
        bet_df = test2_feat[bet_mask]
        hits = bet_df[bet_df['finish_position'] == 1]
        roi = (hits['win_odds'] * 100).sum() / (len(bet_df) * 100) * 100
        hit_rate = len(hits) / len(bet_df) * 100
        print(f"  Top{top_n}: ROI={roi:.1f}%, 的中率={hit_rate:.2f}%, ベット={len(bet_df):,}")
    
    # Top1 + オッズ範囲
    print("\n--- Top1 + オッズ範囲 ---")
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
    
    # Top1 + EV >= 1.0
    print("\n--- Top1 + EV >= 1.0 ---")
    test2_feat['ev'] = test2_feat['pred_prob'] * test2_feat['win_odds'] * 0.8
    bet_mask = (test2_feat['rank_in_race'] == 1) & (test2_feat['ev'] >= 1.0)
    bet_df = test2_feat[bet_mask]
    hits = bet_df[bet_df['finish_position'] == 1]
    roi = (hits['win_odds'] * 100).sum() / (len(bet_df) * 100) * 100
    print(f"  Top1+EV>=1.0: ROI={roi:.1f}%, ベット={len(bet_df):,}")
    
    # キャリブレーション確認
    print("\n" + "=" * 70)
    print("9. キャリブレーション確認")
    print("=" * 70)
    
    for low, high in [(0, 0.05), (0.05, 0.1), (0.1, 0.2), (0.2, 1.0)]:
        subset = test2_feat[(test2_feat['pred_prob'] >= low) & (test2_feat['pred_prob'] < high)]
        if len(subset) > 0:
            actual_rate = (subset['finish_position'] == 1).mean()
            expected_rate = subset['pred_prob'].mean()
            diff = actual_rate - expected_rate
            print(f"  予測{low:.0%}-{high:.0%}: 実際={actual_rate:.1%}, 予測={expected_rate:.1%}, 差={diff:+.1%}")
    
    # 予測20%以上の勝率をチェック（リークチェック）
    high_prob = test2_feat[test2_feat['pred_prob'] >= 0.2]
    if len(high_prob) > 0:
        actual_win_rate = (high_prob['finish_position'] == 1).mean()
        print(f"\n予測20%以上の馬の実際の勝率: {actual_win_rate:.1%}")
        if actual_win_rate > 0.95:
            print("⚠️ リークの可能性あり！")
        else:
            print("✅ リークなし")


if __name__ == '__main__':
    main()
