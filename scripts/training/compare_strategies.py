"""
V1, V2改善, ハイブリッドの3戦略を比較検証

戦略1: V1（37特徴量）をそのまま使用
戦略2: V2（81特徴量）+ 特徴量選択 + 正則化強化
戦略3: V1とV2のハイブリッド（重要特徴量を厳選）
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np
from typing import Dict, List
import logging

logging.basicConfig(level=logging.WARNING)

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from keibaai.src.features.leak_free_feature_engineer import LeakFreeFeatureEngineer
from keibaai.src.features.leak_free_feature_engineer_v2 import LeakFreeFeatureEngineerV2
from keibaai.src.models.ability_estimator import Layer1_AbilityEstimator
from keibaai.src.models.probability_calibrator import Layer2_ProbabilityCalibrator


def train_and_evaluate(train_feat, valid_feat, test2_feat, feature_cols, config, strategy_name):
    """訓練と評価"""
    # NaN処理
    for col in feature_cols:
        if col in train_feat.columns:
            median_val = train_feat[col].median()
            median_val = median_val if pd.notna(median_val) else 0
            for df in [train_feat, valid_feat, test2_feat]:
                if col in df.columns:
                    df[col] = df[col].fillna(median_val)
    
    feature_cols = [c for c in feature_cols if c in train_feat.columns]
    
    X_train = train_feat[feature_cols]
    y_train = train_feat['finish_position']
    groups_train = train_feat['race_id']
    X_valid = valid_feat[feature_cols]
    y_valid = valid_feat['finish_position']
    groups_valid = valid_feat['race_id']
    
    layer1 = Layer1_AbilityEstimator(config=config)
    layer1.train(X_train, y_train, groups_train, X_valid, y_valid, groups_valid)
    
    train_pred = layer1.predict(X_train, groups_train)
    train_scores = train_pred['ability_score'].values
    layer2 = Layer2_ProbabilityCalibrator(method='isotonic_softmax')
    layer2.fit(train_scores, y_train.values, groups_train.values)
    
    X_test2 = test2_feat[feature_cols]
    groups_test2 = test2_feat['race_id']
    test2_pred = layer1.predict(X_test2, groups_test2)
    test2_scores = test2_pred['ability_score'].values
    test2_probs_df = layer2.transform(test2_scores, groups_test2.values)
    test2_probs = test2_probs_df['win_prob'].values
    
    test2_eval = test2_feat.copy()
    test2_eval['pred_prob'] = test2_probs
    test2_eval['rank_in_race'] = test2_eval.groupby('race_id')['pred_prob'].rank(ascending=False, method='first')
    test2_eval['ev'] = test2_eval['pred_prob'] * test2_eval['win_odds'] * 0.8
    
    results = {'strategy': strategy_name, 'n_features': len(feature_cols)}
    
    # Top1
    bet_mask = test2_eval['rank_in_race'] == 1
    bet_df = test2_eval[bet_mask]
    hits = bet_df[bet_df['finish_position'] == 1]
    results['top1_roi'] = (hits['win_odds'] * 100).sum() / (len(bet_df) * 100) * 100
    
    # Top1 + EV >= 1.0
    bet_mask = (test2_eval['rank_in_race'] == 1) & (test2_eval['ev'] >= 1.0)
    bet_df = test2_eval[bet_mask]
    hits = bet_df[bet_df['finish_position'] == 1]
    results['top1_ev_roi'] = (hits['win_odds'] * 100).sum() / (len(bet_df) * 100) * 100 if len(bet_df) > 0 else 0
    results['top1_ev_bets'] = len(bet_df)
    
    # Top1 + オッズ20-50
    bet_mask = (test2_eval['rank_in_race'] == 1) & (test2_eval['win_odds'] >= 20) & (test2_eval['win_odds'] < 50)
    bet_df = test2_eval[bet_mask]
    hits = bet_df[bet_df['finish_position'] == 1]
    results['top1_odds2050_roi'] = (hits['win_odds'] * 100).sum() / (len(bet_df) * 100) * 100 if len(bet_df) > 0 else 0
    results['top1_odds2050_bets'] = len(bet_df)
    
    return results


def main():
    print("=" * 70)
    print("V1, V2改善, ハイブリッドの3戦略比較")
    print("=" * 70)
    
    # データ読み込み
    data_dir = Path('keibaai/data')
    
    races = pd.read_parquet(data_dir / "parsed/parquet/races/races.parquet")
    races['race_date'] = pd.to_datetime(races['race_date'])
    races = races.dropna(subset=['finish_position', 'win_odds'])
    
    pedigrees = pd.read_parquet(data_dir / "parsed/parquet/pedigrees/pedigrees.parquet")
    
    train_end = pd.Timestamp('2023-06-30')
    valid_end = pd.Timestamp('2023-12-31')
    test1_end = pd.Timestamp('2024-06-30')
    
    train_df = races[races['race_date'] <= train_end].copy()
    valid_df = races[(races['race_date'] > train_end) & (races['race_date'] <= valid_end)].copy()
    test2_df = races[races['race_date'] > test1_end].copy()
    
    print(f"\nTrain: {len(train_df):,}, Valid: {len(valid_df):,}, Test2: {len(test2_df):,}")
    
    # ランダムベースライン
    winners = test2_df[test2_df['finish_position'] == 1]
    random_roi = (winners['win_odds'] * 100).sum() / (len(test2_df) * 100) * 100
    print(f"ランダムROI: {random_roi:.1f}%")
    
    all_results = []
    
    # ========================================
    # 戦略1: V1（37特徴量）
    # ========================================
    print("\n" + "=" * 70)
    print("戦略1: V1（37特徴量）")
    print("=" * 70)
    
    fe_v1 = LeakFreeFeatureEngineer()
    fe_v1.fit(train_df, pedigrees_df=pedigrees)
    
    train_v1 = fe_v1.transform(train_df)
    valid_v1 = fe_v1.transform(valid_df)
    test2_v1 = fe_v1.transform(test2_df)
    
    feature_cols_v1 = fe_v1.get_feature_columns()
    config_v1 = {'name': 'v1', 'n_estimators': 500, 'learning_rate': 0.05, 'max_depth': 4, 'min_child_samples': 50}
    
    result_v1 = train_and_evaluate(train_v1, valid_v1, test2_v1, feature_cols_v1, config_v1, 'V1_37feat')
    all_results.append(result_v1)
    print(f"  Top1: {result_v1['top1_roi']:.1f}%")
    print(f"  Top1+EV>=1.0: {result_v1['top1_ev_roi']:.1f}% ({result_v1['top1_ev_bets']}件)")
    print(f"  Top1+オッズ20-50: {result_v1['top1_odds2050_roi']:.1f}% ({result_v1['top1_odds2050_bets']}件)")
    
    # ========================================
    # 戦略2: V2 + 正則化強化
    # ========================================
    print("\n" + "=" * 70)
    print("戦略2: V2（81特徴量）+ 正則化強化")
    print("=" * 70)
    
    fe_v2 = LeakFreeFeatureEngineerV2()
    fe_v2.fit(train_df, pedigrees_df=pedigrees)
    
    train_v2 = fe_v2.transform(train_df)
    valid_v2 = fe_v2.transform(valid_df)
    test2_v2 = fe_v2.transform(test2_df)
    
    feature_cols_v2 = fe_v2.get_feature_columns()
    
    # 正則化強化: depth=3, min_child=100
    config_v2_reg = {'name': 'v2_reg', 'n_estimators': 500, 'learning_rate': 0.03, 'max_depth': 3, 'min_child_samples': 100}
    
    result_v2 = train_and_evaluate(train_v2.copy(), valid_v2.copy(), test2_v2.copy(), feature_cols_v2, config_v2_reg, 'V2_81feat_reg')
    all_results.append(result_v2)
    print(f"  Top1: {result_v2['top1_roi']:.1f}%")
    print(f"  Top1+EV>=1.0: {result_v2['top1_ev_roi']:.1f}% ({result_v2['top1_ev_bets']}件)")
    print(f"  Top1+オッズ20-50: {result_v2['top1_odds2050_roi']:.1f}% ({result_v2['top1_odds2050_bets']}件)")
    
    # ========================================
    # 戦略3: ハイブリッド（V1ベース + V2から厳選）
    # ========================================
    print("\n" + "=" * 70)
    print("戦略3: ハイブリッド（V1 + V2から厳選）")
    print("=" * 70)
    
    # V1の特徴量 + V2から追加する有望な特徴量
    hybrid_features = [
        # V1からの基本特徴量
        'jockey_win_rate', 'jockey_top3_rate', 'jockey_overall_races',
        'jockey_venue_win_rate', 'jockey_dist_win_rate', 'jockey_surface_win_rate',
        'trainer_win_rate', 'trainer_top3_rate', 'trainer_overall_races',
        'trainer_venue_win_rate',
        'sire_win_rate', 'sire_races', 'sire_dist_win_rate',
        'sire_surface_win_rate', 'sire_cond_win_rate',
        'horse_win_rate', 'horse_top3_rate', 'horse_avg_finish',
        'horse_total_races', 'horse_last3_avg_finish', 'horse_best_finish',
        'horse_last3f_avg', 'horse_last3f_best',
        'horse_dist_win_rate', 'horse_dist_avg_finish',
        'horse_surface_win_rate', 'horse_surface_avg_finish',
        'horse_venue_win_rate', 'horse_cond_win_rate',
        'bracket_bias',
        'distance_category_encoded', 'track_condition_encoded', 'track_surface_encoded',
        'bracket_number', 'horse_weight', 'age', 'distance_m',
        # V2から追加する有望な特徴量
        'horse_finish_cv', 'horse_experience_log', 'horse_finish_trend',
        'jh_win_rate', 'jh_avg_finish',
        'jt_win_rate', 'jt_avg_finish',
        'days_since_last_race', 'is_after_long_rest',
        'last_finish_position',
        'horse_win_rate_vs_race_mean', 'horse_win_rate_rank_in_race',
    ]
    
    config_hybrid = {'name': 'hybrid', 'n_estimators': 500, 'learning_rate': 0.05, 'max_depth': 4, 'min_child_samples': 50}
    
    result_hybrid = train_and_evaluate(train_v2.copy(), valid_v2.copy(), test2_v2.copy(), hybrid_features, config_hybrid, 'Hybrid_50feat')
    all_results.append(result_hybrid)
    print(f"  Top1: {result_hybrid['top1_roi']:.1f}%")
    print(f"  Top1+EV>=1.0: {result_hybrid['top1_ev_roi']:.1f}% ({result_hybrid['top1_ev_bets']}件)")
    print(f"  Top1+オッズ20-50: {result_hybrid['top1_odds2050_roi']:.1f}% ({result_hybrid['top1_odds2050_bets']}件)")
    
    # ========================================
    # 追加戦略: V2 + 特徴量選択
    # ========================================
    print("\n" + "=" * 70)
    print("追加戦略: V2 + 上位特徴量のみ")
    print("=" * 70)
    
    # 重要そうな特徴量を厳選
    selected_v2_features = [
        # 馬の過去成績（最重要）
        'horse_win_rate', 'horse_top3_rate', 'horse_avg_finish',
        'horse_best_finish', 'horse_last3_avg_finish', 'horse_last_finish',
        'horse_total_races',
        # 馬の条件適性
        'horse_dist_win_rate', 'horse_dist_avg_finish',
        'horse_surface_win_rate', 'horse_surface_avg_finish',
        # 騎手
        'jockey_win_rate', 'jockey_avg_finish',
        'jockey_venue_win_rate',
        # 調教師
        'trainer_win_rate', 'trainer_avg_finish',
        # 血統
        'sire_win_rate', 'sire_avg_finish',
        'sire_dist_win_rate', 'sire_surface_win_rate',
        # 安定性
        'horse_finish_cv', 'horse_experience_log',
        # 馬場バイアス
        'bracket_bias',
        # 当日情報
        'bracket_number', 'horse_weight', 'age', 'distance_m',
        'distance_category_encoded', 'track_condition_encoded', 'track_surface_encoded',
    ]
    
    config_selected = {'name': 'selected', 'n_estimators': 500, 'learning_rate': 0.05, 'max_depth': 4, 'min_child_samples': 50}
    
    result_selected = train_and_evaluate(train_v2.copy(), valid_v2.copy(), test2_v2.copy(), selected_v2_features, config_selected, 'V2_selected_30feat')
    all_results.append(result_selected)
    print(f"  Top1: {result_selected['top1_roi']:.1f}%")
    print(f"  Top1+EV>=1.0: {result_selected['top1_ev_roi']:.1f}% ({result_selected['top1_ev_bets']}件)")
    print(f"  Top1+オッズ20-50: {result_selected['top1_odds2050_roi']:.1f}% ({result_selected['top1_odds2050_bets']}件)")
    
    # ========================================
    # 結果比較
    # ========================================
    print("\n" + "=" * 70)
    print("最終比較")
    print("=" * 70)
    
    print(f"\n{'戦略':<25} {'特徴量':<8} {'Top1':>8} {'Top1+EV':>10} {'Top1+オッズ20-50':>15}")
    print("-" * 70)
    for r in all_results:
        print(f"{r['strategy']:<25} {r['n_features']:<8} {r['top1_roi']:>7.1f}% {r['top1_ev_roi']:>9.1f}% {r['top1_odds2050_roi']:>14.1f}%")
    
    print(f"\nランダムベースライン: {random_roi:.1f}%")
    
    # ベスト戦略を特定
    best = max(all_results, key=lambda x: x['top1_odds2050_roi'])
    print(f"\n🏆 ベスト戦略: {best['strategy']} (Top1+オッズ20-50 ROI: {best['top1_odds2050_roi']:.1f}%)")


if __name__ == '__main__':
    main()
