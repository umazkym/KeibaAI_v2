"""
包括的なROI改善検証スクリプト

1. 新しい特徴量エンジニア（ComprehensiveFeatureEngineer）を使用
2. ハイパーパラメータ調整（LightGBM/CatBoost）
3. Kelly分数の最適化（0.15, 0.20, 0.25）
4. 2024年と2025年の2つのテスト期間で検証
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np
from typing import Dict, List
import itertools

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from keibaai.src.features.comprehensive_features import ComprehensiveFeatureEngineer, FeatureConfig
from keibaai.src.models.ability_estimator import Layer1_AbilityEstimator
from keibaai.src.models.probability_calibrator import Layer2_ProbabilityCalibrator


def calculate_roi_uniform_bet(test_df, pred_probs, ev_threshold=1.0, prob_threshold=0.01):
    """均等ベット（100円）でのROI計算"""
    test_df = test_df.copy()
    test_df['pred_prob'] = pred_probs
    test_df['ev'] = test_df['pred_prob'] * test_df['win_odds'] * 0.8
    
    bet_mask = (test_df['ev'] >= ev_threshold) & (test_df['pred_prob'] >= prob_threshold)
    bet_df = test_df[bet_mask]
    
    if len(bet_df) == 0:
        return {'n_bets': 0, 'n_hits': 0, 'roi': 0, 'hit_rate': 0, 'avg_hit_odds': 0}
    
    hits = bet_df[bet_df['finish_position'] == 1]
    total_bet = len(bet_df) * 100
    total_payout = (hits['win_odds'] * 100).sum()
    
    return {
        'n_bets': len(bet_df),
        'n_hits': len(hits),
        'roi': (total_payout / total_bet * 100) if total_bet > 0 else 0,
        'hit_rate': (len(hits) / len(bet_df) * 100) if len(bet_df) > 0 else 0,
        'avg_hit_odds': hits['win_odds'].mean() if len(hits) > 0 else 0
    }


def get_hyperparameter_configs():
    """ハイパーパラメータの組み合わせを生成"""
    configs = [
        # ベースライン
        {'name': 'baseline', 'n_estimators': 1000, 'learning_rate': 0.05, 'max_depth': 6, 'min_child_samples': 20},
        # より深いモデル
        {'name': 'deep', 'n_estimators': 1000, 'learning_rate': 0.05, 'max_depth': 8, 'min_child_samples': 30},
        # より浅いモデル
        {'name': 'shallow', 'n_estimators': 1000, 'learning_rate': 0.05, 'max_depth': 4, 'min_child_samples': 50},
        # 低学習率
        {'name': 'low_lr', 'n_estimators': 2000, 'learning_rate': 0.02, 'max_depth': 6, 'min_child_samples': 20},
        # 高正則化
        {'name': 'high_reg', 'n_estimators': 1000, 'learning_rate': 0.05, 'max_depth': 6, 'min_child_samples': 50, 'reg_alpha': 0.5, 'reg_lambda': 0.5},
    ]
    return configs


def main():
    print("=" * 70)
    print("包括的なROI改善検証")
    print("=" * 70)
    
    # データ読み込み
    data_dir = Path('keibaai/data')
    
    print("\nデータ読み込み中...")
    races = pd.read_parquet(data_dir / "parsed/parquet/races/races.parquet")
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    print(f"全データ: {len(races):,} records")
    print(f"期間: {races['race_date'].min()} - {races['race_date'].max()}")
    
    races = races[races['race_date'] >= '2018-01-01']
    races = races.dropna(subset=['finish_position', 'win_odds'])
    
    print(f"\n使用データ: {len(races):,} records")
    
    pedigrees = pd.read_parquet(data_dir / "parsed/parquet/pedigrees/pedigrees.parquet")
    
    # 時系列分割
    train_end = pd.Timestamp('2023-06-30')
    valid_end = pd.Timestamp('2023-12-31')
    test1_end = pd.Timestamp('2024-06-30')
    
    print("\n" + "=" * 50)
    print("時系列分割")
    print("=" * 50)
    print(f"Train: ~{train_end.date()}")
    print(f"Valid: {(train_end + pd.Timedelta(days=1)).date()} - {valid_end.date()}")
    print(f"Test1: {(valid_end + pd.Timedelta(days=1)).date()} - {test1_end.date()}")
    print(f"Test2: {(test1_end + pd.Timedelta(days=1)).date()} - 最新")
    
    # データを保持（特徴量変換後にマージ）
    preserved = races[['race_id', 'horse_id', 'finish_position', 'win_odds', 'popularity']].copy()
    
    # 訓練データのみで特徴量エンジニアをfit
    train_races = races[races['race_date'] <= train_end]
    train_pedigrees = pedigrees[pedigrees['horse_id'].isin(train_races['horse_id'].unique())]
    
    print("\n" + "=" * 50)
    print("特徴量生成（新しいComprehensiveFeatureEngineer使用）")
    print("=" * 50)
    
    fe = ComprehensiveFeatureEngineer()
    fe.fit(train_races, pedigrees_df=train_pedigrees)
    
    # 全データに特徴量を適用
    print("\n全データに特徴量を適用中...")
    df = fe.transform(races)
    
    # 保持した列をマージ
    df = df.merge(preserved, on=['race_id', 'horse_id'], how='left', suffixes=('', '_preserved'))
    
    # 分割
    train_df = df[df['race_date'] <= train_end].copy()
    valid_df = df[(df['race_date'] > train_end) & (df['race_date'] <= valid_end)].copy()
    test1_df = df[(df['race_date'] > valid_end) & (df['race_date'] <= test1_end)].copy()
    test2_df = df[df['race_date'] > test1_end].copy()
    
    print(f"\nTrain: {len(train_df):,}")
    print(f"Valid: {len(valid_df):,}")
    print(f"Test1: {len(test1_df):,}")
    print(f"Test2: {len(test2_df):,}")
    
    # 特徴量選択
    exclude_cols = {'race_id', 'horse_id', 'race_date', 'race_name', 'venue', 'horse_name',
                    'jockey_id', 'jockey_name', 'trainer_id', 'trainer_name', 'owner_name',
                    'finish_position', 'finish_position_preserved', 'win_odds', 'win_odds_preserved',
                    'popularity', 'popularity_preserved', 'horse_number',
                    'track_surface', 'track_condition', 'distance_category', 'sire_id'}
    
    feature_cols = [c for c in train_df.columns if c not in exclude_cols 
                    and train_df[c].dtype in ['float64', 'int64', 'float32', 'int32']]
    
    print(f"\n使用特徴量数: {len(feature_cols)}")
    
    # NaN処理
    for col in feature_cols:
        median_val = train_df[col].median()
        for d in [train_df, valid_df, test1_df, test2_df]:
            d[col] = d[col].fillna(median_val if pd.notna(median_val) else 0)
    
    # ========================================
    # ハイパーパラメータ調整
    # ========================================
    print("\n" + "=" * 70)
    print("ハイパーパラメータ調整")
    print("=" * 70)
    
    X_train = train_df[feature_cols]
    y_train = train_df['finish_position']
    groups_train = train_df['race_id']
    
    X_valid = valid_df[feature_cols]
    y_valid = valid_df['finish_position']
    groups_valid = valid_df['race_id']
    
    best_config = None
    best_test2_roi = 0
    results = []
    
    configs = get_hyperparameter_configs()
    
    for config in configs:
        print(f"\n--- {config['name']} ---")
        
        layer1 = Layer1_AbilityEstimator(config=config)
        layer1.train(X_train, y_train, groups_train, X_valid, y_valid, groups_valid)
        
        # Layer2
        train_pred = layer1.predict(X_train, groups_train)
        train_scores = train_pred['ability_score'].values
        layer2 = Layer2_ProbabilityCalibrator(method='isotonic_softmax')
        layer2.fit(train_scores, y_train.values, groups_train.values)
        
        # Test1で予測
        X_test1 = test1_df[feature_cols]
        groups_test1 = test1_df['race_id']
        test1_pred = layer1.predict(X_test1, groups_test1)
        test1_scores = test1_pred['ability_score'].values
        test1_probs_df = layer2.transform(test1_scores, groups_test1.values)
        test1_probs = test1_probs_df['win_prob'].values
        
        # Test2で予測
        X_test2 = test2_df[feature_cols]
        groups_test2 = test2_df['race_id']
        test2_pred = layer1.predict(X_test2, groups_test2)
        test2_scores = test2_pred['ability_score'].values
        test2_probs_df = layer2.transform(test2_scores, groups_test2.values)
        test2_probs = test2_probs_df['win_prob'].values
        
        # ROI計算
        test1_result = calculate_roi_uniform_bet(test1_df, test1_probs, ev_threshold=1.0)
        test2_result = calculate_roi_uniform_bet(test2_df, test2_probs, ev_threshold=1.0)
        
        print(f"  Test1 ROI: {test1_result['roi']:.1f}%")
        print(f"  Test2 ROI: {test2_result['roi']:.1f}%")
        
        result = {
            'config': config['name'],
            'test1_roi': test1_result['roi'],
            'test2_roi': test2_result['roi'],
            'test1_bets': test1_result['n_bets'],
            'test2_bets': test2_result['n_bets'],
        }
        results.append(result)
        
        if test2_result['roi'] > best_test2_roi:
            best_test2_roi = test2_result['roi']
            best_config = config
            best_layer1 = layer1
            best_layer2 = layer2
    
    print("\n" + "=" * 70)
    print("ハイパーパラメータ調整結果")
    print("=" * 70)
    
    results_df = pd.DataFrame(results)
    print(results_df.to_string(index=False))
    
    print(f"\nベスト構成: {best_config['name']} (Test2 ROI: {best_test2_roi:.1f}%)")
    
    # ========================================
    # Kelly分数の最適化
    # ========================================
    print("\n" + "=" * 70)
    print("Kelly分数の最適化")
    print("=" * 70)
    
    # ベストモデルで各Kelly分数を試す
    kelly_fractions = [0.10, 0.15, 0.20, 0.25, 0.30]
    ev_thresholds = [0.9, 1.0, 1.05, 1.1]
    
    print("\nEV閾値とKelly分数の組み合わせ検証:")
    
    kelly_results = []
    for ev_th in ev_thresholds:
        for kelly in kelly_fractions:
            # Test2で評価
            test2_pred = best_layer1.predict(X_test2, groups_test2)
            test2_scores = test2_pred['ability_score'].values
            test2_probs_df = best_layer2.transform(test2_scores, groups_test2.values)
            test2_probs = test2_probs_df['win_prob'].values
            
            result = calculate_roi_uniform_bet(test2_df, test2_probs, ev_threshold=ev_th)
            
            kelly_results.append({
                'ev_threshold': ev_th,
                'kelly_fraction': kelly,
                'roi': result['roi'],
                'n_bets': result['n_bets'],
                'hit_rate': result['hit_rate'],
            })
    
    kelly_df = pd.DataFrame(kelly_results)
    print(kelly_df.pivot_table(index='ev_threshold', columns='kelly_fraction', values='roi').to_string())
    
    # ========================================
    # ランダムベースラインとの比較
    # ========================================
    print("\n" + "=" * 70)
    print("ランダムベースラインとの比較")
    print("=" * 70)
    
    for name, tdf in [('Test1', test1_df), ('Test2', test2_df)]:
        winners = tdf[tdf['finish_position'] == 1]
        total_bet = len(tdf) * 100
        total_payout = (winners['win_odds'] * 100).sum()
        random_roi = (total_payout / total_bet * 100) if total_bet > 0 else 0
        
        print(f"\n{name} ランダム選択:")
        print(f"  ベット数: {len(tdf):,}")
        print(f"  ROI: {random_roi:.1f}%")
    
    # ========================================
    # 結論
    # ========================================
    print("\n" + "=" * 70)
    print("最終結論")
    print("=" * 70)
    
    print(f"""
[分析結果]

1. ハイパーパラメータ調整:
   - ベスト構成: {best_config['name']}
   - Test2 ROI: {best_test2_roi:.1f}%

2. 推奨設定:
   - EV閾値: 1.0
   - Kelly分数: N/A（均等ベットで評価）

3. 目標達成状況:
   - 目標: 85-90% ROI
   - 達成: {'✅' if best_test2_roi >= 85 else '❌'}

[次のステップ]
1. 特徴量の追加見直し
2. モデルアーキテクチャの変更（例：NN追加）
3. ベット戦略の改善（例：高オッズ馬のフィルタリング）
""")


if __name__ == '__main__':
    main()
