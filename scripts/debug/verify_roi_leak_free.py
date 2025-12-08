"""
リークを完全に排除したROI検証

問題:
ComprehensiveFeatureEngineerはtransform時にfinish_positionを使って
特徴量を計算するため、テストデータにリークが発生している。

解決策:
時系列分割後、テストデータのfinish_positionを使わずに特徴量を計算する。
→ 訓練データのみで全ての履歴特徴量を計算し、テストデータにはその統計をマージする

このスクリプトでは、シンプルな特徴量のみを使用してリークを防止する。
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from keibaai.src.modules.models.ability_estimator import Layer1_AbilityEstimator
from keibaai.src.modules.models.probability_calibrator import Layer2_ProbabilityCalibrator


def create_leak_free_features(train_df: pd.DataFrame, test_df: pd.DataFrame) -> tuple:
    """
    リークを完全に排除した特徴量を作成
    
    訓練データのみで統計を計算し、テストデータに適用
    テストデータのfinish_positionは使用しない
    """
    # ========================================
    # 1. 訓練データで統計を計算
    # ========================================
    
    # 騎手統計（訓練データのみ）
    jockey_stats = train_df.groupby('jockey_id').apply(
        lambda x: pd.Series({
            'jockey_win_rate': (x['finish_position'] == 1).mean(),
            'jockey_top3_rate': (x['finish_position'] <= 3).mean(),
            'jockey_races': len(x),
        }), include_groups=False
    ).reset_index()
    
    # 調教師統計（訓練データのみ）
    trainer_stats = train_df.groupby('trainer_id').apply(
        lambda x: pd.Series({
            'trainer_win_rate': (x['finish_position'] == 1).mean(),
            'trainer_top3_rate': (x['finish_position'] <= 3).mean(),
            'trainer_races': len(x),
        }), include_groups=False
    ).reset_index()
    
    # 馬の過去成績（訓練データの最終時点まで）
    # 各馬の累積成績を計算
    train_sorted = train_df.sort_values(['horse_id', 'race_date'])
    horse_stats = train_sorted.groupby('horse_id').apply(
        lambda x: pd.Series({
            'horse_total_races': len(x),
            'horse_total_wins': (x['finish_position'] == 1).sum(),
            'horse_total_top3': (x['finish_position'] <= 3).sum(),
            'horse_avg_finish': x['finish_position'].mean(),
            'horse_last3_avg_finish': x['finish_position'].tail(3).mean() if len(x) >= 3 else x['finish_position'].mean(),
        }), include_groups=False
    ).reset_index()
    horse_stats['horse_win_rate'] = horse_stats['horse_total_wins'] / horse_stats['horse_total_races']
    horse_stats['horse_top3_rate'] = horse_stats['horse_total_top3'] / horse_stats['horse_total_races']
    
    # ========================================
    # 2. 訓練データに特徴量を追加
    # ========================================
    train_features = train_df[['race_id', 'horse_id', 'race_date', 'finish_position', 'win_odds', 
                                'jockey_id', 'trainer_id', 'distance_m', 'bracket_number',
                                'horse_weight', 'age']].copy()
    
    # 静的特徴量（当日情報）
    train_features['distance_category'] = pd.cut(
        train_features['distance_m'],
        bins=[0, 1400, 1800, 2200, 9999],
        labels=[0, 1, 2, 3]
    ).astype(float)
    
    # 騎手・調教師統計をマージ（訓練期間の統計）
    train_features = train_features.merge(jockey_stats, on='jockey_id', how='left')
    train_features = train_features.merge(trainer_stats, on='trainer_id', how='left')
    
    # 馬統計は各レース時点での累積を計算（リークを防ぐためshift）
    train_sorted = train_features.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
    
    # 各馬の過去成績を計算（このレースより前のデータのみ使用）
    train_sorted['_is_win'] = (train_sorted['finish_position'] == 1).astype(int)
    train_sorted['_is_top3'] = (train_sorted['finish_position'] <= 3).astype(int)
    
    train_sorted['horse_cumsum_wins'] = train_sorted.groupby('horse_id')['_is_win'].transform(lambda x: x.shift(1).cumsum())
    train_sorted['horse_cumsum_top3'] = train_sorted.groupby('horse_id')['_is_top3'].transform(lambda x: x.shift(1).cumsum())
    train_sorted['horse_cumsum_races'] = train_sorted.groupby('horse_id').cumcount()
    train_sorted['horse_cumsum_finish'] = train_sorted.groupby('horse_id')['finish_position'].transform(lambda x: x.shift(1).cumsum())
    
    train_sorted['horse_win_rate_at_race'] = train_sorted['horse_cumsum_wins'] / train_sorted['horse_cumsum_races'].clip(lower=1)
    train_sorted['horse_top3_rate_at_race'] = train_sorted['horse_cumsum_top3'] / train_sorted['horse_cumsum_races'].clip(lower=1)
    train_sorted['horse_avg_finish_at_race'] = train_sorted['horse_cumsum_finish'] / train_sorted['horse_cumsum_races'].clip(lower=1)
    
    train_features = train_sorted.drop(columns=['_is_win', '_is_top3', 'horse_cumsum_wins', 
                                                  'horse_cumsum_top3', 'horse_cumsum_races', 'horse_cumsum_finish'])
    
    # ========================================
    # 3. テストデータに特徴量を追加
    # ========================================
    test_features = test_df[['race_id', 'horse_id', 'race_date', 'finish_position', 'win_odds', 
                              'jockey_id', 'trainer_id', 'distance_m', 'bracket_number',
                              'horse_weight', 'age']].copy()
    
    test_features['distance_category'] = pd.cut(
        test_features['distance_m'],
        bins=[0, 1400, 1800, 2200, 9999],
        labels=[0, 1, 2, 3]
    ).astype(float)
    
    # 騎手・調教師統計をマージ（訓練期間の統計を使用）
    test_features = test_features.merge(jockey_stats, on='jockey_id', how='left')
    test_features = test_features.merge(trainer_stats, on='trainer_id', how='left')
    
    # 馬統計をマージ（訓練期間終了時点の累積統計を使用）
    # これがリークを防ぐポイント: テストデータのfinish_positionは使わない
    test_features = test_features.merge(
        horse_stats[['horse_id', 'horse_win_rate', 'horse_top3_rate', 'horse_avg_finish', 
                     'horse_total_races', 'horse_last3_avg_finish']],
        on='horse_id', how='left'
    )
    test_features = test_features.rename(columns={
        'horse_win_rate': 'horse_win_rate_at_race',
        'horse_top3_rate': 'horse_top3_rate_at_race',
        'horse_avg_finish': 'horse_avg_finish_at_race',
    })
    
    return train_features, test_features


def main():
    print("=" * 70)
    print("リークを完全に排除したROI検証")
    print("=" * 70)
    
    # データ読み込み
    data_dir = Path('keibaai/data')
    
    print("\nデータ読み込み中...")
    races = pd.read_parquet(data_dir / "parsed/parquet/races/races.parquet")
    races['race_date'] = pd.to_datetime(races['race_date'])
    races = races.dropna(subset=['finish_position', 'win_odds'])
    
    # 時系列分割
    train_end = pd.Timestamp('2023-06-30')
    valid_end = pd.Timestamp('2023-12-31')
    test1_end = pd.Timestamp('2024-06-30')
    
    train_raw = races[races['race_date'] <= train_end].copy()
    valid_raw = races[(races['race_date'] > train_end) & (races['race_date'] <= valid_end)].copy()
    test1_raw = races[(races['race_date'] > valid_end) & (races['race_date'] <= test1_end)].copy()
    test2_raw = races[races['race_date'] > test1_end].copy()
    
    print(f"\nTrain: {len(train_raw):,}")
    print(f"Valid: {len(valid_raw):,}")
    print(f"Test1: {len(test1_raw):,}")
    print(f"Test2: {len(test2_raw):,}")
    
    # リークを排除した特徴量を作成
    print("\nリークを排除した特徴量を作成中...")
    
    # 訓練+バリデーションを使って統計計算
    train_valid_raw = pd.concat([train_raw, valid_raw])
    
    # 訓練データの特徴量
    train_df, valid_df = create_leak_free_features(train_raw, valid_raw)
    _, test1_df = create_leak_free_features(train_valid_raw, test1_raw)
    _, test2_df = create_leak_free_features(train_valid_raw, test2_raw)
    
    # 特徴量選択
    feature_cols = ['distance_category', 'bracket_number', 'horse_weight', 'age',
                    'jockey_win_rate', 'jockey_top3_rate', 'jockey_races',
                    'trainer_win_rate', 'trainer_top3_rate', 'trainer_races',
                    'horse_win_rate_at_race', 'horse_top3_rate_at_race', 
                    'horse_avg_finish_at_race', 'horse_total_races', 'horse_last3_avg_finish']
    
    # NaN処理
    for col in feature_cols:
        if col in train_df.columns:
            median_val = train_df[col].median()
            for d in [train_df, valid_df, test1_df, test2_df]:
                if col in d.columns:
                    d[col] = d[col].fillna(median_val if pd.notna(median_val) else 0)
    
    feature_cols = [c for c in feature_cols if c in train_df.columns]
    print(f"\n使用特徴量: {feature_cols}")
    
    # モデル訓練
    print("\nモデル訓練中...")
    X_train = train_df[feature_cols]
    y_train = train_df['finish_position']
    groups_train = train_df['race_id']
    
    X_valid = valid_df[feature_cols]
    y_valid = valid_df['finish_position']
    groups_valid = valid_df['race_id']
    
    config = {'name': 'shallow', 'n_estimators': 500, 'learning_rate': 0.05, 'max_depth': 4, 'min_child_samples': 50}
    layer1 = Layer1_AbilityEstimator(config=config)
    layer1.train(X_train, y_train, groups_train, X_valid, y_valid, groups_valid)
    
    # Layer2
    train_pred = layer1.predict(X_train, groups_train)
    train_scores = train_pred['ability_score'].values
    layer2 = Layer2_ProbabilityCalibrator(method='isotonic_softmax')
    layer2.fit(train_scores, y_train.values, groups_train.values)
    
    # 予測
    print("\n予測中...")
    results = {}
    for name, tdf in [('Valid', valid_df), ('Test1', test1_df), ('Test2', test2_df)]:
        X = tdf[feature_cols]
        groups = tdf['race_id']
        pred = layer1.predict(X, groups)
        scores = pred['ability_score'].values
        probs_df = layer2.transform(scores, groups.values)
        probs = probs_df['win_prob'].values
        
        tdf = tdf.copy()
        tdf['pred_prob'] = probs
        tdf['ev'] = tdf['pred_prob'] * tdf['win_odds'] * 0.8
        
        bet_mask = tdf['ev'] >= 1.0
        bet_df = tdf[bet_mask]
        
        if len(bet_df) > 0:
            hits = bet_df[bet_df['finish_position'] == 1]
            total_bet = len(bet_df) * 100
            total_payout = (hits['win_odds'] * 100).sum()
            roi = (total_payout / total_bet * 100) if total_bet > 0 else 0
            hit_rate = len(hits) / len(bet_df) * 100
        else:
            roi, hit_rate = 0, 0
        
        results[name] = {'roi': roi, 'n_bets': len(bet_df), 'hit_rate': hit_rate}
    
    # ランダムベースライン
    print("\n" + "=" * 70)
    print("結果")
    print("=" * 70)
    
    for name in ['Valid', 'Test1', 'Test2']:
        print(f"\n{name}:")
        print(f"  ベット数: {results[name]['n_bets']:,}")
        print(f"  的中率: {results[name]['hit_rate']:.2f}%")
        print(f"  ROI: {results[name]['roi']:.1f}%")
    
    # ランダムベースライン
    print("\n" + "-" * 50)
    print("ランダムベースライン:")
    for name, tdf in [('Test1', test1_df), ('Test2', test2_df)]:
        winners = tdf[tdf['finish_position'] == 1]
        total_bet = len(tdf) * 100
        total_payout = (winners['win_odds'] * 100).sum()
        random_roi = (total_payout / total_bet * 100) if total_bet > 0 else 0
        print(f"  {name}: ROI={random_roi:.1f}%")
    
    print("\n" + "=" * 70)
    print("キャリブレーション確認")
    print("=" * 70)
    
    # Test2のキャリブレーション
    test2_df = test2_df.copy()
    X = test2_df[feature_cols]
    groups = test2_df['race_id']
    pred = layer1.predict(X, groups)
    scores = pred['ability_score'].values
    probs_df = layer2.transform(scores, groups.values)
    test2_df['pred_prob'] = probs_df['win_prob'].values
    
    print("\n予測確率別の実際の勝率（Test2）:")
    for low, high in [(0, 0.05), (0.05, 0.1), (0.1, 0.2), (0.2, 1.0)]:
        subset = test2_df[(test2_df['pred_prob'] >= low) & (test2_df['pred_prob'] < high)]
        if len(subset) > 0:
            actual_rate = (subset['finish_position'] == 1).mean()
            expected_rate = subset['pred_prob'].mean()
            diff = actual_rate - expected_rate
            print(f"  予測{low:.0%}-{high:.0%}: 実際={actual_rate:.1%}, 予測={expected_rate:.1%}, 差={diff:+.1%}")


if __name__ == '__main__':
    main()
