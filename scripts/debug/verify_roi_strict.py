"""
均等ベットでの厳密なROI検証

2つのテスト期間:
- Test1: 2024年1月-6月（現行）
- Test2: 2024年7月-2025年10月（完全未知データ）

均等ベット（100円/馬）で計算し、過学習・リークを検証
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np
from tqdm import tqdm

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from keibaai.src.features.hybrid_features import HybridFeatureEngineerV3, FeatureConfig
from keibaai.src.models.ability_estimator import Layer1_AbilityEstimator
from keibaai.src.models.probability_calibrator import Layer2_ProbabilityCalibrator


def calculate_roi_uniform_bet(test_df, pred_probs, ev_threshold=1.0, prob_threshold=0.01):
    """
    均等ベット（100円）でのROI計算
    
    EV >= ev_threshold かつ pred_prob >= prob_threshold の馬に100円ベット
    """
    test_df = test_df.copy()
    test_df['pred_prob'] = pred_probs
    test_df['ev'] = test_df['pred_prob'] * test_df['win_odds'] * 0.8
    
    # ベット対象を選択
    bet_mask = (test_df['ev'] >= ev_threshold) & (test_df['pred_prob'] >= prob_threshold)
    bet_df = test_df[bet_mask]
    
    if len(bet_df) == 0:
        return {
            'n_bets': 0, 'n_hits': 0, 'hit_rate': 0,
            'total_bet': 0, 'total_payout': 0, 'roi': 0,
            'avg_hit_odds': 0
        }
    
    # 的中判定
    hits = bet_df[bet_df['finish_position'] == 1]
    
    # 均等ベット（100円）
    total_bet = len(bet_df) * 100
    total_payout = (hits['win_odds'] * 100).sum()
    
    roi = (total_payout / total_bet * 100) if total_bet > 0 else 0
    hit_rate = (len(hits) / len(bet_df) * 100) if len(bet_df) > 0 else 0
    avg_hit_odds = hits['win_odds'].mean() if len(hits) > 0 else 0
    
    return {
        'n_bets': len(bet_df),
        'n_hits': len(hits),
        'hit_rate': hit_rate,
        'total_bet': total_bet,
        'total_payout': total_payout,
        'roi': roi,
        'avg_hit_odds': avg_hit_odds
    }


def main():
    print("=" * 70)
    print("均等ベットでの厳密なROI検証")
    print("=" * 70)
    
    # データ読み込み
    data_dir = Path('keibaai/data')
    
    print("\nデータ読み込み中...")
    races = pd.read_parquet(data_dir / "parsed/parquet/races/races.parquet")
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    print(f"全データ: {len(races):,} records")
    print(f"期間: {races['race_date'].min()} - {races['race_date'].max()}")
    
    # 2020年以降のデータを使用（より古いデータで訓練）
    races = races[races['race_date'] >= '2018-01-01']
    races = races.dropna(subset=['finish_position', 'win_odds'])
    
    print(f"\n使用データ: {len(races):,} records")
    print(f"期間: {races['race_date'].min()} - {races['race_date'].max()}")
    
    pedigrees = pd.read_parquet(data_dir / "parsed/parquet/pedigrees/pedigrees.parquet")
    
    # === 時系列分割 ===
    # Train: ~2023年6月
    # Valid: 2023年7月-12月
    # Test1: 2024年1月-6月
    # Test2: 2024年7月-2025年10月（完全未知）
    
    train_end = pd.Timestamp('2023-06-30')
    valid_end = pd.Timestamp('2023-12-31')
    test1_end = pd.Timestamp('2024-06-30')
    # Test2は残り全て
    
    print("\n" + "=" * 50)
    print("時系列分割")
    print("=" * 50)
    print(f"Train: ~{train_end.date()}")
    print(f"Valid: {(train_end + pd.Timedelta(days=1)).date()} - {valid_end.date()}")
    print(f"Test1: {(valid_end + pd.Timedelta(days=1)).date()} - {test1_end.date()}")
    print(f"Test2: {(test1_end + pd.Timedelta(days=1)).date()} - 最新")
    
    # finish_positionとwin_oddsを保持
    preserved = races[['race_id', 'horse_id', 'finish_position', 'win_odds', 'popularity']].copy()
    
    # === 特徴量生成（訓練データのみで統計を計算）===
    print("\n" + "=" * 50)
    print("特徴量生成（訓練データのみで統計計算）")
    print("=" * 50)
    
    # 訓練データのみでfit
    train_races = races[races['race_date'] <= train_end]
    train_pedigrees = pedigrees[pedigrees['horse_id'].isin(train_races['horse_id'].unique())]
    
    print(f"\nFitに使用するデータ: {len(train_races):,} records")
    
    fe = HybridFeatureEngineerV3()
    fe.fit(train_races, pedigrees_df=train_pedigrees)
    
    # 全データに特徴量を適用
    print("\n全データに特徴量を適用中...")
    df = fe.transform(races)
    
    # 保持した列をマージ
    df = df.merge(preserved, on=['race_id', 'horse_id'], how='left')
    
    # 分割
    train_df = df[df['race_date'] <= train_end].copy()
    valid_df = df[(df['race_date'] > train_end) & (df['race_date'] <= valid_end)].copy()
    test1_df = df[(df['race_date'] > valid_end) & (df['race_date'] <= test1_end)].copy()
    test2_df = df[df['race_date'] > test1_end].copy()
    
    print(f"\nTrain: {len(train_df):,} ({train_df['race_date'].min().date()} - {train_df['race_date'].max().date()})")
    print(f"Valid: {len(valid_df):,} ({valid_df['race_date'].min().date()} - {valid_df['race_date'].max().date()})")
    print(f"Test1: {len(test1_df):,} ({test1_df['race_date'].min().date()} - {test1_df['race_date'].max().date()})")
    print(f"Test2: {len(test2_df):,} ({test2_df['race_date'].min().date()} - {test2_df['race_date'].max().date()})")
    
    # === 特徴量選択 ===
    exclude_cols = {'race_id', 'horse_id', 'race_date', 'race_name', 'venue', 'horse_name',
                    'jockey_id', 'jockey_name', 'trainer_id', 'trainer_name', 'owner_name',
                    'finish_position', 'win_odds', 'popularity', 'horse_number',
                    'track_surface', 'track_condition', 'distance_category', 'sire_id'}
    
    feature_cols = [c for c in train_df.columns if c not in exclude_cols 
                    and train_df[c].dtype in ['float64', 'int64', 'float32', 'int32']]
    
    print(f"\n使用特徴量数: {len(feature_cols)}")
    
    # NaN処理
    for col in feature_cols:
        median_val = train_df[col].median()
        for d in [train_df, valid_df, test1_df, test2_df]:
            d[col] = d[col].fillna(median_val if pd.notna(median_val) else 0)
    
    # === モデル訓練 ===
    print("\n" + "=" * 50)
    print("モデル訓練")
    print("=" * 50)
    
    X_train = train_df[feature_cols]
    y_train = train_df['finish_position']
    groups_train = train_df['race_id']
    
    X_valid = valid_df[feature_cols]
    y_valid = valid_df['finish_position']
    groups_valid = valid_df['race_id']
    
    print("\nLayer 1 (能力推定モデル) 訓練中...")
    layer1 = Layer1_AbilityEstimator()
    layer1.train(X_train, y_train, groups_train, X_valid, y_valid, groups_valid)
    
    print("\nLayer 2 (確率キャリブレーション) 訓練中...")
    train_prediction = layer1.predict(X_train, groups_train)
    train_scores = train_prediction['ability_score'].values
    layer2 = Layer2_ProbabilityCalibrator(method='isotonic_softmax')
    layer2.fit(train_scores, y_train.values, groups_train.values)
    
    # === 予測 ===
    print("\n" + "=" * 50)
    print("予測")
    print("=" * 50)
    
    # Valid
    valid_prediction = layer1.predict(X_valid, groups_valid)
    valid_scores = valid_prediction['ability_score'].values
    valid_probs_df = layer2.transform(valid_scores, groups_valid.values)
    valid_probs = valid_probs_df['win_prob'].values
    
    # Test1
    X_test1 = test1_df[feature_cols]
    groups_test1 = test1_df['race_id']
    test1_prediction = layer1.predict(X_test1, groups_test1)
    test1_scores = test1_prediction['ability_score'].values
    test1_probs_df = layer2.transform(test1_scores, groups_test1.values)
    test1_probs = test1_probs_df['win_prob'].values
    
    # Test2
    X_test2 = test2_df[feature_cols]
    groups_test2 = test2_df['race_id']
    test2_prediction = layer1.predict(X_test2, groups_test2)
    test2_scores = test2_prediction['ability_score'].values
    test2_probs_df = layer2.transform(test2_scores, groups_test2.values)
    test2_probs = test2_probs_df['win_prob'].values
    
    # === 均等ベットでのROI計算 ===
    print("\n" + "=" * 70)
    print("均等ベット（100円/馬）でのROI検証")
    print("=" * 70)
    
    ev_thresholds = [0.8, 0.9, 1.0, 1.05, 1.1, 1.15, 1.2]
    
    print("\n[Valid期間]")
    valid_results = []
    for ev_th in ev_thresholds:
        result = calculate_roi_uniform_bet(valid_df, valid_probs, ev_threshold=ev_th)
        result['ev_threshold'] = ev_th
        valid_results.append(result)
        print(f"  EV>={ev_th:.2f}: ベット{result['n_bets']:,}件, 的中{result['n_hits']}件 ({result['hit_rate']:.1f}%), ROI={result['roi']:.1f}%, 的中平均オッズ={result['avg_hit_odds']:.1f}倍")
    
    print("\n[Test1期間（2024年1-6月）]")
    test1_results = []
    for ev_th in ev_thresholds:
        result = calculate_roi_uniform_bet(test1_df, test1_probs, ev_threshold=ev_th)
        result['ev_threshold'] = ev_th
        test1_results.append(result)
        print(f"  EV>={ev_th:.2f}: ベット{result['n_bets']:,}件, 的中{result['n_hits']}件 ({result['hit_rate']:.1f}%), ROI={result['roi']:.1f}%, 的中平均オッズ={result['avg_hit_odds']:.1f}倍")
    
    print("\n[Test2期間（2024年7月-2025年10月）] ★完全未知データ★")
    test2_results = []
    for ev_th in ev_thresholds:
        result = calculate_roi_uniform_bet(test2_df, test2_probs, ev_threshold=ev_th)
        result['ev_threshold'] = ev_th
        test2_results.append(result)
        print(f"  EV>={ev_th:.2f}: ベット{result['n_bets']:,}件, 的中{result['n_hits']}件 ({result['hit_rate']:.1f}%), ROI={result['roi']:.1f}%, 的中平均オッズ={result['avg_hit_odds']:.1f}倍")
    
    # === 過学習・リーク検証 ===
    print("\n" + "=" * 70)
    print("過学習・リーク検証")
    print("=" * 70)
    
    # 最良のEV閾値でのROI比較
    best_ev = 1.0
    valid_roi = next(r['roi'] for r in valid_results if r['ev_threshold'] == best_ev)
    test1_roi = next(r['roi'] for r in test1_results if r['ev_threshold'] == best_ev)
    test2_roi = next(r['roi'] for r in test2_results if r['ev_threshold'] == best_ev)
    
    print(f"\nEV >= {best_ev} でのROI比較:")
    print(f"  Valid: {valid_roi:.1f}%")
    print(f"  Test1: {test1_roi:.1f}%")
    print(f"  Test2: {test2_roi:.1f}% ★完全未知★")
    
    roi_drop = valid_roi - test2_roi
    print(f"\n  Valid → Test2 の低下: {roi_drop:.1f}ポイント")
    
    if roi_drop > 20:
        print("\n  ⚠️ 警告: Valid→Test2でROIが大幅に低下。過学習の可能性あり。")
    elif roi_drop > 10:
        print("\n  ⚠️ 注意: Valid→Test2でROIがやや低下。軽度の過学習の可能性。")
    else:
        print("\n  ✅ 良好: Valid→Test2でROIの低下が小さい。汎化性能は良好。")
    
    # === ランダムベースラインとの比較 ===
    print("\n" + "=" * 70)
    print("ランダムベースラインとの比較")
    print("=" * 70)
    
    # テスト期間の全馬に均等ベットした場合のROI
    for name, tdf in [('Test1', test1_df), ('Test2', test2_df)]:
        winners = tdf[tdf['finish_position'] == 1]
        total_bet = len(tdf) * 100
        total_payout = (winners['win_odds'] * 100).sum()
        random_roi = (total_payout / total_bet * 100) if total_bet > 0 else 0
        
        print(f"\n{name} ランダム選択（全馬に均等ベット）:")
        print(f"  ベット数: {len(tdf):,}")
        print(f"  的中数: {len(winners)}")
        print(f"  ROI: {random_roi:.1f}%")
    
    # === 結論 ===
    print("\n" + "=" * 70)
    print("結論")
    print("=" * 70)
    
    print(f"""
[分析結果]

1. 均等ベットでのROI（EV >= 1.0）:
   - Valid: {valid_roi:.1f}%
   - Test1 (2024年1-6月): {test1_roi:.1f}%
   - Test2 (2024年7月-2025年10月): {test2_roi:.1f}%

2. 過学習・リーク評価:
   - Valid → Test2 の低下: {roi_drop:.1f}ポイント
   - 評価: {"⚠️ 過学習の可能性" if roi_drop > 10 else "✅ 汎化性能良好"}

3. 目標達成状況:
   - 目標ROI: 85-90%
   - Test2 ROI: {test2_roi:.1f}%
   - 判定: {"✅ 達成" if test2_roi >= 85 else "❌ 未達成"}

[重要な発見]
- 均等ベットでの計算により、Kelly分数の影響を排除
- 完全未知のTest2期間でのROIが真の性能指標
- {"Test2でも高いROIを維持 → リークなしの可能性高" if test2_roi > 80 else "Test2でROI低下 → データリークまたは過学習の可能性"}
""")


if __name__ == '__main__':
    main()
