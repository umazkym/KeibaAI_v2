# -*- coding: utf-8 -*-
"""
ROI 113.7% 報告値の独立検証スクリプト

【検証方針】
1. 生データ（races.parquet）を確認
2. V15モデルを年別にTrain/Test分割で学習
3. オッズ帯戦略のROIを計算
4. リーク・過学習のチェック

【リーク防止チェックポイント】
- fit()はTrain期間のデータのみ
- transform()時にTest期間の結果が統計に混入しないか
- win_oddsは評価時のみ使用（学習時はsample_weightとしても使わない）
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
import warnings
warnings.filterwarnings('ignore')

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


def load_raw_data():
    """生データを読み込み、基本統計を表示"""
    data_dir = Path("keibaai/data/parsed/parquet")
    
    print("=" * 70)
    print("Phase 1: 生データの確認")
    print("=" * 70)
    
    races = pd.read_parquet(data_dir / "races/races.parquet")
    print(f"\n[races.parquet]")
    print(f"  行数: {len(races):,}")
    print(f"  カラム数: {len(races.columns)}")
    
    # 日付変換
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    # 期間確認
    print(f"  日付範囲: {races['race_date'].min()} ～ {races['race_date'].max()}")
    
    # finish_positionの確認
    races = races[races['finish_position'].notna()].copy()
    print(f"  着順データあり: {len(races):,}")
    
    # win_oddsの確認
    valid_odds = races[races['win_odds'].notna() & (races['win_odds'] > 0)]
    print(f"  有効なオッズあり: {len(valid_odds):,}")
    
    # 年別レコード数
    print("\n  [年別レコード数]")
    for year in sorted(races['race_date'].dt.year.unique()):
        count = len(races[races['race_date'].dt.year == year])
        print(f"    {year}: {count:,}")
    
    return races


def check_for_leaks(races):
    """潜在的なリーク源を確認"""
    print("\n" + "=" * 70)
    print("Phase 2: リークチェック")
    print("=" * 70)
    
    # win_oddsとfinish_positionの関係
    print("\n[オッズと着順の関係]")
    for pos in [1, 2, 3, 5, 10]:
        mask = races['finish_position'] == pos
        if mask.sum() > 0:
            avg_odds = races.loc[mask, 'win_odds'].mean()
            print(f"  {pos}着馬の平均オッズ: {avg_odds:.2f}")
    
    # これは自然な関係（リークではない）であることを確認
    print("\n  ✓ これは自然な相関（人気馬ほど勝ちやすい）")
    print("  ✓ リークではないが、オッズを特徴量に使うのは禁止")


def train_and_evaluate(races, year, train_end, test_start, test_end):
    """指定期間でモデルを学習し評価"""
    from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
    
    # データ読み込み
    data_dir = Path("keibaai/data/parsed/parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    # Train/Test分割
    train = races[races['race_date'] <= train_end].copy()
    test = races[(races['race_date'] >= test_start) & (races['race_date'] < test_end)].copy()
    
    print(f"\n  [{year}年]")
    print(f"    Train期間: ～{train_end} ({len(train):,}件)")
    print(f"    Test期間: {test_start}～{test_end} ({len(test):,}件)")
    
    # 特徴量生成
    # ⚠️ リークチェック: fit()はTrainのみ
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    print(f"    特徴量数: {len(feature_cols)}")
    
    # リークフリー確認: finish_positionやwin_oddsが特徴量に入っていないか
    forbidden = ['finish_position', 'win_odds', 'popularity', 'finish_time_seconds', 
                 'last_3f_time', 'passing_order_1', 'passing_order_2', 'passing_order_3', 'passing_order_4']
    leaked = [f for f in feature_cols if f in forbidden]
    if leaked:
        print(f"    ⚠️ リーク検出: {leaked}")
        return None
    else:
        print(f"    ✓ リークなし（禁止特徴量は含まれていない）")
    
    # モデル学習
    X_train = train_f[feature_cols].fillna(0)
    X_test = test_f[feature_cols].fillna(0)
    y_train = (train_f['finish_position'] == 1).astype(int)
    
    params = {
        'objective': 'binary',
        'metric': 'auc',
        'verbosity': -1,
        'learning_rate': 0.03,
        'num_leaves': 20,
        'max_depth': 3,
        'min_child_samples': 100,
        'reg_alpha': 3.0,
        'reg_lambda': 5.0,
        'bagging_fraction': 0.7,
        'bagging_freq': 3,
        'feature_fraction': 0.7,
    }
    
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    
    # Train/Test予測
    train_f['pred'] = model.predict(X_train)
    test_f['pred'] = model.predict(X_test)
    
    # Train ROI計算（過学習チェック用）
    train_f['pred_rank'] = train_f.groupby('race_id')['pred'].rank(ascending=False, method='first')
    train_top1 = train_f[train_f['pred_rank'] == 1]
    train_wins = train_top1[train_top1['finish_position'] == 1]
    train_roi = train_wins['win_odds'].sum() / len(train_top1) * 100 if len(train_top1) > 0 else 0
    train_hit = len(train_wins) / len(train_top1) * 100 if len(train_top1) > 0 else 0
    
    # Test ROI計算
    test_f['pred_rank'] = test_f.groupby('race_id')['pred'].rank(ascending=False, method='first')
    test_top1 = test_f[test_f['pred_rank'] == 1]
    test_wins = test_top1[test_top1['finish_position'] == 1]
    baseline_roi = test_wins['win_odds'].sum() / len(test_top1) * 100 if len(test_top1) > 0 else 0
    baseline_hit = len(test_wins) / len(test_top1) * 100 if len(test_top1) > 0 else 0
    
    # オッズ10-50倍戦略
    odds_mask = (test_f['pred_rank'] == 1) & (test_f['win_odds'] >= 10) & (test_f['win_odds'] < 50)
    odds_bets = test_f[odds_mask]
    odds_wins = odds_bets[odds_bets['finish_position'] == 1]
    odds_roi = odds_wins['win_odds'].sum() / len(odds_bets) * 100 if len(odds_bets) > 0 else 0
    odds_hit = len(odds_wins) / len(odds_bets) * 100 if len(odds_bets) > 0 else 0
    
    # 過学習チェック
    gap = train_roi - baseline_roi
    print(f"    Train ROI: {train_roi:.1f}% (的中率: {train_hit:.1f}%)")
    print(f"    Test ROI (ベースライン): {baseline_roi:.1f}% (的中率: {baseline_hit:.1f}%)")
    print(f"    Test ROI (オッズ10-50): {odds_roi:.1f}% (的中率: {odds_hit:.1f}%, n={len(odds_bets)})")
    print(f"    Train-Test Gap: {gap:.1f}%")
    
    if gap > 40:
        print(f"    ⚠️ 過学習の可能性あり（Gap > 40%）")
    else:
        print(f"    ✓ Gap許容範囲内")
    
    return {
        'year': year,
        'train_roi': train_roi,
        'baseline_roi': baseline_roi,
        'baseline_hit': baseline_hit,
        'baseline_n': len(test_top1),
        'odds_roi': odds_roi,
        'odds_hit': odds_hit,
        'odds_n': len(odds_bets),
        'gap': gap
    }


def main():
    print("=" * 70)
    print("ROI 113.7% 報告値の独立検証")
    print("=" * 70)
    print("\n目的: 「Binary + オッズ10-50倍戦略でROI 113.7%」を再現検証")
    print("注意: リーク・過学習・過適合に厳格にチェック")
    
    # Phase 1: 生データ確認
    races = load_raw_data()
    
    # Phase 2: リークチェック
    check_for_leaks(races)
    
    # Phase 3: 年別検証
    print("\n" + "=" * 70)
    print("Phase 3: 年別ROI検証")
    print("=" * 70)
    
    test_periods = [
        ('2025', '2024-12-31', '2025-01-01', '2025-11-01'),
        ('2024', '2023-12-31', '2024-01-01', '2025-01-01'),
        ('2023', '2022-12-31', '2023-01-01', '2024-01-01'),
        ('2022', '2021-12-31', '2022-01-01', '2023-01-01'),
    ]
    
    results = []
    for year, train_end, test_start, test_end in test_periods:
        result = train_and_evaluate(races, year, train_end, test_start, test_end)
        if result:
            results.append(result)
    
    # Phase 4: サマリー
    print("\n" + "=" * 70)
    print("Phase 4: 検証結果サマリー")
    print("=" * 70)
    
    print(f"\n{'年':>6} {'ベースROI':>12} {'オッズ戦略':>12} {'改善幅':>10} {'n':>6} {'Gap':>8}")
    print("-" * 60)
    
    for r in results:
        improvement = r['odds_roi'] - r['baseline_roi']
        print(f"{r['year']:>6} {r['baseline_roi']:>11.1f}% {r['odds_roi']:>11.1f}% {improvement:>+9.1f}% {r['odds_n']:>6} {r['gap']:>7.1f}%")
    
    # 平均
    if results:
        avg_baseline = np.mean([r['baseline_roi'] for r in results])
        avg_odds = np.mean([r['odds_roi'] for r in results])
        avg_gap = np.mean([r['gap'] for r in results])
        total_n = sum([r['odds_n'] for r in results])
        
        print("-" * 60)
        print(f"{'平均':>6} {avg_baseline:>11.1f}% {avg_odds:>11.1f}% {avg_odds - avg_baseline:>+9.1f}% {total_n:>6} {avg_gap:>7.1f}%")
        
        print("\n" + "=" * 70)
        print("結論")
        print("=" * 70)
        print(f"\n  報告値: オッズ10-50倍戦略で4年平均ROI 113.7%")
        print(f"  実測値: オッズ10-50倍戦略で4年平均ROI {avg_odds:.1f}%")
        
        if abs(avg_odds - 113.7) < 5:
            print(f"\n  ✅ 報告値とほぼ一致（差: {avg_odds - 113.7:+.1f}%）")
        elif avg_odds > 100:
            print(f"\n  ⚠️ 報告値より低いが、ROI 100%超えは達成")
        else:
            print(f"\n  ❌ 報告値と大きく異なる（差: {avg_odds - 113.7:+.1f}%）")
        
        print(f"\n  平均Gap: {avg_gap:.1f}%（40%以下なら許容範囲）")


if __name__ == "__main__":
    main()
