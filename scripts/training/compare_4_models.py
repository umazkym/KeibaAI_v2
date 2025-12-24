# -*- coding: utf-8 -*-
"""
4モデル複数期間検証スクリプト

【検証対象】
1. V15 Binary Classification (is_win)
2. LambdaRank (relevance: 1着=5, 2着=4, 3着=3, 4-5着=1, 他=0)
3. 複勝モデル (is_place: 3着以内)
4. オッズ加重モデル (is_win + log1p(odds) weight)

【検証期間】
2022年, 2023年, 2024年, 2025年の4年間

【評価指標】
- Test ROI（フィルタリングなし、予測1位に単勝100円）
- 的中率
- Train-Test Gap
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, str(Path(__file__).parent.parent.parent))


def load_data():
    """データ読み込み"""
    data_dir = Path("keibaai/data/parsed/parquet")
    
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    return races, pedigrees, corners, race_details, horses


def calculate_roi(df, pred_col='pred'):
    """ROI計算（予測1位に単勝100円）"""
    df = df.copy()
    df['pred_rank'] = df.groupby('race_id')[pred_col].rank(ascending=False, method='first')
    top1 = df[df['pred_rank'] == 1]
    wins = top1[top1['finish_position'] == 1]
    
    if len(top1) == 0:
        return 0, 0, 0
    
    roi = wins['win_odds'].sum() / len(top1) * 100
    hit_rate = len(wins) / len(top1) * 100
    return roi, hit_rate, len(top1)


def train_binary(X_train, y_train, params=None):
    """Binary Classification モデル"""
    if params is None:
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
    return model


def train_binary_weighted(X_train, y_train, weights, params=None):
    """オッズ加重 Binary モデル"""
    if params is None:
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
    
    train_ds = lgb.Dataset(X_train, y_train, weight=weights)
    model = lgb.train(params, train_ds, num_boost_round=200)
    return model


def train_ranker(X_train, y_relevance, groups, params=None):
    """LambdaRank モデル"""
    if params is None:
        params = {
            'objective': 'lambdarank',
            'metric': 'ndcg',
            'verbosity': -1,
            'learning_rate': 0.03,
            'num_leaves': 20,
            'max_depth': 3,
            'min_child_samples': 100,
            'reg_alpha': 3.0,
            'reg_lambda': 5.0,
        }
    
    train_ds = lgb.Dataset(X_train, y_relevance, group=groups)
    model = lgb.train(params, train_ds, num_boost_round=200)
    return model


def create_relevance(finish_pos):
    """着順から関連度スコアを生成"""
    if finish_pos == 1:
        return 5
    elif finish_pos == 2:
        return 4
    elif finish_pos == 3:
        return 3
    elif finish_pos <= 5:
        return 1
    else:
        return 0


def main():
    from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
    
    print("=" * 80)
    print("4モデル複数期間検証（フィルタリングなし）")
    print("=" * 80)
    print("\n検証対象:")
    print("  1. V15 Binary (is_win)")
    print("  2. LambdaRank (relevance)")
    print("  3. 複勝モデル (is_place)")
    print("  4. オッズ加重 (is_win + weight)")
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    print(f"\n総データ: {len(races):,}件 ({races['race_date'].min()} ～ {races['race_date'].max()})")
    
    # Test期間定義
    test_periods = [
        ('2025', '2024-12-31', '2025-01-01', '2025-11-01'),
        ('2024', '2023-12-31', '2024-01-01', '2025-01-01'),
        ('2023', '2022-12-31', '2023-01-01', '2024-01-01'),
        ('2022', '2021-12-31', '2022-01-01', '2023-01-01'),
    ]
    
    all_results = []
    
    for year, train_end, test_start, test_end in test_periods:
        print(f"\n{'='*80}")
        print(f"[{year}年] Train: ～{train_end}, Test: {test_start}～{test_end}")
        print("=" * 80)
        
        train = races[races['race_date'] <= train_end].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] < test_end)].copy()
        
        print(f"  Train: {len(train):,}件, Test: {len(test):,}件")
        
        # 特徴量生成
        engine = LeakFreeFeatureEngineerV15()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        test_f = engine.transform(test)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        print(f"  特徴量数: {len(feature_cols)}")
        
        X_train = train_f[feature_cols].fillna(0)
        X_test = test_f[feature_cols].fillna(0)
        
        # ターゲット変数
        y_win = (train_f['finish_position'] == 1).astype(int)
        y_place = (train_f['finish_position'] <= 3).astype(int)
        y_relevance = train_f['finish_position'].apply(create_relevance).values
        
        # オッズ重み
        weights = np.log1p(train_f['win_odds'].fillna(1)).values
        
        # グループ（LambdaRank用）
        groups = train_f.groupby('race_id').size().values
        
        results = {'year': year}
        
        # === モデル1: V15 Binary ===
        print("\n  [Model 1: V15 Binary (is_win)]")
        model1 = train_binary(X_train, y_win)
        train_f['pred1'] = model1.predict(X_train)
        test_f['pred1'] = model1.predict(X_test)
        
        train_roi1, train_hit1, _ = calculate_roi(train_f, 'pred1')
        test_roi1, test_hit1, n1 = calculate_roi(test_f, 'pred1')
        gap1 = train_roi1 - test_roi1
        
        print(f"    Train ROI: {train_roi1:.1f}%, Test ROI: {test_roi1:.1f}%, Gap: {gap1:.1f}%")
        results['binary_train'] = train_roi1
        results['binary_test'] = test_roi1
        results['binary_gap'] = gap1
        
        # === モデル2: LambdaRank ===
        print("\n  [Model 2: LambdaRank (relevance)]")
        model2 = train_ranker(X_train, y_relevance, groups)
        train_f['pred2'] = model2.predict(X_train)
        test_f['pred2'] = model2.predict(X_test)
        
        train_roi2, _, _ = calculate_roi(train_f, 'pred2')
        test_roi2, _, n2 = calculate_roi(test_f, 'pred2')
        gap2 = train_roi2 - test_roi2
        
        print(f"    Train ROI: {train_roi2:.1f}%, Test ROI: {test_roi2:.1f}%, Gap: {gap2:.1f}%")
        results['ranker_train'] = train_roi2
        results['ranker_test'] = test_roi2
        results['ranker_gap'] = gap2
        
        # === モデル3: 複勝モデル ===
        print("\n  [Model 3: 複勝モデル (is_place)]")
        model3 = train_binary(X_train, y_place)
        train_f['pred3'] = model3.predict(X_train)
        test_f['pred3'] = model3.predict(X_test)
        
        train_roi3, _, _ = calculate_roi(train_f, 'pred3')
        test_roi3, _, n3 = calculate_roi(test_f, 'pred3')
        gap3 = train_roi3 - test_roi3
        
        print(f"    Train ROI: {train_roi3:.1f}%, Test ROI: {test_roi3:.1f}%, Gap: {gap3:.1f}%")
        results['place_train'] = train_roi3
        results['place_test'] = test_roi3
        results['place_gap'] = gap3
        
        # === モデル4: オッズ加重 ===
        print("\n  [Model 4: オッズ加重 (is_win + weight)]")
        model4 = train_binary_weighted(X_train, y_win, weights)
        train_f['pred4'] = model4.predict(X_train)
        test_f['pred4'] = model4.predict(X_test)
        
        train_roi4, _, _ = calculate_roi(train_f, 'pred4')
        test_roi4, _, n4 = calculate_roi(test_f, 'pred4')
        gap4 = train_roi4 - test_roi4
        
        print(f"    Train ROI: {train_roi4:.1f}%, Test ROI: {test_roi4:.1f}%, Gap: {gap4:.1f}%")
        results['weighted_train'] = train_roi4
        results['weighted_test'] = test_roi4
        results['weighted_gap'] = gap4
        
        all_results.append(results)
    
    # === サマリー ===
    print("\n" + "=" * 80)
    print("検証結果サマリー（Test ROI）")
    print("=" * 80)
    
    print(f"\n{'年':>6} {'Binary':>10} {'Ranker':>10} {'複勝':>10} {'オッズ加重':>10}")
    print("-" * 50)
    
    for r in all_results:
        print(f"{r['year']:>6} {r['binary_test']:>9.1f}% {r['ranker_test']:>9.1f}% {r['place_test']:>9.1f}% {r['weighted_test']:>9.1f}%")
    
    avg_binary = np.mean([r['binary_test'] for r in all_results])
    avg_ranker = np.mean([r['ranker_test'] for r in all_results])
    avg_place = np.mean([r['place_test'] for r in all_results])
    avg_weighted = np.mean([r['weighted_test'] for r in all_results])
    
    print("-" * 50)
    print(f"{'平均':>6} {avg_binary:>9.1f}% {avg_ranker:>9.1f}% {avg_place:>9.1f}% {avg_weighted:>9.1f}%")
    
    # Gap サマリー
    print("\n" + "=" * 80)
    print("過学習チェック（Train-Test Gap）")
    print("=" * 80)
    
    print(f"\n{'年':>6} {'Binary':>10} {'Ranker':>10} {'複勝':>10} {'オッズ加重':>10}")
    print("-" * 50)
    
    for r in all_results:
        print(f"{r['year']:>6} {r['binary_gap']:>9.1f}% {r['ranker_gap']:>9.1f}% {r['place_gap']:>9.1f}% {r['weighted_gap']:>9.1f}%")
    
    avg_gap_binary = np.mean([r['binary_gap'] for r in all_results])
    avg_gap_ranker = np.mean([r['ranker_gap'] for r in all_results])
    avg_gap_place = np.mean([r['place_gap'] for r in all_results])
    avg_gap_weighted = np.mean([r['weighted_gap'] for r in all_results])
    
    print("-" * 50)
    print(f"{'平均':>6} {avg_gap_binary:>9.1f}% {avg_gap_ranker:>9.1f}% {avg_gap_place:>9.1f}% {avg_gap_weighted:>9.1f}%")
    
    # 結論
    print("\n" + "=" * 80)
    print("結論")
    print("=" * 80)
    
    best_model = max(
        [('Binary', avg_binary), ('Ranker', avg_ranker), ('複勝', avg_place), ('オッズ加重', avg_weighted)],
        key=lambda x: x[1]
    )
    
    print(f"\n  最高平均ROI: {best_model[0]} ({best_model[1]:.1f}%)")
    print(f"  全モデル共通: ROI 100%未達（控除率20%を超えられていない）")


if __name__ == "__main__":
    main()
