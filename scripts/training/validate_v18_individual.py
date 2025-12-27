# -*- coding: utf-8 -*-
"""
アプローチ1: 個別分離検証

V17で同時適用した改善を1つずつ個別に検証し、
どれが効果があり、どれが悪影響かを特定する。

【検証パターン】
A. 現行（ベースライン）
B. 季節sin/cosのみ（2特徴量追加）
C. 馬の競馬場別勝率のみ（1特徴量追加）

【過学習防止】
- 追加特徴量を最小限（1-2個）に抑える
- 各改善を完全に分離して検証
- Gap 30%超は却下
"""

import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
import warnings
warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))


def get_v15_params():
    return {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 3.0, 'reg_lambda': 5.0,
        'bagging_fraction': 0.7, 'bagging_freq': 3, 'feature_fraction': 0.7,
    }


def get_v44_residual_params():
    return {
        'objective': 'lambdarank', 'boosting_type': 'gbdt',
        'num_leaves': 20, 'max_depth': 3, 'min_child_samples': 200,
        'learning_rate': 0.03, 'reg_alpha': 8.0, 'reg_lambda': 12.0,
        'feature_fraction': 0.5, 'bagging_fraction': 0.6, 'bagging_freq': 3,
        'verbose': -1, 'random_state': 42, 'label_gain': list(range(100))
    }


def add_season_features(df):
    """季節特徴量のみ追加（2特徴量）"""
    df = df.copy()
    df['month'] = df['race_date'].dt.month
    df['season_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['season_cos'] = np.cos(2 * np.pi * df['month'] / 12)
    return df, ['season_sin', 'season_cos']


def add_venue_features(df, races_raw):
    """馬の競馬場別勝率のみ追加（1特徴量、shift(1)適用）"""
    df = df.copy()
    
    races_sorted = races_raw.sort_values(['horse_id', 'race_date'])
    races_sorted['is_win'] = (races_sorted['finish_position'] == 1).astype(int)
    
    # 馬の競馬場別過去勝率（shift(1)で過去のみ参照）
    races_sorted['venue_wins_cum'] = (
        races_sorted.groupby(['horse_id', 'venue'])['is_win']
        .apply(lambda x: x.cumsum().shift(1))
        .reset_index(level=[0, 1], drop=True)
    )
    races_sorted['venue_races_cum'] = (
        races_sorted.groupby(['horse_id', 'venue'])
        .cumcount()
    )
    races_sorted['horse_venue_win_rate'] = (
        races_sorted['venue_wins_cum'] / races_sorted['venue_races_cum'].replace(0, np.nan)
    ).fillna(0)
    
    venue_stats = races_sorted[['race_id', 'horse_id', 'horse_venue_win_rate']].drop_duplicates()
    df = df.merge(venue_stats, on=['race_id', 'horse_id'], how='left')
    df['horse_venue_win_rate'] = df['horse_venue_win_rate'].fillna(0)
    
    return df, ['horse_venue_win_rate']


def train_v15_model(train_df, feature_cols, params):
    X_train = train_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] == 1).astype(int)
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    return model


def train_v44_residual(train_df, feature_cols, v15_preds, params):
    train_df = train_df.copy()
    train_df['v15_pred'] = v15_preds
    
    actual = (train_df['finish_position'] == 1).astype(float)
    residual = actual - train_df['v15_pred']
    odds = train_df['win_odds'].fillna(1.0).clip(upper=90)
    log_odds = np.log1p(odds)
    
    residual_gain = np.zeros(len(train_df))
    residual_gain[train_df['finish_position'] == 1] = residual[train_df['finish_position'] == 1] * log_odds[train_df['finish_position'] == 1] * 10
    residual_gain[train_df['finish_position'] == 2] = residual[train_df['finish_position'] == 2] * log_odds[train_df['finish_position'] == 2] * 5
    residual_gain[train_df['finish_position'] == 3] = residual[train_df['finish_position'] == 3] * log_odds[train_df['finish_position'] == 3] * 2
    residual_gain = np.clip(np.maximum(residual_gain, 0), 0, 99)
    
    train_df['target'] = residual_gain.astype(int)
    train_df = train_df.sort_values('race_id')
    groups = train_df.groupby('race_id', sort=False).size().to_list()
    
    model = lgb.LGBMRanker(**params, n_estimators=150)
    model.fit(train_df[feature_cols].fillna(0), train_df['target'], group=groups)
    
    return model


def calc_roi(df, pred_col):
    d = df.copy()
    d['rank'] = d.groupby('race_id')[pred_col].rank(ascending=False, method='first')
    bets = d[d['rank'] == 1]
    hits = bets[bets['finish_position'] == 1]
    if len(bets) == 0:
        return 0, 0
    roi = hits['win_odds'].sum() / len(bets) * 100
    hit_rate = len(hits) / len(bets) * 100
    return roi, hit_rate


def normalize(x):
    return (x - x.min()) / (x.max() - x.min() + 1e-8)


def train_and_evaluate(train_f, test_f, feature_cols, label):
    """モデル訓練と評価"""
    X_train = train_f[feature_cols].fillna(0)
    X_test = test_f[feature_cols].fillna(0)
    
    # V15訓練
    model_v15 = train_v15_model(train_f, feature_cols, get_v15_params())
    train_f[f'v15_{label}'] = model_v15.predict(X_train)
    test_f[f'v15_{label}'] = model_v15.predict(X_test)
    
    # V4.4 Residual訓練
    model_v44 = train_v44_residual(train_f, feature_cols, train_f[f'v15_{label}'], get_v44_residual_params())
    train_f[f'v44_{label}'] = model_v44.predict(X_train)
    test_f[f'v44_{label}'] = model_v44.predict(X_test)
    
    # アンサンブル
    train_f[f'ens_{label}'] = normalize(train_f[f'v15_{label}']) * 0.5 + normalize(train_f[f'v44_{label}']) * 0.5
    test_f[f'ens_{label}'] = normalize(test_f[f'v15_{label}']) * 0.5 + normalize(test_f[f'v44_{label}']) * 0.5
    
    # ROI計算
    train_roi, _ = calc_roi(train_f, f'ens_{label}')
    test_roi, hit_rate = calc_roi(test_f, f'ens_{label}')
    gap = train_roi - test_roi
    
    return train_roi, test_roi, gap, hit_rate


def main():
    from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
    
    print("=" * 80)
    print("アプローチ1: 個別分離検証")
    print("=" * 80)
    print("\n【検証パターン】")
    print("  A. 現行（ベースライン）")
    print("  B. 季節sin/cosのみ追加（2特徴量）")
    print("  C. 馬の競馬場別勝率のみ追加（1特徴量）")
    
    data_dir = project_root / "keibaai/data/parsed/parquet"
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    races_raw = races.copy()
    
    print(f"\n総データ: {len(races):,}件")
    
    test_years = [2021, 2022, 2023, 2024]
    all_results = []
    
    for test_year in test_years:
        train_end = f'{test_year - 2}-12-31'
        test_start = f'{test_year}-01-01'
        test_end = f'{test_year}-12-31'
        
        print(f"\n{'='*80}")
        print(f"Test Year: {test_year}")
        print("=" * 80)
        
        train = races[races['race_date'] <= train_end].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] <= test_end)].copy()
        
        print(f"  Train: {len(train):,}, Test: {len(test):,}")
        
        engine = LeakFreeFeatureEngineerV15Fixed()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        test_f = engine.transform(test)
        
        base_features = [c for c in engine.get_feature_columns() if c in train_f.columns]
        print(f"  基本特徴量: {len(base_features)}")
        
        results = {'year': test_year}
        
        # ===== A. 現行（ベースライン） =====
        print("\n  A. 現行（ベースライン）訓練中...")
        train_roi, test_roi, gap, hit = train_and_evaluate(train_f, test_f, base_features, 'a')
        results['A_train'], results['A_test'], results['A_gap'] = train_roi, test_roi, gap
        
        # ===== B. 季節sin/cosのみ =====
        print("  B. 季節sin/cosのみ追加中...")
        train_f_b, season_cols = add_season_features(train_f)
        test_f_b, _ = add_season_features(test_f)
        features_b = base_features + season_cols
        print(f"     追加特徴量: {season_cols}")
        
        train_roi, test_roi, gap, hit = train_and_evaluate(train_f_b, test_f_b, features_b, 'b')
        results['B_train'], results['B_test'], results['B_gap'] = train_roi, test_roi, gap
        
        # ===== C. 馬の競馬場別勝率のみ =====
        print("  C. 馬の競馬場別勝率のみ追加中...")
        train_f_c, venue_cols = add_venue_features(train_f, races_raw[races_raw['race_date'] <= train_end])
        test_f_c, _ = add_venue_features(test_f, races_raw)
        features_c = base_features + venue_cols
        print(f"     追加特徴量: {venue_cols}")
        
        train_roi, test_roi, gap, hit = train_and_evaluate(train_f_c, test_f_c, features_c, 'c')
        results['C_train'], results['C_test'], results['C_gap'] = train_roi, test_roi, gap
        
        # 結果表示
        print(f"\n  【結果】")
        print(f"    A. 現行:           Test ROI {results['A_test']:.1f}%, Gap {results['A_gap']:.1f}%")
        print(f"    B. +季節sin/cos:   Test ROI {results['B_test']:.1f}%, Gap {results['B_gap']:.1f}%, 差分 {results['B_test'] - results['A_test']:+.1f}%")
        print(f"    C. +競馬場勝率:    Test ROI {results['C_test']:.1f}%, Gap {results['C_gap']:.1f}%, 差分 {results['C_test'] - results['A_test']:+.1f}%")
        
        all_results.append(results)
    
    # サマリー
    print("\n" + "=" * 80)
    print("4年間平均サマリー")
    print("=" * 80)
    
    baseline_roi = np.mean([r['A_test'] for r in all_results])
    baseline_gap = np.mean([r['A_gap'] for r in all_results])
    
    patterns = [
        ('A. 現行', 'A'),
        ('B. +季節sin/cos', 'B'),
        ('C. +競馬場勝率', 'C'),
    ]
    
    print(f"\n{'アプローチ':25s} {'ROI':>8s} {'Gap':>8s} {'現行比':>8s}")
    print("-" * 55)
    
    for label, key in patterns:
        avg_roi = np.mean([r[f'{key}_test'] for r in all_results])
        avg_gap = np.mean([r[f'{key}_gap'] for r in all_results])
        diff = avg_roi - baseline_roi if key != 'A' else 0
        gap_status = "✅" if avg_gap <= 30 else "⚠️"
        roi_status = "↑" if diff > 0 else ("↓" if diff < 0 else "=")
        print(f"  {label:23s} {avg_roi:>7.1f}% {avg_gap:>7.1f}% {gap_status} {diff:>+7.1f}% {roi_status}")
    
    # 効果のある改善を特定
    print("\n【効果判定】")
    
    for label, key in patterns[1:]:
        avg_roi = np.mean([r[f'{key}_test'] for r in all_results])
        avg_gap = np.mean([r[f'{key}_gap'] for r in all_results])
        diff = avg_roi - baseline_roi
        
        consistent = sum([1 for r in all_results if r[f'{key}_test'] > r['A_test']])
        
        if avg_gap <= 30 and diff > 0 and consistent >= 2:
            print(f"  ✅ {label}: ROI +{diff:.1f}%, Gap {avg_gap:.1f}%, 再現性 {consistent}/4 → 採用候補")
        elif avg_gap <= 30 and diff >= -0.5:
            print(f"  △ {label}: ROI {diff:+.1f}%, Gap {avg_gap:.1f}%, 再現性 {consistent}/4 → 要検討")
        else:
            print(f"  ❌ {label}: ROI {diff:+.1f}%, Gap {avg_gap:.1f}%, 再現性 {consistent}/4 → 却下")


if __name__ == "__main__":
    main()
