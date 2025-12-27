#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
μモデル・メタ特徴量検証

【コンセプト】
μモデルの予測タイムを「順位」に変換してメタ特徴量として活用。
これにより：
1. レースペース誤差の影響を排除
2. 保守的予測バイアスを軽減
3. V15との乖離を新たな情報源として活用

【メタ特徴量】
- mu_pred_rank: μモデルによる予測順位（レース内）
- mu_rank_percentile: パーセンタイル順位（頭数正規化）
- mu_v15_rank_diff: μ予測順位とV15予測順位の乖離
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


def calc_roi(df, preds):
    d = df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    bet_df = d[d['rank_pred'] == 1]
    hits = bet_df[bet_df['finish_position'] == 1]
    if len(bet_df) == 0:
        return 0, 0
    roi = hits['win_odds'].sum() / len(bet_df) * 100
    hit_rate = len(hits) / len(bet_df) * 100
    return roi, hit_rate


def train_mu_model(train_df, valid_df, feature_cols):
    """μモデル（タイム予測）を訓練"""
    X_train = train_df[feature_cols].fillna(0)
    X_valid = valid_df[feature_cols].fillna(0)
    y_train = train_df['finish_time_seconds']
    y_valid = valid_df['finish_time_seconds']
    
    params = {
        'objective': 'regression',
        'metric': 'rmse',
        'verbosity': -1,
        'learning_rate': 0.02,
        'num_leaves': 31,
        'max_depth': 4,
        'min_child_samples': 100,
        'reg_alpha': 2.0,
        'reg_lambda': 3.0,
        'bagging_fraction': 0.8,
        'bagging_freq': 3,
        'feature_fraction': 0.8,
    }
    
    train_ds = lgb.Dataset(X_train, y_train)
    valid_ds = lgb.Dataset(X_valid, y_valid, reference=train_ds)
    
    model = lgb.train(params, train_ds, num_boost_round=500, valid_sets=[valid_ds],
                      callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False)])
    return model


def train_v15_model(train_df, valid_df, feature_cols):
    """V15モデル（勝率予測）を訓練"""
    X_train = train_df[feature_cols].fillna(0)
    X_valid = valid_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] == 1).astype(int)
    y_valid = (valid_df['finish_position'] == 1).astype(int)
    
    params = {
        'objective': 'binary',
        'metric': 'auc',
        'verbosity': -1,
        'learning_rate': 0.03,
        'num_leaves': 20,
        'max_depth': 3,
        'min_child_samples': 100,
        'reg_alpha': 5.0,
        'reg_lambda': 8.0,
        'bagging_fraction': 0.7,
        'bagging_freq': 3,
        'feature_fraction': 0.7,
    }
    
    train_ds = lgb.Dataset(X_train, y_train)
    valid_ds = lgb.Dataset(X_valid, y_valid, reference=train_ds)
    
    model = lgb.train(params, train_ds, num_boost_round=500, valid_sets=[valid_ds],
                      callbacks=[lgb.early_stopping(stopping_rounds=20, verbose=False)])
    return model


def add_mu_meta_features(df, mu_preds, v15_preds):
    """μモデル予測からメタ特徴量を生成"""
    df = df.copy()
    df['_mu_pred_time'] = mu_preds
    df['_v15_pred'] = v15_preds
    
    # 1. μ予測順位（タイムが速いほど順位が高い=1に近い）
    df['mu_pred_rank'] = df.groupby('race_id')['_mu_pred_time'].rank()
    
    # 2. パーセンタイル順位（頭数正規化）
    df['mu_rank_percentile'] = df.groupby('race_id')['_mu_pred_time'].rank(pct=True)
    
    # 3. V15予測順位（スコアが高いほど順位が高い=1に近い）
    df['_v15_rank'] = df.groupby('race_id')['_v15_pred'].rank(ascending=False)
    
    # 4. μとV15の順位乖離（負=μが高評価、正=V15が高評価）
    df['mu_v15_rank_diff'] = df['mu_pred_rank'] - df['_v15_rank']
    
    # 5. 乖離の絶対値（両モデルの不一致度）
    df['mu_v15_rank_diff_abs'] = df['mu_v15_rank_diff'].abs()
    
    # 一時カラム削除
    df = df.drop(columns=['_mu_pred_time', '_v15_pred', '_v15_rank'])
    
    return df


def main():
    from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
    
    print("=" * 80)
    print("μモデル・メタ特徴量検証")
    print("=" * 80)
    
    data_dir = project_root / "keibaai/data/parsed/parquet"
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races = races[races['finish_time_seconds'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    print(f"\n総データ: {len(races):,}件")
    
    # 4年検証
    test_periods = [
        ('2025', '2023-12-31', '2024-01-01', '2024-12-31', '2025-01-01', '2025-11-01'),
        ('2024', '2022-12-31', '2023-01-01', '2023-12-31', '2024-01-01', '2025-01-01'),
        ('2023', '2021-12-31', '2022-01-01', '2022-12-31', '2023-01-01', '2024-01-01'),
        ('2022', '2020-12-31', '2021-01-01', '2021-12-31', '2022-01-01', '2023-01-01'),
    ]
    
    all_results = []
    
    for year, train_end, valid_start, valid_end, test_start, test_end in test_periods:
        print(f"\n{'='*80}")
        print(f"[{year}年]")
        print("=" * 80)
        
        train = races[races['race_date'] <= train_end].copy()
        valid = races[(races['race_date'] >= valid_start) & (races['race_date'] < valid_end)].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] < test_end)].copy()
        
        print(f"  Train: {len(train):,}, Valid: {len(valid):,}, Test: {len(test):,}")
        
        # 特徴量生成
        print("  特徴量生成中...")
        engine = LeakFreeFeatureEngineerV15Fixed()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        valid_f = engine.transform(valid)
        test_f = engine.transform(test)
        
        base_features = [c for c in engine.get_feature_columns() if c in train_f.columns]
        print(f"  ベース特徴量数: {len(base_features)}")
        
        # Step 1: μモデル訓練
        print("  μモデル訓練中...")
        mu_model = train_mu_model(train_f, valid_f, base_features)
        
        # μモデル予測
        mu_preds_train = mu_model.predict(train_f[base_features].fillna(0))
        mu_preds_valid = mu_model.predict(valid_f[base_features].fillna(0))
        mu_preds_test = mu_model.predict(test_f[base_features].fillna(0))
        
        # Step 2: V15モデル訓練（μメタ特徴量なし）
        print("  V15ベースライン訓練中...")
        v15_baseline = train_v15_model(train_f, valid_f, base_features)
        
        # V15ベースライン予測
        v15_preds_train = v15_baseline.predict(train_f[base_features].fillna(0))
        v15_preds_valid = v15_baseline.predict(valid_f[base_features].fillna(0))
        v15_preds_test = v15_baseline.predict(test_f[base_features].fillna(0))
        
        # Step 3: μメタ特徴量を追加
        print("  μメタ特徴量追加中...")
        train_f = add_mu_meta_features(train_f, mu_preds_train, v15_preds_train)
        valid_f = add_mu_meta_features(valid_f, mu_preds_valid, v15_preds_valid)
        test_f = add_mu_meta_features(test_f, mu_preds_test, v15_preds_test)
        
        # メタ特徴量リスト
        mu_meta_features = [
            'mu_pred_rank',
            'mu_rank_percentile',
            'mu_v15_rank_diff',
            'mu_v15_rank_diff_abs',
        ]
        
        all_features = base_features + mu_meta_features
        print(f"  拡張特徴量数: {len(all_features)}")
        
        # Step 4: V15 + μメタ特徴量で再訓練
        print("  V15 + μメタ特徴量訓練中...")
        v15_with_mu = train_v15_model(train_f, valid_f, all_features)
        
        # 予測
        v15_with_mu_preds_test = v15_with_mu.predict(test_f[all_features].fillna(0))
        
        # ROI計算
        roi_baseline, hit_baseline = calc_roi(test_f, v15_preds_test)
        roi_with_mu, hit_with_mu = calc_roi(test_f, v15_with_mu_preds_test)
        
        # μ予測順位の相関を確認
        from scipy.stats import spearmanr
        mu_spearman, _ = spearmanr(test_f['mu_pred_rank'], test_f['finish_position'])
        v15_spearman, _ = spearmanr(-v15_preds_test, test_f['finish_position'])
        
        print(f"\n  結果:")
        print(f"    V15ベースライン:        ROI {roi_baseline:.1f}%, Hit {hit_baseline:.1f}%")
        print(f"    V15 + μメタ特徴量:     ROI {roi_with_mu:.1f}%, Hit {hit_with_mu:.1f}%")
        print(f"    改善:                   {roi_with_mu - roi_baseline:+.1f}%")
        print(f"\n    μ予測Spearman:         {mu_spearman:.3f}")
        print(f"    V15予測Spearman:        {v15_spearman:.3f}")
        
        # 乖離分析
        test_f['_rank_diff'] = test_f['mu_v15_rank_diff']
        divergence_analysis = test_f.groupby(pd.cut(test_f['_rank_diff'], bins=[-10, -3, -1, 1, 3, 10])).apply(
            lambda x: pd.Series({
                'count': len(x),
                'hit_rate': (x['finish_position'] == 1).mean() * 100,
                'avg_odds': x[x['finish_position'] == 1]['win_odds'].mean() if (x['finish_position'] == 1).any() else 0,
            })
        )
        print(f"\n    乖離別分析 (μ-V15順位差):")
        print(f"    {'乖離':>12} | {'件数':>8} | {'的中率':>8} | {'平均配当':>10}")
        print(f"    {'-'*12}-+-{'-'*8}-+-{'-'*8}-+-{'-'*10}")
        for idx, row in divergence_analysis.iterrows():
            print(f"    {str(idx):>12} | {row['count']:>8.0f} | {row['hit_rate']:>7.1f}% | {row['avg_odds']:>10.1f}")
        
        all_results.append({
            'year': year,
            'baseline': roi_baseline,
            'with_mu': roi_with_mu,
            'diff': roi_with_mu - roi_baseline,
            'mu_spearman': mu_spearman,
            'v15_spearman': v15_spearman,
        })
    
    # サマリー
    print("\n" + "=" * 80)
    print("検証結果サマリー")
    print("=" * 80)
    
    print(f"\n{'年':>6} {'V15単独':>12} {'V15+μメタ':>12} {'改善':>10} {'μSpearman':>12}")
    print("-" * 60)
    
    for r in all_results:
        print(f"{r['year']:>6} {r['baseline']:>10.1f}% {r['with_mu']:>10.1f}% {r['diff']:>+9.1f}% {r['mu_spearman']:>12.3f}")
    
    avg_baseline = np.mean([r['baseline'] for r in all_results])
    avg_with_mu = np.mean([r['with_mu'] for r in all_results])
    avg_diff = avg_with_mu - avg_baseline
    avg_mu_spearman = np.mean([r['mu_spearman'] for r in all_results])
    
    print("-" * 60)
    print(f"{'平均':>6} {avg_baseline:>10.1f}% {avg_with_mu:>10.1f}% {avg_diff:>+9.1f}% {avg_mu_spearman:>12.3f}")
    
    print("\n" + "=" * 80)
    if avg_diff > 0:
        print(f"結論: μメタ特徴量は有効！ 平均 {avg_diff:+.1f}% 改善")
    else:
        print(f"結論: μメタ特徴量は効果なし（{avg_diff:+.1f}%）")
    print("=" * 80)


if __name__ == "__main__":
    main()
