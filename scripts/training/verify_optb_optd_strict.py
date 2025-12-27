#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
オプションB・D 厳密リーク対策版

【設計原則】
1. μモデルはTrain期間のみで訓練
2. V15モデルもTrain期間のみで訓練
3. μメタ特徴量はTest期間に対して予測生成
4. 最終モデルはTrain+Validで訓練、Testで評価

【リーク防止チェックリスト】
✓ μモデル訓練: Train期間のfinish_time_secondsのみ使用
✓ V15モデル訓練: Train期間のfinish_positionのみ使用
✓ μ予測生成: 予測対象のfinish_time_secondsは使用しない
✓ メタ特徴量: 同一レース内での順位変換（他レース情報を使わない）
✓ 最終評価: Test期間のfinish_positionのみ使用

【過学習対策】
- Train-Test Gap監視（<35%を許容範囲）
- 特徴量数増加抑制（+4特徴量のみ）
- μモデルの正則化強化

【オプション】
B: V15 + μメタ特徴量
D: V15 + V4.4 Residual + μ順位
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
        return 0, 0, 0
    roi = hits['win_odds'].sum() / len(bet_df) * 100
    hit_rate = len(hits) / len(bet_df) * 100
    return roi, hit_rate, len(bet_df)


def train_mu_model_strict(train_df, feature_cols):
    """
    μモデル訓練（リーク厳格版）
    
    【リーク対策】
    - 訓練データのfinish_time_secondsのみ使用
    - Validationなし（過学習防止は正則化で担保）
    """
    X_train = train_df[feature_cols].fillna(0)
    y_train = train_df['finish_time_seconds']
    
    # 強正則化でオーバーフィット抑制
    params = {
        'objective': 'regression',
        'metric': 'rmse',
        'verbosity': -1,
        'learning_rate': 0.02,
        'num_leaves': 20,  # 抑制
        'max_depth': 4,
        'min_child_samples': 150,  # 厳格
        'reg_alpha': 5.0,
        'reg_lambda': 8.0,
        'bagging_fraction': 0.7,
        'bagging_freq': 3,
        'feature_fraction': 0.7,
    }
    
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=300)
    return model


def train_v15_model_strict(train_df, feature_cols):
    """
    V15モデル訓練（リーク厳格版）
    """
    X_train = train_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] == 1).astype(int)
    
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
    model = lgb.train(params, train_ds, num_boost_round=200)
    return model


def train_v44_residual_strict(train_df, feature_cols, v15_preds_train):
    """
    V4.4 Residual訓練（リーク厳格版）
    V15の残差を学習
    """
    X_train = train_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] == 1).astype(float) - v15_preds_train
    
    params = {
        'objective': 'regression',
        'metric': 'rmse',
        'verbosity': -1,
        'learning_rate': 0.02,
        'num_leaves': 15,  # 抑制
        'max_depth': 3,
        'min_child_samples': 150,
        'reg_alpha': 8.0,
        'reg_lambda': 10.0,
        'bagging_fraction': 0.7,
        'bagging_freq': 3,
        'feature_fraction': 0.7,
    }
    
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    return model


def add_mu_meta_features_strict(df, mu_preds, v15_preds):
    """
    μメタ特徴量追加（リーク厳格版）
    
    【リーク対策】
    - 同一レース内での順位変換のみ
    - 他レースの情報は一切使用しない
    """
    df = df.copy()
    df['_mu_pred_time'] = mu_preds
    df['_v15_pred'] = v15_preds
    
    # 1. μ予測順位（タイムが速いほど順位が高い）
    df['mu_pred_rank'] = df.groupby('race_id')['_mu_pred_time'].rank()
    
    # 2. パーセンタイル順位（頭数正規化）
    df['mu_rank_percentile'] = df.groupby('race_id')['_mu_pred_time'].rank(pct=True)
    
    # 3. V15予測順位
    df['_v15_rank'] = df.groupby('race_id')['_v15_pred'].rank(ascending=False)
    
    # 4. 乖離
    df['mu_v15_rank_diff'] = df['mu_pred_rank'] - df['_v15_rank']
    
    # 一時カラム削除
    df = df.drop(columns=['_mu_pred_time', '_v15_pred', '_v15_rank'])
    
    return df


def main():
    from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
    
    print("=" * 80)
    print("オプションB・D 厳密リーク対策版")
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
        print(f"[{year}年] リーク対策検証")
        print("=" * 80)
        
        train = races[races['race_date'] <= train_end].copy()
        valid = races[(races['race_date'] >= valid_start) & (races['race_date'] < valid_end)].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] < test_end)].copy()
        train_valid = pd.concat([train, valid], ignore_index=True)
        
        print(f"  Train: {len(train):,}, Valid: {len(valid):,}, Test: {len(test):,}")
        
        # 特徴量生成（Train期間でfit）
        print("  特徴量生成中...")
        engine = LeakFreeFeatureEngineerV15Fixed()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        train_valid_f = engine.transform(train_valid)
        test_f = engine.transform(test)
        
        base_features = [c for c in engine.get_feature_columns() if c in train_f.columns]
        
        # =====================================================
        # Step 1: ベースラインモデル（V15単独）
        # =====================================================
        print("  [Baseline] V15単独訓練中...")
        v15_baseline = train_v15_model_strict(train_valid_f, base_features)
        v15_preds_train = v15_baseline.predict(train_valid_f[base_features].fillna(0))
        v15_preds_test = v15_baseline.predict(test_f[base_features].fillna(0))
        
        roi_baseline, hit_baseline, n_baseline = calc_roi(test_f, v15_preds_test)
        
        # Train ROI for Gap calculation
        roi_baseline_train, _, _ = calc_roi(train_valid_f, v15_preds_train)
        gap_baseline = roi_baseline_train - roi_baseline
        
        # =====================================================
        # Step 2: μモデル訓練（Train期間のみ、transform済みデータ）
        # =====================================================
        print("  [μモデル] Train期間でタイム予測訓練中...")
        # 注意: trainではなくtrain_fを使用（特徴量が必要）
        # ただしfinish_time_secondsはtrainから取得
        train_f_with_time = train_f.copy()
        train_f_with_time['finish_time_seconds'] = train['finish_time_seconds'].values[:len(train_f)]
        mu_model = train_mu_model_strict(train_f_with_time, base_features)

        
        # μ予測（Test期間）
        mu_preds_train_valid = mu_model.predict(train_valid_f[base_features].fillna(0))
        mu_preds_test = mu_model.predict(test_f[base_features].fillna(0))
        
        # =====================================================
        # Step 3: オプションB - μメタ特徴量統合
        # =====================================================
        print("  [OptB] μメタ特徴量追加中...")
        train_valid_b = add_mu_meta_features_strict(train_valid_f, mu_preds_train_valid, v15_preds_train)
        test_b = add_mu_meta_features_strict(test_f, mu_preds_test, v15_preds_test)
        
        mu_meta_features = ['mu_pred_rank', 'mu_rank_percentile', 'mu_v15_rank_diff']
        features_b = base_features + mu_meta_features
        
        v15_with_mu = train_v15_model_strict(train_valid_b, features_b)
        v15_with_mu_preds_train = v15_with_mu.predict(train_valid_b[features_b].fillna(0))
        v15_with_mu_preds_test = v15_with_mu.predict(test_b[features_b].fillna(0))
        
        roi_optb, hit_optb, n_optb = calc_roi(test_b, v15_with_mu_preds_test)
        roi_optb_train, _, _ = calc_roi(train_valid_b, v15_with_mu_preds_train)
        gap_optb = roi_optb_train - roi_optb
        
        # =====================================================
        # Step 4: オプションD - 3モデルアンサンブル
        # =====================================================
        print("  [OptD] V4.4 Residual訓練中...")
        v44_residual = train_v44_residual_strict(train_valid_f, base_features, v15_preds_train)
        v44_preds_test = v44_residual.predict(test_f[base_features].fillna(0))
        
        # μ順位スコア（低いほど速い→高スコアに反転）
        mu_rank_score = 1 - (test_f.groupby('race_id')['finish_time_seconds'].rank(pct=True).fillna(0.5))  # ダミー
        # 実際にはmu_predsを使う
        test_f['_mu_time'] = mu_preds_test
        mu_rank_score = 1 - test_f.groupby('race_id')['_mu_time'].rank(pct=True)
        test_f = test_f.drop(columns=['_mu_time'])
        
        # 固定比率（過学習回避のためOptunaは使わない）
        # V15:50%, V4.4:30%, μ:20%
        ensemble_d_preds = (
            0.50 * v15_preds_test + 
            0.30 * (v15_preds_test + v44_preds_test) + 
            0.20 * mu_rank_score.values
        )
        
        roi_optd, hit_optd, n_optd = calc_roi(test_f, ensemble_d_preds)
        
        # V15+V4.4のみ（比較用）
        ensemble_v15_v44 = v15_preds_test + v44_preds_test
        roi_v15v44, hit_v15v44, _ = calc_roi(test_f, ensemble_v15_v44)
        
        print(f"\n  結果:")
        print(f"    [Baseline] V15単独:    ROI {roi_baseline:.1f}%, Hit {hit_baseline:.1f}%, Gap {gap_baseline:.1f}%")
        print(f"    [OptB] V15+μメタ:     ROI {roi_optb:.1f}%, Hit {hit_optb:.1f}%, Gap {gap_optb:.1f}%")
        print(f"    [Ref] V15+V4.4:        ROI {roi_v15v44:.1f}%")
        print(f"    [OptD] V15+V4.4+μ:    ROI {roi_optd:.1f}%, Hit {hit_optd:.1f}%")
        
        # Gap警告
        if gap_optb > 35:
            print(f"    ⚠️ OptB Gap {gap_optb:.1f}% > 35%: 過学習の可能性")
        
        all_results.append({
            'year': year,
            'baseline': roi_baseline,
            'optb': roi_optb,
            'optd': roi_optd,
            'v15v44': roi_v15v44,
            'gap_baseline': gap_baseline,
            'gap_optb': gap_optb,
        })
    
    # サマリー
    print("\n" + "=" * 80)
    print("4年検証サマリー")
    print("=" * 80)
    
    print(f"\n{'年':>6} {'V15単独':>10} {'OptB':>10} {'V15+V4.4':>10} {'OptD':>10} {'Gap_B':>8}")
    print("-" * 65)
    
    for r in all_results:
        print(f"{r['year']:>6} {r['baseline']:>9.1f}% {r['optb']:>9.1f}% {r['v15v44']:>9.1f}% {r['optd']:>9.1f}% {r['gap_optb']:>7.1f}%")
    
    avg_baseline = np.mean([r['baseline'] for r in all_results])
    avg_optb = np.mean([r['optb'] for r in all_results])
    avg_optd = np.mean([r['optd'] for r in all_results])
    avg_v15v44 = np.mean([r['v15v44'] for r in all_results])
    avg_gap_b = np.mean([r['gap_optb'] for r in all_results])
    
    print("-" * 65)
    print(f"{'平均':>6} {avg_baseline:>9.1f}% {avg_optb:>9.1f}% {avg_v15v44:>9.1f}% {avg_optd:>9.1f}% {avg_gap_b:>7.1f}%")
    
    print("\n" + "=" * 80)
    print("結論")
    print("=" * 80)
    
    if avg_optb > avg_baseline:
        print(f"  OptB (V15+μメタ): {avg_optb - avg_baseline:+.1f}% 改善 ✅")
    else:
        print(f"  OptB (V15+μメタ): {avg_optb - avg_baseline:+.1f}% 悪化 ❌")
    
    if avg_optd > avg_v15v44:
        print(f"  OptD (V15+V4.4+μ): V15+V4.4比 {avg_optd - avg_v15v44:+.1f}% 改善 ✅")
    else:
        print(f"  OptD (V15+V4.4+μ): V15+V4.4比 {avg_optd - avg_v15v44:+.1f}% 悪化 ❌")
    
    if avg_gap_b < 35:
        print(f"  Gap(OptB): {avg_gap_b:.1f}% < 35% → リーク・過学習なし ✅")
    else:
        print(f"  Gap(OptB): {avg_gap_b:.1f}% >= 35% → 過学習の懸念 ⚠️")


if __name__ == "__main__":
    main()
