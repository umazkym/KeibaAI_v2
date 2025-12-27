# -*- coding: utf-8 -*-
"""
各改修の個別効果検証

【目的】
4つの改修のうち、どれが効果的でどれが悪影響かを特定する

【検証パターン】
A. V15 Base（ベースライン）
B. V15 + 改修1(データ品質)のみ
C. V15 + 改修2(欠損補完)のみ  
D. V15 + 改修1+2（データ品質+欠損補完）
E. V15 + 改修3(正則化微調整)のみ
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


def apply_data_quality(df):
    """改修1: データ品質向上"""
    df = df.copy()
    winsorize_cols = ['finish_time_seconds', 'last_3f_time', 'horse_weight',
                      'weight_change', 'horse_weight_deviation']
    for col in winsorize_cols:
        if col in df.columns:
            valid_mask = df[col].notna()
            if valid_mask.sum() > 100:
                lower = df.loc[valid_mask, col].quantile(0.01)
                upper = df.loc[valid_mask, col].quantile(0.99)
                df[col] = df[col].clip(lower=lower, upper=upper)
    return df


def apply_imputation(df, stats=None):
    """改修2: 欠損補完"""
    df = df.copy()
    continuous_cols = [
        'jockey_win_rate', 'jockey_top3_rate', 'trainer_win_rate',
        'horse_win_rate', 'horse_top3_rate', 'horse_avg_finish',
        'horse_last_finish', 'horse_last3_avg_finish',
        'sire_win_rate', 'bms_win_rate',
    ]
    
    if stats is None:
        stats = {}
        for col in continuous_cols:
            if col in df.columns:
                stats[col] = df[col].median()
    
    for col, val in stats.items():
        if col in df.columns and pd.notna(val):
            df[col] = df[col].fillna(val)
    
    return df, stats


def get_v15_params():
    """V15パラメータ"""
    return {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 5.0, 'reg_lambda': 8.0,
        'bagging_fraction': 0.6, 'bagging_freq': 3, 'feature_fraction': 0.6,
    }


def get_mild_regularization_params():
    """改修3: 軽度の正則化強化"""
    return {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 18, 'max_depth': 3,  # 20→18
        'min_child_samples': 110, 'reg_alpha': 5.5, 'reg_lambda': 8.5,  # 微調整
        'bagging_fraction': 0.6, 'bagging_freq': 3, 'feature_fraction': 0.58,
    }


def calc_roi(df, preds):
    d = df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    top1 = d[d['rank_pred'] == 1]
    hits = top1[top1['finish_position'] == 1]
    if len(top1) == 0:
        return 0, 0
    roi = hits['win_odds'].sum() / len(top1) * 100
    hit_rate = len(hits) / len(top1) * 100
    return roi, hit_rate


def train_model(train_df, valid_df, feature_cols, params):
    X_train = train_df[feature_cols].fillna(0)
    X_valid = valid_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] == 1).astype(int)
    y_valid = (valid_df['finish_position'] == 1).astype(int)
    
    train_ds = lgb.Dataset(X_train, y_train)
    valid_ds = lgb.Dataset(X_valid, y_valid, reference=train_ds)
    
    model = lgb.train(params, train_ds, num_boost_round=500, valid_sets=[valid_ds],
                      callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False)])
    return model


def evaluate_model(model, train_f, test_f, feature_cols):
    """モデル評価"""
    X_test = test_f[feature_cols].fillna(0)
    preds = model.predict(X_test)
    roi, hit = calc_roi(test_f, preds)
    
    X_train = train_f[feature_cols].fillna(0)
    train_preds = model.predict(X_train)
    train_roi, _ = calc_roi(train_f, train_preds)
    gap = train_roi - roi
    
    return roi, hit, gap


def main():
    from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
    
    print("=" * 80)
    print("各改修の個別効果検証")
    print("=" * 80)
    
    data_dir = project_root / "keibaai/data/parsed/parquet"
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    print(f"\n総データ: {len(races):,}件")
    
    test_periods = [
        ('2024', '2022-12-31', '2023-01-01', '2023-12-31', '2024-01-01', '2025-01-01'),
        ('2023', '2021-12-31', '2022-01-01', '2022-12-31', '2023-01-01', '2024-01-01'),
        ('2022', '2020-12-31', '2021-01-01', '2021-12-31', '2022-01-01', '2023-01-01'),
        ('2021', '2019-12-31', '2020-01-01', '2020-12-31', '2021-01-01', '2022-01-01'),
    ]
    
    all_results = []
    
    for year, train_end, valid_start, valid_end, test_start, test_end in test_periods:
        print(f"\n{'='*80}")
        print(f"[{year}年テスト]")
        print("=" * 80)
        
        train = races[races['race_date'] <= train_end].copy()
        valid = races[(races['race_date'] >= valid_start) & (races['race_date'] < valid_end)].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] < test_end)].copy()
        
        print(f"  データ: Train={len(train):,}, Valid={len(valid):,}, Test={len(test):,}")
        
        if len(train) < 10000:
            continue
        
        engine = LeakFreeFeatureEngineerV15Fixed()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_base = engine.transform(train)
        valid_base = engine.transform(valid)
        test_base = engine.transform(test)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_base.columns]
        
        # === A. ベースライン (V15) ===
        model_a = train_model(train_base, valid_base, feature_cols, get_v15_params())
        roi_a, hit_a, gap_a = evaluate_model(model_a, train_base, test_base, feature_cols)
        
        # === B. 改修1のみ (データ品質) ===
        train_b = apply_data_quality(train_base)
        valid_b = apply_data_quality(valid_base)
        test_b = apply_data_quality(test_base)
        model_b = train_model(train_b, valid_b, feature_cols, get_v15_params())
        roi_b, hit_b, gap_b = evaluate_model(model_b, train_b, test_b, feature_cols)
        
        # === C. 改修2のみ (欠損補完) ===
        train_c, stats = apply_imputation(train_base)
        valid_c, _ = apply_imputation(valid_base, stats)
        test_c, _ = apply_imputation(test_base, stats)
        model_c = train_model(train_c, valid_c, feature_cols, get_v15_params())
        roi_c, hit_c, gap_c = evaluate_model(model_c, train_c, test_c, feature_cols)
        
        # === D. 改修1+2 ===
        train_d = apply_data_quality(train_base)
        valid_d = apply_data_quality(valid_base)
        test_d = apply_data_quality(test_base)
        train_d, stats = apply_imputation(train_d)
        valid_d, _ = apply_imputation(valid_d, stats)
        test_d, _ = apply_imputation(test_d, stats)
        model_d = train_model(train_d, valid_d, feature_cols, get_v15_params())
        roi_d, hit_d, gap_d = evaluate_model(model_d, train_d, test_d, feature_cols)
        
        # === E. 改修3のみ (軽度正則化) ===
        model_e = train_model(train_base, valid_base, feature_cols, get_mild_regularization_params())
        roi_e, hit_e, gap_e = evaluate_model(model_e, train_base, test_base, feature_cols)
        
        print(f"\n  結果:")
        print(f"    ┌────────────────────┬──────────┬──────────┐")
        print(f"    │       パターン      │ Test ROI │   Gap    │")
        print(f"    ├────────────────────┼──────────┼──────────┤")
        print(f"    │ A. V15ベースライン  │  {roi_a:>6.1f}% │  {gap_a:>6.1f}% │")
        print(f"    │ B. +データ品質      │  {roi_b:>6.1f}% │  {gap_b:>6.1f}% │")
        print(f"    │ C. +欠損補完        │  {roi_c:>6.1f}% │  {gap_c:>6.1f}% │")
        print(f"    │ D. +データ品質+補完 │  {roi_d:>6.1f}% │  {gap_d:>6.1f}% │")
        print(f"    │ E. +軽度正則化      │  {roi_e:>6.1f}% │  {gap_e:>6.1f}% │")
        print(f"    └────────────────────┴──────────┴──────────┘")
        
        all_results.append({
            'year': year,
            'A': roi_a, 'B': roi_b, 'C': roi_c, 'D': roi_d, 'E': roi_e,
            'gap_A': gap_a, 'gap_B': gap_b, 'gap_C': gap_c, 'gap_D': gap_d, 'gap_E': gap_e,
        })
    
    # サマリー
    print("\n" + "=" * 80)
    print("4年間平均サマリー")
    print("=" * 80)
    
    for key in ['A', 'B', 'C', 'D', 'E']:
        avg_roi = np.mean([r[key] for r in all_results])
        avg_gap = np.mean([r[f'gap_{key}'] for r in all_results])
        labels = {
            'A': 'V15ベースライン',
            'B': '+データ品質',
            'C': '+欠損補完',
            'D': '+データ品質+補完',
            'E': '+軽度正則化',
        }
        diff = avg_roi - np.mean([r['A'] for r in all_results])
        print(f"  {labels[key]:<18}: ROI {avg_roi:>6.1f}% ({diff:+.1f}%), Gap {avg_gap:>5.1f}%")
    
    # 最良の組み合わせを特定
    print("\n" + "=" * 80)
    print("結論")
    print("=" * 80)
    
    avg_A = np.mean([r['A'] for r in all_results])
    best_key = 'A'
    best_roi = avg_A
    
    for key in ['B', 'C', 'D', 'E']:
        avg = np.mean([r[key] for r in all_results])
        if avg > best_roi:
            best_roi = avg
            best_key = key
    
    labels = {'A': 'ベースライン', 'B': 'データ品質', 'C': '欠損補完', 'D': 'データ品質+補完', 'E': '軽度正則化'}
    if best_key == 'A':
        print(f"\n  → ベースライン(V15)が最良。改修効果なし。")
    else:
        diff = best_roi - avg_A
        print(f"\n  ✅ {labels[best_key]}が最良 (ROI +{diff:.1f}%)")


if __name__ == "__main__":
    main()
