# -*- coding: utf-8 -*-
"""
V15 + V4.4 アンサンブル全体への軽度正則化適用検証

【目的】
1. V15 Fixedに軽度正則化を適用
2. V4.4 Residualにも軽度正則化を適用
3. アンサンブル後のROIで最終評価
4. 結果の妥当性を検証

【検証パターン】
A. 現行V15+V4.4（ベースライン）
B. V16.2 + V4.4現行
C. V16.2 + V4.4軽度正則化
D. 各モデル単体の性能比較
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
    """V15現行パラメータ"""
    return {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 5.0, 'reg_lambda': 8.0,
        'bagging_fraction': 0.6, 'bagging_freq': 3, 'feature_fraction': 0.6,
    }


def get_v162_params():
    """V16.2軽度正則化パラメータ"""
    return {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 18, 'max_depth': 3,
        'min_child_samples': 110, 'reg_alpha': 5.5, 'reg_lambda': 8.5,
        'bagging_fraction': 0.6, 'bagging_freq': 3, 'feature_fraction': 0.58,
    }


def get_v44_params():
    """V4.4現行パラメータ"""
    return {
        'objective': 'lambdarank', 'metric': 'ndcg', 'eval_at': [1, 3, 5],
        'boosting_type': 'gbdt', 'num_leaves': 20, 'max_depth': 4,
        'min_child_samples': 150, 'learning_rate': 0.04,
        'reg_alpha': 8.0, 'reg_lambda': 12.0, 'feature_fraction': 0.5,
        'bagging_fraction': 0.6, 'bagging_freq': 5, 'verbose': -1,
        'random_state': 42, 'label_gain': list(range(100))
    }


def get_v44_mild_params():
    """V4.4軽度正則化パラメータ"""
    return {
        'objective': 'lambdarank', 'metric': 'ndcg', 'eval_at': [1, 3, 5],
        'boosting_type': 'gbdt', 'num_leaves': 18, 'max_depth': 4,
        'min_child_samples': 160, 'learning_rate': 0.04,
        'reg_alpha': 9.0, 'reg_lambda': 14.0, 'feature_fraction': 0.48,
        'bagging_fraction': 0.6, 'bagging_freq': 5, 'verbose': -1,
        'random_state': 42, 'label_gain': list(range(100))
    }


def calc_roi(df, preds):
    """単勝ROI計算"""
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


def train_binary_model(train_df, valid_df, feature_cols, params):
    """Binary分類モデル訓練（V15/V16.2用）"""
    X_train = train_df[feature_cols].fillna(0)
    X_valid = valid_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] == 1).astype(int)
    y_valid = (valid_df['finish_position'] == 1).astype(int)
    
    train_ds = lgb.Dataset(X_train, y_train)
    valid_ds = lgb.Dataset(X_valid, y_valid, reference=train_ds)
    
    model = lgb.train(params, train_ds, num_boost_round=500, valid_sets=[valid_ds],
                      callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False)])
    return model


def train_ranker_model(train_df, valid_df, feature_cols, params):
    """LambdaRankモデル訓練（V4.4用）"""
    weight_1st, weight_2nd, weight_3rd = 12.74, 6.73, 3.69
    
    def prepare_target(d):
        d = d.copy()
        odds = d['win_odds'].fillna(1.0).clip(upper=90)
        log_odds = np.log1p(odds)
        gain = np.zeros(len(d))
        gain[d['finish_position'] == 1] = log_odds[d['finish_position'] == 1] * weight_1st
        gain[d['finish_position'] == 2] = log_odds[d['finish_position'] == 2] * weight_2nd
        gain[d['finish_position'] == 3] = log_odds[d['finish_position'] == 3] * weight_3rd
        d['target_relevance'] = gain.astype(int)
        d['sample_weight'] = np.log1p(d['win_odds'].fillna(1.0)).clip(upper=np.log1p(100))
        return d
    
    train_df = prepare_target(train_df)
    valid_df = prepare_target(valid_df)
    
    X_train = train_df[feature_cols].fillna(0)
    X_valid = valid_df[feature_cols].fillna(0)
    
    groups_train = train_df.groupby('race_id').size().to_list()
    groups_valid = valid_df.groupby('race_id').size().to_list()
    
    model = lgb.LGBMRanker(**params, n_estimators=500)
    model.fit(X_train, train_df['target_relevance'], group=groups_train,
              sample_weight=train_df['sample_weight'],
              eval_set=[(X_valid, valid_df['target_relevance'])],
              eval_group=[groups_valid],
              callbacks=[lgb.early_stopping(stopping_rounds=20, verbose=False)])
    return model


def normalize_preds(preds):
    """予測値を0-1に正規化"""
    return (preds - preds.min()) / (preds.max() - preds.min() + 1e-8)


def main():
    from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
    
    print("=" * 80)
    print("V15 + V4.4 アンサンブル全体への軽度正則化適用検証")
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
        
        # 特徴量生成
        engine = LeakFreeFeatureEngineerV15Fixed()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        valid_f = engine.transform(valid)
        test_f = engine.transform(test)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        print(f"  特徴量数: {len(feature_cols)}")
        
        X_test = test_f[feature_cols].fillna(0)
        X_train = train_f[feature_cols].fillna(0)
        
        # ===== モデル訓練 =====
        print("\n  モデル訓練中...")
        
        # V15現行
        model_v15 = train_binary_model(train_f, valid_f, feature_cols, get_v15_params())
        preds_v15 = model_v15.predict(X_test)
        
        # V16.2（軽度正則化）
        model_v162 = train_binary_model(train_f, valid_f, feature_cols, get_v162_params())
        preds_v162 = model_v162.predict(X_test)
        
        # V4.4現行
        model_v44 = train_ranker_model(train_f.copy(), valid_f.copy(), feature_cols, get_v44_params())
        preds_v44 = model_v44.predict(X_test)
        
        # V4.4軽度正則化
        model_v44_mild = train_ranker_model(train_f.copy(), valid_f.copy(), feature_cols, get_v44_mild_params())
        preds_v44_mild = model_v44_mild.predict(X_test)
        
        # ===== 単体ROI =====
        roi_v15, hit_v15 = calc_roi(test_f, preds_v15)
        roi_v162, hit_v162 = calc_roi(test_f, preds_v162)
        roi_v44, hit_v44 = calc_roi(test_f, preds_v44)
        roi_v44_mild, hit_v44_mild = calc_roi(test_f, preds_v44_mild)
        
        # ===== アンサンブル =====
        # 正規化
        preds_v15_n = normalize_preds(preds_v15)
        preds_v162_n = normalize_preds(preds_v162)
        preds_v44_n = normalize_preds(preds_v44)
        preds_v44_mild_n = normalize_preds(preds_v44_mild)
        
        # A. 現行アンサンブル (V15 + V4.4)
        preds_ens_a = (preds_v15_n + preds_v44_n) / 2
        roi_ens_a, hit_ens_a = calc_roi(test_f, preds_ens_a)
        
        # B. V16.2 + V4.4現行
        preds_ens_b = (preds_v162_n + preds_v44_n) / 2
        roi_ens_b, hit_ens_b = calc_roi(test_f, preds_ens_b)
        
        # C. V16.2 + V4.4軽度正則化
        preds_ens_c = (preds_v162_n + preds_v44_mild_n) / 2
        roi_ens_c, hit_ens_c = calc_roi(test_f, preds_ens_c)
        
        # Train ROI（過学習チェック）
        train_preds_v15 = model_v15.predict(X_train)
        train_preds_v162 = model_v162.predict(X_train)
        train_preds_v44 = model_v44.predict(X_train)
        train_preds_v44_mild = model_v44_mild.predict(X_train)
        
        train_roi_v15, _ = calc_roi(train_f, train_preds_v15)
        train_roi_v162, _ = calc_roi(train_f, train_preds_v162)
        train_roi_v44, _ = calc_roi(train_f, train_preds_v44)
        train_roi_v44_mild, _ = calc_roi(train_f, train_preds_v44_mild)
        
        gap_v15 = train_roi_v15 - roi_v15
        gap_v162 = train_roi_v162 - roi_v162
        gap_v44 = train_roi_v44 - roi_v44
        gap_v44_mild = train_roi_v44_mild - roi_v44_mild
        
        # アンサンブルのGap
        train_preds_ens_a = (normalize_preds(train_preds_v15) + normalize_preds(train_preds_v44)) / 2
        train_preds_ens_c = (normalize_preds(train_preds_v162) + normalize_preds(train_preds_v44_mild)) / 2
        train_roi_ens_a, _ = calc_roi(train_f, train_preds_ens_a)
        train_roi_ens_c, _ = calc_roi(train_f, train_preds_ens_c)
        gap_ens_a = train_roi_ens_a - roi_ens_a
        gap_ens_c = train_roi_ens_c - roi_ens_c
        
        print(f"\n  【単体モデル結果】")
        print(f"    ┌─────────────────┬──────────┬─────────┬──────────┐")
        print(f"    │     モデル       │ Test ROI │ 的中率  │ Gap      │")
        print(f"    ├─────────────────┼──────────┼─────────┼──────────┤")
        print(f"    │ V15 現行        │  {roi_v15:>6.1f}% │ {hit_v15:>5.1f}% │ {gap_v15:>6.1f}% │")
        print(f"    │ V16.2 軽度正則化│  {roi_v162:>6.1f}% │ {hit_v162:>5.1f}% │ {gap_v162:>6.1f}% │")
        print(f"    │ V4.4 現行       │  {roi_v44:>6.1f}% │ {hit_v44:>5.1f}% │ {gap_v44:>6.1f}% │")
        print(f"    │ V4.4 軽度正則化 │  {roi_v44_mild:>6.1f}% │ {hit_v44_mild:>5.1f}% │ {gap_v44_mild:>6.1f}% │")
        print(f"    └─────────────────┴──────────┴─────────┴──────────┘")
        
        print(f"\n  【アンサンブル結果】")
        print(f"    ┌──────────────────────────┬──────────┬─────────┬──────────┐")
        print(f"    │      アンサンブル         │ Test ROI │ 的中率  │ Gap      │")
        print(f"    ├──────────────────────────┼──────────┼─────────┼──────────┤")
        print(f"    │ A. V15+V4.4 現行         │  {roi_ens_a:>6.1f}% │ {hit_ens_a:>5.1f}% │ {gap_ens_a:>6.1f}% │")
        print(f"    │ B. V16.2+V4.4 現行       │  {roi_ens_b:>6.1f}% │ {hit_ens_b:>5.1f}% │    -    │")
        print(f"    │ C. V16.2+V4.4 軽度正則化 │  {roi_ens_c:>6.1f}% │ {hit_ens_c:>5.1f}% │ {gap_ens_c:>6.1f}% │")
        print(f"    └──────────────────────────┴──────────┴─────────┴──────────┘")
        
        diff_single = roi_v162 - roi_v15
        diff_ens = roi_ens_c - roi_ens_a
        print(f"    単体改善(V16.2-V15): {diff_single:+.1f}%")
        print(f"    アンサンブル改善(C-A): {diff_ens:+.1f}%")
        
        all_results.append({
            'year': year,
            'roi_v15': roi_v15, 'roi_v162': roi_v162,
            'roi_v44': roi_v44, 'roi_v44_mild': roi_v44_mild,
            'roi_ens_a': roi_ens_a, 'roi_ens_b': roi_ens_b, 'roi_ens_c': roi_ens_c,
            'gap_v15': gap_v15, 'gap_v162': gap_v162,
            'gap_v44': gap_v44, 'gap_v44_mild': gap_v44_mild,
            'gap_ens_a': gap_ens_a, 'gap_ens_c': gap_ens_c,
        })
    
    # ===== サマリー =====
    print("\n" + "=" * 80)
    print("4年間平均サマリー")
    print("=" * 80)
    
    print("\n【単体モデル】")
    print(f"  V15 現行:         ROI {np.mean([r['roi_v15'] for r in all_results]):>6.1f}%, Gap {np.mean([r['gap_v15'] for r in all_results]):>5.1f}%")
    print(f"  V16.2 軽度正則化: ROI {np.mean([r['roi_v162'] for r in all_results]):>6.1f}%, Gap {np.mean([r['gap_v162'] for r in all_results]):>5.1f}%")
    print(f"  V4.4 現行:        ROI {np.mean([r['roi_v44'] for r in all_results]):>6.1f}%, Gap {np.mean([r['gap_v44'] for r in all_results]):>5.1f}%")
    print(f"  V4.4 軽度正則化:  ROI {np.mean([r['roi_v44_mild'] for r in all_results]):>6.1f}%, Gap {np.mean([r['gap_v44_mild'] for r in all_results]):>5.1f}%")
    
    avg_ens_a = np.mean([r['roi_ens_a'] for r in all_results])
    avg_ens_b = np.mean([r['roi_ens_b'] for r in all_results])
    avg_ens_c = np.mean([r['roi_ens_c'] for r in all_results])
    avg_gap_a = np.mean([r['gap_ens_a'] for r in all_results])
    avg_gap_c = np.mean([r['gap_ens_c'] for r in all_results])
    
    print("\n【アンサンブル】")
    print(f"  A. V15+V4.4 現行:          ROI {avg_ens_a:>6.1f}%, Gap {avg_gap_a:>5.1f}%")
    print(f"  B. V16.2+V4.4 現行:        ROI {avg_ens_b:>6.1f}%")
    print(f"  C. V16.2+V4.4 軽度正則化:  ROI {avg_ens_c:>6.1f}%, Gap {avg_gap_c:>5.1f}%")
    
    print("\n" + "=" * 80)
    print("結論")
    print("=" * 80)
    
    diff_c_a = avg_ens_c - avg_ens_a
    if diff_c_a > 0:
        print(f"\n  ✅ 軽度正則化アンサンブル(C)が現行(A)より改善: +{diff_c_a:.1f}%")
    elif diff_c_a > -1:
        print(f"\n  ⚪ 軽度正則化アンサンブル(C)と現行(A)はほぼ同等: {diff_c_a:+.1f}%")
    else:
        print(f"\n  ❌ 軽度正則化アンサンブル(C)は現行(A)より悪化: {diff_c_a:.1f}%")
    
    if avg_gap_c < avg_gap_a:
        print(f"  ✅ 過学習軽減: Gap {avg_gap_c:.1f}% < {avg_gap_a:.1f}%")


if __name__ == "__main__":
    main()
