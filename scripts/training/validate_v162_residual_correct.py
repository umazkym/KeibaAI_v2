# -*- coding: utf-8 -*-
"""
V16.2 + V4.4 Residual 正しい検証

【重要】
- V4.4 Residualは「V15の残差を学習」する必要がある
- 私の以前の検証はV4.4 Originalを使用していた誤り
- solve_overfitting.pyの正しい実装を参考

【検証パターン】
A. V15 Fixed + V4.4 Residual（現行推奨）
B. V16.2 + V4.4 Residual（軽度正則化適用）
C. V16.2 + V4.4 Residual軽度正則化（両方に適用）

【リーク・過適合対策】
- 4年間Walk-forward検証
- Train-Test Gap監視
- 年別ばらつき確認
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
        'min_child_samples': 100, 'reg_alpha': 3.0, 'reg_lambda': 5.0,
        'bagging_fraction': 0.7, 'bagging_freq': 3, 'feature_fraction': 0.7,
    }


def get_v162_params():
    """V16.2軽度正則化パラメータ"""
    return {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 18, 'max_depth': 3,
        'min_child_samples': 110, 'reg_alpha': 3.5, 'reg_lambda': 5.5,  # solve_overfitting.pyに合わせて調整
        'bagging_fraction': 0.7, 'bagging_freq': 3, 'feature_fraction': 0.65,
    }


def get_v44_residual_params():
    """V4.4 Residual パラメータ（solve_overfitting.pyと同じ）"""
    return {
        'objective': 'lambdarank', 'boosting_type': 'gbdt',
        'num_leaves': 20, 'max_depth': 3, 'min_child_samples': 200,
        'learning_rate': 0.03, 'reg_alpha': 8.0, 'reg_lambda': 12.0,
        'feature_fraction': 0.5, 'bagging_fraction': 0.6, 'bagging_freq': 3,
        'verbose': -1, 'random_state': 42, 'label_gain': list(range(100))
    }


def get_v44_residual_mild_params():
    """V4.4 Residual 軽度正則化強化版"""
    return {
        'objective': 'lambdarank', 'boosting_type': 'gbdt',
        'num_leaves': 18, 'max_depth': 3, 'min_child_samples': 220,
        'learning_rate': 0.025, 'reg_alpha': 9.0, 'reg_lambda': 14.0,
        'feature_fraction': 0.45, 'bagging_fraction': 0.55, 'bagging_freq': 3,
        'verbose': -1, 'random_state': 42, 'label_gain': list(range(100))
    }


def calc_roi(df, pred_col):
    """ROI計算"""
    d = df.copy()
    d['rank'] = d.groupby('race_id')[pred_col].rank(ascending=False, method='first')
    bets = d[d['rank'] == 1]
    hits = bets[bets['finish_position'] == 1]
    if len(bets) == 0:
        return 0, 0
    roi = hits['win_odds'].sum() / len(bets) * 100
    hit_rate = len(hits) / len(bets) * 100
    return roi, hit_rate


def train_v15_model(train_df, feature_cols, params):
    """V15/V16.2 Binary分類モデル訓練"""
    X_train = train_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] == 1).astype(int)
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    return model


def train_v44_residual(train_df, feature_cols, v15_preds, params):
    """
    V4.4 Residual 訓練（正しい実装）
    
    【残差学習の仕組み】
    - V15の予測値を使い、V15が過小評価した馬に高スコアを付与
    - target = (actual - v15_pred) * log(1 + odds) * position_weight
    - 負の残差（V15が過大評価）は0にクリップ
    """
    train_df = train_df.copy()
    train_df['v15_pred'] = v15_preds
    
    # 残差計算
    actual = (train_df['finish_position'] == 1).astype(float)
    residual = actual - train_df['v15_pred']
    
    # オッズ加重
    odds = train_df['win_odds'].fillna(1.0).clip(upper=90)
    log_odds = np.log1p(odds)
    
    # 残差×オッズ加重をターゲットに
    residual_gain = np.zeros(len(train_df))
    residual_gain[train_df['finish_position'] == 1] = residual[train_df['finish_position'] == 1] * log_odds[train_df['finish_position'] == 1] * 10
    residual_gain[train_df['finish_position'] == 2] = residual[train_df['finish_position'] == 2] * log_odds[train_df['finish_position'] == 2] * 5
    residual_gain[train_df['finish_position'] == 3] = residual[train_df['finish_position'] == 3] * log_odds[train_df['finish_position'] == 3] * 2
    
    # 負の値を0にクリップ
    residual_gain = np.maximum(residual_gain, 0)
    
    train_df['target'] = residual_gain.astype(int)
    train_df = train_df.sort_values('race_id')
    groups = train_df.groupby('race_id', sort=False).size().to_list()
    
    model = lgb.LGBMRanker(**params, n_estimators=150)
    model.fit(train_df[feature_cols].fillna(0), train_df['target'], group=groups)
    
    return model


def normalize(x):
    """0-1正規化"""
    return (x - x.min()) / (x.max() - x.min() + 1e-8)


def main():
    from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
    
    print("=" * 80)
    print("V16.2 + V4.4 Residual 正しい検証")
    print("=" * 80)
    print("\n【リーク・過適合対策】")
    print("  - 4年間Walk-forward検証")
    print("  - Train-Test Gap監視")
    print("  - 年別ばらつき確認")
    
    data_dir = project_root / "keibaai/data/parsed/parquet"
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
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
        
        if len(train) < 100000 or len(test) < 10000:
            print("  ⚠️ データ不足のためスキップ")
            continue
        
        # 特徴量生成
        engine = LeakFreeFeatureEngineerV15Fixed()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        test_f = engine.transform(test)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        print(f"  特徴量数: {len(feature_cols)}")
        
        X_train = train_f[feature_cols].fillna(0)
        X_test = test_f[feature_cols].fillna(0)
        
        # ===== A. V15 Fixed + V4.4 Residual（現行推奨） =====
        print("\n  A. V15 Fixed + V4.4 Residual（現行）訓練中...")
        model_v15 = train_v15_model(train_f, feature_cols, get_v15_params())
        train_f['v15_pred'] = model_v15.predict(X_train)
        test_f['v15_pred'] = model_v15.predict(X_test)
        
        model_v44_a = train_v44_residual(train_f, feature_cols, train_f['v15_pred'], get_v44_residual_params())
        train_f['v44_a_pred'] = model_v44_a.predict(X_train)
        test_f['v44_a_pred'] = model_v44_a.predict(X_test)
        
        train_f['ens_a_pred'] = normalize(train_f['v15_pred']) * 0.5 + normalize(train_f['v44_a_pred']) * 0.5
        test_f['ens_a_pred'] = normalize(test_f['v15_pred']) * 0.5 + normalize(test_f['v44_a_pred']) * 0.5
        
        # ===== B. V16.2 + V4.4 Residual =====
        print("  B. V16.2 + V4.4 Residual 訓練中...")
        model_v162 = train_v15_model(train_f, feature_cols, get_v162_params())
        train_f['v162_pred'] = model_v162.predict(X_train)
        test_f['v162_pred'] = model_v162.predict(X_test)
        
        # V16.2の予測を基にV4.4 Residualを訓練
        model_v44_b = train_v44_residual(train_f, feature_cols, train_f['v162_pred'], get_v44_residual_params())
        train_f['v44_b_pred'] = model_v44_b.predict(X_train)
        test_f['v44_b_pred'] = model_v44_b.predict(X_test)
        
        train_f['ens_b_pred'] = normalize(train_f['v162_pred']) * 0.5 + normalize(train_f['v44_b_pred']) * 0.5
        test_f['ens_b_pred'] = normalize(test_f['v162_pred']) * 0.5 + normalize(test_f['v44_b_pred']) * 0.5
        
        # ===== C. V16.2 + V4.4 Residual軽度正則化 =====
        print("  C. V16.2 + V4.4 Residual軽度正則化 訓練中...")
        model_v44_c = train_v44_residual(train_f, feature_cols, train_f['v162_pred'], get_v44_residual_mild_params())
        train_f['v44_c_pred'] = model_v44_c.predict(X_train)
        test_f['v44_c_pred'] = model_v44_c.predict(X_test)
        
        train_f['ens_c_pred'] = normalize(train_f['v162_pred']) * 0.5 + normalize(train_f['v44_c_pred']) * 0.5
        test_f['ens_c_pred'] = normalize(test_f['v162_pred']) * 0.5 + normalize(test_f['v44_c_pred']) * 0.5
        
        # ===== ROI計算 =====
        train_roi_a, _ = calc_roi(train_f, 'ens_a_pred')
        test_roi_a, hit_a = calc_roi(test_f, 'ens_a_pred')
        gap_a = train_roi_a - test_roi_a
        
        train_roi_b, _ = calc_roi(train_f, 'ens_b_pred')
        test_roi_b, hit_b = calc_roi(test_f, 'ens_b_pred')
        gap_b = train_roi_b - test_roi_b
        
        train_roi_c, _ = calc_roi(train_f, 'ens_c_pred')
        test_roi_c, hit_c = calc_roi(test_f, 'ens_c_pred')
        gap_c = train_roi_c - test_roi_c
        
        # 単体ROI（参考）
        train_roi_v15, _ = calc_roi(train_f, 'v15_pred')
        test_roi_v15, _ = calc_roi(test_f, 'v15_pred')
        gap_v15 = train_roi_v15 - test_roi_v15
        
        train_roi_v162, _ = calc_roi(train_f, 'v162_pred')
        test_roi_v162, _ = calc_roi(test_f, 'v162_pred')
        gap_v162 = train_roi_v162 - test_roi_v162
        
        print(f"\n  【単体モデル参考】")
        print(f"    V15 Fixed:     Train={train_roi_v15:.1f}%, Test={test_roi_v15:.1f}%, Gap={gap_v15:.1f}%")
        print(f"    V16.2:         Train={train_roi_v162:.1f}%, Test={test_roi_v162:.1f}%, Gap={gap_v162:.1f}%")
        
        print(f"\n  【アンサンブル結果】")
        print(f"    ┌──────────────────────────────┬──────────┬─────────┬──────────┐")
        print(f"    │         アンサンブル          │ Test ROI │ 的中率  │ Gap      │")
        print(f"    ├──────────────────────────────┼──────────┼─────────┼──────────┤")
        print(f"    │ A. V15 + V4.4 Residual(現行) │  {test_roi_a:>6.1f}% │ {hit_a:>5.1f}% │ {gap_a:>6.1f}% │")
        print(f"    │ B. V16.2 + V4.4 Residual     │  {test_roi_b:>6.1f}% │ {hit_b:>5.1f}% │ {gap_b:>6.1f}% │")
        print(f"    │ C. V16.2 + V4.4 Res軽度正則化│  {test_roi_c:>6.1f}% │ {hit_c:>5.1f}% │ {gap_c:>6.1f}% │")
        print(f"    └──────────────────────────────┴──────────┴─────────┴──────────┘")
        
        diff_b = test_roi_b - test_roi_a
        diff_c = test_roi_c - test_roi_a
        print(f"    B-A差分: {diff_b:+.1f}%, C-A差分: {diff_c:+.1f}%")
        
        all_results.append({
            'year': test_year,
            'v15_test': test_roi_v15, 'v15_gap': gap_v15,
            'v162_test': test_roi_v162, 'v162_gap': gap_v162,
            'ens_a_test': test_roi_a, 'ens_a_gap': gap_a,
            'ens_b_test': test_roi_b, 'ens_b_gap': gap_b,
            'ens_c_test': test_roi_c, 'ens_c_gap': gap_c,
        })
    
    # ===== サマリー =====
    print("\n" + "=" * 80)
    print("4年間平均サマリー")
    print("=" * 80)
    
    print("\n【単体モデル】")
    avg_v15 = np.mean([r['v15_test'] for r in all_results])
    avg_v162 = np.mean([r['v162_test'] for r in all_results])
    avg_gap_v15 = np.mean([r['v15_gap'] for r in all_results])
    avg_gap_v162 = np.mean([r['v162_gap'] for r in all_results])
    print(f"  V15 Fixed:  ROI {avg_v15:.1f}%, Gap {avg_gap_v15:.1f}%")
    print(f"  V16.2:      ROI {avg_v162:.1f}%, Gap {avg_gap_v162:.1f}%")
    diff_single = avg_v162 - avg_v15
    print(f"  差分:       {diff_single:+.1f}%")
    
    print("\n【アンサンブル】")
    avg_a = np.mean([r['ens_a_test'] for r in all_results])
    avg_b = np.mean([r['ens_b_test'] for r in all_results])
    avg_c = np.mean([r['ens_c_test'] for r in all_results])
    avg_gap_a = np.mean([r['ens_a_gap'] for r in all_results])
    avg_gap_b = np.mean([r['ens_b_gap'] for r in all_results])
    avg_gap_c = np.mean([r['ens_c_gap'] for r in all_results])
    
    print(f"  A. V15 + V4.4 Residual(現行):  ROI {avg_a:.1f}%, Gap {avg_gap_a:.1f}%")
    print(f"  B. V16.2 + V4.4 Residual:      ROI {avg_b:.1f}%, Gap {avg_gap_b:.1f}%")
    print(f"  C. V16.2 + V4.4 Res軽度正則化: ROI {avg_c:.1f}%, Gap {avg_gap_c:.1f}%")
    
    diff_b = avg_b - avg_a
    diff_c = avg_c - avg_a
    print(f"\n  B-A差分: {diff_b:+.1f}%, C-A差分: {diff_c:+.1f}%")
    
    # 年別ばらつき
    print("\n【年別ばらつき】")
    std_a = np.std([r['ens_a_test'] for r in all_results])
    std_b = np.std([r['ens_b_test'] for r in all_results])
    std_c = np.std([r['ens_c_test'] for r in all_results])
    print(f"  A: σ={std_a:.1f}%, B: σ={std_b:.1f}%, C: σ={std_c:.1f}%")
    
    print("\n" + "=" * 80)
    print("結論")
    print("=" * 80)
    
    best = 'A'
    best_roi = avg_a
    for name, roi in [('B', avg_b), ('C', avg_c)]:
        if roi > best_roi:
            best_roi = roi
            best = name
    
    if best == 'A':
        print(f"\n  → 現行(A)が最良。V16.2への変更効果なし。")
    elif best == 'B':
        print(f"\n  ✅ B(V16.2 + V4.4 Residual)が最良 (A比 {diff_b:+.1f}%)")
    else:
        print(f"\n  ✅ C(V16.2 + V4.4 Res軽度正則化)が最良 (A比 {diff_c:+.1f}%)")
    
    # 再現性評価
    print("\n【再現性評価】")
    consistent_b = sum([1 for r in all_results if r['ens_b_test'] >= r['ens_a_test']])
    consistent_c = sum([1 for r in all_results if r['ens_c_test'] >= r['ens_a_test']])
    print(f"  B >= A の年: {consistent_b}/4")
    print(f"  C >= A の年: {consistent_c}/4")


if __name__ == "__main__":
    main()
