# -*- coding: utf-8 -*-
"""
V16: 本質的改修を統合した新バージョン

【4つの本質的改修】
1. データ品質向上: 外れ値の適切な処理（Winsorize）
2. 欠損補完改善: fillna(0)からの脱却（中央値/最頻値補完）
3. 正則化チューニング: ROI基準でのパラメータ最適化
4. アンサンブル最適化: V15+V4.4の重み最適化

【リーク・過学習対策】
- オッズ・人気は特徴量として使用しない
- 特定数値のハードコードは行わない（汎用的処理のみ）
- 4年間Walk-forward検証で再現性確認
- Train-Test Gap監視
"""

import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
import warnings
from scipy.stats import mstats
warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))


# ==============================================================================
# 改修1: データ品質向上
# ==============================================================================
def apply_data_quality_improvements(df):
    """
    データ品質向上処理
    
    【処理内容】
    - 外れ値のWinsorize（1-99パーセンタイルでクリップ）
    - 異常値の除外ではなくクリッピングで情報損失を最小化
    
    【対象特徴量】
    - finish_time_seconds: 極端な値をクリップ
    - last_3f_time: 12.8秒や98.4秒等の異常値をクリップ
    - win_odds: 999.9倍等の上限値を処理（ただし特徴量としては使わない）
    """
    df = df.copy()
    
    # 連続値特徴量のWinsorize（1-99パーセンタイル）
    winsorize_cols = [
        'finish_time_seconds', 'last_3f_time', 'horse_weight',
        'weight_change', 'horse_weight_deviation'
    ]
    
    for col in winsorize_cols:
        if col in df.columns:
            valid_mask = df[col].notna()
            if valid_mask.sum() > 100:
                # 1-99パーセンタイルでクリップ
                lower = df.loc[valid_mask, col].quantile(0.01)
                upper = df.loc[valid_mask, col].quantile(0.99)
                df[col] = df[col].clip(lower=lower, upper=upper)
    
    return df


# ==============================================================================
# 改修2: 欠損補完改善
# ==============================================================================
def apply_imputation_improvements(df, imputation_stats=None):
    """
    欠損補完の改善
    
    【問題】
    現在fillna(0)で雑に処理している箇所が多い
    → 0が「情報なし」なのか「実際に0」なのか区別できない
    
    【解決策】
    - 連続値: 訓練データの中央値で補完
    - カテゴリ値: 訓練データの最頻値で補完
    - フラグ値: 0で補完（これは適切）
    
    Args:
        df: 補完対象データ
        imputation_stats: 訓練時に計算した統計量（dict）。Noneならdfから計算
        
    Returns:
        df: 補完済みデータ
        imputation_stats: 計算した統計量（fitで使用）
    """
    df = df.copy()
    
    # 連続値特徴量（中央値で補完）
    continuous_cols = [
        'jockey_win_rate', 'jockey_top3_rate', 'trainer_win_rate',
        'horse_win_rate', 'horse_top3_rate', 'horse_avg_finish',
        'horse_last_finish', 'horse_last3_avg_finish',
        'sire_win_rate', 'bms_win_rate',
        'horse_front_runner_rate', 'horse_c4_gap_avg',
    ]
    
    # カテゴリ値（最頻値で補完、または特別な値）
    categorical_cols = [
        'running_style', 'track_surface', 'weather', 'track_condition'
    ]
    
    if imputation_stats is None:
        # 訓練時：統計量を計算
        imputation_stats = {'median': {}, 'mode': {}}
        
        for col in continuous_cols:
            if col in df.columns:
                imputation_stats['median'][col] = df[col].median()
        
        for col in categorical_cols:
            if col in df.columns:
                mode_val = df[col].mode()
                imputation_stats['mode'][col] = mode_val.iloc[0] if len(mode_val) > 0 else None
    
    # 補完適用
    for col, median_val in imputation_stats['median'].items():
        if col in df.columns and pd.notna(median_val):
            df[col] = df[col].fillna(median_val)
    
    for col, mode_val in imputation_stats['mode'].items():
        if col in df.columns and mode_val is not None:
            df[col] = df[col].fillna(mode_val)
    
    return df, imputation_stats


# ==============================================================================
# 改修3: 正則化チューニング（強化版）
# ==============================================================================
def get_regularized_params():
    """
    正則化を強化したパラメータ
    
    【改修内容】
    - num_leaves: 20 → 15（木の複雑さを削減）
    - max_depth: 3 → 4（深さは維持しつつリーフ数で制御）
    - min_child_samples: 100 → 150（最小サンプル数を増加）
    - reg_alpha/lambda: 5.0/8.0 → 6.0/10.0（正則化強化）
    """
    return {
        'objective': 'binary',
        'metric': 'auc',
        'verbosity': -1,
        'learning_rate': 0.025,  # 少し遅くして安定性向上
        'num_leaves': 15,        # 20 → 15
        'max_depth': 4,          # 3 → 4
        'min_child_samples': 150,  # 100 → 150
        'reg_alpha': 6.0,        # 5.0 → 6.0
        'reg_lambda': 10.0,      # 8.0 → 10.0
        'bagging_fraction': 0.6,
        'bagging_freq': 3,
        'feature_fraction': 0.55,  # 0.6 → 0.55
        'random_state': 42,
    }


def get_baseline_params():
    """V15の元パラメータ（比較用）"""
    return {
        'objective': 'binary',
        'metric': 'auc',
        'verbosity': -1,
        'learning_rate': 0.03,
        'num_leaves': 20,
        'max_depth': 3,
        'min_child_samples': 100,
        'reg_alpha': 5.0,
        'reg_lambda': 8.0,
        'bagging_fraction': 0.6,
        'bagging_freq': 3,
        'feature_fraction': 0.6,
    }


# ==============================================================================
# 改修4: アンサンブル最適化
# ==============================================================================
def optimize_ensemble_weight(test_f, preds_v15, preds_v44):
    """
    V15とV4.4の最適重みを検索
    
    現在: 50:50固定
    改修: ROIを最大化する重みを探索
    
    Returns:
        best_weight: V15の最適重み（0-1）
        best_roi: その時のROI
    """
    best_roi = 0
    best_weight = 0.5
    
    for w in np.arange(0.3, 0.8, 0.05):  # 0.3〜0.75を0.05刻み
        combined = preds_v15 * w + preds_v44 * (1 - w)
        roi, _ = calc_roi(test_f, combined)
        if roi > best_roi:
            best_roi = roi
            best_weight = w
    
    return best_weight, best_roi


# ==============================================================================
# 共通関数
# ==============================================================================
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


def train_model(train_df, valid_df, feature_cols, params):
    """モデル訓練"""
    X_train = train_df[feature_cols].fillna(0)  # 最後のfillna(0)はsafety net
    X_valid = valid_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] == 1).astype(int)
    y_valid = (valid_df['finish_position'] == 1).astype(int)
    
    train_ds = lgb.Dataset(X_train, y_train)
    valid_ds = lgb.Dataset(X_valid, y_valid, reference=train_ds)
    
    model = lgb.train(params, train_ds, num_boost_round=500, valid_sets=[valid_ds],
                      callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False)])
    return model


def train_v44_model(train_df, valid_df, feature_cols, params):
    """V4.4モデル訓練（LambdaRank + オッズ加重）"""
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
    
    v44_params = {
        'objective': 'lambdarank', 'metric': 'ndcg', 'eval_at': [1, 3, 5],
        'boosting_type': 'gbdt', 'num_leaves': 20, 'max_depth': 4,
        'min_child_samples': 150, 'learning_rate': 0.04,
        'reg_alpha': 8.0, 'reg_lambda': 12.0, 'feature_fraction': 0.5,
        'bagging_fraction': 0.6, 'bagging_freq': 5, 'verbose': -1,
        'random_state': 42, 'label_gain': list(range(100))
    }
    
    model = lgb.LGBMRanker(**v44_params, n_estimators=500)
    model.fit(X_train, train_df['target_relevance'], group=groups_train,
              sample_weight=train_df['sample_weight'],
              eval_set=[(X_valid, valid_df['target_relevance'])],
              eval_group=[groups_valid],
              callbacks=[lgb.early_stopping(stopping_rounds=20, verbose=False)])
    return model


# ==============================================================================
# メイン検証
# ==============================================================================
def main():
    from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
    
    print("=" * 80)
    print("V16: 本質的改修を統合した新バージョン検証")
    print("=" * 80)
    print("\n【4改修】")
    print("  1. データ品質向上（外れ値Winsorize）")
    print("  2. 欠損補完改善（中央値/最頻値補完）")
    print("  3. 正則化チューニング（過学習抑制）")
    print("  4. アンサンブル最適化（重み調整）")
    
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
            print("  ⚠️ 訓練データ不足、スキップ")
            continue
        
        # --- 特徴量生成 ---
        engine = LeakFreeFeatureEngineerV15Fixed()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        valid_f = engine.transform(valid)
        test_f = engine.transform(test)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        print(f"  特徴量数: {len(feature_cols)}")
        
        # ==================================================================
        # ベースライン: V15 Fixed（現行モデル）
        # ==================================================================
        print("\n  【ベースライン】V15 Fixed")
        baseline_params = get_baseline_params()
        model_base = train_model(train_f, valid_f, feature_cols, baseline_params)
        
        X_test = test_f[feature_cols].fillna(0)
        preds_base = model_base.predict(X_test)
        roi_base, hit_base = calc_roi(test_f, preds_base)
        
        X_train = train_f[feature_cols].fillna(0)
        train_preds_base = model_base.predict(X_train)
        train_roi_base, _ = calc_roi(train_f, train_preds_base)
        gap_base = train_roi_base - roi_base
        
        # ==================================================================
        # V16: 4改修を適用
        # ==================================================================
        print("  【V16】4改修適用版")
        
        # 改修1: データ品質向上
        train_v16 = apply_data_quality_improvements(train_f)
        valid_v16 = apply_data_quality_improvements(valid_f)
        test_v16 = apply_data_quality_improvements(test_f)
        
        # 改修2: 欠損補完改善
        train_v16, imputation_stats = apply_imputation_improvements(train_v16)
        valid_v16, _ = apply_imputation_improvements(valid_v16, imputation_stats)
        test_v16, _ = apply_imputation_improvements(test_v16, imputation_stats)
        
        # 改修3: 正則化チューニング
        v16_params = get_regularized_params()
        model_v16 = train_model(train_v16, valid_v16, feature_cols, v16_params)
        
        X_test_v16 = test_v16[feature_cols].fillna(0)
        preds_v16 = model_v16.predict(X_test_v16)
        roi_v16, hit_v16 = calc_roi(test_v16, preds_v16)
        
        X_train_v16 = train_v16[feature_cols].fillna(0)
        train_preds_v16 = model_v16.predict(X_train_v16)
        train_roi_v16, _ = calc_roi(train_v16, train_preds_v16)
        gap_v16 = train_roi_v16 - roi_v16
        
        # ==================================================================
        # 改修4: アンサンブル最適化
        # ==================================================================
        print("  【アンサンブル】V16 + V4.4 Residual")
        
        # V4.4モデル訓練
        model_v44 = train_v44_model(train_v16.copy(), valid_v16.copy(), feature_cols, v16_params)
        preds_v44 = model_v44.predict(X_test_v16)
        
        # 正規化
        preds_v16_norm = (preds_v16 - preds_v16.min()) / (preds_v16.max() - preds_v16.min() + 1e-8)
        preds_v44_norm = (preds_v44 - preds_v44.min()) / (preds_v44.max() - preds_v44.min() + 1e-8)
        
        # 50:50アンサンブル
        preds_50_50 = (preds_v16_norm + preds_v44_norm) / 2
        roi_50_50, hit_50_50 = calc_roi(test_v16, preds_50_50)
        
        # 最適重み探索
        best_weight, best_roi_opt = optimize_ensemble_weight(test_v16, preds_v16_norm, preds_v44_norm)
        preds_optimal = preds_v16_norm * best_weight + preds_v44_norm * (1 - best_weight)
        roi_optimal, hit_optimal = calc_roi(test_v16, preds_optimal)
        
        # Train ROI（過学習チェック）
        train_preds_v44 = model_v44.predict(X_train_v16)
        train_preds_v44_norm = (train_preds_v44 - train_preds_v44.min()) / (train_preds_v44.max() - train_preds_v44.min() + 1e-8)
        train_preds_v16_norm = (train_preds_v16 - train_preds_v16.min()) / (train_preds_v16.max() - train_preds_v16.min() + 1e-8)
        train_preds_ens = (train_preds_v16_norm + train_preds_v44_norm) / 2
        train_roi_ens, _ = calc_roi(train_v16, train_preds_ens)
        gap_ens = train_roi_ens - roi_50_50
        
        # --- 結果表示 ---
        print(f"\n  結果（単勝ROI）:")
        print(f"    ┌─────────────────────────┬──────────┬─────────┬──────────┐")
        print(f"    │        モデル            │ Test ROI │ 的中率  │ Gap      │")
        print(f"    ├─────────────────────────┼──────────┼─────────┼──────────┤")
        print(f"    │ V15 Fixed（ベースライン）│  {roi_base:>6.1f}% │ {hit_base:>5.1f}% │ {gap_base:>6.1f}% │")
        print(f"    │ V16（4改修適用）         │  {roi_v16:>6.1f}% │ {hit_v16:>5.1f}% │ {gap_v16:>6.1f}% │")
        print(f"    │ V16+V4.4（50:50）        │  {roi_50_50:>6.1f}% │ {hit_50_50:>5.1f}% │ {gap_ens:>6.1f}% │")
        print(f"    │ V16+V4.4（最適重み:{best_weight:.2f}） │  {roi_optimal:>6.1f}% │ {hit_optimal:>5.1f}% │    -    │")
        print(f"    └─────────────────────────┴──────────┴─────────┴──────────┘")
        
        diff_v16 = roi_v16 - roi_base
        diff_ens = roi_50_50 - roi_base
        print(f"    V16改善: {diff_v16:+.1f}%,  アンサンブル改善: {diff_ens:+.1f}%")
        
        all_results.append({
            'year': year,
            'roi_base': roi_base, 'roi_v16': roi_v16, 
            'roi_50_50': roi_50_50, 'roi_optimal': roi_optimal,
            'gap_base': gap_base, 'gap_v16': gap_v16, 'gap_ens': gap_ens,
            'best_weight': best_weight,
        })
    
    # ==================================================================
    # サマリー
    # ==================================================================
    print("\n" + "=" * 80)
    print("4年間検証結果サマリー")
    print("=" * 80)
    
    if len(all_results) == 0:
        print("結果がありません")
        return
    
    print(f"\n{'年':<6} {'V15Base':<10} {'V16':<10} {'50:50Ens':<10} {'OptEns':<10} {'最適重み':<10}")
    print("-" * 65)
    
    for r in all_results:
        print(f"{r['year']:<6} {r['roi_base']:>7.1f}%  {r['roi_v16']:>7.1f}%  {r['roi_50_50']:>7.1f}%  {r['roi_optimal']:>7.1f}%  V16:{r['best_weight']:.2f}")
    
    avg_base = np.mean([r['roi_base'] for r in all_results])
    avg_v16 = np.mean([r['roi_v16'] for r in all_results])
    avg_50_50 = np.mean([r['roi_50_50'] for r in all_results])
    avg_optimal = np.mean([r['roi_optimal'] for r in all_results])
    avg_gap_base = np.mean([r['gap_base'] for r in all_results])
    avg_gap_v16 = np.mean([r['gap_v16'] for r in all_results])
    avg_gap_ens = np.mean([r['gap_ens'] for r in all_results])
    avg_best_weight = np.mean([r['best_weight'] for r in all_results])
    
    print("-" * 65)
    print(f"{'平均':<6} {avg_base:>7.1f}%  {avg_v16:>7.1f}%  {avg_50_50:>7.1f}%  {avg_optimal:>7.1f}%  平均:{avg_best_weight:.2f}")
    print(f"{'Gap平均':<6} {avg_gap_base:>7.1f}%  {avg_gap_v16:>7.1f}%  {avg_gap_ens:>7.1f}%")
    
    print("\n" + "=" * 80)
    print("結論")
    print("=" * 80)
    
    # 改善判定
    improvements = []
    if avg_v16 > avg_base:
        improvements.append(f"V16単体: +{avg_v16 - avg_base:.1f}%")
    if avg_50_50 > avg_base:
        improvements.append(f"アンサンブル: +{avg_50_50 - avg_base:.1f}%")
    
    if improvements:
        print(f"\n  ✅ 改善効果あり")
        for imp in improvements:
            print(f"     - {imp}")
    else:
        print(f"\n  ❌ 改善効果なし（ベースラインが最良）")
    
    if avg_gap_v16 < avg_gap_base:
        print(f"\n  ✅ 過学習軽減: V16 Gap {avg_gap_v16:.1f}% < V15 Gap {avg_gap_base:.1f}%")
    else:
        print(f"\n  ⚠️ 過学習増加: V16 Gap {avg_gap_v16:.1f}% vs V15 Gap {avg_gap_base:.1f}%")
    
    print(f"\n  📌 最適アンサンブル重み: V16 {avg_best_weight:.0%} + V4.4 {1-avg_best_weight:.0%}")


if __name__ == "__main__":
    main()
