#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SP値特徴量統合 全モデル網羅検証 (Phase 2)

【検証対象】
A: V15 Binary (ベースライン vs +SP_lite)
B: V4.4 LambdaRank (ベースライン vs +SP_lite)
C: Ensemble V15+V4.4 (ベースライン vs +SP_lite)

【SP_lite 特徴量（Phase 1で効果確認済み3個のみ）】
1. horse_sp_race_rank_avg  ← 全体8位/72の高重要度
2. horse_last3f_index_avg  ← 上がり3F能力の正規化版
3. horse_sp_avg            ← SP値の絶対能力値

【Walk-Forward期間】
2021〜2025年（5年間）
"""

import sys
import gc
import warnings
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
import lightgbm as lgb
import logging

warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
from keibaai.src.features.leak_free_feature_engineer_v16 import LeakFreeFeatureEngineerV16
from keibaai.src.analysis.speed_index.calculator import SpeedIndexCalculator
from keibaai.src.analysis.speed_index.pace_correction import PaceCorrectionEngine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# SP_lite: 効果確認済みの3特徴量のみ
SP_LITE_COLS = [
    'horse_sp_race_rank_avg',
    'horse_last3f_index_avg',
    'horse_sp_avg',
]

# ============================================================
# データロード
# ============================================================
def load_data():
    data_dir = project_root / "keibaai/data/parsed/parquet"
    logger.info("データ読み込み中...")
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    races['race_id'] = races['race_id'].astype(str)
    logger.info(f"  races: {len(races):,}件 ({races['race_date'].min()} ~ {races['race_date'].max()})")
    return races, pedigrees, corners, race_details, horses

# ============================================================
# SP値特徴量生成（前回と同じロジック・lite版）
# ============================================================
def compute_sp_features_lite(races_df, race_details_df, train_end_date):
    """SP_lite: 3特徴量のみ生成"""
    logger.info("  SP値を算出中...")
    train_mask = races_df['race_date'] <= pd.Timestamp(train_end_date)
    train_races = races_df[train_mask].copy()

    sp_calc = SpeedIndexCalculator(weight_coeff=0.3, use_class_offset=False,
                                   use_last3f=True, use_condition_base=True)
    sp_calc.fit(train_races)
    sp_result = sp_calc.calculate(races_df)

    rd = race_details_df.copy()
    rd['race_id'] = rd['race_id'].astype(str)
    train_rd_ids = set(train_races['race_id'].unique())
    train_rd = rd[rd['race_id'].isin(train_rd_ids)]
    if len(train_rd) > 0:
        pace_engine = PaceCorrectionEngine(leading_coeff=0.3, closing_coeff=0.2)
        pace_engine.fit(train_races, rd)
        all_rd_ids = set(rd['race_id'].unique())
        sp_with_rd = sp_result[sp_result['race_id'].isin(all_rd_ids)]
        if len(sp_with_rd) > 0:
            sp_result = pace_engine.apply(sp_result, races_df, rd)

    sp_col = 'sp_extended' if 'sp_extended' in sp_result.columns and sp_result['sp_extended'].notna().any() else 'sp_raw'
    sp_df = sp_result[['race_id', 'horse_id', 'race_date', sp_col, 'last3f_index']].copy()
    sp_df = sp_df.rename(columns={sp_col: 'sp_value'})
    sp_df['horse_id'] = sp_df['horse_id'].astype(str)
    sp_df['race_id'] = sp_df['race_id'].astype(str)
    sp_df['race_date'] = pd.to_datetime(sp_df['race_date'])
    sp_df = sp_df.dropna(subset=['sp_value'])

    sp_df['sp_rank_in_race'] = sp_df.groupby('race_id')['sp_value'].rank(ascending=False)
    sp_df['sp_field_size'] = sp_df.groupby('race_id')['sp_value'].transform('count')
    sp_df['sp_relative_rank'] = sp_df['sp_rank_in_race'] / sp_df['sp_field_size']
    sp_df = sp_df.sort_values(['horse_id', 'race_date', 'race_id'])

    # 1. horse_sp_avg
    sp_df['horse_sp_avg'] = sp_df.groupby('horse_id')['sp_value'].transform(
        lambda x: x.expanding().mean().shift(1))
    # 2. horse_last3f_index_avg
    sp_df['horse_last3f_index_avg'] = sp_df.groupby('horse_id')['last3f_index'].transform(
        lambda x: x.expanding().mean().shift(1))
    # 3. horse_sp_race_rank_avg
    sp_df['horse_sp_race_rank_avg'] = sp_df.groupby('horse_id')['sp_relative_rank'].transform(
        lambda x: x.expanding().mean().shift(1))

    # 最低3走フィルタ
    sp_df['cum_count'] = sp_df.groupby('horse_id')['sp_value'].transform(
        lambda x: x.expanding().count().shift(1))
    mask = sp_df['cum_count'] < 3
    for col in SP_LITE_COLS:
        sp_df.loc[mask, col] = np.nan

    result = sp_df[['race_id', 'horse_id'] + SP_LITE_COLS].drop_duplicates(['race_id', 'horse_id'])
    return result

# ============================================================
# ROI計算
# ============================================================
def calc_roi(df, preds, min_odds=0.0):
    d = df.copy()
    d['pred'] = preds
    d['rank'] = d.groupby('race_id')['pred'].rank(ascending=False, method='first')
    bets = d[d['rank'] == 1]
    if min_odds > 0:
        bets = bets[bets['win_odds'] >= min_odds]
    if len(bets) == 0:
        return 0, 0, 0
    hits = bets[bets['finish_position'] == 1]
    roi = hits['win_odds'].sum() / len(bets) * 100
    hit_rate = len(hits) / len(bets) * 100
    return roi, hit_rate, len(bets)

# ============================================================
# V15モデル訓練
# ============================================================
V15_PARAMS = {
    'objective': 'binary', 'metric': 'auc', 'learning_rate': 0.03,
    'max_depth': 3, 'num_leaves': 20, 'min_child_samples': 100,
    'reg_alpha': 3.0, 'reg_lambda': 5.0, 'bagging_fraction': 0.7,
    'feature_fraction': 0.7, 'bagging_freq': 3, 'verbose': -1, 'seed': 42,
}

def train_v15(train_f, test_f, feature_cols):
    X_train = train_f[feature_cols].fillna(0)
    y_train = (train_f['finish_position'] == 1).astype(int)
    X_test = test_f[feature_cols].fillna(0)
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(V15_PARAMS, train_ds, num_boost_round=200)
    return model.predict(X_train), model.predict(X_test), model

# ============================================================
# V4.4モデル訓練（LambdaRank）
# ============================================================
def prepare_v44_target(df):
    weight_1st, weight_2nd, weight_3rd = 12.74, 6.73, 3.69
    gain = np.zeros(len(df))
    odds = df['win_odds'].fillna(1.0).clip(upper=90)
    log_odds = np.log1p(odds)
    gain[df['finish_position'] == 1] = log_odds[df['finish_position'] == 1] * weight_1st
    gain[df['finish_position'] == 2] = log_odds[df['finish_position'] == 2] * weight_2nd
    gain[df['finish_position'] == 3] = log_odds[df['finish_position'] == 3] * weight_3rd
    return gain.astype(int)

V44_PARAMS = {
    'objective': 'lambdarank', 'metric': 'ndcg', 'eval_at': [1, 3],
    'boosting_type': 'gbdt', 'learning_rate': 0.05, 'num_leaves': 31,
    'min_child_samples': 100, 'max_depth': 6, 'reg_alpha': 1.0, 'reg_lambda': 2.0,
    'colsample_bytree': 0.8, 'subsample': 0.8, 'random_state': 42, 'verbose': -1,
    'label_gain': list(range(100)),
}

def train_v44(train_f, test_f, feature_cols):
    train_sorted = train_f.sort_values('race_id')
    test_sorted = test_f.sort_values('race_id')
    X_train = train_sorted[feature_cols].fillna(0)
    X_test = test_sorted[feature_cols].fillna(0)
    y_train = prepare_v44_target(train_sorted)
    y_test = prepare_v44_target(test_sorted)
    q_train = train_sorted.groupby('race_id', sort=False).size().to_list()
    q_test = test_sorted.groupby('race_id', sort=False).size().to_list()

    model = lgb.LGBMRanker(**V44_PARAMS, n_estimators=1000)
    model.fit(X_train, y_train, group=q_train,
              eval_set=[(X_test, y_test)], eval_group=[q_test],
              callbacks=[lgb.early_stopping(50), lgb.log_evaluation(0)])

    train_pred = model.predict(X_train)
    test_pred = model.predict(X_test)
    # 元のインデックスに戻す
    train_pred_full = pd.Series(train_pred, index=train_sorted.index)
    test_pred_full = pd.Series(test_pred, index=test_sorted.index)
    return train_pred_full.reindex(train_f.index).values, test_pred_full.reindex(test_f.index).values, model

# ============================================================
# Ensembleスコア計算
# ============================================================
def ensemble_scores(v15_pred, v44_pred, alpha=0.5):
    """V15とV4.4のスコアを正規化してアンサンブル"""
    def normalize(arr):
        std = arr.std()
        if std == 0: return arr - arr.mean()
        return (arr - arr.mean()) / std
    return alpha * normalize(v15_pred) + (1 - alpha) * normalize(v44_pred)

# ============================================================
# メイン
# ============================================================
def main():
    logger.info("=" * 70)
    logger.info("SP値特徴量統合 全モデル網羅検証 (Phase 2: SP_lite)")
    logger.info("=" * 70)
    start_time = datetime.now()

    races, pedigrees, corners, race_details, horses = load_data()

    periods = [
        {'year': 2021, 'train_end': '2020-12-31', 'test_start': '2021-01-01', 'test_end': '2021-12-31'},
        {'year': 2022, 'train_end': '2021-12-31', 'test_start': '2022-01-01', 'test_end': '2022-12-31'},
        {'year': 2023, 'train_end': '2022-12-31', 'test_start': '2023-01-01', 'test_end': '2023-12-31'},
        {'year': 2024, 'train_end': '2023-12-31', 'test_start': '2024-01-01', 'test_end': '2024-12-31'},
        {'year': 2025, 'train_end': '2024-12-31', 'test_start': '2025-01-01', 'test_end': '2025-11-01'},
    ]

    all_results = []

    for period in periods:
        year = period['year']
        logger.info(f"\n{'='*60}")
        logger.info(f"Year {year}")
        logger.info(f"{'='*60}")

        train = races[races['race_date'] <= period['train_end']].copy()
        test = races[(races['race_date'] >= period['test_start']) &
                     (races['race_date'] < period['test_end'])].copy()
        if len(test) == 0:
            continue
        logger.info(f"  Train: {len(train):,}, Test: {len(test):,}")

        # ========== V15特徴量 ==========
        logger.info("  V15特徴量生成中...")
        eng_v15 = LeakFreeFeatureEngineerV15()
        eng_v15.fit(train, pedigrees, corners, race_details, horses_df=horses)
        train_v15f = eng_v15.transform(train)
        test_v15f = eng_v15.transform(test)
        v15_cols = [c for c in eng_v15.get_feature_columns() if c in train_v15f.columns]

        # ========== V16特徴量（V4.4用） ==========
        logger.info("  V16特徴量生成中...")
        eng_v16 = LeakFreeFeatureEngineerV16()
        eng_v16.fit(train, pedigrees, corners, race_details, horses_df=horses)
        train_v16f = eng_v16.transform(train)
        test_v16f = eng_v16.transform(test)
        v16_cols = [c for c in eng_v16.get_feature_columns() if c in train_v16f.columns]

        # ========== SP_lite特徴量 ==========
        logger.info("  SP_lite特徴量生成中...")
        all_data = pd.concat([train, test], ignore_index=True)
        sp_features = compute_sp_features_lite(all_data, race_details, period['train_end'])

        # SP特徴量をマージ
        def merge_sp(df):
            df = df.copy()
            df['race_id'] = df['race_id'].astype(str)
            df['horse_id'] = df['horse_id'].astype(str)
            return df.merge(sp_features, on=['race_id', 'horse_id'], how='left')

        train_v15sp = merge_sp(train_v15f)
        test_v15sp = merge_sp(test_v15f)
        train_v16sp = merge_sp(train_v16f)
        test_v16sp = merge_sp(test_v16f)

        v15sp_cols = v15_cols + SP_LITE_COLS
        v16sp_cols = v16_cols + SP_LITE_COLS

        # カバレッジ確認
        for col in SP_LITE_COLS:
            tc = test_v15sp[col].notna().mean() * 100
            logger.info(f"    {col}: Test {tc:.1f}%")

        result = {'year': year}

        # ========== (1) V15 Base vs V15+SP ==========
        logger.info("  [1] V15 Base...")
        tr_pred, te_pred, _ = train_v15(train_v15f, test_v15f, v15_cols)
        v15_base_roi, v15_base_hit, v15_base_n = calc_roi(test_v15f, te_pred)
        v15_base_train_roi, _, _ = calc_roi(train_v15f, tr_pred)

        logger.info("  [1] V15+SP_lite...")
        tr_pred_sp, te_pred_sp, _ = train_v15(train_v15sp, test_v15sp, v15sp_cols)
        v15_sp_roi, v15_sp_hit, v15_sp_n = calc_roi(test_v15sp, te_pred_sp)
        v15_sp_train_roi, _, _ = calc_roi(train_v15sp, tr_pred_sp)

        result['v15_base'] = v15_base_roi
        result['v15_sp'] = v15_sp_roi
        result['v15_diff'] = v15_sp_roi - v15_base_roi
        result['v15_gap_base'] = v15_base_train_roi - v15_base_roi
        result['v15_gap_sp'] = v15_sp_train_roi - v15_sp_roi
        result['v15_hit_base'] = v15_base_hit
        result['v15_hit_sp'] = v15_sp_hit

        logger.info(f"    V15 Base: {v15_base_roi:.1f}% | V15+SP: {v15_sp_roi:.1f}% | Diff: {v15_sp_roi - v15_base_roi:+.1f}pt")

        # V15の予測値を保存（Ensemble用）
        v15_te_base = te_pred
        v15_te_sp = te_pred_sp
        v15_tr_base = tr_pred
        v15_tr_sp = tr_pred_sp

        # ========== (2) V4.4 Base vs V4.4+SP ==========
        logger.info("  [2] V4.4 Base...")
        tr_pred44, te_pred44, _ = train_v44(train_v16f, test_v16f, v16_cols)
        v44_base_roi, v44_base_hit, v44_base_n = calc_roi(test_v16f, te_pred44)
        v44_base_train_roi, _, _ = calc_roi(train_v16f, tr_pred44)

        logger.info("  [2] V4.4+SP_lite...")
        tr_pred44sp, te_pred44sp, _ = train_v44(train_v16sp, test_v16sp, v16sp_cols)
        v44_sp_roi, v44_sp_hit, v44_sp_n = calc_roi(test_v16sp, te_pred44sp)
        v44_sp_train_roi, _, _ = calc_roi(train_v16sp, tr_pred44sp)

        result['v44_base'] = v44_base_roi
        result['v44_sp'] = v44_sp_roi
        result['v44_diff'] = v44_sp_roi - v44_base_roi
        result['v44_gap_base'] = v44_base_train_roi - v44_base_roi
        result['v44_gap_sp'] = v44_sp_train_roi - v44_sp_roi
        result['v44_hit_base'] = v44_base_hit
        result['v44_hit_sp'] = v44_sp_hit

        logger.info(f"    V4.4 Base: {v44_base_roi:.1f}% | V4.4+SP: {v44_sp_roi:.1f}% | Diff: {v44_sp_roi - v44_base_roi:+.1f}pt")

        # ========== (3) Ensemble Base vs Ensemble+SP ==========
        logger.info("  [3] Ensemble...")
        ens_base_te = ensemble_scores(v15_te_base, te_pred44)
        ens_sp_te = ensemble_scores(v15_te_sp, te_pred44sp)
        ens_base_tr = ensemble_scores(v15_tr_base, tr_pred44)
        ens_sp_tr = ensemble_scores(v15_tr_sp, tr_pred44sp)

        ens_base_roi, ens_base_hit, ens_base_n = calc_roi(test_v15f, ens_base_te)
        ens_sp_roi, ens_sp_hit, ens_sp_n = calc_roi(test_v15sp, ens_sp_te)
        ens_base_train_roi, _, _ = calc_roi(train_v15f, ens_base_tr)
        ens_sp_train_roi, _, _ = calc_roi(train_v15sp, ens_sp_tr)

        result['ens_base'] = ens_base_roi
        result['ens_sp'] = ens_sp_roi
        result['ens_diff'] = ens_sp_roi - ens_base_roi
        result['ens_gap_base'] = ens_base_train_roi - ens_base_roi
        result['ens_gap_sp'] = ens_sp_train_roi - ens_sp_roi
        result['ens_hit_base'] = ens_base_hit
        result['ens_hit_sp'] = ens_sp_hit

        logger.info(f"    Ens Base: {ens_base_roi:.1f}% | Ens+SP: {ens_sp_roi:.1f}% | Diff: {ens_sp_roi - ens_base_roi:+.1f}pt")

        all_results.append(result)
        del train_v15f, test_v15f, train_v16f, test_v16f
        del train_v15sp, test_v15sp, train_v16sp, test_v16sp
        gc.collect()

    # ============================================================
    # 最終サマリー
    # ============================================================
    df = pd.DataFrame(all_results)

    logger.info("\n" + "=" * 80)
    logger.info("最終結果サマリー (SP_lite: 3特徴量)")
    logger.info("=" * 80)

    for model_name, base_col, sp_col, diff_col, gap_b, gap_s in [
        ('V15 Binary',  'v15_base', 'v15_sp', 'v15_diff', 'v15_gap_base', 'v15_gap_sp'),
        ('V4.4 Ranker',  'v44_base', 'v44_sp', 'v44_diff', 'v44_gap_base', 'v44_gap_sp'),
        ('Ensemble',     'ens_base', 'ens_sp', 'ens_diff', 'ens_gap_base', 'ens_gap_sp'),
    ]:
        logger.info(f"\n--- {model_name} ---")
        logger.info(f"{'年度':>6} | {'Base':>8} | {'+SP':>8} | {'差分':>7} | {'Gap(B)':>8} | {'Gap(S)':>8}")
        logger.info("-" * 60)
        for _, r in df.iterrows():
            emoji = "✅" if r[diff_col] > 0 else "❌"
            logger.info(f"{int(r['year']):>6} | {r[base_col]:>7.1f}% | {r[sp_col]:>7.1f}% | "
                        f"{r[diff_col]:>+6.1f} {emoji} | {r[gap_b]:>7.1f}% | {r[gap_s]:>7.1f}%")
        logger.info("-" * 60)
        mean_b = df[base_col].mean()
        mean_s = df[sp_col].mean()
        mean_d = df[diff_col].mean()
        mean_gb = df[gap_b].mean()
        mean_gs = df[gap_s].mean()
        wins = (df[diff_col] > 0).sum()
        logger.info(f"{'平均':>6} | {mean_b:>7.1f}% | {mean_s:>7.1f}% | {mean_d:>+6.1f}   | {mean_gb:>7.1f}% | {mean_gs:>7.1f}%")
        logger.info(f"  改善年数: {wins}/5, 平均ROI差: {mean_d:+.1f}pt, Gap変化: {(mean_gs-mean_gb):+.1f}pt")

    # t検定
    logger.info("\n--- 統計的有意性（対応のあるt検定） ---")
    from scipy import stats
    for name, base_col, sp_col in [
        ('V15', 'v15_base', 'v15_sp'),
        ('V4.4', 'v44_base', 'v44_sp'),
        ('Ensemble', 'ens_base', 'ens_sp'),
    ]:
        t_stat, p_val = stats.ttest_rel(df[sp_col], df[base_col])
        sig = "有意 ✅" if p_val < 0.05 else f"p={p_val:.3f}"
        logger.info(f"  {name:>10}: t={t_stat:+.3f}, p={p_val:.4f} → {sig}")

    # 保存
    output_path = project_root / "outputs/analysis/sp_all_models_results.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info(f"\n結果保存: {output_path}")
    logger.info(f"総実行時間: {datetime.now() - start_time}")

    return df

if __name__ == "__main__":
    main()
