#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SP値特徴量 最適構成検証 (Phase 3)

【深層推論分析に基づく検証内容】

Phase 1-2の結果から導出された核心的知見:
- V15 + SP6特徴量: +1.5pt (3/5年) ← V15には有効
- V4.4 + SP: -1.4pt (1/5年)        ← V4.4には逆効果
- Ensemble両方SP: +1.5pt (3/5年)   ← V4.4が足を引っ張っている

→ 最適構成仮説: V15(+SP) + V4.4(SPなし) の混合Ensemble
  V4.4のSP悪化を回避しつつV15の改善を享受

【検証項目】
1. V15+SP（Phase 1の6特徴量、V15SP特徴量エンジニア使用） 
2. 混合Ensemble: V15(+SP) + V4.4(base) at alpha=0.5
3. 混合Ensemble: V15(+SP) + V4.4(base) at alpha=0.6 (V15寄り)
4. 混合Ensemble: V15(+SP) + V4.4(base) at alpha=0.7 (V15重視)
5. ベースライン: V15(base) + V4.4(base) at alpha=0.5

→ 最も優れたEnsemble構成を特定
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
from keibaai.src.features.leak_free_feature_engineer_v15_sp import LeakFreeFeatureEngineerV15SP
from keibaai.src.features.leak_free_feature_engineer_v16 import LeakFreeFeatureEngineerV16

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================
# 共通
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
    logger.info(f"  races: {len(races):,}件")
    return races, pedigrees, corners, race_details, horses

def calc_roi(df, preds):
    d = df.copy()
    d['pred'] = preds
    d['rank'] = d.groupby('race_id')['pred'].rank(ascending=False, method='first')
    bets = d[d['rank'] == 1]
    if len(bets) == 0: return 0, 0, 0
    hits = bets[bets['finish_position'] == 1]
    roi = hits['win_odds'].sum() / len(bets) * 100
    hit_rate = len(hits) / len(bets) * 100
    return roi, hit_rate, len(bets)

V15_PARAMS = {
    'objective': 'binary', 'metric': 'auc', 'learning_rate': 0.03,
    'max_depth': 3, 'num_leaves': 20, 'min_child_samples': 100,
    'reg_alpha': 3.0, 'reg_lambda': 5.0, 'bagging_fraction': 0.7,
    'feature_fraction': 0.7, 'bagging_freq': 3, 'verbose': -1, 'seed': 42,
}

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

def train_binary(train_f, test_f, cols):
    X_tr = train_f[cols].fillna(0)
    y_tr = (train_f['finish_position'] == 1).astype(int)
    model = lgb.train(V15_PARAMS, lgb.Dataset(X_tr, y_tr), num_boost_round=200)
    return model.predict(X_tr), model.predict(test_f[cols].fillna(0)), model

def train_ranker(train_f, test_f, cols):
    ts = train_f.sort_values('race_id')
    vs = test_f.sort_values('race_id')
    X_tr, X_te = ts[cols].fillna(0), vs[cols].fillna(0)
    y_tr, y_te = prepare_v44_target(ts), prepare_v44_target(vs)
    q_tr = ts.groupby('race_id', sort=False).size().to_list()
    q_te = vs.groupby('race_id', sort=False).size().to_list()
    model = lgb.LGBMRanker(**V44_PARAMS, n_estimators=1000)
    model.fit(X_tr, y_tr, group=q_tr, eval_set=[(X_te, y_te)],
              eval_group=[q_te], callbacks=[lgb.early_stopping(50), lgb.log_evaluation(0)])
    tr_pred = pd.Series(model.predict(X_tr), index=ts.index).reindex(train_f.index).values
    te_pred = pd.Series(model.predict(X_te), index=vs.index).reindex(test_f.index).values
    return tr_pred, te_pred, model

def ensemble(p1, p2, alpha=0.5):
    def norm(a):
        s = a.std()
        if s == 0: return a - a.mean()
        return (a - a.mean()) / s
    return alpha * norm(p1) + (1-alpha) * norm(p2)

# ============================================================
# メイン
# ============================================================
def main():
    logger.info("=" * 70)
    logger.info("SP値 最適構成検証 (Phase 3)")
    logger.info("=" * 70)
    start = datetime.now()
    races, pedigrees, corners, race_details, horses = load_data()

    periods = [
        {'year': 2021, 'train_end': '2020-12-31', 'test_start': '2021-01-01', 'test_end': '2021-12-31'},
        {'year': 2022, 'train_end': '2021-12-31', 'test_start': '2022-01-01', 'test_end': '2022-12-31'},
        {'year': 2023, 'train_end': '2022-12-31', 'test_start': '2023-01-01', 'test_end': '2023-12-31'},
        {'year': 2024, 'train_end': '2023-12-31', 'test_start': '2024-01-01', 'test_end': '2024-12-31'},
        {'year': 2025, 'train_end': '2024-12-31', 'test_start': '2025-01-01', 'test_end': '2025-11-01'},
    ]

    results = []
    for p in periods:
        year = p['year']
        logger.info(f"\n{'='*60}\nYear {year}\n{'='*60}")

        train = races[races['race_date'] <= p['train_end']].copy()
        test = races[(races['race_date'] >= p['test_start']) & (races['race_date'] < p['test_end'])].copy()
        if len(test) == 0: continue
        logger.info(f"  Train: {len(train):,}, Test: {len(test):,}")

        # ===== V15 Base =====
        logger.info("  [V15 Base]")
        eng_base = LeakFreeFeatureEngineerV15()
        eng_base.fit(train, pedigrees, corners, race_details, horses_df=horses)
        tr_base = eng_base.transform(train)
        te_base = eng_base.transform(test)
        v15_cols = [c for c in eng_base.get_feature_columns() if c in tr_base.columns]
        v15b_tr, v15b_te, _ = train_binary(tr_base, te_base, v15_cols)

        # ===== V15+SP (V15SP特徴量エンジニア) =====
        logger.info("  [V15+SP]")
        eng_sp = LeakFreeFeatureEngineerV15SP()
        eng_sp.fit(train, pedigrees, corners, race_details, horses_df=horses)
        tr_sp = eng_sp.transform(train)
        te_sp = eng_sp.transform(test)
        v15sp_cols = [c for c in eng_sp.get_feature_columns() if c in tr_sp.columns]
        v15sp_tr, v15sp_te, mdl_sp = train_binary(tr_sp, te_sp, v15sp_cols)

        logger.info(f"    V15 Base特徴量: {len(v15_cols)}, V15+SP特徴量: {len(v15sp_cols)}")

        # ===== V4.4 Base (V16特徴量, SPなし) =====
        logger.info("  [V4.4 Base]")
        eng_v16 = LeakFreeFeatureEngineerV16()
        eng_v16.fit(train, pedigrees, corners, race_details, horses_df=horses)
        tr_v16 = eng_v16.transform(train)
        te_v16 = eng_v16.transform(test)
        v16_cols = [c for c in eng_v16.get_feature_columns() if c in tr_v16.columns]
        v44b_tr, v44b_te, _ = train_ranker(tr_v16, te_v16, v16_cols)

        # ===== 各構成のROI計算 =====
        row = {'year': year}

        # (A) V15 Base 単体
        roi_a, hit_a, n_a = calc_roi(te_base, v15b_te)
        roi_a_tr, _, _ = calc_roi(tr_base, v15b_tr)
        row['v15_base'] = roi_a
        row['v15_base_hit'] = hit_a
        row['v15_base_gap'] = roi_a_tr - roi_a
        logger.info(f"    [A] V15 Base:           ROI={roi_a:.1f}%, Hit={hit_a:.1f}%")

        # (B) V15+SP 単体
        roi_b, hit_b, n_b = calc_roi(te_sp, v15sp_te)
        roi_b_tr, _, _ = calc_roi(tr_sp, v15sp_tr)
        row['v15sp'] = roi_b
        row['v15sp_hit'] = hit_b
        row['v15sp_gap'] = roi_b_tr - roi_b
        row['v15sp_diff'] = roi_b - roi_a
        logger.info(f"    [B] V15+SP:             ROI={roi_b:.1f}%, Hit={hit_b:.1f}%, Diff={roi_b-roi_a:+.1f}")

        # (C) V4.4 Base 単体
        roi_c, hit_c, _ = calc_roi(te_v16, v44b_te)
        roi_c_tr, _, _ = calc_roi(tr_v16, v44b_tr)
        row['v44_base'] = roi_c
        row['v44_base_gap'] = roi_c_tr - roi_c
        logger.info(f"    [C] V4.4 Base:          ROI={roi_c:.1f}%, Gap={roi_c_tr-roi_c:.1f}%")

        # (D) Base Ensemble: V15(base) + V4.4(base) α=0.5
        ens_d = ensemble(v15b_te, v44b_te, 0.5)
        roi_d, hit_d, _ = calc_roi(te_base, ens_d)
        row['ens_base_50'] = roi_d
        row['ens_base_50_hit'] = hit_d
        logger.info(f"    [D] Ens Base α=0.5:     ROI={roi_d:.1f}%, Hit={hit_d:.1f}%")

        # (E) Mixed Ensemble: V15(+SP) + V4.4(base) α=0.5
        ens_e = ensemble(v15sp_te, v44b_te, 0.5)
        roi_e, hit_e, _ = calc_roi(te_sp, ens_e)
        row['mix_ens_50'] = roi_e
        row['mix_ens_50_hit'] = hit_e
        row['mix_ens_50_diff'] = roi_e - roi_d
        logger.info(f"    [E] Mix Ens α=0.5:      ROI={roi_e:.1f}%, Hit={hit_e:.1f}%, vs Base Ens: {roi_e-roi_d:+.1f}")

        # (F) Mixed Ensemble: V15(+SP) + V4.4(base) α=0.6
        ens_f = ensemble(v15sp_te, v44b_te, 0.6)
        roi_f, hit_f, _ = calc_roi(te_sp, ens_f)
        row['mix_ens_60'] = roi_f
        row['mix_ens_60_hit'] = hit_f
        row['mix_ens_60_diff'] = roi_f - roi_d
        logger.info(f"    [F] Mix Ens α=0.6:      ROI={roi_f:.1f}%, Hit={hit_f:.1f}%, vs Base Ens: {roi_f-roi_d:+.1f}")

        # (G) Mixed Ensemble: V15(+SP) + V4.4(base) α=0.7
        ens_g = ensemble(v15sp_te, v44b_te, 0.7)
        roi_g, hit_g, _ = calc_roi(te_sp, ens_g)
        row['mix_ens_70'] = roi_g
        row['mix_ens_70_hit'] = hit_g
        row['mix_ens_70_diff'] = roi_g - roi_d
        logger.info(f"    [G] Mix Ens α=0.7:      ROI={roi_g:.1f}%, Hit={hit_g:.1f}%, vs Base Ens: {roi_g-roi_d:+.1f}")

        # SP特徴量重要度
        imp = pd.DataFrame({'feature': v15sp_cols, 'importance': mdl_sp.feature_importance('gain')})
        imp = imp.sort_values('importance', ascending=False)
        sp_imp = imp[imp['feature'].isin(LeakFreeFeatureEngineerV15SP.SP_FEATURE_COLS)]
        logger.info("    SP特徴量重要度:")
        for _, r in sp_imp.iterrows():
            rank = (imp['importance'] > r['importance']).sum() + 1
            logger.info(f"      {r['feature']}: gain={r['importance']:.0f} ({rank}/{len(imp)}位)")

        results.append(row)
        del tr_base, te_base, tr_sp, te_sp, tr_v16, te_v16
        gc.collect()

    # ============================================================
    # 最終サマリー
    # ============================================================
    df = pd.DataFrame(results)

    logger.info("\n" + "=" * 80)
    logger.info("最終結果サマリー")
    logger.info("=" * 80)

    configs = [
        ('V15 Base',           'v15_base'),
        ('V15+SP',             'v15sp'),
        ('V4.4 Base',          'v44_base'),
        ('Base Ens α=0.5',     'ens_base_50'),
        ('Mix Ens α=0.5',      'mix_ens_50'),
        ('Mix Ens α=0.6',      'mix_ens_60'),
        ('Mix Ens α=0.7',      'mix_ens_70'),
    ]

    logger.info(f"\n{'構成':>20} | {'2021':>7} | {'2022':>7} | {'2023':>7} | {'2024':>7} | {'2025':>7} | {'平均':>7}")
    logger.info("-" * 80)
    for name, col in configs:
        vals = df[col].values
        avg = vals.mean()
        row_str = f"{name:>20} | "
        row_str += " | ".join(f"{v:>6.1f}%" for v in vals)
        row_str += f" | {avg:>6.1f}%"
        logger.info(row_str)

    # ベストEnsemble構成
    logger.info("\n--- Base Ensemble vs Mixed Ensemble 差分 ---")
    for name, diff_col in [
        ('Mix α=0.5', 'mix_ens_50_diff'),
        ('Mix α=0.6', 'mix_ens_60_diff'),
        ('Mix α=0.7', 'mix_ens_70_diff'),
    ]:
        if diff_col in df.columns:
            d = df[diff_col]
            wins = (d > 0).sum()
            logger.info(f"  {name}: 平均{d.mean():+.1f}pt, 改善{wins}/5年, "
                        f"範囲[{d.min():+.1f}, {d.max():+.1f}]")

    # 最適構成の決定
    best_col = None
    best_avg = -999
    for name, col in configs:
        avg = df[col].mean()
        if avg > best_avg:
            best_avg = avg
            best_col = (name, col)

    logger.info(f"\n★ 最適構成: {best_col[0]} (平均ROI: {best_avg:.1f}%)")

    # t検定
    from scipy import stats
    logger.info("\n--- 統計的有意性 (vs Base Ensemble) ---")
    for name, col in [('V15+SP単体', 'v15sp'), ('Mix α=0.5', 'mix_ens_50'),
                      ('Mix α=0.6', 'mix_ens_60'), ('Mix α=0.7', 'mix_ens_70')]:
        if col in df.columns:
            t, p = stats.ttest_rel(df[col], df['ens_base_50'])
            logger.info(f"  {name} vs Base Ens: t={t:+.3f}, p={p:.4f}")

    # 保存
    out = project_root / "outputs/analysis/sp_optimal_config_results.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    logger.info(f"\n結果保存: {out}")
    logger.info(f"総実行時間: {datetime.now() - start}")

if __name__ == "__main__":
    main()
