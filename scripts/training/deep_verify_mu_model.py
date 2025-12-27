#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
μモデル深層検証スクリプト

【目的】
+μ 10%の効果（2021/2022で改善、2023/2024で悪化）を詳細分析し、
リーク、過学習、過適合の有無を検証する。

【検証項目】
1. Train-Test Gap計算（過学習検出）
2. μ特徴量の年別分布（データドリフト検出）
3. μ予測とV15/V4.4の相関（情報重複検出）
4. 的中/外れパターンの年別分析
5. オッズ帯別の効果分析
6. 具体的なレースでの予測比較
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
import logging
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))


def load_data():
    data_dir = project_root / "keibaai/data/parsed/parquet"
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    return races, pedigrees, corners, race_details, horses


def calc_roi(df, preds):
    d = df.copy()
    d['score'] = preds
    d['rank'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    bets = d[d['rank'] == 1]
    hits = bets[bets['finish_position'] == 1]
    if len(bets) == 0:
        return 0, 0, 0
    roi = hits['win_odds'].sum() / len(bets) * 100
    hit_rate = len(hits) / len(bets) * 100
    return roi, hit_rate, len(bets)


def normalize(x):
    return (x - x.min()) / (x.max() - x.min() + 1e-8)


def train_v15(train_df, feature_cols):
    params = {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 3.0, 'reg_lambda': 5.0,
        'bagging_fraction': 0.7, 'bagging_freq': 3, 'feature_fraction': 0.7,
    }
    y_train = (train_df['finish_position'] == 1).astype(int)
    train_ds = lgb.Dataset(train_df[feature_cols].fillna(0), y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    return model


def train_v44_residual(train_df, feature_cols, v15_preds):
    train_df = train_df.copy()
    train_df['v15_pred'] = v15_preds
    
    odds = train_df['win_odds'].fillna(1.0).clip(upper=90)
    log_odds = np.log1p(odds)
    
    actual = (train_df['finish_position'] == 1).astype(float)
    residual = actual - train_df['v15_pred']
    
    residual_gain = np.zeros(len(train_df))
    residual_gain[train_df['finish_position'] == 1] = residual[train_df['finish_position'] == 1] * log_odds[train_df['finish_position'] == 1] * 10
    residual_gain[train_df['finish_position'] == 2] = residual[train_df['finish_position'] == 2] * log_odds[train_df['finish_position'] == 2] * 5
    residual_gain[train_df['finish_position'] == 3] = residual[train_df['finish_position'] == 3] * log_odds[train_df['finish_position'] == 3] * 2
    residual_gain = np.maximum(residual_gain, 0)
    
    train_df['target'] = residual_gain.astype(int)
    train_df = train_df.sort_values('race_id')
    groups = train_df.groupby('race_id', sort=False).size().to_list()
    
    params = {
        'objective': 'lambdarank', 'boosting_type': 'gbdt',
        'num_leaves': 20, 'max_depth': 3, 'min_child_samples': 200,
        'learning_rate': 0.03, 'reg_alpha': 8.0, 'reg_lambda': 12.0,
        'feature_fraction': 0.5, 'bagging_fraction': 0.6, 'bagging_freq': 3,
        'verbose': -1, 'random_state': 42, 'label_gain': list(range(100))
    }
    
    model = lgb.LGBMRanker(**params, n_estimators=150)
    model.fit(train_df[feature_cols].fillna(0), train_df['target'], group=groups)
    return model


def train_mu_time(train_df, mu_feature_cols):
    train_valid = train_df.dropna(subset=['normalized_time']).copy()
    
    params = {
        'objective': 'regression', 'metric': 'rmse', 'verbosity': -1,
        'learning_rate': 0.02, 'num_leaves': 31, 'max_depth': 4,
        'min_child_samples': 100, 'reg_alpha': 2.0, 'reg_lambda': 3.0,
        'bagging_fraction': 0.8, 'bagging_freq': 3, 'feature_fraction': 0.8,
    }
    
    y_train = train_valid['normalized_time']
    train_ds = lgb.Dataset(train_valid[mu_feature_cols].fillna(0), y_train)
    model = lgb.train(params, train_ds, num_boost_round=300)
    return model


def main():
    logger.info("=" * 80)
    logger.info("μモデル深層検証")
    logger.info("=" * 80)
    
    from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
    from keibaai.src.features.time_feature_engineer_v3 import TimeFeatureEngineerV3
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    # ===== 検証1: 生データの年別分布 =====
    logger.info("\n" + "=" * 60)
    logger.info("検証1: 生データの年別分布")
    logger.info("=" * 60)
    
    races['year'] = races['race_date'].dt.year
    
    for year in [2021, 2022, 2023, 2024]:
        year_data = races[races['year'] == year]
        logger.info(f"\n{year}年:")
        logger.info(f"  レコード数: {len(year_data):,}")
        logger.info(f"  レース数: {year_data['race_id'].nunique():,}")
        logger.info(f"  平均オッズ: {year_data['win_odds'].mean():.1f}")
        logger.info(f"  1番人気勝率: {(year_data.groupby('race_id').apply(lambda x: (x['win_odds'].idxmin() == x[x['finish_position']==1].index[0]) if len(x[x['finish_position']==1]) > 0 else False, include_groups=False).mean()*100):.1f}%")
        
        # finish_time_secondsの欠損率
        time_missing = year_data['finish_time_seconds'].isna().mean() * 100
        logger.info(f"  タイム欠損率: {time_missing:.1f}%")
    
    # ===== 検証2: 詳細なモデル検証 =====
    test_years = [2021, 2022, 2023, 2024]
    all_detailed_results = []
    
    for test_year in test_years:
        train_end = f'{test_year - 2}-12-31'
        test_start = f'{test_year}-01-01'
        test_end = f'{test_year}-12-31'
        
        logger.info(f"\n{'='*60}")
        logger.info(f"詳細検証: {test_year}年")
        logger.info("=" * 60)
        
        train = races[races['race_date'] <= train_end].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] <= test_end)].copy()
        
        # V15特徴量
        engine_v15 = LeakFreeFeatureEngineerV15Fixed()
        engine_v15.fit(train, pedigrees, corners, race_details, horses_df=horses)
        train_v15f = engine_v15.transform(train)
        test_v15f = engine_v15.transform(test)
        v15_feature_cols = [c for c in engine_v15.get_feature_columns() if c in train_v15f.columns]
        
        # μ特徴量
        engine_mu = TimeFeatureEngineerV3(min_samples=30)
        engine_mu.fit(train)
        train_muf = engine_mu.transform(train)
        test_muf = engine_mu.transform(test)
        mu_feature_cols = [c for c in engine_mu.get_feature_columns() if c in train_muf.columns]
        
        # 基本特徴量追加
        basic_cols = ['distance_m', 'bracket_number', 'horse_weight', 'age', 'basis_weight']
        for col in ['track_surface', 'track_condition', 'venue']:
            if col in train_muf.columns:
                train_muf[col + '_enc'] = train_muf[col].astype('category').cat.codes
                test_muf[col + '_enc'] = test_muf[col].astype('category').cat.codes
                basic_cols.append(col + '_enc')
        mu_feature_cols_full = mu_feature_cols + [c for c in basic_cols if c in train_muf.columns]
        
        # ===== 検証2a: μ特徴量の分布比較 =====
        logger.info(f"\n--- μ特徴量の分布（Train vs Test）---")
        for feat in ['horse_normalized_time_avg', 'horse_time_best', 'horse_last3f_avg']:
            if feat in train_muf.columns and feat in test_muf.columns:
                train_mean = train_muf[feat].mean()
                train_std = train_muf[feat].std()
                test_mean = test_muf[feat].mean()
                test_std = test_muf[feat].std()
                drift = abs(train_mean - test_mean) / (train_std + 1e-8)
                logger.info(f"  {feat}: Train={train_mean:.3f}±{train_std:.3f}, Test={test_mean:.3f}±{test_std:.3f}, Drift={drift:.2f}σ")
        
        # モデル学習
        model_v15 = train_v15(train_v15f, v15_feature_cols)
        v15_train_pred = model_v15.predict(train_v15f[v15_feature_cols].fillna(0))
        v15_test_pred = model_v15.predict(test_v15f[v15_feature_cols].fillna(0))
        
        model_v44 = train_v44_residual(train_v15f, v15_feature_cols, v15_train_pred)
        v44_train_pred = model_v44.predict(train_v15f[v15_feature_cols].fillna(0))
        v44_test_pred = model_v44.predict(test_v15f[v15_feature_cols].fillna(0))
        
        model_mu = train_mu_time(train_muf, mu_feature_cols_full)
        mu_train_pred = model_mu.predict(train_muf[mu_feature_cols_full].fillna(0))
        mu_test_pred = model_mu.predict(test_muf[mu_feature_cols_full].fillna(0))
        
        # μ予測を順位スコアに変換
        train_muf['_mu_pred'] = mu_train_pred
        test_muf['_mu_pred'] = mu_test_pred
        mu_train_rank = train_muf.groupby('race_id')['_mu_pred'].rank(pct=True, ascending=True).values
        mu_test_rank = test_muf.groupby('race_id')['_mu_pred'].rank(pct=True, ascending=True).values
        
        # ===== 検証2b: Train-Test Gap =====
        logger.info(f"\n--- Train-Test Gap分析 ---")
        
        norm_v15_train = normalize(v15_train_pred)
        norm_v44_train = normalize(v44_train_pred)
        norm_mu_train = mu_train_rank
        
        norm_v15_test = normalize(v15_test_pred)
        norm_v44_test = normalize(v44_test_pred)
        norm_mu_test = mu_test_rank
        
        # Base (V15+V4.4 50:50)
        base_train = norm_v15_train * 0.5 + norm_v44_train * 0.5
        base_test = norm_v15_test * 0.5 + norm_v44_test * 0.5
        roi_base_train, _, _ = calc_roi(train_v15f, base_train)
        roi_base_test, _, _ = calc_roi(test_v15f, base_test)
        gap_base = roi_base_train - roi_base_test
        
        # +μ 10%
        mu10_train = norm_v15_train * 0.45 + norm_v44_train * 0.45 + norm_mu_train * 0.10
        mu10_test = norm_v15_test * 0.45 + norm_v44_test * 0.45 + norm_mu_test * 0.10
        roi_mu10_train, _, _ = calc_roi(train_v15f, mu10_train)
        roi_mu10_test, _, _ = calc_roi(test_v15f, mu10_test)
        gap_mu10 = roi_mu10_train - roi_mu10_test
        
        logger.info(f"  Base:   Train={roi_base_train:.1f}%, Test={roi_base_test:.1f}%, Gap={gap_base:.1f}%")
        logger.info(f"  +μ10%:  Train={roi_mu10_train:.1f}%, Test={roi_mu10_test:.1f}%, Gap={gap_mu10:.1f}%")
        
        if gap_mu10 > gap_base + 10:
            logger.warning(f"  ⚠️ μ追加でGap悪化: +{gap_mu10 - gap_base:.1f}%")
        
        # ===== 検証2c: 予測の相関分析 =====
        logger.info(f"\n--- 予測相関分析 ---")
        from scipy.stats import spearmanr
        
        corr_v15_v44 = spearmanr(v15_test_pred, v44_test_pred)[0]
        corr_v15_mu = spearmanr(v15_test_pred, mu_test_pred)[0]
        corr_v44_mu = spearmanr(v44_test_pred, mu_test_pred)[0]
        
        logger.info(f"  V15 vs V4.4: {corr_v15_v44:.3f}")
        logger.info(f"  V15 vs μ:    {corr_v15_mu:.3f}")
        logger.info(f"  V4.4 vs μ:   {corr_v44_mu:.3f}")
        
        if abs(corr_v15_mu) > 0.7:
            logger.warning(f"  ⚠️ V15とμの相関が高い（情報重複の可能性）")
        
        # ===== 検証2d: オッズ帯別の効果 =====
        logger.info(f"\n--- オッズ帯別効果 ---")
        
        test_v15f['base_score'] = base_test
        test_v15f['mu10_score'] = mu10_test
        test_v15f['base_rank'] = test_v15f.groupby('race_id')['base_score'].rank(ascending=False, method='first')
        test_v15f['mu10_rank'] = test_v15f.groupby('race_id')['mu10_score'].rank(ascending=False, method='first')
        
        for odds_low, odds_high, label in [(1, 5, '本命'), (5, 15, '中穴'), (15, 50, '大穴')]:
            mask = (test_v15f['win_odds'] >= odds_low) & (test_v15f['win_odds'] < odds_high)
            sub = test_v15f[mask]
            
            base_bets = sub[sub['base_rank'] == 1]
            mu10_bets = sub[sub['mu10_rank'] == 1]
            
            if len(base_bets) > 0 and len(mu10_bets) > 0:
                base_hits = base_bets[base_bets['finish_position'] == 1]
                mu10_hits = mu10_bets[mu10_bets['finish_position'] == 1]
                
                base_roi = base_hits['win_odds'].sum() / len(base_bets) * 100 if len(base_bets) > 0 else 0
                mu10_roi = mu10_hits['win_odds'].sum() / len(mu10_bets) * 100 if len(mu10_bets) > 0 else 0
                
                logger.info(f"  {label} ({odds_low}-{odds_high}倍): Base={base_roi:.1f}%, +μ10%={mu10_roi:.1f}%, 差={mu10_roi-base_roi:+.1f}%")
        
        # ===== 検証2e: μモデル単体の精度 =====
        logger.info(f"\n--- μモデル単体精度（タイム予測） ---")
        
        test_valid = test_muf.dropna(subset=['normalized_time']).copy()
        if len(test_valid) > 0:
            test_valid['mu_pred'] = model_mu.predict(test_valid[mu_feature_cols_full].fillna(0))
            
            # RMSE
            rmse = np.sqrt(((test_valid['normalized_time'] - test_valid['mu_pred']) ** 2).mean())
            
            # レース内順位相関
            def calc_race_spearman(g):
                if len(g) < 3:
                    return np.nan
                actual_rank = g['normalized_time'].rank()
                pred_rank = g['mu_pred'].rank()
                return spearmanr(actual_rank, pred_rank)[0]
            
            race_corrs = test_valid.groupby('race_id').apply(calc_race_spearman, include_groups=False)
            mean_corr = race_corrs.mean()
            
            # Top1精度
            def calc_top1_acc(g):
                pred_top = g.loc[g['mu_pred'].idxmin()]
                actual_top = g.loc[g['normalized_time'].idxmin()]
                return pred_top.name == actual_top.name
            
            top1_acc = test_valid.groupby('race_id').apply(calc_top1_acc, include_groups=False).mean()
            
            logger.info(f"  RMSE: {rmse:.4f}")
            logger.info(f"  レース内Spearman: {mean_corr:.4f}")
            logger.info(f"  Top1精度: {top1_acc:.1%}")
        
        # 結果保存
        all_detailed_results.append({
            'year': test_year,
            'base_train': roi_base_train,
            'base_test': roi_base_test,
            'base_gap': gap_base,
            'mu10_train': roi_mu10_train,
            'mu10_test': roi_mu10_test,
            'mu10_gap': gap_mu10,
            'corr_v15_mu': corr_v15_mu,
            'mu_rmse': rmse if 'rmse' in dir() else None,
            'mu_spearman': mean_corr if 'mean_corr' in dir() else None,
        })
        
        # 一時カラムを削除
        for col in ['base_score', 'mu10_score', 'base_rank', 'mu10_rank']:
            if col in test_v15f.columns:
                test_v15f = test_v15f.drop(columns=[col])
    
    # ===== 最終サマリー =====
    logger.info("\n" + "=" * 80)
    logger.info("深層検証サマリー")
    logger.info("=" * 80)
    
    logger.info("\n--- Train-Test Gap比較 ---")
    logger.info(f"{'Year':>6} {'Base Train':>12} {'Base Test':>11} {'Base Gap':>10} {'μ10 Train':>11} {'μ10 Test':>10} {'μ10 Gap':>9}")
    logger.info("-" * 80)
    
    for r in all_detailed_results:
        logger.info(f"{r['year']:>6} {r['base_train']:>11.1f}% {r['base_test']:>10.1f}% {r['base_gap']:>9.1f}% {r['mu10_train']:>10.1f}% {r['mu10_test']:>9.1f}% {r['mu10_gap']:>8.1f}%")
    
    # 平均Gap
    avg_base_gap = np.mean([r['base_gap'] for r in all_detailed_results])
    avg_mu10_gap = np.mean([r['mu10_gap'] for r in all_detailed_results])
    
    logger.info("-" * 80)
    logger.info(f"{'Avg':>6} {'':>12} {'':>11} {avg_base_gap:>9.1f}% {'':>11} {'':>10} {avg_mu10_gap:>8.1f}%")
    
    logger.info("\n--- 相関分析（V15 vs μ）---")
    for r in all_detailed_results:
        logger.info(f"  {r['year']}年: {r['corr_v15_mu']:.3f}")
    
    # 最終判定
    logger.info("\n" + "=" * 80)
    logger.info("最終判定")
    logger.info("=" * 80)
    
    gap_increase = avg_mu10_gap - avg_base_gap
    if gap_increase > 5:
        logger.error(f"❌ μ追加でGapが+{gap_increase:.1f}%悪化 → 過学習の可能性が高い")
    elif gap_increase > 0:
        logger.warning(f"⚠️ μ追加でGapが+{gap_increase:.1f}%増加 → 軽度の過学習リスク")
    else:
        logger.info(f"✅ μ追加でGapは悪化せず（{gap_increase:+.1f}%）→ 過学習リスクは低い")
    
    # 2023/2024での悪化について
    recent_effect = np.mean([r['mu10_test'] - r['base_test'] for r in all_detailed_results if r['year'] >= 2023])
    if recent_effect < 0:
        logger.warning(f"⚠️ 直近2年（2023-2024）でのμ効果: {recent_effect:+.1f}% → 市場変化/過適合の可能性")


if __name__ == "__main__":
    main()
