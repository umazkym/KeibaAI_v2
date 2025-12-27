#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
μモデル効果の条件別詳細分析

【目的】
2021/2022年で有効、2023/2024年で無効になった原因を特定し、
条件付きでμを適用する戦略を検証する。

【分析項目】
1. 四半期別のμ効果推移
2. オッズ帯×年別のクロス分析
3. V15/V4.4/μの予測一致度と効果
4. Base ROIとμ効果の相関
5. 条件付きμ適用戦略の検証
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
    logger.info("μモデル効果の条件別詳細分析")
    logger.info("=" * 80)
    
    from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
    from keibaai.src.features.time_feature_engineer_v3 import TimeFeatureEngineerV3
    
    races, pedigrees, corners, race_details, horses = load_data()
    races['year'] = races['race_date'].dt.year
    races['quarter'] = races['race_date'].dt.quarter
    races['month'] = races['race_date'].dt.month
    
    test_years = [2021, 2022, 2023, 2024]
    
    all_quarterly_results = []
    all_odds_band_results = []
    all_agreement_results = []
    all_conditional_results = []
    
    for test_year in test_years:
        train_end = f'{test_year - 2}-12-31'
        test_start = f'{test_year}-01-01'
        test_end = f'{test_year}-12-31'
        
        logger.info(f"\n{'='*60}")
        logger.info(f"分析: {test_year}年")
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
        
        basic_cols = ['distance_m', 'bracket_number', 'horse_weight', 'age', 'basis_weight']
        for col in ['track_surface', 'track_condition', 'venue']:
            if col in train_muf.columns:
                train_muf[col + '_enc'] = train_muf[col].astype('category').cat.codes
                test_muf[col + '_enc'] = test_muf[col].astype('category').cat.codes
                basic_cols.append(col + '_enc')
        mu_feature_cols_full = mu_feature_cols + [c for c in basic_cols if c in train_muf.columns]
        
        # モデル学習
        model_v15 = train_v15(train_v15f, v15_feature_cols)
        v15_train_pred = model_v15.predict(train_v15f[v15_feature_cols].fillna(0))
        v15_test_pred = model_v15.predict(test_v15f[v15_feature_cols].fillna(0))
        
        model_v44 = train_v44_residual(train_v15f, v15_feature_cols, v15_train_pred)
        v44_test_pred = model_v44.predict(test_v15f[v15_feature_cols].fillna(0))
        
        model_mu = train_mu_time(train_muf, mu_feature_cols_full)
        mu_test_pred = model_mu.predict(test_muf[mu_feature_cols_full].fillna(0))
        
        # スコア計算
        test_muf['_mu_pred'] = mu_test_pred
        mu_rank_score = test_muf.groupby('race_id')['_mu_pred'].rank(pct=True, ascending=True).values
        
        norm_v15 = normalize(v15_test_pred)
        norm_v44 = normalize(v44_test_pred)
        norm_mu = mu_rank_score
        
        base_score = norm_v15 * 0.5 + norm_v44 * 0.5
        mu10_score = norm_v15 * 0.45 + norm_v44 * 0.45 + norm_mu * 0.10
        
        # 各馬のTop1判定
        test_v15f = test_v15f.copy()
        test_v15f['base_score'] = base_score
        test_v15f['mu10_score'] = mu10_score
        test_v15f['v15_score'] = norm_v15
        test_v15f['v44_score'] = norm_v44
        test_v15f['mu_score'] = norm_mu
        test_v15f['quarter'] = test['quarter'].values
        test_v15f['month'] = test['month'].values
        
        test_v15f['base_rank'] = test_v15f.groupby('race_id')['base_score'].rank(ascending=False, method='first')
        test_v15f['mu10_rank'] = test_v15f.groupby('race_id')['mu10_score'].rank(ascending=False, method='first')
        test_v15f['v15_rank'] = test_v15f.groupby('race_id')['v15_score'].rank(ascending=False, method='first')
        test_v15f['v44_rank'] = test_v15f.groupby('race_id')['v44_score'].rank(ascending=False, method='first')
        test_v15f['mu_rank'] = test_v15f.groupby('race_id')['mu_score'].rank(ascending=False, method='first')
        
        # ===== 分析1: 四半期別効果 =====
        logger.info(f"\n--- 四半期別効果 ---")
        for q in [1, 2, 3, 4]:
            q_data = test_v15f[test_v15f['quarter'] == q]
            if len(q_data) > 0:
                base_roi, _, n = calc_roi(q_data, q_data['base_score'])
                mu10_roi, _, _ = calc_roi(q_data, q_data['mu10_score'])
                effect = mu10_roi - base_roi
                logger.info(f"  Q{q}: Base={base_roi:.1f}%, +μ10%={mu10_roi:.1f}%, 効果={effect:+.1f}% (N={n})")
                all_quarterly_results.append({
                    'year': test_year, 'quarter': q,
                    'base': base_roi, 'mu10': mu10_roi, 'effect': effect, 'n': n
                })
        
        # ===== 分析2: オッズ帯別効果（詳細） =====
        logger.info(f"\n--- オッズ帯別効果 ---")
        odds_bands = [(1, 3, '超本命1-3'), (3, 5, '本命3-5'), (5, 10, '中穴5-10'), 
                      (10, 20, '穴10-20'), (20, 50, '大穴20-50'), (50, 200, '超穴50+')]
        
        for low, high, label in odds_bands:
            mask = (test_v15f['win_odds'] >= low) & (test_v15f['win_odds'] < high)
            sub = test_v15f[mask]
            if len(sub) > 100:
                base_bets = sub[sub['base_rank'] == 1]
                mu10_bets = sub[sub['mu10_rank'] == 1]
                
                if len(base_bets) > 0 and len(mu10_bets) > 0:
                    base_hits = base_bets[base_bets['finish_position'] == 1]
                    mu10_hits = mu10_bets[mu10_bets['finish_position'] == 1]
                    
                    base_roi = base_hits['win_odds'].sum() / len(base_bets) * 100
                    mu10_roi = mu10_hits['win_odds'].sum() / len(mu10_bets) * 100
                    effect = mu10_roi - base_roi
                    
                    logger.info(f"  {label}: Base={base_roi:.1f}%, +μ10%={mu10_roi:.1f}%, 効果={effect:+.1f}%")
                    all_odds_band_results.append({
                        'year': test_year, 'odds_band': label,
                        'base': base_roi, 'mu10': mu10_roi, 'effect': effect
                    })
        
        # ===== 分析3: V15/V4.4/μの一致度と効果 =====
        logger.info(f"\n--- 予測一致度分析 ---")
        
        # レースごとにTop1の一致度を計算
        race_analysis = []
        for race_id in test_v15f['race_id'].unique():
            race_data = test_v15f[test_v15f['race_id'] == race_id]
            v15_top1 = race_data[race_data['v15_rank'] == 1].index[0]
            v44_top1 = race_data[race_data['v44_rank'] == 1].index[0]
            mu_top1 = race_data[race_data['mu_rank'] == 1].index[0]
            base_top1 = race_data[race_data['base_rank'] == 1].index[0]
            mu10_top1 = race_data[race_data['mu10_rank'] == 1].index[0]
            
            v15_v44_agree = (v15_top1 == v44_top1)
            v15_mu_agree = (v15_top1 == mu_top1)
            all_agree = (v15_top1 == v44_top1 == mu_top1)
            
            winner = race_data[race_data['finish_position'] == 1]
            if len(winner) > 0:
                winner_idx = winner.index[0]
                base_hit = (base_top1 == winner_idx)
                mu10_hit = (mu10_top1 == winner_idx)
                winner_odds = winner['win_odds'].values[0]
            else:
                base_hit = False
                mu10_hit = False
                winner_odds = 0
            
            race_analysis.append({
                'race_id': race_id,
                'v15_v44_agree': v15_v44_agree,
                'v15_mu_agree': v15_mu_agree,
                'all_agree': all_agree,
                'base_hit': base_hit,
                'mu10_hit': mu10_hit,
                'winner_odds': winner_odds
            })
        
        race_df = pd.DataFrame(race_analysis)
        
        # 一致度別の効果
        for condition, label in [
            (race_df['all_agree'], '全モデル一致'),
            (race_df['v15_v44_agree'] & ~race_df['v15_mu_agree'], 'V15/V4.4一致、μ不一致'),
            (~race_df['v15_v44_agree'] & race_df['v15_mu_agree'], 'V15/μ一致、V4.4不一致'),
            (~race_df['v15_v44_agree'] & ~race_df['v15_mu_agree'], '全モデル不一致'),
        ]:
            sub = race_df[condition]
            if len(sub) > 50:
                base_hits = sub['base_hit'].sum()
                mu10_hits = sub['mu10_hit'].sum()
                base_roi = sub[sub['base_hit']]['winner_odds'].sum() / len(sub) * 100
                mu10_roi = sub[sub['mu10_hit']]['winner_odds'].sum() / len(sub) * 100
                effect = mu10_roi - base_roi
                
                logger.info(f"  {label} (N={len(sub)}): Base={base_roi:.1f}%, +μ10%={mu10_roi:.1f}%, 効果={effect:+.1f}%")
                all_agreement_results.append({
                    'year': test_year, 'condition': label,
                    'n': len(sub), 'base': base_roi, 'mu10': mu10_roi, 'effect': effect
                })
        
        # ===== 分析4: 条件付きμ適用戦略 =====
        logger.info(f"\n--- 条件付きμ適用戦略 ---")
        
        # 戦略1: オッズ3倍以上でのみμ適用
        strategy1_score = np.where(test_v15f['win_odds'] >= 3, mu10_score, base_score)
        roi_s1, _, _ = calc_roi(test_v15f, strategy1_score)
        
        # 戦略2: V15/V4.4不一致時のみμ適用（μで決着）
        v15_v44_disagree = (test_v15f['v15_rank'] != test_v15f['v44_rank'])
        strategy2_score = np.where(
            test_v15f.groupby('race_id')['v15_rank'].transform(lambda x: (x == 1).any()) != 
            test_v15f.groupby('race_id')['v44_rank'].transform(lambda x: (x == 1).any()),
            mu10_score, base_score
        )
        # 簡略化: 全体でmu10適用（後で改良）
        
        # 戦略3: 本命オッズ帯（1-10倍）ではμ多め、大穴（10+）ではμ控えめ
        mu_weight_dynamic = np.where(test_v15f['win_odds'] < 10, 0.15, 0.05)
        strategy3_score = (norm_v15 * (0.5 - mu_weight_dynamic/2) + 
                          norm_v44 * (0.5 - mu_weight_dynamic/2) + 
                          norm_mu * mu_weight_dynamic)
        roi_s3, _, _ = calc_roi(test_v15f, strategy3_score)
        
        # 戦略4: 3モデル一致時のみベット（高確信度）
        race_df_with_roi = race_df.copy()
        all_agree_races = race_df[race_df['all_agree']]['race_id'].values
        all_agree_mask = test_v15f['race_id'].isin(all_agree_races)
        sub_all_agree = test_v15f[all_agree_mask]
        if len(sub_all_agree) > 0:
            roi_s4, _, n_s4 = calc_roi(sub_all_agree, sub_all_agree['mu10_score'])
        else:
            roi_s4, n_s4 = 0, 0
        
        base_roi_full, _, _ = calc_roi(test_v15f, test_v15f['base_score'])
        mu10_roi_full, _, _ = calc_roi(test_v15f, test_v15f['mu10_score'])
        
        logger.info(f"  全体Base: {base_roi_full:.1f}%")
        logger.info(f"  全体+μ10%: {mu10_roi_full:.1f}%")
        logger.info(f"  戦略1(オッズ3+でμ): {roi_s1:.1f}%")
        logger.info(f"  戦略3(オッズ帯別重み): {roi_s3:.1f}%")
        logger.info(f"  戦略4(3モデル一致のみ): {roi_s4:.1f}% (N={n_s4})")
        
        all_conditional_results.append({
            'year': test_year,
            'base': base_roi_full,
            'mu10': mu10_roi_full,
            's1_odds3plus': roi_s1,
            's3_dynamic': roi_s3,
            's4_all_agree': roi_s4,
            's4_n': n_s4
        })

        # 一時カラム削除
        for col in ['base_score', 'mu10_score', 'v15_score', 'v44_score', 'mu_score', 
                    'base_rank', 'mu10_rank', 'v15_rank', 'v44_rank', 'mu_rank', 'quarter', 'month']:
            if col in test_v15f.columns:
                test_v15f = test_v15f.drop(columns=[col])
    
    # ===== 最終サマリー =====
    logger.info("\n" + "=" * 80)
    logger.info("条件別効果サマリー")
    logger.info("=" * 80)
    
    # 四半期別サマリー
    logger.info("\n--- 四半期別効果（年×四半期） ---")
    q_df = pd.DataFrame(all_quarterly_results)
    pivot = q_df.pivot_table(values='effect', index='quarter', columns='year', aggfunc='mean')
    logger.info(pivot.to_string())
    
    # オッズ帯別サマリー
    logger.info("\n--- オッズ帯別効果（年平均） ---")
    odds_df = pd.DataFrame(all_odds_band_results)
    odds_pivot = odds_df.pivot_table(values='effect', index='odds_band', columns='year', aggfunc='mean')
    logger.info(odds_pivot.to_string())
    
    # 一致度別サマリー
    logger.info("\n--- 予測一致度別効果（年平均） ---")
    agree_df = pd.DataFrame(all_agreement_results)
    agree_pivot = agree_df.pivot_table(values='effect', index='condition', columns='year', aggfunc='mean')
    logger.info(agree_pivot.to_string())
    
    # 条件付き戦略サマリー
    logger.info("\n--- 条件付き戦略 ---")
    cond_df = pd.DataFrame(all_conditional_results)
    logger.info(f"\n{'Year':>6} {'Base':>8} {'+μ10%':>8} {'S1(3+)':>8} {'S3(動的)':>8} {'S4(一致)':>8}")
    logger.info("-" * 60)
    for _, r in cond_df.iterrows():
        logger.info(f"{r['year']:>6} {r['base']:>7.1f}% {r['mu10']:>7.1f}% {r['s1_odds3plus']:>7.1f}% {r['s3_dynamic']:>7.1f}% {r['s4_all_agree']:>7.1f}%")
    
    logger.info("-" * 60)
    logger.info(f"{'Avg':>6} {cond_df['base'].mean():>7.1f}% {cond_df['mu10'].mean():>7.1f}% {cond_df['s1_odds3plus'].mean():>7.1f}% {cond_df['s3_dynamic'].mean():>7.1f}% {cond_df['s4_all_agree'].mean():>7.1f}%")
    
    # 最終推奨
    logger.info("\n" + "=" * 80)
    logger.info("最終推奨")
    logger.info("=" * 80)
    
    best_strategy = 'Base'
    best_roi = cond_df['base'].mean()
    
    for col, name in [('mu10', '+μ10%'), ('s1_odds3plus', 'S1(オッズ3+)'), 
                      ('s3_dynamic', 'S3(動的重み)'), ('s4_all_agree', 'S4(3モデル一致)')]:
        avg = cond_df[col].mean()
        if avg > best_roi:
            best_roi = avg
            best_strategy = name
    
    logger.info(f"  最良戦略: {best_strategy} ({best_roi:.1f}%)")
    logger.info(f"  Baseからの改善: {best_roi - cond_df['base'].mean():+.1f}%")


if __name__ == "__main__":
    main()
