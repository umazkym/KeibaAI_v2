#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
本番モデル統合検証: V15 vs V16 (jw_rank_in_race追加)

【検証内容】
1. LeakFreeFeatureEngineerV15（現行）vs V16（jw_rank追加）
2. MultiTargetPredictorで実際の予測
3. 複数期間（2020-2025）で的中率・回収率の再現性確認

【リーク・過学習防止】
- Walk-forward: 過去データで学習、未来データで検証
- 年度別独立検証
- 本番と同じ設定を使用

作成日: 2026-01-12
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')
import logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
from keibaai.src.features.leak_free_feature_engineer_v16 import LeakFreeFeatureEngineerV16
from keibaai.src.models.multi_target_predictor import MultiTargetPredictor

print("="*70)
print("本番モデル統合検証: V15 vs V16")
print("="*70)

# ============================================================
# データ読み込み
# ============================================================
data_dir = project_root / "keibaai/data/parsed/parquet"

races = pd.read_parquet(data_dir / "races/races.parquet")
pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
horses = pd.read_parquet(data_dir / "horses/horses.parquet")
returns = pd.read_parquet(data_dir / "returns/returns.parquet")

races = races[races['finish_position'].notna()].copy()
races['race_date'] = pd.to_datetime(races['race_date'])

# 除外
new_horse_mask = races['race_name'].str.contains('新馬', na=False)
obstacle_mask = races['track_surface'].str.contains('障', na=False) | races['race_name'].str.contains('障害', na=False)
races = races[~(new_horse_mask | obstacle_mask)].copy()

print(f"データ: {len(races):,}件")

# ============================================================
# Walk-forward検証関数
# ============================================================
def run_year_validation(races, pedigrees, corners, race_details, horses, test_year, version='V15'):
    """1年分の予測（V15 or V16）"""
    test_start_dt = pd.to_datetime(f'{test_year}-01-01')
    test_end_dt = pd.to_datetime(f'{test_year}-12-31')
    train_end = test_start_dt - timedelta(days=1)
    valid_start = train_end - timedelta(days=365)
    
    train = races[races['race_date'] <= train_end].copy()
    test = races[(races['race_date'] >= test_start_dt) & (races['race_date'] <= test_end_dt)].copy()
    
    if len(test) < 5000:
        return None
    
    # 特徴量エンジン選択
    if version == 'V15':
        engine = LeakFreeFeatureEngineerV15()
    else:
        engine = LeakFreeFeatureEngineerV16()
    
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    all_data = pd.concat([train, test], ignore_index=True)
    all_data = all_data.drop_duplicates(subset=['race_id', 'horse_number']).reset_index(drop=True)
    all_data_f = engine.transform(all_data)
    
    train_f = all_data_f[all_data_f['race_date'] <= train_end].copy()
    valid_f = train_f[train_f['race_date'] > valid_start].copy()
    test_f = all_data_f[(all_data_f['race_date'] >= test_start_dt) & (all_data_f['race_date'] <= test_end_dt)].copy()
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    # 予測モデル
    predictor = MultiTargetPredictor(
        surface_specific=True,
        use_v44_residual=True,
        regularization_level='strong',
        use_early_stopping=False,
        fixed_iterations=50
    )
    predictor.fit(train_f, valid_f, feature_cols, num_boost_round=300)
    
    test_preds = predictor.predict(test_f)
    
    test_preds = test_preds.merge(
        test_f[['race_id', 'horse_number', 'finish_position', 'win_odds', 'popularity', 'race_date']],
        on=['race_id', 'horse_number'],
        how='left'
    )
    
    test_preds['year'] = test_year
    test_preds['rank'] = test_preds.groupby('race_id')['ensemble_score'].rank(ascending=False, method='first')
    test_preds['version'] = version
    
    return test_preds

# ============================================================
# 単勝ROI計算
# ============================================================
def calc_tansho_roi(preds_df, target_ranks):
    """単勝ROI計算"""
    targets = preds_df[preds_df['rank'].isin(target_ranks)]
    hits = targets[targets['finish_position'] == 1]
    
    total_bet = len(targets)
    total_return = hits['win_odds'].sum() if 'win_odds' in hits.columns else 0
    
    roi = total_return / total_bet * 100 if total_bet > 0 else 0
    hit_rate = len(hits) / total_bet * 100 if total_bet > 0 else 0
    
    return roi, total_bet, len(hits), hit_rate

def calc_place_hit_rate(preds_df, target_ranks):
    """複勝的中率"""
    targets = preds_df[preds_df['rank'].isin(target_ranks)]
    hits = targets[targets['finish_position'] <= 3]
    
    total_bet = len(targets)
    hit_rate = len(hits) / total_bet * 100 if total_bet > 0 else 0
    
    return hit_rate, total_bet, len(hits)

# ============================================================
# 複数年検証
# ============================================================
print("\n" + "="*70)
print("【Walk-Forward検証】2020-2025")
print("="*70)

test_years = [2020, 2021, 2022, 2023, 2024]
results = []

for test_year in test_years:
    print(f"\n■ {test_year}年:")
    
    for version in ['V15', 'V16']:
        print(f"  {version} 予測中...", end=" ", flush=True)
        try:
            preds = run_year_validation(races, pedigrees, corners, race_details, horses, test_year, version)
            
            if preds is not None:
                # Top1単勝ROI
                roi, bets, hits, hit_rate = calc_tansho_roi(preds, [1])
                # Top3複勝的中率
                place_hit, place_bets, place_hits = calc_place_hit_rate(preds, [1, 2, 3])
                
                results.append({
                    'year': test_year,
                    'version': version,
                    'tansho_roi': roi,
                    'tansho_bets': bets,
                    'tansho_hits': hits,
                    'tansho_hit_rate': hit_rate,
                    'place_hit_rate': place_hit,
                    'n_features': len([c for c in preds.columns if 'pred' not in c.lower()]),
                })
                
                print(f"単勝ROI {roi:.1f}%, 複勝的中率 {place_hit:.1f}%")
            else:
                print("データ不足でスキップ")
        except Exception as e:
            print(f"エラー: {e}")

# ============================================================
# 結果サマリ
# ============================================================
print("\n" + "="*70)
print("【結果サマリ】")
print("="*70)

results_df = pd.DataFrame(results)

if len(results_df) > 0:
    # 年度別比較
    print("\n■ 年度別単勝ROI比較:")
    pivot = results_df.pivot(index='year', columns='version', values='tansho_roi')
    if 'V15' in pivot.columns and 'V16' in pivot.columns:
        pivot['V16-V15'] = pivot['V16'] - pivot['V15']
    print(pivot.to_string())
    
    # 平均
    print("\n■ 5年平均:")
    for version in ['V15', 'V16']:
        v_data = results_df[results_df['version'] == version]
        if len(v_data) > 0:
            avg_roi = v_data['tansho_roi'].mean()
            avg_place = v_data['place_hit_rate'].mean()
            total_bets = v_data['tansho_bets'].sum()
            total_hits = v_data['tansho_hits'].sum()
            print(f"  {version}: 単勝ROI {avg_roi:.1f}%, 複勝的中率 {avg_place:.1f}% (計{total_bets:,}件, {total_hits:,}的中)")
    
    # 改善率
    if 'V15' in results_df['version'].values and 'V16' in results_df['version'].values:
        v15_avg = results_df[results_df['version'] == 'V15']['tansho_roi'].mean()
        v16_avg = results_df[results_df['version'] == 'V16']['tansho_roi'].mean()
        improvement = v16_avg - v15_avg
        
        print(f"\n■ V16改善効果: {improvement:+.1f}%")
        
        # 再現性チェック
        years_v16_wins = 0
        for year in test_years:
            y_data = results_df[results_df['year'] == year]
            if len(y_data) == 2:
                v15_roi = y_data[y_data['version'] == 'V15']['tansho_roi'].values[0]
                v16_roi = y_data[y_data['version'] == 'V16']['tansho_roi'].values[0]
                if v16_roi > v15_roi:
                    years_v16_wins += 1
        
        print(f"■ V16勝利年度: {years_v16_wins}/{len(test_years)}年 ({years_v16_wins/len(test_years)*100:.0f}%)")
    
    # 保存
    results_df.to_csv(project_root / 'outputs/analysis/v15_v16_comparison.csv', index=False)
    print("\n結果保存: v15_v16_comparison.csv")

# ============================================================
# 結論
# ============================================================
print("\n" + "="*70)
print("【結論】")
print("="*70)

if len(results_df) > 0 and 'V15' in results_df['version'].values and 'V16' in results_df['version'].values:
    v15_avg = results_df[results_df['version'] == 'V15']['tansho_roi'].mean()
    v16_avg = results_df[results_df['version'] == 'V16']['tansho_roi'].mean()
    
    print(f"""
■ 本番モデル比較結果:
  V15（現行）: 単勝ROI {v15_avg:.1f}%
  V16（jw_rank追加）: 単勝ROI {v16_avg:.1f}%
  改善: {v16_avg - v15_avg:+.1f}%

■ 再現性:
  V16が優位な年度: {years_v16_wins}/{len(test_years)}年
  
■ 推奨:
  {'V16の本番採用を推奨' if v16_avg > v15_avg else '追加検証が必要'}
""")
