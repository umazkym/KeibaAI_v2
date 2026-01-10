# -*- coding: utf-8 -*-
"""
生データ深層分析スクリプト

具体的なレース事例を詳細に分析し、モデルの予測パターンを理解する
- 的中レースの特徴
- 失敗レースの特徴
- 人気と予測の関係
- オッズ効率性の詳細分析
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
import warnings
from tqdm import tqdm
warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15


def load_data():
    """データを読み込む"""
    print("データ読み込み中...")
    data_dir = project_root / "keibaai/data/parsed/parquet"
    
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    return races, pedigrees, corners, race_details, horses


def train_v15(train_df, valid_df, feature_cols):
    """V15モデル学習"""
    params = {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 5.0, 'reg_lambda': 8.0,
        'bagging_fraction': 0.6, 'bagging_freq': 3, 'feature_fraction': 0.6,
    }
    
    X_train = train_df[feature_cols].fillna(0)
    X_valid = valid_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] == 1).astype(int)
    y_valid = (valid_df['finish_position'] == 1).astype(int)
    
    train_ds = lgb.Dataset(X_train, y_train)
    valid_ds = lgb.Dataset(X_valid, y_valid, reference=train_ds)
    
    model = lgb.train(params, train_ds, num_boost_round=300, valid_sets=[valid_ds],
                      callbacks=[lgb.early_stopping(stopping_rounds=20, verbose=False)])
    return model


def analyze_prediction_patterns(test_df, preds, feature_cols, model):
    """予測パターンの詳細分析"""
    d = test_df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    # 予測Top1馬を抽出
    bet_df = d[d['rank_pred'] == 1].copy()
    bet_df['is_hit'] = bet_df['finish_position'] == 1
    
    # 勝馬の情報を取得
    winners = d[d['finish_position'] == 1][['race_id', 'horse_number', 'win_odds', 'score', 'rank_pred', 'popularity']].copy()
    winners.columns = ['race_id', 'winner_num', 'winner_odds', 'winner_score', 'winner_rank', 'winner_pop']
    
    bet_df = bet_df.merge(winners, on='race_id', how='left')
    
    return bet_df


def detailed_case_analysis(bet_df, test_df):
    """具体的事例の詳細分析"""
    results = {}
    
    # 的中 vs 失敗の基本統計
    hits = bet_df[bet_df['is_hit']]
    misses = bet_df[~bet_df['is_hit']]
    
    results['total_bets'] = len(bet_df)
    results['hits'] = len(hits)
    results['misses'] = len(misses)
    results['hit_rate'] = len(hits) / len(bet_df) * 100
    results['roi'] = hits['win_odds'].sum() / len(bet_df) * 100
    
    # 人気との相関分析
    if 'popularity' in bet_df.columns:
        # 予測馬が1番人気だった割合
        results['pred_is_1st_pop_rate'] = (bet_df['popularity'] == 1).mean() * 100
        # 予測馬が1-3番人気だった割合
        results['pred_is_top3_pop_rate'] = (bet_df['popularity'] <= 3).mean() * 100
        
        # 的中時と失敗時の人気差
        results['hit_avg_pop'] = hits['popularity'].mean() if len(hits) > 0 else 0
        results['miss_avg_pop'] = misses['popularity'].mean() if len(misses) > 0 else 0
    
    # モデルが人気をどれだけ独自に評価しているか
    # 人気1位を予測Top1にしなかった割合（独自性）
    not_1st_pop = bet_df[bet_df['popularity'] != 1]
    results['non_favorite_selection_rate'] = len(not_1st_pop) / len(bet_df) * 100
    
    # その場合のROI
    not_1st_hits = not_1st_pop[not_1st_pop['is_hit']]
    results['non_favorite_roi'] = not_1st_hits['win_odds'].sum() / len(not_1st_pop) * 100 if len(not_1st_pop) > 0 else 0
    
    # オッズ効率性分析
    results['avg_hit_odds'] = hits['win_odds'].mean() if len(hits) > 0 else 0
    results['avg_miss_odds'] = misses['win_odds'].mean() if len(misses) > 0 else 0
    
    # 失敗時の勝馬分析
    results['winner_was_rank2_rate'] = (misses['winner_rank'] == 2).mean() * 100 if len(misses) > 0 else 0
    results['winner_was_rank3_rate'] = (misses['winner_rank'] == 3).mean() * 100 if len(misses) > 0 else 0
    results['winner_was_top3_rate'] = (misses['winner_rank'] <= 3).mean() * 100 if len(misses) > 0 else 0
    results['winner_was_top5_rate'] = (misses['winner_rank'] <= 5).mean() * 100 if len(misses) > 0 else 0
    results['avg_winner_rank'] = misses['winner_rank'].mean() if len(misses) > 0 else 0
    
    # 穴馬（10倍以上）を当てた割合と貢献度
    high_odds_hits = hits[hits['win_odds'] >= 10]
    results['high_odds_hit_count'] = len(high_odds_hits)
    results['high_odds_hit_rate'] = len(high_odds_hits) / len(bet_df) * 100
    results['high_odds_contribution'] = high_odds_hits['win_odds'].sum() / len(bet_df) * 100  # ROIへの貢献
    
    return results


def analyze_model_vs_market(test_df, preds):
    """モデル予測 vs 市場（人気順）の比較"""
    d = test_df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    results = {}
    
    # 市場（人気順）のROI
    market_top1 = d[d['popularity'] == 1]
    market_hits = market_top1[market_top1['finish_position'] == 1]
    results['market_roi'] = market_hits['win_odds'].sum() / len(market_top1) * 100 if len(market_top1) > 0 else 0
    results['market_hit_rate'] = len(market_hits) / len(market_top1) * 100 if len(market_top1) > 0 else 0
    
    # モデルのROI
    model_top1 = d[d['rank_pred'] == 1]
    model_hits = model_top1[model_top1['finish_position'] == 1]
    results['model_roi'] = model_hits['win_odds'].sum() / len(model_top1) * 100 if len(model_top1) > 0 else 0
    results['model_hit_rate'] = len(model_hits) / len(model_top1) * 100 if len(model_top1) > 0 else 0
    
    # モデルと市場の一致度
    model_top1_set = set(zip(model_top1['race_id'], model_top1['horse_number']))
    market_top1_set = set(zip(market_top1['race_id'], market_top1['horse_number']))
    agreement = len(model_top1_set & market_top1_set) / len(model_top1_set) * 100
    results['model_market_agreement'] = agreement
    
    # 一致時のROI
    model_agrees_market = model_top1[model_top1['popularity'] == 1]
    agree_hits = model_agrees_market[model_agrees_market['finish_position'] == 1]
    results['agreement_roi'] = agree_hits['win_odds'].sum() / len(model_agrees_market) * 100 if len(model_agrees_market) > 0 else 0
    
    # 不一致時のROI（モデル独自選択）
    model_disagrees = model_top1[model_top1['popularity'] != 1]
    disagree_hits = model_disagrees[model_disagrees['finish_position'] == 1]
    results['disagreement_roi'] = disagree_hits['win_odds'].sum() / len(model_disagrees) * 100 if len(model_disagrees) > 0 else 0
    results['disagreement_count'] = len(model_disagrees)
    
    return results


def analyze_race_conditions(test_df, preds):
    """レース条件別の詳細分析"""
    d = test_df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    results = {}
    
    # 出走頭数別
    if 'field_size' in d.columns:
        for size_min, size_max, label in [(1, 8, '少頭数'), (9, 13, '中頭数'), (14, 18, '多頭数')]:
            mask = (d['field_size'] >= size_min) & (d['field_size'] <= size_max)
            bet_df = d[(d['rank_pred'] == 1) & mask]
            hits = bet_df[bet_df['finish_position'] == 1]
            if len(bet_df) > 100:
                results[f'roi_{label}'] = hits['win_odds'].sum() / len(bet_df) * 100
                results[f'count_{label}'] = len(bet_df)
    
    # クラス別
    if 'race_class' in d.columns:
        for rc in d['race_class'].dropna().unique():
            mask = d['race_class'] == rc
            bet_df = d[(d['rank_pred'] == 1) & mask]
            hits = bet_df[bet_df['finish_position'] == 1]
            if len(bet_df) > 50:
                results[f'roi_class_{int(rc)}'] = hits['win_odds'].sum() / len(bet_df) * 100
    
    return results


def show_specific_cases(bet_df, test_df, n_cases=10):
    """具体的なレース事例を表示"""
    print("\n" + "=" * 80)
    print("【具体的レース事例分析】")
    print("=" * 80)
    
    # 高配当的中事例
    hits = bet_df[bet_df['is_hit']].copy()
    if len(hits) > 0:
        high_odds_hits = hits.nlargest(min(n_cases, len(hits)), 'win_odds')
        print("\n■ 高配当的中事例 Top10")
        print("-" * 70)
        for _, row in high_odds_hits.iterrows():
            print(f"  {row['race_id']} | 的中: {row['horse_number']}番 | オッズ: {row['win_odds']:.1f}倍 | 人気: {row.get('popularity', '?')}人気")
    
    # 失敗事例（勝馬がモデル2位だった場合）
    misses = bet_df[~bet_df['is_hit']].copy()
    if len(misses) > 0:
        rank2_misses = misses[misses['winner_rank'] == 2].head(n_cases)
        print("\n■ 失敗事例（勝馬がモデル2位）Top10")
        print("-" * 70)
        for _, row in rank2_misses.iterrows():
            print(f"  {row['race_id']} | 予測: {row['horse_number']}番({row.get('popularity', '?')}人気) | 勝馬: {row['winner_num']}番({row['winner_pop']}人気) オッズ{row['winner_odds']:.1f}倍")
    
    # モデルが人気1位と異なる選択をして失敗した事例
    if 'popularity' in misses.columns:
        disagree_misses = misses[(misses['popularity'] != 1) & (misses['winner_pop'] == 1)].head(n_cases)
        print("\n■ 人気1位を外して失敗した事例 Top10")
        print("-" * 70)
        for _, row in disagree_misses.iterrows():
            print(f"  {row['race_id']} | 予測: {row['horse_number']}番({row['popularity']}人気) | 勝馬: 1番人気({row['winner_odds']:.1f}倍)")


def main():
    print("=" * 80)
    print("生データ深層分析")
    print("=" * 80)
    
    races, pedigrees, corners, race_details, horses = load_data()
    print(f"\n総データ: {len(races):,}件")
    
    # 直近2年（2024-2025）を詳細分析
    test_periods = [
        ('2025', '2023-12-31', '2024-01-01', '2024-12-31', '2025-01-01', '2025-12-31'),
        ('2024', '2022-12-31', '2023-01-01', '2023-12-31', '2024-01-01', '2024-12-31'),
    ]
    
    all_results = []
    
    for year, train_end, valid_start, valid_end, test_start, test_end in tqdm(test_periods, desc="分析中"):
        print(f"\n{'='*80}")
        print(f"[{year}年 詳細分析]")
        print("=" * 80)
        
        train = races[races['race_date'] <= train_end].copy()
        valid = races[(races['race_date'] >= valid_start) & (races['race_date'] < valid_end)].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] < test_end)].copy()
        
        print(f"Train: {len(train):,}, Valid: {len(valid):,}, Test: {len(test):,}")
        
        # 特徴量生成
        engine = LeakFreeFeatureEngineerV15()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        valid_f = engine.transform(valid)
        test_f = engine.transform(test)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        
        # モデル学習
        print("モデル学習中...")
        model = train_v15(train_f, valid_f, feature_cols)
        
        # 予測
        X_test = test_f[feature_cols].fillna(0)
        preds = model.predict(X_test)
        
        # 予測パターン分析
        bet_df = analyze_prediction_patterns(test_f, preds, feature_cols, model)
        
        # 詳細分析
        case_results = detailed_case_analysis(bet_df, test_f)
        market_results = analyze_model_vs_market(test_f, preds)
        condition_results = analyze_race_conditions(test_f, preds)
        
        print(f"\n【基本統計】")
        print(f"  ROI: {case_results['roi']:.1f}%, 的中率: {case_results['hit_rate']:.1f}%")
        
        print(f"\n【モデル vs 市場（人気）】")
        print(f"  市場（1番人気）ROI: {market_results['market_roi']:.1f}%, 的中率: {market_results['market_hit_rate']:.1f}%")
        print(f"  モデル ROI: {market_results['model_roi']:.1f}%, 的中率: {market_results['model_hit_rate']:.1f}%")
        print(f"  モデルと市場の一致率: {market_results['model_market_agreement']:.1f}%")
        print(f"  一致時ROI: {market_results['agreement_roi']:.1f}%")
        print(f"  不一致時ROI: {market_results['disagreement_roi']:.1f}% ({market_results['disagreement_count']}件)")
        
        print(f"\n【人気との関係】")
        print(f"  予測Top1が1番人気だった割合: {case_results['pred_is_1st_pop_rate']:.1f}%")
        print(f"  予測Top1が1-3番人気だった割合: {case_results['pred_is_top3_pop_rate']:.1f}%")
        print(f"  非1番人気選択時のROI: {case_results['non_favorite_roi']:.1f}%")
        
        print(f"\n【穴馬分析（オッズ10倍以上）】")
        print(f"  高配当的中数: {case_results['high_odds_hit_count']}件")
        print(f"  高配当貢献度: {case_results['high_odds_contribution']:.1f}% (ROIへの寄与)")
        
        print(f"\n【失敗分析】")
        print(f"  失敗時の勝馬のモデル順位（平均）: {case_results['avg_winner_rank']:.1f}位")
        print(f"  勝馬がTop3だった割合: {case_results['winner_was_top3_rate']:.1f}%")
        print(f"  勝馬がTop5だった割合: {case_results['winner_was_top5_rate']:.1f}%")
        
        # 具体的事例表示
        show_specific_cases(bet_df, test_f)
        
        all_results.append({
            'year': year,
            **case_results,
            **market_results,
            **condition_results
        })
    
    # 結果比較
    print("\n" + "=" * 80)
    print("2024年 vs 2025年 比較")
    print("=" * 80)
    
    if len(all_results) >= 2:
        r2024 = [r for r in all_results if r['year'] == '2024'][0] if any(r['year'] == '2024' for r in all_results) else None
        r2025 = [r for r in all_results if r['year'] == '2025'][0] if any(r['year'] == '2025' for r in all_results) else None
        
        if r2024 and r2025:
            print(f"\n{'指標':<30} {'2024年':>12} {'2025年':>12} {'変化':>12}")
            print("-" * 70)
            
            metrics = [
                ('ROI', 'roi', '%'),
                ('的中率', 'hit_rate', '%'),
                ('モデル-市場一致率', 'model_market_agreement', '%'),
                ('不一致時ROI', 'disagreement_roi', '%'),
                ('高配当貢献度', 'high_odds_contribution', '%'),
                ('失敗時勝馬平均順位', 'avg_winner_rank', '位'),
            ]
            
            for label, key, unit in metrics:
                v2024 = r2024.get(key, 0)
                v2025 = r2025.get(key, 0)
                diff = v2025 - v2024
                print(f"{label:<30} {v2024:>10.1f}{unit} {v2025:>10.1f}{unit} {diff:>+10.1f}{unit}")


if __name__ == "__main__":
    main()
