# -*- coding: utf-8 -*-
"""
V15+V4.4 Ensemble 詳細分析スクリプト v2

進捗表示付き、多角的分析版
リーク防止を徹底
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
import warnings
from tqdm import tqdm
from datetime import datetime
import time
warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15


def load_data():
    """データを読み込む（進捗表示付き）"""
    print("データ読み込み中...")
    data_dir = project_root / "keibaai/data/parsed/parquet"
    
    races = pd.read_parquet(data_dir / "races/races.parquet")
    print(f"  races: {len(races):,}件")
    
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    print(f"  pedigrees: {len(pedigrees):,}件")
    
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    print(f"  corners: {len(corners):,}件")
    
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    print(f"  race_details: {len(race_details):,}件")
    
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    print(f"  horses: {len(horses):,}件")
    
    # 前処理
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    return races, pedigrees, corners, race_details, horses


def train_v15(train_df, valid_df, feature_cols):
    """V15モデル学習（Binary Classification）"""
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


def train_v44(train_df, valid_df, feature_cols):
    """V4.4モデル学習（LambdaRank + オッズ加重）"""
    params = {
        'objective': 'lambdarank', 'metric': 'ndcg', 'eval_at': [1, 3, 5],
        'boosting_type': 'gbdt', 'num_leaves': 25, 'max_depth': 4,
        'min_child_samples': 150, 'learning_rate': 0.05,
        'reg_alpha': 8.0, 'reg_lambda': 12.0, 'feature_fraction': 0.5,
        'bagging_fraction': 0.6, 'bagging_freq': 5, 'verbose': -1,
        'random_state': 42, 'label_gain': list(range(100))
    }
    
    weight_1st, weight_2nd, weight_3rd = 12.74, 6.73, 3.69
    
    train_df = train_df.copy()
    valid_df = valid_df.copy()
    
    for d in [train_df, valid_df]:
        odds = d['win_odds'].fillna(1.0).clip(upper=90)
        log_odds = np.log1p(odds)
        gain = np.zeros(len(d))
        gain[d['finish_position'] == 1] = log_odds[d['finish_position'] == 1] * weight_1st
        gain[d['finish_position'] == 2] = log_odds[d['finish_position'] == 2] * weight_2nd
        gain[d['finish_position'] == 3] = log_odds[d['finish_position'] == 3] * weight_3rd
        d['target_relevance'] = gain.astype(int)
        d['sample_weight'] = np.log1p(d['win_odds'].fillna(1.0)).clip(upper=np.log1p(100))
    
    X_train = train_df[feature_cols].fillna(0)
    X_valid = valid_df[feature_cols].fillna(0)
    
    groups_train = train_df.groupby('race_id').size().to_list()
    groups_valid = valid_df.groupby('race_id').size().to_list()
    
    model = lgb.LGBMRanker(**params, n_estimators=300)
    model.fit(X_train, train_df['target_relevance'], group=groups_train, 
              sample_weight=train_df['sample_weight'],
              eval_set=[(X_valid, valid_df['target_relevance'])],
              eval_group=[groups_valid],
              callbacks=[lgb.early_stopping(stopping_rounds=20, verbose=False)])
    return model


def analyze_predictions(test_df, preds, model_name):
    """予測結果の詳細分析（リークなし）"""
    d = test_df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    results = {}
    
    # Top1予測のROI・的中率
    bet_df = d[d['rank_pred'] == 1].copy()
    hits = bet_df[bet_df['finish_position'] == 1]
    
    results['roi'] = hits['win_odds'].sum() / len(bet_df) * 100 if len(bet_df) > 0 else 0
    results['hit_rate'] = len(hits) / len(bet_df) * 100 if len(bet_df) > 0 else 0
    results['bet_count'] = len(bet_df)
    results['hit_count'] = len(hits)
    results['avg_odds_hit'] = hits['win_odds'].mean() if len(hits) > 0 else 0
    
    # 予測Top1馬の人気分析
    if 'popularity' in bet_df.columns:
        results['avg_popularity_bet'] = bet_df['popularity'].mean()
        # 的中時vs外れ時の人気差
        results['avg_popularity_hit'] = hits['popularity'].mean() if len(hits) > 0 else 0
        
        # 人気帯別の詳細
        for pop_min, pop_max, label in [(1,1,'1人気'), (2,3,'2-3人気'), (4,6,'4-6人気'), (7,10,'7-10人気'), (11,99,'11人気～')]:
            mask = (bet_df['popularity'] >= pop_min) & (bet_df['popularity'] <= pop_max)
            pop_bets = bet_df[mask]
            pop_hits = pop_bets[pop_bets['finish_position'] == 1]
            if len(pop_bets) > 0:
                results[f'roi_{label}'] = pop_hits['win_odds'].sum() / len(pop_bets) * 100
                results[f'hit_rate_{label}'] = len(pop_hits) / len(pop_bets) * 100
                results[f'count_{label}'] = len(pop_bets)
    
    # オッズ帯別の詳細
    if 'win_odds' in bet_df.columns:
        for odds_min, odds_max, label in [(1,2,'1-2倍'), (2,5,'2-5倍'), (5,10,'5-10倍'), (10,30,'10-30倍'), (30,999,'30倍～')]:
            mask = (bet_df['win_odds'] >= odds_min) & (bet_df['win_odds'] < odds_max)
            odds_bets = bet_df[mask]
            odds_hits = odds_bets[odds_bets['finish_position'] == 1]
            if len(odds_bets) > 0:
                results[f'roi_odds_{label}'] = odds_hits['win_odds'].sum() / len(odds_bets) * 100
                results[f'hit_rate_odds_{label}'] = len(odds_hits) / len(odds_bets) * 100
                results[f'count_odds_{label}'] = len(odds_bets)
    
    return results


def analyze_by_month(test_df, preds):
    """月別分析"""
    d = test_df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    d['month'] = d['race_date'].dt.month
    
    results = []
    for month in range(1, 13):
        mask = d['month'] == month
        if mask.sum() < 50:
            continue
        
        bet_df = d[(d['rank_pred'] == 1) & mask]
        hits = bet_df[bet_df['finish_position'] == 1]
        
        if len(bet_df) > 0:
            results.append({
                'month': month,
                'roi': hits['win_odds'].sum() / len(bet_df) * 100,
                'hit_rate': len(hits) / len(bet_df) * 100,
                'bet_count': len(bet_df)
            })
    
    return pd.DataFrame(results)


def analyze_by_venue(test_df, preds):
    """競馬場別分析"""
    d = test_df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    results = []
    for venue in d['venue'].dropna().unique():
        mask = d['venue'] == venue
        if mask.sum() < 100:
            continue
        
        bet_df = d[(d['rank_pred'] == 1) & mask]
        hits = bet_df[bet_df['finish_position'] == 1]
        
        if len(bet_df) > 0:
            results.append({
                'venue': venue,
                'roi': hits['win_odds'].sum() / len(bet_df) * 100,
                'hit_rate': len(hits) / len(bet_df) * 100,
                'bet_count': len(bet_df)
            })
    
    return pd.DataFrame(results)


def analyze_failures(test_df, preds):
    """失敗パターンの詳細分析"""
    d = test_df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    # Top1予測で外したケース
    bet_df = d[d['rank_pred'] == 1].copy()
    failures = bet_df[bet_df['finish_position'] != 1].copy()
    
    # 勝った馬の情報を取得
    winners = d[d['finish_position'] == 1][['race_id', 'popularity', 'win_odds', 'rank_pred']].copy()
    winners.columns = ['race_id', 'winner_pop', 'winner_odds', 'winner_model_rank']
    
    failures = failures.merge(winners, on='race_id', how='left')
    
    return {
        'failure_count': len(failures),
        'avg_predicted_odds': failures['win_odds'].mean() if len(failures) > 0 else 0,
        'avg_winner_odds': failures['winner_odds'].mean() if len(failures) > 0 else 0,
        'avg_winner_model_rank': failures['winner_model_rank'].mean() if len(failures) > 0 else 0,
        'winner_was_rank2_pct': (failures['winner_model_rank'] == 2).mean() * 100 if len(failures) > 0 else 0,
        'winner_was_rank3_pct': (failures['winner_model_rank'] == 3).mean() * 100 if len(failures) > 0 else 0,
        'winner_was_top3_pct': (failures['winner_model_rank'] <= 3).mean() * 100 if len(failures) > 0 else 0,
    }


def main():
    start_time = time.time()
    
    print("=" * 80)
    print("V15+V4.4 Ensemble 詳細分析 v2")
    print("=" * 80)
    
    races, pedigrees, corners, race_details, horses = load_data()
    print(f"\n総データ: {len(races):,}件 ({races['race_date'].min().date()} - {races['race_date'].max().date()})")
    
    # 6年間のWalk-forward検証（高速化のため期間短縮）
    test_periods = [
        ('2025', '2023-12-31', '2024-01-01', '2024-12-31', '2025-01-01', '2025-12-31'),
        ('2024', '2022-12-31', '2023-01-01', '2023-12-31', '2024-01-01', '2024-12-31'),
        ('2023', '2021-12-31', '2022-01-01', '2022-12-31', '2023-01-01', '2023-12-31'),
        ('2022', '2020-12-31', '2021-01-01', '2021-12-31', '2022-01-01', '2022-12-31'),
        ('2021', '2019-12-31', '2020-01-01', '2020-12-31', '2021-01-01', '2021-12-31'),
        ('2020', '2018-12-31', '2019-01-01', '2019-12-31', '2020-01-01', '2020-12-31'),
    ]
    
    all_results = []
    all_month_results = []
    all_venue_results = []
    all_failure_analysis = []
    
    for i, (year, train_end, valid_start, valid_end, test_start, test_end) in enumerate(tqdm(test_periods, desc="Walk-forward検証")):
        print(f"\n[{year}年] 処理中...")
        
        train = races[races['race_date'] <= train_end].copy()
        valid = races[(races['race_date'] >= valid_start) & (races['race_date'] < valid_end)].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] < test_end)].copy()
        
        print(f"  Train: {len(train):,}, Valid: {len(valid):,}, Test: {len(test):,}")
        
        # 特徴量生成
        print("  特徴量生成中...")
        engine = LeakFreeFeatureEngineerV15()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        valid_f = engine.transform(valid)
        test_f = engine.transform(test)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        print(f"  特徴量数: {len(feature_cols)}")
        
        # モデル学習
        print("  V15学習中...")
        model_v15 = train_v15(train_f, valid_f, feature_cols)
        
        print("  V4.4学習中...")
        model_v44 = train_v44(train_f, valid_f, feature_cols)
        
        # 予測
        X_test = test_f[feature_cols].fillna(0)
        preds_v15 = model_v15.predict(X_test)
        preds_v44 = model_v44.predict(X_test)
        
        # 正規化
        preds_v15_norm = (preds_v15 - preds_v15.min()) / (preds_v15.max() - preds_v15.min() + 1e-8)
        preds_v44_norm = (preds_v44 - preds_v44.min()) / (preds_v44.max() - preds_v44.min() + 1e-8)
        
        # アンサンブル（50:50平均）
        preds_ensemble = (preds_v15_norm + preds_v44_norm) / 2
        
        # 詳細分析
        result_v15 = analyze_predictions(test_f, preds_v15, 'V15')
        result_v44 = analyze_predictions(test_f, preds_v44, 'V4.4')
        result_ensemble = analyze_predictions(test_f, preds_ensemble, 'Ensemble')
        
        print(f"  V15:      ROI {result_v15['roi']:.1f}%, 的中率 {result_v15['hit_rate']:.1f}%")
        print(f"  V4.4:     ROI {result_v44['roi']:.1f}%, 的中率 {result_v44['hit_rate']:.1f}%")
        print(f"  Ensemble: ROI {result_ensemble['roi']:.1f}%, 的中率 {result_ensemble['hit_rate']:.1f}%")
        
        all_results.append({
            'year': year,
            **{f'v15_{k}': v for k, v in result_v15.items()},
            **{f'v44_{k}': v for k, v in result_v44.items()},
            **{f'ens_{k}': v for k, v in result_ensemble.items()},
        })
        
        # 月別分析
        month_df = analyze_by_month(test_f, preds_ensemble)
        month_df['year'] = year
        all_month_results.append(month_df)
        
        # 競馬場別分析
        venue_df = analyze_by_venue(test_f, preds_ensemble)
        venue_df['year'] = year
        all_venue_results.append(venue_df)
        
        # 失敗分析
        failure_result = analyze_failures(test_f, preds_ensemble)
        failure_result['year'] = year
        all_failure_analysis.append(failure_result)
    
    elapsed = time.time() - start_time
    print(f"\n\n処理時間: {elapsed/60:.1f}分")
    
    # ===== サマリー表示 =====
    print("\n" + "=" * 80)
    print("総合サマリー")
    print("=" * 80)
    
    results_df = pd.DataFrame(all_results)
    
    print("\n【年別ROI推移】")
    print("-" * 70)
    print(f"{'年':<6} {'V15 ROI':>10} {'V4.4 ROI':>10} {'Ens ROI':>10} {'Ens的中率':>10} {'ベット数':>10}")
    print("-" * 70)
    for _, row in results_df.iterrows():
        print(f"{row['year']:<6} {row['v15_roi']:>9.1f}% {row['v44_roi']:>9.1f}% {row['ens_roi']:>9.1f}% {row['ens_hit_rate']:>9.1f}% {int(row['ens_bet_count']):>10}")
    print("-" * 70)
    print(f"{'平均':<6} {results_df['v15_roi'].mean():>9.1f}% {results_df['v44_roi'].mean():>9.1f}% {results_df['ens_roi'].mean():>9.1f}% {results_df['ens_hit_rate'].mean():>9.1f}%")
    print(f"{'σ':<6} {results_df['v15_roi'].std():>9.1f}% {results_df['v44_roi'].std():>9.1f}% {results_df['ens_roi'].std():>9.1f}%")
    
    # 人気帯別ROI
    print("\n【人気帯別ROI（Ensemble, 全期間平均）】")
    print("-" * 50)
    for label in ['1人気', '2-3人気', '4-6人気', '7-10人気', '11人気～']:
        roi_col = f'ens_roi_{label}'
        hr_col = f'ens_hit_rate_{label}'
        cnt_col = f'ens_count_{label}'
        if roi_col in results_df.columns:
            avg_roi = results_df[roi_col].mean()
            avg_hr = results_df[hr_col].mean()
            total_cnt = results_df[cnt_col].sum()
            print(f"  {label:>10}: ROI {avg_roi:>6.1f}%, 的中率 {avg_hr:>5.1f}%, 計{int(total_cnt):>5}件")
    
    # オッズ帯別ROI
    print("\n【オッズ帯別ROI（Ensemble, 全期間平均）】")
    print("-" * 50)
    for label in ['1-2倍', '2-5倍', '5-10倍', '10-30倍', '30倍～']:
        roi_col = f'ens_roi_odds_{label}'
        hr_col = f'ens_hit_rate_odds_{label}'
        cnt_col = f'ens_count_odds_{label}'
        if roi_col in results_df.columns:
            avg_roi = results_df[roi_col].mean()
            avg_hr = results_df[hr_col].mean()
            total_cnt = results_df[cnt_col].sum()
            print(f"  {label:>10}: ROI {avg_roi:>6.1f}%, 的中率 {avg_hr:>5.1f}%, 計{int(total_cnt):>5}件")
    
    # 月別サマリー
    print("\n【月別ROI（Ensemble, 全期間集計）】")
    print("-" * 50)
    month_all = pd.concat(all_month_results, ignore_index=True)
    month_summary = month_all.groupby('month').agg({'roi': 'mean', 'hit_rate': 'mean', 'bet_count': 'sum'}).round(1)
    for month in range(1, 13):
        if month in month_summary.index:
            row = month_summary.loc[month]
            print(f"  {month:>2}月: ROI {row['roi']:>6.1f}%, 的中率 {row['hit_rate']:>5.1f}%, 計{int(row['bet_count']):>5}件")
    
    # 競馬場別サマリー
    print("\n【競馬場別ROI（Ensemble, 全期間集計）】")
    print("-" * 50)
    venue_all = pd.concat(all_venue_results, ignore_index=True)
    venue_summary = venue_all.groupby('venue').agg({'roi': 'mean', 'hit_rate': 'mean', 'bet_count': 'sum'}).round(1)
    venue_summary = venue_summary.sort_values('roi', ascending=False)
    for venue in venue_summary.index:
        row = venue_summary.loc[venue]
        print(f"  {venue:>4}: ROI {row['roi']:>6.1f}%, 的中率 {row['hit_rate']:>5.1f}%, 計{int(row['bet_count']):>5}件")
    
    # 失敗分析サマリー
    print("\n【失敗パターン分析（Ensemble）】")
    print("-" * 50)
    failure_df = pd.DataFrame(all_failure_analysis)
    print(f"  総失敗数: {int(failure_df['failure_count'].sum()):,}件")
    print(f"  外した時の予測馬の平均オッズ: {failure_df['avg_predicted_odds'].mean():.1f}倍")
    print(f"  外した時の勝馬の平均オッズ: {failure_df['avg_winner_odds'].mean():.1f}倍")
    print(f"  勝馬がモデル2位だった割合: {failure_df['winner_was_rank2_pct'].mean():.1f}%")
    print(f"  勝馬がモデルTop3だった割合: {failure_df['winner_was_top3_pct'].mean():.1f}%")
    
    # 結果保存
    output_dir = project_root / "outputs" / "analysis"
    output_dir.mkdir(parents=True, exist_ok=True)
    results_df.to_csv(output_dir / "ensemble_detailed_analysis.csv", index=False)
    print(f"\n結果を {output_dir / 'ensemble_detailed_analysis.csv'} に保存しました。")
    
    print("\n" + "=" * 80)
    print("分析完了")
    print("=" * 80)


if __name__ == "__main__":
    main()
