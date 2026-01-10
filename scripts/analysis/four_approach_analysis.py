# -*- coding: utf-8 -*-
"""
ROI>100%達成 - 4アプローチ包括検証

1. ワイド/馬連での期待値計算
2. 2022年崩壊パターン分析
3. 複合条件フィルタ探索
4. 馬券種動的選定シミュレーション
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
import warnings
from itertools import combinations
warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from keibaai.src.features.leak_free_feature_engineer_v33 import LeakFreeFeatureEngineerV33


def load_data():
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


def train_model(train_df, valid_df, feature_cols):
    params = {
        'objective': 'binary',
        'metric': 'auc',
        'verbosity': -1,
        'learning_rate': 0.02,
        'num_leaves': 8,
        'max_depth': 2,
        'min_child_samples': 300,
        'reg_alpha': 15.0,
        'reg_lambda': 20.0,
        'bagging_fraction': 0.5,
        'bagging_freq': 3,
        'feature_fraction': 0.5,
        'random_state': 42,
    }
    
    X_train = train_df[feature_cols].fillna(0)
    X_valid = valid_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] == 1).astype(int)
    y_valid = (valid_df['finish_position'] == 1).astype(int)
    
    train_ds = lgb.Dataset(X_train, y_train)
    valid_ds = lgb.Dataset(X_valid, y_valid, reference=train_ds)
    
    model = lgb.train(params, train_ds, num_boost_round=500, valid_sets=[valid_ds],
                      callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False)])
    
    return model


# =====================================================================
# アプローチ1: ワイド/馬連での期待値計算
# =====================================================================
def analyze_wide_umaren(test_df, preds):
    """
    ワイド・馬連でのROI分析
    モデルのTop2馬でワイド/馬連を購入した場合のシミュレーション
    """
    d = test_df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    # レースごとにTop2馬を取得
    top2 = d[d['rank_pred'] <= 2].copy()
    
    results_by_race = []
    
    for race_id, group in top2.groupby('race_id'):
        if len(group) < 2:
            continue
        
        horses = group.sort_values('rank_pred')
        h1 = horses.iloc[0]
        h2 = horses.iloc[1]
        
        # 実際の着順
        pos1 = h1['finish_position']
        pos2 = h2['finish_position']
        
        # ワイド的中条件: 両方がTop3
        wide_hit = (pos1 <= 3) and (pos2 <= 3)
        
        # 馬連的中条件: 両方がTop2
        umaren_hit = (pos1 <= 2) and (pos2 <= 2)
        
        results_by_race.append({
            'race_id': race_id,
            'year': h1['race_date'].year,
            'wide_hit': wide_hit,
            'umaren_hit': umaren_hit,
            'h1_odds': h1['win_odds'],
            'h2_odds': h2['win_odds'],
            'h1_pos': pos1,
            'h2_pos': pos2,
        })
    
    result_df = pd.DataFrame(results_by_race)
    
    # ワイド/馬連オッズは実際のデータがないため概算
    # ワイドオッズ ≈ (単勝オッズ1 + 単勝オッズ2) / 3
    # 馬連オッズ ≈ 単勝オッズ1 × 単勝オッズ2 / 10
    result_df['wide_odds_est'] = (result_df['h1_odds'] + result_df['h2_odds']) / 3
    result_df['umaren_odds_est'] = result_df['h1_odds'] * result_df['h2_odds'] / 10
    
    return result_df


# =====================================================================
# アプローチ2: 2022年崩壊パターン分析
# =====================================================================
def analyze_2022_failure(test_df, preds, year):
    """
    特定年の失敗パターンを分析
    """
    d = test_df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    bet_df = d[d['rank_pred'] == 1].copy()
    bet_df['is_hit'] = bet_df['finish_position'] == 1
    
    # オッズ帯別
    bet_df['odds_band'] = pd.cut(bet_df['win_odds'], 
        bins=[0, 2, 5, 10, 20, 50, 1000], 
        labels=['1-2倍', '2-5倍', '5-10倍', '10-20倍', '20-50倍', '50倍以上'])
    
    # 20-50倍帯の失敗レースを分析
    high_odds = bet_df[(bet_df['win_odds'] >= 20) & (bet_df['win_odds'] < 50)]
    failed = high_odds[~high_odds['is_hit']]
    
    # 失敗レースの特徴を分析
    analysis = {
        'year': year,
        'total_bets': len(high_odds),
        'hit_count': high_odds['is_hit'].sum(),
        'hit_rate': high_odds['is_hit'].mean() * 100,
        'avg_score': high_odds['score'].mean(),
        'failed_avg_score': failed['score'].mean() if len(failed) > 0 else 0,
    }
    
    return analysis


# =====================================================================
# アプローチ3: 複合条件フィルタ探索
# =====================================================================
def search_compound_filters(test_df, preds, year):
    """
    複合条件（オッズ帯 + 期待値 + スコア閾値）の探索
    """
    d = test_df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    bet_df = d[d['rank_pred'] == 1].copy()
    bet_df['is_hit'] = bet_df['finish_position'] == 1
    
    # スコアを正規化
    min_score = bet_df['score'].min()
    max_score = bet_df['score'].max()
    bet_df['norm_score'] = (bet_df['score'] - min_score) / (max_score - min_score + 1e-10)
    bet_df['ev_estimate'] = bet_df['norm_score'] * bet_df['win_odds']
    
    results = []
    
    # 複合条件の探索
    odds_ranges = [(5, 50), (10, 50), (20, 50), (10, 30)]
    ev_thresholds = [0.8, 1.0, 1.2, 1.5]
    score_thresholds = [0.5, 0.6, 0.7]
    
    for odds_low, odds_high in odds_ranges:
        for ev_thresh in ev_thresholds:
            for score_thresh in score_thresholds:
                # フィルタ適用
                filtered = bet_df[
                    (bet_df['win_odds'] >= odds_low) & 
                    (bet_df['win_odds'] < odds_high) &
                    (bet_df['ev_estimate'] >= ev_thresh) &
                    (bet_df['norm_score'] >= score_thresh)
                ]
                
                if len(filtered) >= 10:  # 最低10件
                    hits = filtered[filtered['is_hit']]
                    roi = hits['win_odds'].sum() / len(filtered) * 100
                    
                    results.append({
                        'year': year,
                        'odds_range': f'{odds_low}-{odds_high}',
                        'ev_threshold': ev_thresh,
                        'score_threshold': score_thresh,
                        'roi': roi,
                        'bet_count': len(filtered),
                        'hit_count': len(hits),
                    })
    
    return pd.DataFrame(results)


# =====================================================================
# アプローチ4: 馬券種動的選定
# =====================================================================
def simulate_dynamic_bet_selection(test_df, preds, year):
    """
    レース条件に応じた馬券種選定シミュレーション
    
    ルール例:
    - 高オッズ（20-50倍）＆高スコア → 単勝
    - 中オッズ（5-20倍）＆Top2どちらも有力 → ワイド/馬連
    - 本命オッズ（2-5倍）＆高的中率 → 単勝小額
    """
    d = test_df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    results = []
    
    for race_id, group in d.groupby('race_id'):
        top1 = group[group['rank_pred'] == 1].iloc[0]
        top2 = group[group['rank_pred'] == 2].iloc[0] if len(group) >= 2 else None
        
        # 動的選定ロジック
        h1_odds = top1['win_odds']
        h1_score = top1['score']
        h1_pos = top1['finish_position']
        
        bet_type = 'skip'
        hit = False
        return_amount = 0
        
        if h1_odds >= 20 and h1_odds < 50:
            # 高オッズ → 単勝
            bet_type = '単勝'
            hit = (h1_pos == 1)
            return_amount = h1_odds if hit else 0
            
        elif h1_odds >= 5 and h1_odds < 20 and top2 is not None:
            # 中オッズ → ワイド（Top2両方がTop3なら的中）
            bet_type = 'ワイド'
            h2_pos = top2['finish_position']
            hit = (h1_pos <= 3) and (h2_pos <= 3)
            # ワイドオッズ概算
            if hit:
                return_amount = (h1_odds + top2['win_odds']) / 3
        
        if bet_type != 'skip':
            results.append({
                'race_id': race_id,
                'year': year,
                'bet_type': bet_type,
                'hit': hit,
                'return': return_amount,
            })
    
    return pd.DataFrame(results)


def main():
    print("=" * 80)
    print("ROI>100%達成 - 4アプローチ包括検証")
    print("=" * 80)
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    # 6年検証
    test_periods = [
        (2025, '2023-12-31', '2025-01-01', '2025-12-31'),
        (2024, '2022-12-31', '2024-01-01', '2024-12-31'),
        (2023, '2021-12-31', '2023-01-01', '2023-12-31'),
        (2022, '2020-12-31', '2022-01-01', '2022-12-31'),
        (2021, '2019-12-31', '2021-01-01', '2021-12-31'),
        (2020, '2018-12-31', '2020-01-01', '2020-12-31'),
    ]
    
    all_wide_results = []
    all_failure_analysis = []
    all_compound_results = []
    all_dynamic_results = []
    
    for year, train_end, test_start, test_end in test_periods:
        print(f"\n【{year}年】分析中...")
        
        train = races[races['race_date'] <= train_end].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] <= test_end)].copy()
        
        if len(test) == 0:
            continue
        
        engine = LeakFreeFeatureEngineerV33()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        test_f = engine.transform(test)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        
        val_start = pd.to_datetime(train_end) - pd.DateOffset(years=1)
        train_sub = train_f[train_f['race_date'] < val_start]
        valid_sub = train_f[train_f['race_date'] >= val_start]
        
        model = train_model(train_sub, valid_sub, feature_cols)
        
        X_test = test_f[feature_cols].fillna(0)
        preds = model.predict(X_test)
        
        # アプローチ1: ワイド/馬連
        wide_result = analyze_wide_umaren(test_f, preds)
        all_wide_results.append(wide_result)
        
        # アプローチ2: 崩壊分析
        failure = analyze_2022_failure(test_f, preds, year)
        all_failure_analysis.append(failure)
        
        # アプローチ3: 複合条件
        compound = search_compound_filters(test_f, preds, year)
        all_compound_results.append(compound)
        
        # アプローチ4: 動的選定
        dynamic = simulate_dynamic_bet_selection(test_f, preds, year)
        all_dynamic_results.append(dynamic)
    
    # ========================
    # 結果集計
    # ========================
    
    print("\n" + "=" * 80)
    print("【アプローチ1: ワイド/馬連 年別ROI】")
    print("=" * 80)
    
    wide_df = pd.concat(all_wide_results)
    for year in [2020, 2021, 2022, 2023, 2024, 2025]:
        year_data = wide_df[wide_df['year'] == year]
        if len(year_data) == 0:
            continue
        
        wide_hits = year_data['wide_hit'].sum()
        wide_return = year_data[year_data['wide_hit']]['wide_odds_est'].sum()
        wide_roi = wide_return / len(year_data) * 100
        
        umaren_hits = year_data['umaren_hit'].sum()
        umaren_return = year_data[year_data['umaren_hit']]['umaren_odds_est'].sum()
        umaren_roi = umaren_return / len(year_data) * 100
        
        mark_w = "★" if wide_roi >= 100 else ""
        mark_u = "★" if umaren_roi >= 100 else ""
        
        print(f"  {year}年: ワイドROI {wide_roi:.1f}%{mark_w} ({wide_hits}的中), 馬連ROI {umaren_roi:.1f}%{mark_u} ({umaren_hits}的中), レース数: {len(year_data)}")
    
    print("\n" + "=" * 80)
    print("【アプローチ2: 崩壊年分析（20-50倍帯）】")
    print("=" * 80)
    
    for analysis in all_failure_analysis:
        mark = "★ROI崩壊★" if analysis['hit_rate'] < 2 else ""
        print(f"  {analysis['year']}年: 的中率 {analysis['hit_rate']:.1f}%, 馬券数 {analysis['total_bets']}, 平均スコア {analysis['avg_score']:.3f} {mark}")
    
    print("\n" + "=" * 80)
    print("【アプローチ3: 複合条件フィルタ - ROI>100%達成条件】")
    print("=" * 80)
    
    compound_df = pd.concat(all_compound_results)
    high_roi = compound_df[compound_df['roi'] >= 100].copy()
    
    if len(high_roi) > 0:
        # 年間安定してROI>100%を達成する条件を探索
        condition_counts = high_roi.groupby(['odds_range', 'ev_threshold', 'score_threshold']).size()
        best_conditions = condition_counts[condition_counts >= 3]  # 3年以上達成
        
        print(f"  ROI≥100%達成件数: {len(high_roi)}")
        print(f"  3年以上達成の条件数: {len(best_conditions)}")
        
        if len(best_conditions) > 0:
            print("\n  【安定してROI>100%を達成する条件】")
            for (odds, ev, score), count in best_conditions.items():
                subset = high_roi[(high_roi['odds_range'] == odds) & 
                                  (high_roi['ev_threshold'] == ev) & 
                                  (high_roi['score_threshold'] == score)]
                avg_roi = subset['roi'].mean()
                avg_bets = subset['bet_count'].mean()
                print(f"    オッズ{odds}, EV≥{ev}, スコア≥{score}: 達成{count}年, 平均ROI {avg_roi:.1f}%, 平均馬券数 {avg_bets:.0f}")
    else:
        print("  ROI≥100%を達成する条件なし")
    
    print("\n" + "=" * 80)
    print("【アプローチ4: 動的馬券選定 年別ROI】")
    print("=" * 80)
    
    dynamic_df = pd.concat(all_dynamic_results)
    for year in [2020, 2021, 2022, 2023, 2024, 2025]:
        year_data = dynamic_df[dynamic_df['year'] == year]
        if len(year_data) == 0:
            continue
        
        total_bets = len(year_data)
        total_return = year_data['return'].sum()
        roi = total_return / total_bets * 100
        hits = year_data['hit'].sum()
        
        mark = "★" if roi >= 100 else ""
        print(f"  {year}年: ROI {roi:.1f}%{mark}, 的中 {hits}/{total_bets}件")
    
    print("\n" + "=" * 80)
    print("分析完了")
    print("=" * 80)
    
    # 結果保存
    output_dir = project_root / "outputs" / "analysis"
    compound_df.to_csv(output_dir / "compound_filter_results.csv", index=False)
    print(f"\n結果を {output_dir} に保存しました。")


if __name__ == "__main__":
    main()
