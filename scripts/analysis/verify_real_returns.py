# -*- coding: utf-8 -*-
"""
実払い戻しデータを使用した正確なROI計算

問題点の検証:
1. 現在の分析では win_odds（単勝オッズ）のみ使用し、複勝は概算
2. returns.parquet には実際の払い戻し金額(payout)がある
3. これを使用して正確なROIを計算し直す

検証項目:
- 単勝: 現在のwin_odds計算が正しいか確認
- 複勝: 実際の払い戻しでROI再計算
- 馬単: 実際の払い戻しでROI再計算
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

from keibaai.src.features.leak_free_feature_engineer_v33 import LeakFreeFeatureEngineerV33


def load_data():
    print("データ読み込み中...")
    data_dir = project_root / "keibaai/data/parsed/parquet"
    
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    returns = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    return races, pedigrees, corners, race_details, horses, returns


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


def analyze_with_real_returns(test_df, preds, returns_df, year):
    """
    実払い戻しデータを使用したROI計算
    """
    d = test_df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    # Top1馬を取得
    bet_df = d[d['rank_pred'] == 1].copy()
    bet_df['is_hit'] = bet_df['finish_position'] == 1
    bet_df['is_top3'] = bet_df['finish_position'] <= 3
    
    # 払い戻しデータと結合
    # 単勝払い戻し
    tansho_returns = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']].copy()
    tansho_returns.rename(columns={'payout': 'tansho_payout'}, inplace=True)
    # payoutは100円あたりの払い戻し金額
    
    # 複勝払い戻し
    fukusho_returns = returns_df[returns_df['bet_type'] == 'fukusho'][['race_id', 'horse_number', 'payout']].copy()
    fukusho_returns.rename(columns={'payout': 'fukusho_payout'}, inplace=True)
    
    # bet_dfにマージ
    bet_df = bet_df.merge(tansho_returns, on=['race_id', 'horse_number'], how='left')
    bet_df = bet_df.merge(fukusho_returns, on=['race_id', 'horse_number'], how='left')
    
    # payoutはNaNの場合は0（外れ）
    bet_df['tansho_payout'] = bet_df['tansho_payout'].fillna(0)
    bet_df['fukusho_payout'] = bet_df['fukusho_payout'].fillna(0)
    
    # 払い戻しを100円単位からオッズに変換（/100）
    bet_df['tansho_odds_actual'] = bet_df['tansho_payout'] / 100
    bet_df['fukusho_odds_actual'] = bet_df['fukusho_payout'] / 100
    
    results = {'year': year}
    results['total_races'] = len(bet_df)
    
    # ===== 単勝ROI検証 =====
    # 現在のwin_oddsで計算したROI
    hit_with_odds = bet_df[bet_df['is_hit']]
    roi_win_odds = hit_with_odds['win_odds'].sum() / len(bet_df) * 100
    
    # 実際の払い戻しで計算したROI
    roi_tansho_actual = bet_df['tansho_odds_actual'].sum() / len(bet_df) * 100
    
    results['tansho_roi_from_win_odds'] = roi_win_odds
    results['tansho_roi_from_returns'] = roi_tansho_actual
    results['tansho_difference'] = roi_tansho_actual - roi_win_odds
    
    # ===== 複勝ROI（実払い戻し）=====
    roi_fukusho_actual = bet_df['fukusho_odds_actual'].sum() / len(bet_df) * 100
    
    # 概算（win_odds / 3）との比較
    top3_hits = bet_df[bet_df['is_top3']]
    roi_fukusho_estimate = (top3_hits['win_odds'] / 3).sum() / len(bet_df) * 100
    
    results['fukusho_roi_actual'] = roi_fukusho_actual
    results['fukusho_roi_estimate'] = roi_fukusho_estimate
    results['fukusho_difference'] = roi_fukusho_actual - roi_fukusho_estimate
    
    # ===== オッズ帯別分析（実払い戻し）=====
    odds_bands = [(1, 2), (2, 5), (5, 10), (10, 20), (20, 50), (50, 1000)]
    
    for low, high in odds_bands:
        key = f'odds_{low}_{high}'
        subset = bet_df[(bet_df['win_odds'] >= low) & (bet_df['win_odds'] < high)]
        if len(subset) >= 10:
            # 単勝（実払い戻し）
            tansho_return = subset['tansho_odds_actual'].sum()
            tansho_roi = tansho_return / len(subset) * 100
            
            # 複勝（実払い戻し）
            fukusho_return = subset['fukusho_odds_actual'].sum()
            fukusho_roi = fukusho_return / len(subset) * 100
            
            results[f'{key}_tansho_actual'] = tansho_roi
            results[f'{key}_fukusho_actual'] = fukusho_roi
            results[f'{key}_count'] = len(subset)
        else:
            results[f'{key}_tansho_actual'] = None
            results[f'{key}_fukusho_actual'] = None
            results[f'{key}_count'] = len(subset)
    
    # ===== 馬単分析 =====
    # Top1馬が1着、Top2馬が2着の場合に馬単的中
    # 馬単払い戻しはrace_idで検索が必要
    
    # Top2馬を取得
    top2 = d[(d['rank_pred'] == 1) | (d['rank_pred'] == 2)].copy()
    
    umatan_returns = returns_df[returns_df['bet_type'] == 'umatan'][['race_id', 'payout', 'horse_number', 'horse_2']].copy()
    
    umatan_results = []
    for race_id in bet_df['race_id'].unique():
        race_top2 = top2[top2['race_id'] == race_id].sort_values('rank_pred')
        if len(race_top2) < 2:
            continue
        
        h1 = race_top2.iloc[0]
        h2 = race_top2.iloc[1]
        
        # Top1が1着、Top2が2着なら馬単的中
        is_umatan_hit = (h1['finish_position'] == 1) and (h2['finish_position'] == 2)
        
        # 払い戻し確認
        if is_umatan_hit:
            umatan_race = umatan_returns[umatan_returns['race_id'] == race_id]
            if len(umatan_race) > 0:
                payout = umatan_race.iloc[0]['payout'] / 100  # オッズに変換
            else:
                payout = 0
        else:
            payout = 0
        
        umatan_results.append({
            'race_id': race_id,
            'is_hit': is_umatan_hit,
            'payout': payout,
            'h1_odds': h1['win_odds'],
            'h2_odds': h2['win_odds'],
        })
    
    umatan_df = pd.DataFrame(umatan_results)
    if len(umatan_df) > 0:
        results['umatan_hit_count'] = umatan_df['is_hit'].sum()
        results['umatan_total'] = len(umatan_df)
        results['umatan_roi_actual'] = umatan_df['payout'].sum() / len(umatan_df) * 100
        
        # 実際の馬単オッズと単勝オッズの関係確認
        if results['umatan_hit_count'] > 0:
            umatan_hits = umatan_df[umatan_df['is_hit']]
            results['umatan_avg_payout'] = umatan_hits['payout'].mean()
            results['umatan_h1_avg_odds'] = umatan_hits['h1_odds'].mean()
            results['umatan_h2_avg_odds'] = umatan_hits['h2_odds'].mean()
    
    return results


def main():
    print("=" * 80)
    print("実払い戻しデータを使用した正確なROI検証")
    print("=" * 80)
    
    races, pedigrees, corners, race_details, horses, returns = load_data()
    
    print(f"\n払い戻しデータ件数: {len(returns):,}")
    print(f"レースデータ件数: {len(races):,}")
    
    # 払い戻しデータの年範囲確認
    returns['race_id_str'] = returns['race_id'].astype(str)
    returns['year_from_id'] = returns['race_id_str'].str[:4].astype(int)
    print(f"\n払い戻しデータ年範囲: {returns['year_from_id'].min()} - {returns['year_from_id'].max()}")
    print(returns['year_from_id'].value_counts().sort_index())
    
    # 2024-2025年で検証（払い戻しデータがある範囲）
    test_periods = [
        (2025, '2023-12-31', '2025-01-01', '2025-12-31'),
        (2024, '2022-12-31', '2024-01-01', '2024-12-31'),
    ]
    
    all_results = []
    
    for year, train_end, test_start, test_end in test_periods:
        print(f"\n【{year}年】処理中...")
        
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
        
        results = analyze_with_real_returns(test_f, preds, returns, year)
        all_results.append(results)
    
    results_df = pd.DataFrame(all_results)
    
    print("\n" + "=" * 80)
    print("【検証結果: 単勝ROI比較】")
    print("=" * 80)
    print(f"\n{'年':>6} | {'win_odds計算':>12} | {'実払い戻し':>12} | {'差分':>8}")
    print("-" * 50)
    
    for _, row in results_df.iterrows():
        print(f"{int(row['year']):>6} | {row['tansho_roi_from_win_odds']:>11.1f}% | {row['tansho_roi_from_returns']:>11.1f}% | {row['tansho_difference']:>+7.1f}%")
    
    print("\n" + "=" * 80)
    print("【検証結果: 複勝ROI比較】")
    print("=" * 80)
    print(f"\n{'年':>6} | {'実払い戻し':>12} | {'概算(単勝/3)':>12} | {'差分':>8}")
    print("-" * 50)
    
    for _, row in results_df.iterrows():
        print(f"{int(row['year']):>6} | {row['fukusho_roi_actual']:>11.1f}% | {row['fukusho_roi_estimate']:>11.1f}% | {row['fukusho_difference']:>+7.1f}%")
    
    print("\n" + "=" * 80)
    print("【検証結果: 馬単ROI】")
    print("=" * 80)
    
    for _, row in results_df.iterrows():
        print(f"\n■ {int(row['year'])}年")
        print(f"  的中数: {int(row.get('umatan_hit_count', 0))}/{int(row.get('umatan_total', 0))}件")
        print(f"  馬単ROI（実払い戻し）: {row.get('umatan_roi_actual', 0):.1f}%")
        if row.get('umatan_hit_count', 0) > 0:
            print(f"  的中時平均払い戻し: {row.get('umatan_avg_payout', 0):.1f}倍")
            print(f"  的中時Top1平均単勝オッズ: {row.get('umatan_h1_avg_odds', 0):.1f}倍")
            print(f"  的中時Top2平均単勝オッズ: {row.get('umatan_h2_avg_odds', 0):.1f}倍")
    
    print("\n" + "=" * 80)
    print("【オッズ帯別: 単勝 vs 複勝 ROI（実払い戻し）】")
    print("=" * 80)
    
    odds_bands = ['1_2', '2_5', '5_10', '10_20', '20_50', '50_1000']
    band_labels = {'1_2': '1-2倍', '2_5': '2-5倍', '5_10': '5-10倍', 
                   '10_20': '10-20倍', '20_50': '20-50倍', '50_1000': '50倍以上'}
    
    for year_row in results_df.iterrows():
        _, row = year_row
        print(f"\n■ {int(row['year'])}年")
        print(f"{'オッズ帯':>12} | {'単勝ROI':>10} | {'複勝ROI':>10} | {'件数':>8}")
        print("-" * 50)
        
        for band in odds_bands:
            tansho = row.get(f'odds_{band}_tansho_actual')
            fukusho = row.get(f'odds_{band}_fukusho_actual')
            count = row.get(f'odds_{band}_count', 0)
            
            if tansho is not None:
                t_mark = "★" if tansho >= 100 else ""
                f_mark = "★" if fukusho and fukusho >= 100 else ""
                print(f"{band_labels[band]:>12} | {tansho:>9.1f}%{t_mark} | {fukusho if fukusho else 0:>9.1f}%{f_mark} | {int(count):>8}")
    
    # 結果保存
    output_dir = project_root / "outputs" / "analysis"
    results_df.to_csv(output_dir / "real_returns_verification.csv", index=False)
    
    print(f"\n結果を {output_dir} に保存しました。")
    
    print("\n" + "=" * 80)
    print("検証完了")
    print("=" * 80)


if __name__ == "__main__":
    main()
