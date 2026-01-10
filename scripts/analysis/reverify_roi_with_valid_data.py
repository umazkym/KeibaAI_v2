# -*- coding: utf-8 -*-
"""
払い戻しデータが存在するレース限定でのROI再計算

発見された事実:
2025年のレースデータ3455件に対し、払い戻しデータは2817件（約81.5%）しかない。
欠損した638レース分が「配当0」として計算されているため、ROIが不当に低くなっている可能性が高い。

修正方針:
払い戻しデータが存在する（payoutがある）レースのみを母数としてROIを計算する。
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


def analyze_with_valid_returns(test_df, preds, returns_df, year):
    """
    払い戻しデータがあるレースのみでROI計算
    """
    d = test_df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    # 全てのレースID
    all_race_ids = set(d['race_id'].unique())
    
    # 払い戻しデータがあるレースID
    race_ids_with_returns = set(returns_df[returns_df['race_id'].isin(all_race_ids)]['race_id'].unique())
    
    print(f"\n【{year}年 データ網羅率】")
    print(f"  対象全レース数: {len(all_race_ids)}")
    print(f"  払い戻しあり: {len(race_ids_with_returns)}")
    print(f"  欠損レース数: {len(all_race_ids) - len(race_ids_with_returns)} ({100 - len(race_ids_with_returns)/len(all_race_ids)*100:.1f}%)")
    
    # フィルタリング
    d_filtered = d[d['race_id'].isin(race_ids_with_returns)].copy()
    
    # Top1馬を取得（フィルタ済みデータから）
    bet_df = d_filtered[d_filtered['rank_pred'] == 1].copy()
    bet_df['is_hit'] = bet_df['finish_position'] == 1
    bet_df['is_top3'] = bet_df['finish_position'] <= 3
    
    # 払い戻しデータと結合
    tansho_returns = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']].copy()
    tansho_returns.rename(columns={'payout': 'tansho_payout'}, inplace=True)
    
    fukusho_returns = returns_df[returns_df['bet_type'] == 'fukusho'][['race_id', 'horse_number', 'payout']].copy()
    fukusho_returns.rename(columns={'payout': 'fukusho_payout'}, inplace=True)
    
    bet_df = bet_df.merge(tansho_returns, on=['race_id', 'horse_number'], how='left')
    bet_df = bet_df.merge(fukusho_returns, on=['race_id', 'horse_number'], how='left')
    
    bet_df['tansho_payout'] = bet_df['tansho_payout'].fillna(0)
    bet_df['fukusho_payout'] = bet_df['fukusho_payout'].fillna(0)
    
    bet_df['tansho_odds_actual'] = bet_df['tansho_payout'] / 100
    bet_df['fukusho_odds_actual'] = bet_df['fukusho_payout'] / 100
    
    results = {'year': year}
    results['valid_races'] = len(bet_df)
    
    # ROI計算
    # 1. win_odds計算 (シミュレーション)
    hit_with_odds = bet_df[bet_df['is_hit']]
    roi_win_odds = hit_with_odds['win_odds'].sum() / len(bet_df) * 100
    
    # 2. 実払い戻し計算
    roi_tansho_actual = bet_df['tansho_odds_actual'].sum() / len(bet_df) * 100
    roi_fukusho_actual = bet_df['fukusho_odds_actual'].sum() / len(bet_df) * 100
    
    results['tansho_roi_sim'] = roi_win_odds
    results['tansho_roi_real'] = roi_tansho_actual
    results['fukusho_roi_real'] = roi_fukusho_actual
    
    # オッズ帯別（実払い戻し・フィルタ済み）
    odds_bands = [(1, 2), (2, 5), (5, 10), (10, 20), (20, 50), (50, 1000)]
    for low, high in odds_bands:
        key = f'odds_{low}_{high}'
        subset = bet_df[(bet_df['win_odds'] >= low) & (bet_df['win_odds'] < high)]
        if len(subset) >= 10:
            results[f'{key}_tansho'] = subset['tansho_odds_actual'].sum() / len(subset) * 100
            results[f'{key}_fukusho'] = subset['fukusho_odds_actual'].sum() / len(subset) * 100
            results[f'{key}_count'] = len(subset)
    
    return results


def main():
    print("=" * 80)
    print("払い戻しデータが存在するレース限定でのROI再検証")
    print("=" * 80)
    
    races, pedigrees, corners, race_details, horses, returns = load_data()
    
    # 2024-2025年で検証
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
        
        results = analyze_with_valid_returns(test_f, preds, returns, year)
        all_results.append(results)
    
    results_df = pd.DataFrame(all_results)
    
    print("\n" + "=" * 80)
    print("【再検証結果: 払い戻しデータあり母集団のみ】")
    print("=" * 80)
    
    print(f"\n{'年':>6} | {'単勝ROI(Sim)':>12} | {'単勝ROI(実)':>12} | {'複勝ROI(実)':>12} | {'有効レース数':>10}")
    print("-" * 65)
    
    for _, row in results_df.iterrows():
        print(f"{int(row['year']):>6} | {row['tansho_roi_sim']:>11.1f}% | {row['tansho_roi_real']:>11.1f}% | {row['fukusho_roi_real']:>11.1f}% | {int(row['valid_races']):>9}")
    
    print("\n" + "=" * 80)
    print("【オッズ帯別: 実ROI（有効レース限定）】")
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
            tansho = row.get(f'odds_{band}_tansho')
            fukusho = row.get(f'odds_{band}_fukusho')
            count = row.get(f'odds_{band}_count', 0)
            
            if tansho is not None and not pd.isna(tansho):
                t_mark = "★" if tansho >= 100 else ""
                f_mark = "★" if fukusho and fukusho >= 100 else ""
                print(f"{band_labels[band]:>12} | {tansho:>9.1f}%{t_mark} | {fukusho if fukusho else 0:>9.1f}%{f_mark} | {int(count):>8}")
    
    print("\n" + "=" * 80)
    print("分析完了")
    print("=" * 80)


if __name__ == "__main__":
    main()
