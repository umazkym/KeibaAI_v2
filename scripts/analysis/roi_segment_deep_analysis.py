# -*- coding: utf-8 -*-
"""
ROI>100%セグメント深掘り分析

発見: 20-50倍帯でROI>100%達成
→ 年別安定性、ワイド/馬連等への拡張可能性を分析
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


def analyze_yearly_odds_band(test_df, preds, years):
    """年別×オッズ帯別のROI分析"""
    d = test_df.copy()
    d['score'] = preds
    d['year'] = d['race_date'].dt.year
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    bet_df = d[d['rank_pred'] == 1].copy()
    bet_df['is_hit'] = bet_df['finish_position'] == 1
    
    # オッズ帯分類
    bet_df['odds_band'] = pd.cut(bet_df['win_odds'], 
        bins=[0, 2, 5, 10, 20, 50, 1000], 
        labels=['1-2倍', '2-5倍', '5-10倍', '10-20倍', '20-50倍', '50倍以上'])
    
    results = []
    for year in years:
        for odds_band in ['1-2倍', '2-5倍', '5-10倍', '10-20倍', '20-50倍', '50倍以上']:
            subset = bet_df[(bet_df['year'] == year) & (bet_df['odds_band'] == odds_band)]
            if len(subset) > 0:
                hits = subset[subset['is_hit']]
                roi = hits['win_odds'].sum() / len(subset) * 100
                hit_rate = len(hits) / len(subset) * 100
                bet_count = len(subset)
            else:
                roi, hit_rate, bet_count = 0, 0, 0
            
            results.append({
                'year': year,
                'odds_band': odds_band,
                'roi': roi,
                'hit_rate': hit_rate,
                'bet_count': bet_count,
            })
    
    return pd.DataFrame(results)


def analyze_top3_coverage(test_df, preds):
    """
    Top3に何頭入るか分析（ワイド/馬連/三連複の可能性）
    """
    d = test_df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    d['is_top3'] = d['finish_position'] <= 3
    
    results = []
    for top_n in [1, 2, 3, 4, 5]:
        # モデルのTop N馬
        top_n_df = d[d['rank_pred'] <= top_n]
        
        # そのうち実際のTop3に入った馬数
        top3_in_pred = top_n_df.groupby('race_id')['is_top3'].sum()
        
        # レースごとに分析
        for count in range(min(top_n, 3) + 1):
            race_count = (top3_in_pred == count).sum()
            results.append({
                'top_n_prediction': top_n,
                'actual_top3_count': count,
                'race_count': race_count,
            })
    
    return pd.DataFrame(results)


def analyze_multi_bet_potential(test_df, preds):
    """
    多頭買い（モデルTop N馬を購入）のROI分析
    """
    d = test_df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    results = []
    for top_n in [1, 2, 3, 4, 5]:
        # Top N馬を全て購入
        bet_df = d[d['rank_pred'] <= top_n].copy()
        bet_df['is_hit'] = bet_df['finish_position'] == 1
        
        hits = bet_df[bet_df['is_hit']]
        
        # ROI = 的中配当 / 購入金額
        # 購入金額 = レース数 × top_n × 100円
        total_cost = bet_df['race_id'].nunique() * top_n
        total_return = hits['win_odds'].sum()
        roi = total_return / total_cost * 100
        hit_rate = len(hits) / bet_df['race_id'].nunique() * 100
        
        results.append({
            'top_n': top_n,
            'roi': roi,
            'hit_rate': hit_rate,
            'total_bets': total_cost,
            'races': bet_df['race_id'].nunique(),
        })
    
    return pd.DataFrame(results)


def analyze_high_odds_multi_buy(test_df, preds):
    """
    高オッズ帯限定×多頭買いのシミュレーション
    """
    d = test_df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    # Top1が20-50倍帯のレースを抽出
    top1 = d[d['rank_pred'] == 1].copy()
    high_odds_races = top1[(top1['win_odds'] >= 20) & (top1['win_odds'] < 50)]['race_id'].unique()
    
    print(f"\n高オッズ（20-50倍）レース数: {len(high_odds_races)}")
    
    results = []
    for top_n in [1, 2, 3]:
        # 高オッズレースのTop N馬を購入
        bet_df = d[(d['race_id'].isin(high_odds_races)) & (d['rank_pred'] <= top_n)].copy()
        bet_df['is_hit'] = bet_df['finish_position'] == 1
        
        hits = bet_df[bet_df['is_hit']]
        
        total_cost = len(high_odds_races) * top_n
        total_return = hits['win_odds'].sum()
        roi = total_return / total_cost * 100 if total_cost > 0 else 0
        hit_rate = len(hits) / len(high_odds_races) * 100 if len(high_odds_races) > 0 else 0
        
        results.append({
            'top_n': top_n,
            'roi': roi,
            'hit_rate': hit_rate,
            'races': len(high_odds_races),
            'total_bets': total_cost,
        })
    
    return pd.DataFrame(results)


def main():
    print("=" * 80)
    print("ROI>100%セグメント深掘り分析")
    print("=" * 80)
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    # 8年Walk-forward検証（各年ごとに分析）
    test_periods = [
        ('2025', '2023-12-31', '2025-01-01', '2025-12-31'),
        ('2024', '2022-12-31', '2024-01-01', '2024-12-31'),
        ('2023', '2021-12-31', '2023-01-01', '2023-12-31'),
        ('2022', '2020-12-31', '2022-01-01', '2022-12-31'),
        ('2021', '2019-12-31', '2021-01-01', '2021-12-31'),
        ('2020', '2018-12-31', '2020-01-01', '2020-12-31'),
    ]
    
    all_yearly_results = []
    
    for year, train_end, test_start, test_end in test_periods:
        print(f"\n【{year}年】分析中...")
        
        train = races[races['race_date'] <= train_end].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] <= test_end)].copy()
        
        if len(test) == 0:
            continue
        
        # V33で分析（穴馬発掘力最高）
        engine = LeakFreeFeatureEngineerV33()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        test_f = engine.transform(test)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        
        # Validation split
        val_start = pd.to_datetime(train_end) - pd.DateOffset(years=1)
        train_sub = train_f[train_f['race_date'] < val_start]
        valid_sub = train_f[train_f['race_date'] >= val_start]
        
        model = train_model(train_sub, valid_sub, feature_cols)
        
        X_test = test_f[feature_cols].fillna(0)
        preds = model.predict(X_test)
        
        # オッズ帯別分析
        yearly = analyze_yearly_odds_band(test_f, preds, [int(year)])
        all_yearly_results.append(yearly)
        
        # 20-50倍帯の結果
        high_odds = yearly[yearly['odds_band'] == '20-50倍'].iloc[0] if len(yearly[yearly['odds_band'] == '20-50倍']) > 0 else None
        if high_odds is not None:
            mark = "★" if high_odds['roi'] >= 100 else ""
            print(f"  20-50倍帯: ROI {high_odds['roi']:.1f}%{mark}, 馬券数: {high_odds['bet_count']}")
    
    # 結果集計
    print("\n" + "=" * 80)
    print("【年別 × 20-50倍帯 ROI推移】")
    print("=" * 80)
    
    yearly_df = pd.concat(all_yearly_results)
    high_odds_df = yearly_df[yearly_df['odds_band'] == '20-50倍']
    
    print(f"\n{'年':>6} {'ROI':>10} {'的中率':>10} {'馬券数':>10}")
    print("-" * 40)
    for _, row in high_odds_df.iterrows():
        mark = "★" if row['roi'] >= 100 else ""
        print(f"{int(row['year']):>6} {row['roi']:>9.1f}%{mark} {row['hit_rate']:>9.1f}% {int(row['bet_count']):>10}")
    
    print("-" * 40)
    print(f"{'平均':>6} {high_odds_df['roi'].mean():>9.1f}% {high_odds_df['hit_rate'].mean():>9.1f}% {int(high_odds_df['bet_count'].mean()):>10}")
    print(f"{'σ':>6} {high_odds_df['roi'].std():>9.1f}%")
    
    # ROI>100%達成年の割合
    roi_over_100 = (high_odds_df['roi'] >= 100).sum()
    print(f"\nROI≥100%達成年: {roi_over_100}/{len(high_odds_df)} ({roi_over_100/len(high_odds_df)*100:.0f}%)")
    
    print("\n" + "=" * 80)
    print("【結論と次のステップ】")
    print("=" * 80)
    
    avg_roi = high_odds_df['roi'].mean()
    avg_bets = high_odds_df['bet_count'].mean()
    
    print(f"""
■ 20-50倍帯の実績
  - 平均ROI: {avg_roi:.1f}%
  - 年間平均馬券数: {avg_bets:.0f}件
  - ROI≥100%達成年: {roi_over_100}/{len(high_odds_df)}

■ 課題
  - 馬券数が年間{avg_bets:.0f}件と少ない
  - ROIのσが{high_odds_df['roi'].std():.1f}%と変動あり

■ 次のステップ候補
  1. 複数オッズ帯の組み合わせ（10-50倍など）
  2. ワイド/馬連への拡張（Top2-3使用）
  3. 期待値フィルタの追加適用
""")
    
    # 結果保存
    output_dir = project_root / "outputs" / "analysis"
    yearly_df.to_csv(output_dir / "yearly_odds_band_roi.csv", index=False)
    print(f"\n結果を {output_dir / 'yearly_odds_band_roi.csv'} に保存しました。")


if __name__ == "__main__":
    main()
