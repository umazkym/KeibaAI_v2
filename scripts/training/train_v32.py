# -*- coding: utf-8 -*-
"""
V32モデル学習・検証スクリプト

C4位置×上がり×ペースの複合特徴量を追加したV32モデルを8年間Walk-forward検証
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

from keibaai.src.features.leak_free_feature_engineer_v32 import LeakFreeFeatureEngineerV32


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


def train_v32(train_df, valid_df, feature_cols):
    """V32モデル学習（強化正則化）"""
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


def analyze_predictions(test_df, preds):
    """予測結果の詳細分析"""
    d = test_df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    bet_df = d[d['rank_pred'] == 1].copy()
    bet_df['is_hit'] = bet_df['finish_position'] == 1
    hits = bet_df[bet_df['is_hit']]
    
    roi = hits['win_odds'].sum() / len(bet_df) * 100 if len(bet_df) > 0 else 0
    hit_rate = len(hits) / len(bet_df) * 100 if len(bet_df) > 0 else 0
    
    if 'popularity' in bet_df.columns:
        pop1_bets = bet_df[bet_df['popularity'] == 1]
        non_pop1_bets = bet_df[bet_df['popularity'] != 1]
        
        pop1_hits = pop1_bets[pop1_bets['is_hit']]
        non_pop1_hits = non_pop1_bets[non_pop1_bets['is_hit']]
        
        pop1_roi = pop1_hits['win_odds'].sum() / len(pop1_bets) * 100 if len(pop1_bets) > 0 else 0
        non_pop1_roi = non_pop1_hits['win_odds'].sum() / len(non_pop1_bets) * 100 if len(non_pop1_bets) > 0 else 0
        pop1_rate = len(pop1_bets) / len(bet_df) * 100 if len(bet_df) > 0 else 0
    else:
        pop1_roi, non_pop1_roi, pop1_rate = 0, 0, 0
    
    return {
        'roi': roi,
        'hit_rate': hit_rate,
        'bet_count': len(bet_df),
        'hit_count': len(hits),
        'pop1_rate': pop1_rate,
        'pop1_roi': pop1_roi,
        'non_pop1_roi': non_pop1_roi,
    }


def main():
    print("=" * 80)
    print("V32モデル学習・検証")
    print("=" * 80)
    
    races, pedigrees, corners, race_details, horses = load_data()
    print(f"\n総データ: {len(races):,}件")
    
    test_periods = [
        ('2025', '2023-12-31', '2024-01-01', '2024-12-31', '2025-01-01', '2025-12-31'),
        ('2024', '2022-12-31', '2023-01-01', '2023-12-31', '2024-01-01', '2024-12-31'),
        ('2023', '2021-12-31', '2022-01-01', '2022-12-31', '2023-01-01', '2023-12-31'),
        ('2022', '2020-12-31', '2021-01-01', '2021-12-31', '2022-01-01', '2022-12-31'),
        ('2021', '2019-12-31', '2020-01-01', '2020-12-31', '2021-01-01', '2021-12-31'),
        ('2020', '2018-12-31', '2019-01-01', '2019-12-31', '2020-01-01', '2020-12-31'),
        ('2019', '2017-12-31', '2018-01-01', '2018-12-31', '2019-01-01', '2019-12-31'),
        ('2018', '2016-12-31', '2017-01-01', '2017-12-31', '2018-01-01', '2018-12-31'),
    ]
    
    all_results = []
    
    for year, train_end, valid_start, valid_end, test_start, test_end in tqdm(test_periods, desc="Walk-forward検証"):
        print(f"\n[{year}年] 処理中...")
        
        train = races[races['race_date'] <= train_end].copy()
        valid = races[(races['race_date'] >= valid_start) & (races['race_date'] < valid_end)].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] < test_end)].copy()
        
        print(f"  Train: {len(train):,}, Valid: {len(valid):,}, Test: {len(test):,}")
        
        print("  V32特徴量生成中...")
        engine = LeakFreeFeatureEngineerV32()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        valid_f = engine.transform(valid)
        test_f = engine.transform(test)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        print(f"  特徴量数: {len(feature_cols)}")
        
        print("  V32学習中...")
        model = train_v32(train_f, valid_f, feature_cols)
        
        X_test = test_f[feature_cols].fillna(0)
        preds = model.predict(X_test)
        result = analyze_predictions(test_f, preds)
        result['year'] = year
        
        print(f"  ROI: {result['roi']:.1f}%, 的中率: {result['hit_rate']:.1f}%")
        print(f"  1番人気選択率: {result['pop1_rate']:.1f}%, 非1番人気ROI: {result['non_pop1_roi']:.1f}%")
        
        all_results.append(result)
    
    print("\n" + "=" * 80)
    print("V32モデル 総合サマリー")
    print("=" * 80)
    
    results_df = pd.DataFrame(all_results)
    
    print("\n【年別ROI推移】")
    print("-" * 70)
    print(f"{'年':<6} {'ROI':>10} {'的中率':>10} {'1人気率':>10} {'非1人気ROI':>12}")
    print("-" * 70)
    for _, row in results_df.iterrows():
        print(f"{row['year']:<6} {row['roi']:>9.1f}% {row['hit_rate']:>9.1f}% {row['pop1_rate']:>9.1f}% {row['non_pop1_roi']:>11.1f}%")
    
    print("-" * 70)
    print(f"{'平均':<6} {results_df['roi'].mean():>9.1f}% {results_df['hit_rate'].mean():>9.1f}% {results_df['pop1_rate'].mean():>9.1f}% {results_df['non_pop1_roi'].mean():>11.1f}%")
    print(f"{'σ':<6} {results_df['roi'].std():>9.1f}%")
    
    print(f"\n【指標】")
    print(f"  8年平均ROI: {results_df['roi'].mean():.1f}%")
    print(f"  ROI σ: {results_df['roi'].std():.1f}%")
    print(f"  最低年ROI: {results_df['roi'].min():.1f}% ({results_df.loc[results_df['roi'].idxmin(), 'year']}年)")
    print(f"  最高年ROI: {results_df['roi'].max():.1f}% ({results_df.loc[results_df['roi'].idxmax(), 'year']}年)")
    print(f"  非1番人気選択時ROI平均: {results_df['non_pop1_roi'].mean():.1f}%")
    
    output_dir = project_root / "outputs" / "analysis"
    output_dir.mkdir(parents=True, exist_ok=True)
    results_df.to_csv(output_dir / "v32_evaluation.csv", index=False)
    print(f"\n結果を {output_dir / 'v32_evaluation.csv'} に保存しました。")
    
    print("\n" + "=" * 80)
    print("V32モデル検証完了")
    print("=" * 80)


if __name__ == "__main__":
    main()
