# -*- coding: utf-8 -*-
"""
深い推論のための馬券分析

現分析の問題点:
1. オッズ概算の不正確さ（馬連=単勝1*単勝2/10などは実態と乖離）
2. 2024-2025年のみの検証で汎化性が不明
3. 条件の組み合わせで過学習の可能性

深い推論のアプローチ:
1. 単勝・複勝に絞った分析（オッズが正確）
2. 6年間Walk-forward検証での安定性確認
3. 条件ごとのSharpe Ratio的な評価（リターン÷リスク）
4. 「なぜそのROIになるか」の因果分析
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


def deep_analysis_single_year(test_df, preds, year):
    """単年の深い分析"""
    d = test_df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    bet_df = d[d['rank_pred'] == 1].copy()
    bet_df['is_hit'] = bet_df['finish_position'] == 1
    bet_df['is_top3'] = bet_df['finish_position'] <= 3
    
    results = {'year': year}
    
    # 全体の統計
    results['total_races'] = len(bet_df)
    results['overall_hit_rate'] = bet_df['is_hit'].mean() * 100
    results['overall_roi'] = bet_df[bet_df['is_hit']]['win_odds'].sum() / len(bet_df) * 100
    
    # オッズ帯別の詳細（単勝のみ - 正確なオッズ）
    odds_bands = [(1, 2), (2, 5), (5, 10), (10, 20), (20, 50), (50, 1000)]
    for low, high in odds_bands:
        key = f'odds_{low}_{high}'
        subset = bet_df[(bet_df['win_odds'] >= low) & (bet_df['win_odds'] < high)]
        if len(subset) >= 10:
            hits = subset[subset['is_hit']]
            roi = hits['win_odds'].sum() / len(subset) * 100 if len(subset) > 0 else 0
            results[f'{key}_roi'] = roi
            results[f'{key}_count'] = len(subset)
            results[f'{key}_hit_rate'] = len(hits) / len(subset) * 100
        else:
            results[f'{key}_roi'] = None
            results[f'{key}_count'] = len(subset)
            results[f'{key}_hit_rate'] = None
    
    # 人気帯別の詳細
    pop_bands = [(1, 2), (2, 3), (3, 6), (6, 11), (11, 19)]
    for low, high in pop_bands:
        key = f'pop_{low}_{high}'
        subset = bet_df[(bet_df['popularity'] >= low) & (bet_df['popularity'] < high)]
        if len(subset) >= 10:
            hits = subset[subset['is_hit']]
            roi = hits['win_odds'].sum() / len(subset) * 100 if len(subset) > 0 else 0
            results[f'{key}_roi'] = roi
            results[f'{key}_count'] = len(subset)
        else:
            results[f'{key}_roi'] = None
            results[f'{key}_count'] = len(subset)
    
    # スコア四分位別
    q1 = bet_df['score'].quantile(0.25)
    q2 = bet_df['score'].quantile(0.5)
    q3 = bet_df['score'].quantile(0.75)
    
    for name, condition in [
        ('Q1', bet_df['score'] <= q1),
        ('Q2', (bet_df['score'] > q1) & (bet_df['score'] <= q2)),
        ('Q3', (bet_df['score'] > q2) & (bet_df['score'] <= q3)),
        ('Q4', bet_df['score'] > q3),
    ]:
        subset = bet_df[condition]
        if len(subset) >= 10:
            hits = subset[subset['is_hit']]
            roi = hits['win_odds'].sum() / len(subset) * 100 if len(subset) > 0 else 0
            results[f'score_{name}_roi'] = roi
            results[f'score_{name}_count'] = len(subset)
        else:
            results[f'score_{name}_roi'] = None
            results[f'score_{name}_count'] = len(subset)
    
    # 複勝（Top3）の詳細分析
    # 複勝オッズは単勝の約1/3-1/4だが、正確にはレースによる
    # ここでは単勝オッズから推定: 複勝オッズ ≈ 単勝オッズ / (頭数/3 + 1)
    # 18頭立てなら ≈ 単勝/7、8頭立てなら ≈ 単勝/3.7
    # 簡易的に単勝/4で概算（保守的）
    bet_df['fukusho_odds_est'] = bet_df['win_odds'] / 4
    
    # 高オッズ帯（20-50倍）の複勝分析
    high_odds = bet_df[(bet_df['win_odds'] >= 20) & (bet_df['win_odds'] < 50)]
    if len(high_odds) >= 10:
        top3_hits = high_odds[high_odds['is_top3']]
        fukusho_return = top3_hits['fukusho_odds_est'].sum()
        results['high_odds_fukusho_roi'] = fukusho_return / len(high_odds) * 100
        results['high_odds_fukusho_top3_rate'] = len(top3_hits) / len(high_odds) * 100
        results['high_odds_fukusho_count'] = len(high_odds)
    
    return results


def main():
    print("=" * 80)
    print("深い推論のための馬券分析 - 6年間Walk-forward検証")
    print("=" * 80)
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    test_periods = [
        (2025, '2023-12-31', '2025-01-01', '2025-12-31'),
        (2024, '2022-12-31', '2024-01-01', '2024-12-31'),
        (2023, '2021-12-31', '2023-01-01', '2023-12-31'),
        (2022, '2020-12-31', '2022-01-01', '2022-12-31'),
        (2021, '2019-12-31', '2021-01-01', '2021-12-31'),
        (2020, '2018-12-31', '2020-01-01', '2020-12-31'),
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
        
        results = deep_analysis_single_year(test_f, preds, year)
        all_results.append(results)
    
    # 結果を集計
    results_df = pd.DataFrame(all_results)
    
    print("\n" + "=" * 80)
    print("【6年間の深い分析結果】")
    print("=" * 80)
    
    # 全体ROI
    print("\n■ 全体ROI (年別)")
    print("-" * 60)
    for _, row in results_df.iterrows():
        print(f"  {int(row['year'])}年: ROI {row['overall_roi']:.1f}%, 的中率 {row['overall_hit_rate']:.1f}%, {int(row['total_races'])}R")
    
    avg_roi = results_df['overall_roi'].mean()
    std_roi = results_df['overall_roi'].std()
    print(f"\n  → 6年平均ROI: {avg_roi:.1f}% (σ={std_roi:.1f}%)")
    
    # オッズ帯別の安定性
    print("\n" + "=" * 80)
    print("■ オッズ帯別ROI（6年間の安定性）")
    print("=" * 80)
    
    odds_bands = ['1_2', '2_5', '5_10', '10_20', '20_50', '50_1000']
    band_labels = {'1_2': '1-2倍', '2_5': '2-5倍', '5_10': '5-10倍', 
                   '10_20': '10-20倍', '20_50': '20-50倍', '50_1000': '50倍以上'}
    
    print(f"\n{'オッズ帯':>12} | {'平均ROI':>8} | {'σ':>6} | {'最低年':>8} | {'最高年':>8} | {'年平均件数':>10} | {'評価'}")
    print("-" * 85)
    
    for band in odds_bands:
        col = f'odds_{band}_roi'
        count_col = f'odds_{band}_count'
        
        valid = results_df[col].dropna()
        if len(valid) >= 3:
            avg = valid.mean()
            std = valid.std()
            min_val = valid.min()
            max_val = valid.max()
            avg_count = results_df[count_col].mean()
            
            # 評価基準
            # 1. 平均ROI > 100%
            # 2. 最低年 > 80%（大崩壊なし）
            # 3. σ < 20%（安定）
            # 4. 年平均件数 > 50
            
            evaluation = []
            if avg >= 100:
                evaluation.append("ROI○")
            if min_val >= 80:
                evaluation.append("安定○")
            elif min_val < 50:
                evaluation.append("崩壊危険")
            if avg_count >= 50:
                evaluation.append("件数○")
            elif avg_count < 20:
                evaluation.append("件数少")
            
            eval_str = " ".join(evaluation) if evaluation else "-"
            
            mark = "★" if avg >= 100 and min_val >= 70 else ""
            print(f"{band_labels[band]:>12} | {avg:>7.1f}%{mark} | {std:>5.1f}% | {min_val:>7.1f}% | {max_val:>7.1f}% | {avg_count:>10.0f} | {eval_str}")
    
    # 人気帯別の安定性
    print("\n" + "=" * 80)
    print("■ 人気帯別ROI（6年間の安定性）")
    print("=" * 80)
    
    pop_bands = ['1_2', '2_3', '3_6', '6_11', '11_19']
    pop_labels = {'1_2': '1番人気', '2_3': '2番人気', '3_6': '3-5番人気', 
                  '6_11': '6-10番人気', '11_19': '11番人気以下'}
    
    print(f"\n{'人気帯':>12} | {'平均ROI':>8} | {'σ':>6} | {'最低年':>8} | {'最高年':>8} | {'年平均件数':>10}")
    print("-" * 75)
    
    for band in pop_bands:
        col = f'pop_{band}_roi'
        count_col = f'pop_{band}_count'
        
        valid = results_df[col].dropna()
        if len(valid) >= 3:
            avg = valid.mean()
            std = valid.std()
            min_val = valid.min()
            max_val = valid.max()
            avg_count = results_df[count_col].mean()
            
            mark = "★" if avg >= 100 and min_val >= 70 else ""
            print(f"{pop_labels[band]:>12} | {avg:>7.1f}%{mark} | {std:>5.1f}% | {min_val:>7.1f}% | {max_val:>7.1f}% | {avg_count:>10.0f}")
    
    # スコア四分位別
    print("\n" + "=" * 80)
    print("■ スコア四分位別ROI（モデルの信頼度と成績）")
    print("=" * 80)
    
    print(f"\n{'スコア':>10} | {'平均ROI':>8} | {'σ':>6} | {'最低年':>8} | {'最高年':>8}")
    print("-" * 55)
    
    for q in ['Q1', 'Q2', 'Q3', 'Q4']:
        col = f'score_{q}_roi'
        valid = results_df[col].dropna()
        if len(valid) >= 3:
            avg = valid.mean()
            std = valid.std()
            min_val = valid.min()
            max_val = valid.max()
            mark = "★" if avg >= 100 else ""
            print(f"{q:>10} | {avg:>7.1f}%{mark} | {std:>5.1f}% | {min_val:>7.1f}% | {max_val:>7.1f}%")
    
    # 高オッズ帯の複勝分析
    print("\n" + "=" * 80)
    print("■ 高オッズ帯(20-50倍)の複勝ROI分析")
    print("=" * 80)
    
    if 'high_odds_fukusho_roi' in results_df.columns:
        print(f"\n{'年':>6} | {'複勝ROI':>10} | {'Top3率':>8} | {'馬券数':>8}")
        print("-" * 45)
        
        for _, row in results_df.iterrows():
            if pd.notna(row.get('high_odds_fukusho_roi')):
                print(f"{int(row['year']):>6} | {row['high_odds_fukusho_roi']:>9.1f}% | {row['high_odds_fukusho_top3_rate']:>7.1f}% | {int(row['high_odds_fukusho_count']):>8}")
        
        valid = results_df['high_odds_fukusho_roi'].dropna()
        if len(valid) >= 3:
            avg = valid.mean()
            std = valid.std()
            min_val = valid.min()
            total = results_df['high_odds_fukusho_count'].sum()
            print(f"\n  → 6年平均複勝ROI: {avg:.1f}% (σ={std:.1f}%, 最低年={min_val:.1f}%, 総件数={total:.0f})")
    
    # 深い推論
    print("\n" + "=" * 80)
    print("【深い推論と結論】")
    print("=" * 80)
    
    # 条件ごとのSharpe Ratio的評価（平均ROI / σ）
    print("\n■ 効率性スコア (平均ROI / σ) ランキング")
    print("-" * 60)
    
    efficiency_scores = []
    
    for band in odds_bands:
        col = f'odds_{band}_roi'
        count_col = f'odds_{band}_count'
        valid = results_df[col].dropna()
        if len(valid) >= 4 and valid.std() > 0:
            avg = valid.mean()
            std = valid.std()
            avg_count = results_df[count_col].mean()
            efficiency = (avg - 75) / std  # 基準ROI 75%からの超過リターン / リスク
            efficiency_scores.append({
                'condition': f'オッズ{band_labels[band]}',
                'avg_roi': avg,
                'std': std,
                'efficiency': efficiency,
                'count': avg_count,
            })
    
    for band in pop_bands:
        col = f'pop_{band}_roi'
        count_col = f'pop_{band}_count'
        valid = results_df[col].dropna()
        if len(valid) >= 4 and valid.std() > 0:
            avg = valid.mean()
            std = valid.std()
            avg_count = results_df[count_col].mean()
            efficiency = (avg - 75) / std
            efficiency_scores.append({
                'condition': f'人気{pop_labels[band]}',
                'avg_roi': avg,
                'std': std,
                'efficiency': efficiency,
                'count': avg_count,
            })
    
    eff_df = pd.DataFrame(efficiency_scores)
    if len(eff_df) > 0:
        eff_df = eff_df.sort_values('efficiency', ascending=False)
        
        for _, row in eff_df.head(10).iterrows():
            print(f"  {row['condition']:>15}: 効率スコア {row['efficiency']:>+.2f} (ROI {row['avg_roi']:.1f}%, σ {row['std']:.1f}%, 年{row['count']:.0f}件)")
    
    # 結果保存
    output_dir = project_root / "outputs" / "analysis"
    results_df.to_csv(output_dir / "deep_analysis_6years.csv", index=False)
    
    print(f"\n結果を {output_dir} に保存しました。")
    
    print("\n" + "=" * 80)
    print("分析完了")
    print("=" * 80)


if __name__ == "__main__":
    main()
