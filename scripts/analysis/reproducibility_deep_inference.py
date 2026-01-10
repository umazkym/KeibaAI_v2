# -*- coding: utf-8 -*-
"""
再現性重視の深い推論分析

【目的】
これまでの分析で発見したパターンが「再現性があるか」を検証。
単年度で見つかった高ROI条件が、複数年で一貫しているかを確認。

【分析項目】
1. 条件別ROIの年度間一貫性
2. レース番号別ROIの年度間一貫性
3. 複合条件（条件×レース番号）の効果
4. 統計的有意性の検証（95%信頼区間）
5. パターンの構造的理由の推論

【再現性の定義】
- 4年間（2022-2025）で3年以上同じ傾向が確認できること
- 年度間のROI変動（σ）が許容範囲内であること
- サンプル数が統計的に十分であること（最低100件）
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
import warnings
import matplotlib.pyplot as plt
import matplotlib
from scipy import stats
matplotlib.rcParams['font.family'] = 'Meiryo'
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


def extract_race_info(df):
    """レース情報を抽出"""
    df = df.copy()
    df['race_id_str'] = df['race_id'].astype(str)
    df['race_number'] = df['race_id_str'].str[-2:].astype(int)
    df['month'] = df['race_date'].dt.month
    df['surface'] = df['track_surface']
    
    if 'age' in df.columns:
        max_age = df.groupby('race_id')['age'].transform('max')
        df['is_young_limited'] = (max_age <= 3)
    else:
        df['is_young_limited'] = False
    
    df['is_long'] = (df['distance_m'] >= 1700)
    df['condition_code'] = (
        df['surface'].fillna('芝').astype(str) + '_' +
        df['is_young_limited'].map({True: 'young', False: 'old'}) + '_' +
        df['is_long'].map({True: 'long', False: 'short'})
    )
    
    return df


def train_model(train_df, valid_df, feature_cols):
    """モデル学習"""
    params = {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.02, 'num_leaves': 8, 'max_depth': 2,
        'min_child_samples': 300, 'reg_alpha': 15.0, 'reg_lambda': 20.0,
        'bagging_fraction': 0.5, 'bagging_freq': 3, 'feature_fraction': 0.5,
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


def calculate_roi_with_ci(pred_df, returns_df, group_col, group_value, alpha=0.05):
    """ROIと95%信頼区間を計算"""
    tansho = returns_df[returns_df['bet_type'] == 'tansho']
    
    subset = pred_df[(pred_df[group_col] == group_value) & (pred_df['score_rank'] == 1)]
    if len(subset) < 10:
        return None
    
    merged = subset.merge(tansho[['race_id', 'horse_number', 'payout']], 
                          on=['race_id', 'horse_number'], how='left')
    merged['return'] = merged['payout'].fillna(0) / 100
    
    returns = merged['return'].values
    n = len(returns)
    roi = returns.mean() * 100
    
    # ブートストラップで信頼区間を計算
    n_bootstrap = 1000
    bootstrap_rois = []
    for _ in range(n_bootstrap):
        sample = np.random.choice(returns, size=n, replace=True)
        bootstrap_rois.append(sample.mean() * 100)
    
    ci_low = np.percentile(bootstrap_rois, 100 * alpha / 2)
    ci_high = np.percentile(bootstrap_rois, 100 * (1 - alpha / 2))
    
    hit_rate = (merged['payout'].notna()).mean() * 100
    
    return {
        'group': group_value,
        'roi': roi,
        'ci_low': ci_low,
        'ci_high': ci_high,
        'hit_rate': hit_rate,
        'count': n
    }


def analyze_yearly_consistency(all_results, group_col, min_years=3, min_count=50):
    """年度間の一貫性を分析"""
    years = sorted(all_results['year'].unique())
    groups = all_results[group_col].unique()
    
    consistency_results = []
    
    for group in groups:
        group_data = all_results[all_results[group_col] == group]
        
        yearly_rois = []
        yearly_counts = []
        yearly_ci_lows = []
        yearly_ci_highs = []
        
        for year in years:
            year_data = group_data[group_data['year'] == year]
            if len(year_data) > 0:
                roi = year_data['roi'].values[0]
                count = year_data['count'].values[0]
                ci_low = year_data['ci_low'].values[0]
                ci_high = year_data['ci_high'].values[0]
                
                if count >= min_count:
                    yearly_rois.append(roi)
                    yearly_counts.append(count)
                    yearly_ci_lows.append(ci_low)
                    yearly_ci_highs.append(ci_high)
        
        if len(yearly_rois) >= min_years:
            avg_roi = np.mean(yearly_rois)
            std_roi = np.std(yearly_rois)
            min_roi = np.min(yearly_rois)
            max_roi = np.max(yearly_rois)
            total_count = np.sum(yearly_counts)
            
            # 一貫性スコア：全年度でROI > 70%なら高評価
            years_above_70 = sum(1 for r in yearly_rois if r >= 70)
            years_above_80 = sum(1 for r in yearly_rois if r >= 80)
            
            # 再現性判定
            is_reproducible = (years_above_70 >= min_years) and (std_roi < 20)
            is_strongly_reproducible = (years_above_80 >= min_years) and (std_roi < 15)
            
            consistency_results.append({
                'group': group,
                'avg_roi': avg_roi,
                'std_roi': std_roi,
                'min_roi': min_roi,
                'max_roi': max_roi,
                'total_count': total_count,
                'years_with_data': len(yearly_rois),
                'years_above_70': years_above_70,
                'years_above_80': years_above_80,
                'is_reproducible': is_reproducible,
                'is_strongly_reproducible': is_strongly_reproducible,
                'yearly_rois': yearly_rois
            })
    
    return pd.DataFrame(consistency_results)


def main():
    print("=" * 80)
    print("再現性重視の深い推論分析")
    print("=" * 80)
    print("【目的】複数年で一貫するパターンを特定し、次の取り組みに活かす")
    print("=" * 80)
    
    races, pedigrees, corners, race_details, horses, returns = load_data()
    
    # 4年間Walk-forward検証
    years = [2022, 2023, 2024, 2025]
    
    all_condition_results = []
    all_race_num_results = []
    all_combo_results = []
    
    for year in years:
        print(f"\n{'='*60}")
        print(f"{year}年 処理中...")
        
        train_end = f'{year-1}-12-31'
        test_start = f'{year}-01-01'
        test_end = f'{year}-12-31'
        
        train = races[races['race_date'] <= train_end].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] <= test_end)].copy()
        
        if len(test) == 0:
            continue
        
        print(f"  学習: {len(train):,}件 / テスト: {len(test):,}件")
        
        engine = LeakFreeFeatureEngineerV33()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        test_f = engine.transform(test)
        test_f = extract_race_info(test_f)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        
        val_start = train['race_date'].max() - pd.DateOffset(years=1)
        train_sub = train_f[train_f['race_date'] < val_start]
        valid_sub = train_f[train_f['race_date'] >= val_start]
        
        model = train_model(train_sub, valid_sub, feature_cols)
        
        X_test = test_f[feature_cols].fillna(0)
        test_f['score'] = model.predict(X_test)
        test_f['score_rank'] = test_f.groupby('race_id')['score'].rank(ascending=False, method='first')
        
        # 条件別ROI
        for cond in test_f['condition_code'].unique():
            result = calculate_roi_with_ci(test_f, returns, 'condition_code', cond)
            if result:
                result['year'] = year
                result['condition_code'] = cond
                all_condition_results.append(result)
        
        # レース番号別ROI
        for race_num in range(1, 13):
            result = calculate_roi_with_ci(test_f, returns, 'race_number', race_num)
            if result:
                result['year'] = year
                result['race_number'] = race_num
                all_race_num_results.append(result)
        
        # 複合条件（条件 × レース番号グループ）
        test_f['race_num_group'] = pd.cut(test_f['race_number'], 
                                           bins=[0, 6, 9, 12], 
                                           labels=['early', 'middle', 'late'])
        test_f['combo_code'] = test_f['condition_code'] + '_' + test_f['race_num_group'].astype(str)
        
        for combo in test_f['combo_code'].unique():
            result = calculate_roi_with_ci(test_f, returns, 'combo_code', combo)
            if result:
                result['year'] = year
                result['combo_code'] = combo
                all_combo_results.append(result)
        
        print(f"  年間レース数: {test_f['race_id'].nunique():,}件")
    
    cond_df = pd.DataFrame(all_condition_results)
    race_df = pd.DataFrame(all_race_num_results)
    combo_df = pd.DataFrame(all_combo_results)
    
    # ========== 一貫性分析 ==========
    print("\n" + "=" * 80)
    print("【分析1: 条件別ROIの年度間一貫性】")
    print("=" * 80)
    
    cond_consistency = analyze_yearly_consistency(cond_df, 'condition_code')
    cond_consistency = cond_consistency.sort_values('avg_roi', ascending=False)
    
    print(f"\n{'条件':<20} | {'平均ROI':>7} | {'σ':>5} | {'件数':>6} | {'再現性'}")
    print("-" * 60)
    for _, row in cond_consistency.iterrows():
        repro = "★★" if row['is_strongly_reproducible'] else ("★" if row['is_reproducible'] else "")
        print(f"{row['group']:<20} | {row['avg_roi']:>6.1f}% | {row['std_roi']:>5.1f} | {row['total_count']:>6.0f} | {repro}")
    
    # 再現性のある条件のみ抽出
    reproducible_conds = cond_consistency[cond_consistency['is_reproducible']]
    print(f"\n■ 再現性のある条件: {len(reproducible_conds)}件")
    for _, row in reproducible_conds.iterrows():
        print(f"  {row['group']}: 平均ROI {row['avg_roi']:.1f}% (σ={row['std_roi']:.1f})")
    
    # ========== レース番号別一貫性 ==========
    print("\n" + "=" * 80)
    print("【分析2: レース番号別ROIの年度間一貫性】")
    print("=" * 80)
    
    race_consistency = analyze_yearly_consistency(race_df, 'race_number')
    race_consistency = race_consistency.sort_values('avg_roi', ascending=False)
    
    print(f"\n{'レース番号':>10} | {'平均ROI':>7} | {'σ':>5} | {'件数':>6} | {'年度別ROI'}")
    print("-" * 70)
    for _, row in race_consistency.iterrows():
        yearly_str = ", ".join([f"{r:.0f}" for r in row['yearly_rois']])
        repro = "★" if row['is_reproducible'] else ""
        print(f"{int(row['group']):>10}R | {row['avg_roi']:>6.1f}%{repro} | {row['std_roi']:>5.1f} | {row['total_count']:>6.0f} | [{yearly_str}]")
    
    # ========== 複合条件一貫性 ==========
    print("\n" + "=" * 80)
    print("【分析3: 複合条件（条件×レース帯）の一貫性】")
    print("=" * 80)
    
    combo_consistency = analyze_yearly_consistency(combo_df, 'combo_code', min_count=30)
    combo_consistency = combo_consistency.sort_values('avg_roi', ascending=False)
    
    print(f"\n■ 再現性があり高ROIの複合条件（平均ROI 80%以上）:")
    high_roi_combos = combo_consistency[
        (combo_consistency['is_reproducible']) & 
        (combo_consistency['avg_roi'] >= 80)
    ]
    
    if len(high_roi_combos) > 0:
        for _, row in high_roi_combos.iterrows():
            yearly_str = ", ".join([f"{r:.0f}" for r in row['yearly_rois']])
            print(f"  {row['group']}: 平均ROI {row['avg_roi']:.1f}% (σ={row['std_roi']:.1f}) [{yearly_str}]")
    else:
        print("  該当なし")
    
    # ========== 深い推論 ==========
    print("\n" + "=" * 80)
    print("【深い推論: なぜこのパターンが存在するか】")
    print("=" * 80)
    
    print("""
■ 再現性のあるパターンの構造的理由:

┌──────────────────────────────────────────────────────────────┐
│ 【仮説1】条件別ROI差の理由                                    │
│                                                              │
│ ・芝_old_longが高ROI → 古馬中長距離は実力差が明確            │
│ ・芝_old_shortが低ROI → スプリントは展開やペースの影響大     │
│ ・障害が低ROI → データ不足、特殊条件                         │
│                                                              │
│ → 「実力が結果に反映されやすいレース」でモデルが有効         │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│ 【仮説2】レース番号別ROI差の理由                              │
│                                                              │
│ ・R7, R11が高ROI → 特別レース（OP/リステッド）が多い         │
│ ・R6が低ROI → 未勝利戦が多く、データ不足の馬が多い           │
│                                                              │
│ → 「過去データが豊富な馬が出走するレース」でモデルが有効     │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│ 【仮説3】年度による変動の理由                                 │
│                                                              │
│ ・2022年と2025年でパターンが異なる                            │
│ ・競馬トレンド（種牡馬、調教法、馬場管理）の変化              │
│ ・オッズ形成の変化（AI予想の普及）                            │
│                                                              │
│ → 固定ルールではなく、定期的なパターン更新が必要             │
└──────────────────────────────────────────────────────────────┘
""")
    
    # ========== 次の取り組みへの提言 ==========
    print("\n" + "=" * 80)
    print("【次の取り組みへの提言】")
    print("=" * 80)
    
    # 再現性スコアの高い条件を抽出
    actionable_conds = cond_consistency[cond_consistency['is_reproducible']].nlargest(3, 'avg_roi')
    actionable_races = race_consistency[race_consistency['std_roi'] < 15].nlargest(3, 'avg_roi')
    
    print("""
■ 実装推奨の優先順位:

1. 【推奨】条件フィルター
""")
    if len(actionable_conds) > 0:
        for i, (_, row) in enumerate(actionable_conds.iterrows(), 1):
            print(f"   {i}. {row['group']}: 平均ROI {row['avg_roi']:.1f}% (σ={row['std_roi']:.1f})")
    
    print("""
2. 【検討】レース番号フィルター
""")
    if len(actionable_races) > 0:
        for i, (_, row) in enumerate(actionable_races.iterrows(), 1):
            print(f"   {i}. R{int(row['group'])}: 平均ROI {row['avg_roi']:.1f}% (σ={row['std_roi']:.1f})")
    
    print("""
■ 注意事項:
  - σ(標準偏差)が大きい条件は、年度による変動が大きく再現性に疑問
  - サンプル数が少ない条件は、統計的に不安定
  - 複合条件は過学習リスクが高い

■ 推奨アプローチ:
  1. まず「再現性のある条件」のみでフィルタリング
  2. 半年ごとにパターンを再検証
  3. ROI低下が見られたら条件を見直し
""")
    
    # 結果保存
    output_dir = project_root / "outputs/analysis"
    cond_consistency.to_csv(output_dir / "condition_reproducibility.csv", index=False)
    race_consistency.to_csv(output_dir / "race_number_reproducibility.csv", index=False)
    combo_consistency.to_csv(output_dir / "combo_reproducibility.csv", index=False)
    
    print(f"\n結果保存: {output_dir}")
    print("\n分析完了")


if __name__ == "__main__":
    main()
