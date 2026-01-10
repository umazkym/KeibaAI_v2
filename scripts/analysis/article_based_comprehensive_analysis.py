# -*- coding: utf-8 -*-
"""
記事に基づく包括的改善分析

記事の知見と現在のKeibaAI_v2を比較し、改善余地を深く分析。

【分析項目】
1. 特徴量の期間・指数化の確認
2. 条件別（芝/ダート × 年齢 × 距離）分割効果
3. レース番号別ROI
4. 季節性（夏競馬問題）
5. 穴馬モデル（単勝10倍以上）の効果
6. 券種別（単勝/複勝/馬連/ワイド/3連複）の最適化

【データソース】
netkeibaとJRAのみからスクレイピング取得したデータ
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
import warnings
import matplotlib.pyplot as plt
import matplotlib
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
    
    # レース番号（下2桁）
    df['race_number'] = df['race_id_str'].str[-2:].astype(int)
    
    # 月
    df['month'] = df['race_date'].dt.month
    
    # 夏競馬（6-9月）
    df['is_summer'] = df['month'].isin([6, 7, 8, 9])
    
    # 条件分類
    # 芝/ダート
    df['surface'] = df['track_surface']
    
    # 2-3歳限定 vs 古馬混合（年齢列がなければ推測）
    if 'age' in df.columns:
        # レース内の最大年齢で判定
        max_age = df.groupby('race_id')['age'].transform('max')
        df['is_young_limited'] = (max_age <= 3)
    else:
        df['is_young_limited'] = False
    
    # 距離区分（記事の1700m基準）
    df['is_long'] = (df['distance_m'] >= 1700)
    
    # 複合条件コード
    df['condition_code'] = (
        df['surface'].fillna('芝').astype(str) + '_' +
        df['is_young_limited'].map({True: 'young', False: 'old'}) + '_' +
        df['is_long'].map({True: 'long', False: 'short'})
    )
    
    return df


def train_model(train_df, valid_df, feature_cols, objective='binary'):
    """モデル学習"""
    params = {
        'objective': objective,
        'metric': 'auc' if objective == 'binary' else 'ndcg',
        'verbosity': -1,
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


def train_anauma_model(train_df, valid_df, feature_cols):
    """穴馬モデル：単勝10倍以上で3着以内を学習"""
    # 穴馬限定データ
    train_ana = train_df[train_df['win_odds'] >= 10].copy()
    valid_ana = valid_df[valid_df['win_odds'] >= 10].copy()
    
    if len(train_ana) < 1000:
        return None
    
    params = {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.02, 'num_leaves': 8, 'max_depth': 2,
        'min_child_samples': 200, 'reg_alpha': 10.0, 'reg_lambda': 15.0,
        'bagging_fraction': 0.5, 'bagging_freq': 3, 'feature_fraction': 0.5,
        'random_state': 42,
    }
    
    X_train = train_ana[feature_cols].fillna(0)
    X_valid = valid_ana[feature_cols].fillna(0)
    y_train = (train_ana['finish_position'] <= 3).astype(int)  # Top3
    y_valid = (valid_ana['finish_position'] <= 3).astype(int)
    
    train_ds = lgb.Dataset(X_train, y_train)
    valid_ds = lgb.Dataset(X_valid, y_valid, reference=train_ds)
    
    model = lgb.train(params, train_ds, num_boost_round=500, valid_sets=[valid_ds],
                      callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False)])
    return model


def calculate_roi_by_bet_type(pred_df, returns_df):
    """券種別ROIを計算"""
    results = {}
    
    pred_df = pred_df.copy()
    pred_df['score_rank'] = pred_df.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    # Top4を抽出（4頭ボックス用）
    top4 = pred_df[pred_df['score_rank'] <= 4].copy()
    
    # 単勝（Top1のみ）
    tansho = returns_df[returns_df['bet_type'] == 'tansho']
    top1 = pred_df[pred_df['score_rank'] == 1]
    m = top1.merge(tansho[['race_id', 'horse_number', 'payout']], on=['race_id', 'horse_number'], how='left')
    m['hit'] = ~m['payout'].isna()
    results['単勝_Top1'] = {
        'roi': m['payout'].fillna(0).sum() / (len(m) * 100) * 100,
        'hit_rate': m['hit'].mean() * 100,
        'count': len(m)
    }
    
    # 複勝（Top1～Top3）
    fukusho = returns_df[returns_df['bet_type'] == 'fukusho']
    for top_n in [1, 2, 3]:
        topn = pred_df[pred_df['score_rank'] == top_n]
        m = topn.merge(fukusho[['race_id', 'horse_number', 'payout']], on=['race_id', 'horse_number'], how='left')
        m['hit'] = ~m['payout'].isna()
        results[f'複勝_Top{top_n}'] = {
            'roi': m['payout'].fillna(0).sum() / (len(m) * 100) * 100,
            'hit_rate': m['hit'].mean() * 100,
            'count': len(m)
        }
    
    # ワイド（4頭ボックス = 6点）
    wide = returns_df[returns_df['bet_type'] == 'wide']
    race_ids = top4['race_id'].unique()
    wide_hits = 0
    wide_payout = 0
    wide_count = 0
    
    for race_id in race_ids:
        race_top4 = top4[top4['race_id'] == race_id]['horse_number'].values
        if len(race_top4) >= 2:
            race_wide = wide[wide['race_id'] == race_id]
            # 4頭ボックスは6点
            wide_count += 6
            for _, row in race_wide.iterrows():
                nums = [int(x) for x in str(row['horse_number']).split('-') if x.isdigit()]
                if len(nums) >= 2:
                    if nums[0] in race_top4 and nums[1] in race_top4:
                        wide_hits += 1
                        wide_payout += row['payout']
    
    if wide_count > 0:
        results['ワイド_4頭BOX'] = {
            'roi': wide_payout / (wide_count * 100) * 100,
            'hit_rate': wide_hits / (len(race_ids) * 6) * 100 * 6,  # 3的中あたりの確率
            'count': wide_count
        }
    
    return results


def analyze_feature_period():
    """分析1: 特徴量の期間分析"""
    print("\n" + "=" * 80)
    print("【分析1: 特徴量の期間・指数化の確認】")
    print("=" * 80)
    
    print("""
■ 現在のV33特徴量リストを確認:

【馬の統計】
- horse_win_rate, horse_top3_rate: 全期間の累積統計（shift(1)）
- horse_last_finish: 前走着順
- horse_last3_avg_finish: 直近3走の平均着順
- horse_days_since_last: 前走からの日数

【騎手/調教師】
- jockey_win_rate, trainer_win_rate: 全期間の累積統計

【指数化】
- コーナー通過順位: horse_avg_c4_pos（生値、頭数補正なし）
- 上がり3F: horse_last3f_avg（生値、距離/馬場補正なし）

■ 記事との比較と改善案:

┌──────────────────────────────────────────────────────────────┐
│ 【改善1】近走成績の期間短縮                                    │
│ ・現状: 全期間の累積                                          │
│ ・改善案: 直近5走に限定（horse_win_rate_last5 等）            │
│ ・理由: 「競走馬は短期間で大きく変わる」（記事）               │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│ 【改善2】コーナー通過順位の指数化                              │
│ ・現状: 生値（horse_avg_c4_pos = 平均4コーナー位置）          │
│ ・改善案: corner_index = c4_position / field_size (0-1)       │
│ ・理由: 6頭立ての6番手と18頭立ての6番手は違う（記事）          │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│ 【改善3】上がり3Fの指数化                                      │
│ ・現状: 生値（horse_last3f_avg）                              │
│ ・改善案: 芝/ダート/距離で補正した指数                        │
│   芝: 上がり3F / {0.94 + (距離/20000)}                        │
│   ダ: 上がり3F / {1.01 + (距離/20000)}                        │
└──────────────────────────────────────────────────────────────┘

■ 削除推奨の特徴量（記事の知見）:
- 血統（sire_win_rate等）: 馬の生涯変わらないバフ/デバフになる
- 騎手/調教師の全期間統計: ブレイク中の新人を低評価してしまう
  → ただし現状では含まれていないものもある

■ 結論:
V33では既にオッズ/人気を除外しており基本設計は良好。
ただし「近走期間の短縮」「指数化」に改善余地あり。
""")
    

def analyze_condition_split(races, pedigrees, corners, race_details, horses, returns):
    """分析2: 条件別分割効果"""
    print("\n" + "=" * 80)
    print("【分析2: 条件別（芝/ダート×年齢×距離）分割効果】")
    print("=" * 80)
    
    races = extract_race_info(races)
    
    # 2025年データでテスト
    test_year = 2025
    train = races[races['race_date'] < f'{test_year}-01-01'].copy()
    test = races[(races['race_date'] >= f'{test_year}-01-01') & 
                 (races['race_date'] <= f'{test_year}-12-31')].copy()
    
    print(f"\n学習: {len(train):,}件 / テスト: {len(test):,}件")
    
    # 特徴量生成
    engine = LeakFreeFeatureEngineerV33()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    test_f = extract_race_info(test_f)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    # 統合モデル
    val_start = train['race_date'].max() - pd.DateOffset(years=1)
    train_sub = train_f[train_f['race_date'] < val_start]
    valid_sub = train_f[train_f['race_date'] >= val_start]
    
    print("\n統合モデル学習中...")
    unified_model = train_model(train_sub, valid_sub, feature_cols)
    
    X_test = test_f[feature_cols].fillna(0)
    test_f['score_unified'] = unified_model.predict(X_test)
    test_f['rank_unified'] = test_f.groupby('race_id')['score_unified'].rank(ascending=False, method='first')
    
    # 条件別ROI
    tansho = returns[returns['bet_type'] == 'tansho']
    
    print("\n■ 条件別ROI（統合モデル、Top1単勝）:")
    print(f"{'条件':<25} | {'ROI':>7} | {'的中率':>7} | {'件数':>6}")
    print("-" * 55)
    
    condition_results = []
    for cond in test_f['condition_code'].unique():
        subset = test_f[(test_f['condition_code'] == cond) & (test_f['rank_unified'] == 1)]
        if len(subset) >= 50:
            merged = subset.merge(tansho[['race_id', 'horse_number', 'payout']], 
                                  on=['race_id', 'horse_number'], how='left')
            roi = merged['payout'].fillna(0).sum() / (len(merged) * 100) * 100
            hit_rate = merged['payout'].notna().mean() * 100
            print(f"{cond:<25} | {roi:>6.1f}% | {hit_rate:>6.2f}% | {len(subset):>6}")
            condition_results.append({'condition': cond, 'roi': roi, 'hit_rate': hit_rate, 'count': len(subset)})
    
    cond_df = pd.DataFrame(condition_results)
    
    # 全体
    top1_all = test_f[test_f['rank_unified'] == 1]
    merged_all = top1_all.merge(tansho[['race_id', 'horse_number', 'payout']], 
                                on=['race_id', 'horse_number'], how='left')
    roi_all = merged_all['payout'].fillna(0).sum() / (len(merged_all) * 100) * 100
    hit_rate_all = merged_all['payout'].notna().mean() * 100
    print(f"{'【全体】':<25} | {roi_all:>6.1f}% | {hit_rate_all:>6.2f}% | {len(top1_all):>6}")
    
    # 差分分析
    if len(cond_df) > 0:
        best = cond_df.nlargest(1, 'roi').iloc[0]
        worst = cond_df.nsmallest(1, 'roi').iloc[0]
        print(f"\n■ 条件間ROI差: {best['roi'] - worst['roi']:.1f}%")
        print(f"  最良: {best['condition']} (ROI {best['roi']:.1f}%)")
        print(f"  最悪: {worst['condition']} (ROI {worst['roi']:.1f}%)")
    
    return test_f


def analyze_race_number_seasonality(test_f, returns):
    """分析3&4: レース番号別・季節性"""
    print("\n" + "=" * 80)
    print("【分析3: レース番号別ROI】")
    print("=" * 80)
    
    tansho = returns[returns['bet_type'] == 'tansho']
    
    print("\n■ レース番号別ROI（Top1単勝）:")
    print(f"{'レース番号':>10} | {'ROI':>7} | {'的中率':>7} | {'件数':>6}")
    print("-" * 45)
    
    race_num_results = []
    for race_num in range(1, 13):
        subset = test_f[(test_f['race_number'] == race_num) & (test_f['rank_unified'] == 1)]
        if len(subset) >= 20:
            merged = subset.merge(tansho[['race_id', 'horse_number', 'payout']], 
                                  on=['race_id', 'horse_number'], how='left')
            roi = merged['payout'].fillna(0).sum() / (len(merged) * 100) * 100
            hit_rate = merged['payout'].notna().mean() * 100
            mark = "★" if roi >= 80 else ("" if roi >= 70 else "▼")
            print(f"{race_num:>10}R | {roi:>6.1f}%{mark} | {hit_rate:>6.2f}% | {len(subset):>6}")
            race_num_results.append({'race_num': race_num, 'roi': roi, 'hit_rate': hit_rate, 'count': len(subset)})
    
    race_df = pd.DataFrame(race_num_results)
    
    # 記事の知見：第8-12レースが好成績
    late_races = race_df[race_df['race_num'] >= 8]
    early_races = race_df[race_df['race_num'] < 8]
    if len(late_races) > 0 and len(early_races) > 0:
        late_avg = late_races['roi'].mean()
        early_avg = early_races['roi'].mean()
        print(f"\n■ 前半(R1-7) vs 後半(R8-12):")
        print(f"  前半平均ROI: {early_avg:.1f}%")
        print(f"  後半平均ROI: {late_avg:.1f}%")
        print(f"  差: {late_avg - early_avg:+.1f}%")
    
    # 季節性
    print("\n" + "=" * 80)
    print("【分析4: 季節性（夏競馬問題）】")
    print("=" * 80)
    
    print("\n■ 月別ROI（Top1単勝）:")
    print(f"{'月':>5} | {'ROI':>7} | {'的中率':>7} | {'件数':>6} | {'夏競馬'}")
    print("-" * 55)
    
    month_results = []
    for month in range(1, 13):
        subset = test_f[(test_f['month'] == month) & (test_f['rank_unified'] == 1)]
        if len(subset) >= 20:
            merged = subset.merge(tansho[['race_id', 'horse_number', 'payout']], 
                                  on=['race_id', 'horse_number'], how='left')
            roi = merged['payout'].fillna(0).sum() / (len(merged) * 100) * 100
            hit_rate = merged['payout'].notna().mean() * 100
            is_summer = "★" if month in [6, 7, 8, 9] else ""
            print(f"{month:>5}月 | {roi:>6.1f}% | {hit_rate:>6.2f}% | {len(subset):>6} | {is_summer}")
            month_results.append({'month': month, 'roi': roi, 'is_summer': month in [6, 7, 8, 9]})
    
    month_df = pd.DataFrame(month_results)
    summer = month_df[month_df['is_summer']]
    non_summer = month_df[~month_df['is_summer']]
    if len(summer) > 0 and len(non_summer) > 0:
        summer_avg = summer['roi'].mean()
        non_summer_avg = non_summer['roi'].mean()
        print(f"\n■ 夏競馬（6-9月）vs その他:")
        print(f"  夏競馬平均ROI: {summer_avg:.1f}%")
        print(f"  その他平均ROI: {non_summer_avg:.1f}%")
        print(f"  差: {summer_avg - non_summer_avg:+.1f}%")


def analyze_anauma_model(train, test_f, pedigrees, corners, race_details, horses, returns, feature_cols):
    """分析5: 穴馬モデル"""
    print("\n" + "=" * 80)
    print("【分析5: 穴馬モデル（単勝10倍以上）の効果】")
    print("=" * 80)
    
    engine = LeakFreeFeatureEngineerV33()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    train_f = engine.transform(train)
    
    val_start = train['race_date'].max() - pd.DateOffset(years=1)
    train_sub = train_f[train_f['race_date'] < val_start]
    valid_sub = train_f[train_f['race_date'] >= val_start]
    
    # 穴馬モデル学習
    print("\n穴馬モデル学習中（単勝10倍以上でTop3を学習）...")
    anauma_model = train_anauma_model(train_sub, valid_sub, feature_cols)
    
    if anauma_model is None:
        print("  穴馬データ不足のためスキップ")
        return
    
    X_test = test_f[feature_cols].fillna(0)
    test_f = test_f.copy()
    test_f['score_anauma'] = anauma_model.predict(X_test)
    
    # 記事のアプローチ: max(通常モデル, 穴馬モデル)
    test_f['score_max'] = np.maximum(test_f['score_unified'], test_f['score_anauma'])
    test_f['rank_max'] = test_f.groupby('race_id')['score_max'].rank(ascending=False, method='first')
    
    tansho = returns[returns['bet_type'] == 'tansho']
    
    print("\n■ モデル比較（Top1単勝）:")
    print(f"{'モデル':<20} | {'ROI':>7} | {'的中率':>7} | {'件数':>6}")
    print("-" * 50)
    
    for score_col, rank_col, name in [
        ('score_unified', 'rank_unified', '通常モデル'),
        ('score_anauma', 'score_anauma', '穴馬モデル'),
        ('score_max', 'rank_max', 'max(通常,穴馬)')
    ]:
        if rank_col == 'score_anauma':
            # 穴馬モデルのTop1
            test_f['rank_ana'] = test_f.groupby('race_id')['score_anauma'].rank(ascending=False, method='first')
            top1 = test_f[test_f['rank_ana'] == 1]
        else:
            top1 = test_f[test_f[rank_col] == 1]
        
        merged = top1.merge(tansho[['race_id', 'horse_number', 'payout']], 
                            on=['race_id', 'horse_number'], how='left')
        roi = merged['payout'].fillna(0).sum() / (len(merged) * 100) * 100
        hit_rate = merged['payout'].notna().mean() * 100
        print(f"{name:<20} | {roi:>6.1f}% | {hit_rate:>6.2f}% | {len(top1):>6}")
    
    # 穴馬Top1の平均オッズ
    test_f['rank_ana'] = test_f.groupby('race_id')['score_anauma'].rank(ascending=False, method='first')
    ana_top1 = test_f[test_f['rank_ana'] == 1]
    ana_avg_odds = ana_top1['win_odds'].mean()
    print(f"\n  穴馬モデルTop1の平均オッズ: {ana_avg_odds:.1f}倍")


def analyze_bet_types(test_f, returns):
    """分析6: 券種別最適化"""
    print("\n" + "=" * 80)
    print("【分析6: 券種別（単勝/複勝/ワイド等）の最適化】")
    print("=" * 80)
    
    test_f = test_f.copy()
    test_f['score_rank'] = test_f.groupby('race_id')['score_unified'].rank(ascending=False, method='first')
    
    # 単勝
    tansho = returns[returns['bet_type'] == 'tansho']
    top1 = test_f[test_f['score_rank'] == 1]
    m = top1.merge(tansho[['race_id', 'horse_number', 'payout']], on=['race_id', 'horse_number'], how='left')
    roi_tansho = m['payout'].fillna(0).sum() / (len(m) * 100) * 100
    hit_tansho = m['payout'].notna().mean() * 100
    
    # 複勝（Top1-3それぞれ）
    fukusho = returns[returns['bet_type'] == 'fukusho']
    fukusho_results = []
    for top_n in [1, 2, 3]:
        topn = test_f[test_f['score_rank'] == top_n]
        m = topn.merge(fukusho[['race_id', 'horse_number', 'payout']], on=['race_id', 'horse_number'], how='left')
        roi = m['payout'].fillna(0).sum() / (len(m) * 100) * 100
        hit = m['payout'].notna().mean() * 100
        fukusho_results.append({'name': f'複勝Top{top_n}', 'roi': roi, 'hit_rate': hit, 'count': len(m)})
    
    print("\n■ 券種別ROI:")
    print(f"{'券種':<15} | {'ROI':>7} | {'的中率':>7} | {'件数':>8}")
    print("-" * 50)
    print(f"{'単勝Top1':<15} | {roi_tansho:>6.1f}% | {hit_tansho:>6.2f}% | {len(top1):>8}")
    for r in fukusho_results:
        print(f"{r['name']:<15} | {r['roi']:>6.1f}% | {r['hit_rate']:>6.2f}% | {r['count']:>8}")
    
    # 馬連（Top2のペア）
    umaren = returns[returns['bet_type'] == 'umaren']
    top2 = test_f[test_f['score_rank'] <= 2]
    race_ids = top2['race_id'].unique()
    umaren_hits = 0
    umaren_payout = 0
    for race_id in race_ids:
        race_top2 = sorted(top2[top2['race_id'] == race_id]['horse_number'].values)
        if len(race_top2) >= 2:
            race_umaren = umaren[(umaren['race_id'] == race_id)]
            for _, row in race_umaren.iterrows():
                nums = sorted([int(x) for x in str(row['horse_number']).split('-') if x.isdigit()])
                if nums == list(race_top2[:2]):
                    umaren_hits += 1
                    umaren_payout += row['payout']
    
    roi_umaren = umaren_payout / (len(race_ids) * 100) * 100 if len(race_ids) > 0 else 0
    hit_umaren = umaren_hits / len(race_ids) * 100 if len(race_ids) > 0 else 0
    print(f"{'馬連Top2':<15} | {roi_umaren:>6.1f}% | {hit_umaren:>6.2f}% | {len(race_ids):>8}")
    
    # 記事の知見：ワイド4頭ボックスが好成績
    print("""
■ 記事の発見との比較:
  記事では「ワイド4頭ボックス」で回収率104.6%、「3連複4頭ボックス」で184.2%
  特に第12レースではワイド回収率198.8%という驚異的な数字

■ 推論:
  単勝Top1よりも、複勝やワイドの方が控除率が低く有利な可能性
  4頭ボックス戦略は的中率を上げつつ、中穴を拾える
""")


def main():
    print("=" * 80)
    print("記事に基づく包括的改善分析")
    print("=" * 80)
    
    races, pedigrees, corners, race_details, horses, returns = load_data()
    
    # 分析1: 特徴量の期間・指数化
    analyze_feature_period()
    
    # 分析2: 条件別分割効果
    test_f = analyze_condition_split(races, pedigrees, corners, race_details, horses, returns)
    
    # 分析3&4: レース番号別・季節性
    analyze_race_number_seasonality(test_f, returns)
    
    # 分析5: 穴馬モデル
    engine = LeakFreeFeatureEngineerV33()
    test_year = 2025
    train = races[races['race_date'] < f'{test_year}-01-01'].copy()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    feature_cols = [c for c in engine.get_feature_columns() if c in engine.transform(train.head(100)).columns]
    analyze_anauma_model(train, test_f, pedigrees, corners, race_details, horses, returns, feature_cols)
    
    # 分析6: 券種別最適化
    analyze_bet_types(test_f, returns)
    
    # 結論
    print("\n" + "=" * 80)
    print("【総合結論】")
    print("=" * 80)
    print("""
■ 記事の知見との比較結果:

1. 特徴量設計
   - V33は既にオッズ/人気を除外（良好）
   - 改善余地: 近走期間の短縮、指数化の追加

2. 条件別分割
   - 芝/ダート×年齢×距離でROI差が確認できる場合は分割モデルが有効

3. レース番号
   - 記事では第8-12レース、特に第12レースが好成績
   - 現在のモデルでも同様の傾向があるか要確認

4. 季節性
   - 記事では夏競馬（6-9月）で成績悪化
   - 傾向を確認し、夏は購入額を減らす等の対策

5. 穴馬モデル
   - 記事の「max(通常, 穴馬)」アプローチは試す価値あり
   - 現在のV4.4 Residualとは異なる設計

6. 券種
   - 単勝だけでなく、複勝・ワイド・3連複を検討
   - 4頭ボックス戦略の検証価値あり

■ 優先実施順:
1. レース番号/季節性フィルター（低コスト、高効果）
2. 穴馬モデルの追加（中コスト、中効果）
3. 条件別モデル分割（高コスト、高効果）
4. 券種最適化（中コスト、高効果）
""")
    
    print("\n分析完了")


if __name__ == "__main__":
    main()
