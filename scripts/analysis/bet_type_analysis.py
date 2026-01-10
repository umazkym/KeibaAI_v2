# -*- coding: utf-8 -*-
"""
馬券種別の多角的詳細分析

各券種ごとにROIがどう変わるか:
- 単勝: Top1馬の1着
- 複勝: Top1馬の3着以内
- ワイド: Top2馬が両方3着以内
- 馬連: Top2馬が1-2着
- 馬単: Top1が1着かつTop2が2着
- 三連複: Top3馬が全て3着以内
- 三連単: Top3馬が1-2-3着（順番通り）

各券種のオッズは概算で計算（実オッズがない場合）
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


def analyze_bet_types(test_df, preds):
    """
    各馬券種のROIを分析
    """
    d = test_df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    results = []
    
    for race_id, group in d.groupby('race_id'):
        group = group.sort_values('rank_pred')
        
        if len(group) < 3:
            continue
        
        top1 = group.iloc[0]
        top2 = group.iloc[1]
        top3 = group.iloc[2]
        
        # 実際の着順
        pos1 = top1['finish_position']
        pos2 = top2['finish_position']
        pos3 = top3['finish_position']
        
        # 各馬のオッズ
        odds1 = top1['win_odds']
        odds2 = top2['win_odds']
        odds3 = top3['win_odds']
        
        # 各馬券の的中判定とオッズ概算
        result = {
            'race_id': race_id,
            'year': top1['race_date'].year,
            'top1_pop': top1['popularity'],
            'top2_pop': top2['popularity'],
            'top3_pop': top3['popularity'],
            'top1_odds': odds1,
            'top2_odds': odds2,
            'top3_odds': odds3,
        }
        
        # 単勝
        result['tansho_hit'] = (pos1 == 1)
        result['tansho_odds'] = odds1 if result['tansho_hit'] else 0
        
        # 複勝（Top1馬）- オッズは単勝の約1/3
        result['fukusho_hit'] = (pos1 <= 3)
        result['fukusho_odds'] = odds1 / 3 if result['fukusho_hit'] else 0
        
        # ワイド（Top2馬）- 両方がTop3なら的中
        result['wide_hit'] = (pos1 <= 3) and (pos2 <= 3)
        result['wide_odds'] = (odds1 + odds2) / 3 if result['wide_hit'] else 0
        
        # 馬連（Top2馬）- 両方がTop2なら的中（順不同）
        result['umaren_hit'] = {pos1, pos2} == {1, 2}
        result['umaren_odds'] = (odds1 * odds2) / 10 if result['umaren_hit'] else 0
        
        # 馬単（Top1→Top2）- Top1が1着かつTop2が2着
        result['umatan_hit'] = (pos1 == 1) and (pos2 == 2)
        result['umatan_odds'] = (odds1 * odds2) / 5 if result['umatan_hit'] else 0
        
        # 三連複（Top3馬）- 全員がTop3なら的中
        result['sanrenpuku_hit'] = {pos1, pos2, pos3} == {1, 2, 3}
        result['sanrenpuku_odds'] = (odds1 * odds2 * odds3) / 30 if result['sanrenpuku_hit'] else 0
        
        # 三連単（Top1→Top2→Top3）
        result['sanrentan_hit'] = (pos1 == 1) and (pos2 == 2) and (pos3 == 3)
        result['sanrentan_odds'] = (odds1 * odds2 * odds3) / 10 if result['sanrentan_hit'] else 0
        
        results.append(result)
    
    return pd.DataFrame(results)


def detailed_bet_type_analysis(result_df):
    """各馬券種の詳細分析"""
    
    bet_types = [
        ('単勝', 'tansho'),
        ('複勝', 'fukusho'),
        ('ワイド', 'wide'),
        ('馬連', 'umaren'),
        ('馬単', 'umatan'),
        ('三連複', 'sanrenpuku'),
        ('三連単', 'sanrentan'),
    ]
    
    print("\n" + "=" * 80)
    print("【各馬券種の基本ROI】")
    print("=" * 80)
    print(f"{'券種':>10} {'ROI':>10} {'的中率':>10} {'的中数':>10} {'レース数':>10}")
    print("-" * 60)
    
    for label, key in bet_types:
        hit_col = f'{key}_hit'
        odds_col = f'{key}_odds'
        
        total = len(result_df)
        hits = result_df[result_df[hit_col] == True]
        hit_count = len(hits)
        hit_rate = hit_count / total * 100
        total_return = result_df[odds_col].sum()
        roi = total_return / total * 100
        
        mark = "★" if roi >= 100 else ""
        print(f"{label:>10} {roi:>9.1f}%{mark} {hit_rate:>9.1f}% {hit_count:>10} {total:>10}")
    
    # 人気帯別分析
    print("\n" + "=" * 80)
    print("【Top1馬の人気別 × 券種別ROI】")
    print("=" * 80)
    
    result_df['top1_pop_group'] = pd.cut(result_df['top1_pop'], 
        bins=[0, 2, 5, 10, 18], labels=['1-2人気', '3-5人気', '6-10人気', '11人気以下'])
    
    print(f"\n{'人気':>12} | {'単勝':>8} | {'複勝':>8} | {'ワイド':>8} | {'馬連':>8} | {'三連複':>8}")
    print("-" * 70)
    
    for pop_group in ['1-2人気', '3-5人気', '6-10人気', '11人気以下']:
        subset = result_df[result_df['top1_pop_group'] == pop_group]
        if len(subset) < 50:
            continue
        
        row = f"{pop_group:>12} |"
        
        for key in ['tansho', 'fukusho', 'wide', 'umaren', 'sanrenpuku']:
            odds_col = f'{key}_odds'
            total_return = subset[odds_col].sum()
            roi = total_return / len(subset) * 100
            mark = "★" if roi >= 100 else ""
            row += f" {roi:>6.0f}%{mark} |"
        
        print(row)
    
    # オッズ帯別分析（単勝）
    print("\n" + "=" * 80)
    print("【Top1馬のオッズ帯別 × 券種別ROI】")
    print("=" * 80)
    
    result_df['odds_band'] = pd.cut(result_df['top1_odds'], 
        bins=[0, 2, 5, 10, 20, 50, 1000], 
        labels=['1-2倍', '2-5倍', '5-10倍', '10-20倍', '20-50倍', '50倍以上'])
    
    print(f"\n{'オッズ帯':>12} | {'単勝':>8} | {'複勝':>8} | {'ワイド':>8} | {'馬連':>8} | {'三連複':>8}")
    print("-" * 70)
    
    for odds_band in ['1-2倍', '2-5倍', '5-10倍', '10-20倍', '20-50倍', '50倍以上']:
        subset = result_df[result_df['odds_band'] == odds_band]
        if len(subset) < 30:
            continue
        
        row = f"{odds_band:>12} |"
        
        for key in ['tansho', 'fukusho', 'wide', 'umaren', 'sanrenpuku']:
            odds_col = f'{key}_odds'
            total_return = subset[odds_col].sum()
            roi = total_return / len(subset) * 100 if len(subset) > 0 else 0
            mark = "★" if roi >= 100 else ""
            row += f" {roi:>6.0f}%{mark} |"
        
        print(row)
    
    # 年別安定性
    print("\n" + "=" * 80)
    print("【年別 × 券種別ROI】")
    print("=" * 80)
    
    print(f"\n{'年':>6} | {'単勝':>8} | {'複勝':>8} | {'ワイド':>8} | {'馬連':>8} | {'三連複':>8}")
    print("-" * 60)
    
    for year in sorted(result_df['year'].unique()):
        subset = result_df[result_df['year'] == year]
        
        row = f"{year:>6} |"
        
        for key in ['tansho', 'fukusho', 'wide', 'umaren', 'sanrenpuku']:
            odds_col = f'{key}_odds'
            total_return = subset[odds_col].sum()
            roi = total_return / len(subset) * 100
            mark = "★" if roi >= 100 else ""
            row += f" {roi:>6.0f}%{mark} |"
        
        print(row)
    
    # 複合条件での分析
    print("\n" + "=" * 80)
    print("【複合条件での券種別ROI探索】")
    print("=" * 80)
    
    # 条件1: 高オッズで単勝
    print("\n■ 高オッズ（10-50倍）での単勝")
    high_odds = result_df[(result_df['top1_odds'] >= 10) & (result_df['top1_odds'] < 50)]
    if len(high_odds) > 0:
        tansho_roi = high_odds['tansho_odds'].sum() / len(high_odds) * 100
        print(f"  単勝ROI: {tansho_roi:.1f}%, {len(high_odds)}件")
    
    # 条件2: 低オッズでワイド
    print("\n■ 低オッズ（2-5倍）でのワイド")
    low_odds = result_df[(result_df['top1_odds'] >= 2) & (result_df['top1_odds'] < 5)]
    if len(low_odds) > 0:
        wide_roi = low_odds['wide_odds'].sum() / len(low_odds) * 100
        print(f"  ワイドROI: {wide_roi:.1f}%, {len(low_odds)}件")
    
    # 条件3: 人気馬で三連複
    print("\n■ 人気馬（Top1が1-3人気）での三連複")
    pop_top3 = result_df[result_df['top1_pop'] <= 3]
    if len(pop_top3) > 0:
        sanren_roi = pop_top3['sanrenpuku_odds'].sum() / len(pop_top3) * 100
        print(f"  三連複ROI: {sanren_roi:.1f}%, {len(pop_top3)}件")
    
    # 条件4: 穴馬で単勝
    print("\n■ 穴馬（Top1が6人気以下）での単勝")
    dark_horse = result_df[result_df['top1_pop'] >= 6]
    if len(dark_horse) > 0:
        tansho_roi = dark_horse['tansho_odds'].sum() / len(dark_horse) * 100
        print(f"  単勝ROI: {tansho_roi:.1f}%, {len(dark_horse)}件")
    
    # 推奨戦略の探索
    print("\n" + "=" * 80)
    print("【推奨戦略の探索】")
    print("=" * 80)
    
    strategies = []
    
    # 各条件 × 各券種の組み合わせを探索
    conditions = [
        ('人気馬(1-3)', result_df[result_df['top1_pop'] <= 3]),
        ('中人気(4-6)', result_df[(result_df['top1_pop'] >= 4) & (result_df['top1_pop'] <= 6)]),
        ('穴馬(7+)', result_df[result_df['top1_pop'] >= 7]),
        ('低オッズ(1-5倍)', result_df[result_df['top1_odds'] < 5]),
        ('中オッズ(5-15倍)', result_df[(result_df['top1_odds'] >= 5) & (result_df['top1_odds'] < 15)]),
        ('高オッズ(15-50倍)', result_df[(result_df['top1_odds'] >= 15) & (result_df['top1_odds'] < 50)]),
    ]
    
    for cond_name, subset in conditions:
        if len(subset) < 50:
            continue
        
        for label, key in bet_types:
            if key == 'fukusho':  # 複勝は除外
                continue
            
            odds_col = f'{key}_odds'
            total_return = subset[odds_col].sum()
            roi = total_return / len(subset) * 100
            
            if roi >= 95:
                strategies.append({
                    'condition': cond_name,
                    'bet_type': label,
                    'roi': roi,
                    'count': len(subset),
                })
    
    if strategies:
        strategies_df = pd.DataFrame(strategies).sort_values('roi', ascending=False)
        print("\n【ROI≥95%の条件×券種組み合わせ】")
        for _, row in strategies_df.head(15).iterrows():
            mark = "★" if row['roi'] >= 100 else ""
            print(f"  {row['condition']:>15} × {row['bet_type']:>6}: ROI {row['roi']:>6.1f}%{mark}, {row['count']}件")
    
    return result_df


def main():
    print("=" * 80)
    print("馬券種別の多角的詳細分析")
    print("=" * 80)
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    # 直近2年で詳細分析
    train_end = '2023-12-31'
    test_start = '2024-01-01'
    
    train = races[races['race_date'] <= train_end].copy()
    test = races[races['race_date'] >= test_start].copy()
    
    print(f"\n訓練データ: {len(train):,}件")
    print(f"テストデータ: {len(test):,}件（2024-2025年）")
    
    print("\n特徴量生成中...")
    engine = LeakFreeFeatureEngineerV33()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    print(f"特徴量数: {len(feature_cols)}")
    
    val_start = '2023-01-01'
    train_sub = train_f[train_f['race_date'] < val_start]
    valid_sub = train_f[train_f['race_date'] >= val_start]
    
    print("モデル学習中...")
    model = train_model(train_sub, valid_sub, feature_cols)
    
    X_test = test_f[feature_cols].fillna(0)
    preds = model.predict(X_test)
    
    print("\n馬券種別ROI計算中...")
    result_df = analyze_bet_types(test_f, preds)
    
    print(f"\n分析対象レース数: {len(result_df)}")
    
    # 詳細分析
    result_df = detailed_bet_type_analysis(result_df)
    
    # 結果保存
    output_dir = project_root / "outputs" / "analysis"
    result_df.to_csv(output_dir / "bet_type_analysis.csv", index=False)
    
    print("\n" + "=" * 80)
    print("分析完了")
    print("=" * 80)


if __name__ == "__main__":
    main()
