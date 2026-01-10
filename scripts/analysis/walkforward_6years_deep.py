# -*- coding: utf-8 -*-
"""
6年間Walk-forward検証 + 特定条件深掘り + 具体レース分析

分析内容:
1. 6年間(2020-2025)の各条件のROI推移
2. 有望条件の年次安定性評価
3. 具体的な的中レース/外れレースの詳細分析
4. 条件組み合わせの最適化
5. ドローダウン分析
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
from itertools import combinations
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
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.02, 'num_leaves': 8, 'max_depth': 2,
        'min_child_samples': 300, 'reg_alpha': 15.0, 'reg_lambda': 20.0,
        'bagging_fraction': 0.5, 'bagging_freq': 3, 'feature_fraction': 0.5, 'random_state': 42,
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


def analyze_condition(pred_df, returns_df, bet_type, odds_min, odds_max, condition_name):
    """特定条件の分析"""
    top1 = pred_df[pred_df['rank_pred'] == 1].copy()
    filtered = top1[(top1['win_odds'] >= odds_min) & (top1['win_odds'] < odds_max)]
    
    if len(filtered) == 0:
        return None
    
    bt_returns = returns_df[returns_df['bet_type'] == bet_type]
    
    if bet_type == 'tansho':
        merged = filtered.merge(bt_returns[['race_id', 'horse_number', 'payout']], 
                                on=['race_id', 'horse_number'], how='left')
        merged['is_hit'] = ~merged['payout'].isna()
        merged['return'] = merged['payout'].fillna(0) / 100
    elif bet_type == 'fukusho':
        merged = filtered.merge(bt_returns[['race_id', 'horse_number', 'payout']], 
                                on=['race_id', 'horse_number'], how='left')
        merged['is_hit'] = ~merged['payout'].isna()
        merged['return'] = merged['payout'].fillna(0) / 100
    elif bet_type == 'umaren':
        top2 = pred_df[pred_df['rank_pred'] == 2][['race_id', 'horse_number']].copy()
        top2.columns = ['race_id', 'h2']
        filtered = filtered.merge(top2, on='race_id', how='left')
        
        bt_returns_tmp = bt_returns.copy()
        bt_returns_tmp['h_min'] = bt_returns_tmp[['horse_1', 'horse_2']].min(axis=1)
        bt_returns_tmp['h_max'] = bt_returns_tmp[['horse_1', 'horse_2']].max(axis=1)
        
        filtered['h_min'] = filtered[['horse_number', 'h2']].min(axis=1)
        filtered['h_max'] = filtered[['horse_number', 'h2']].max(axis=1)
        
        merged = filtered.merge(bt_returns_tmp[['race_id', 'h_min', 'h_max', 'payout']], 
                                on=['race_id', 'h_min', 'h_max'], how='left')
        merged['is_hit'] = ~merged['payout'].isna()
        merged['return'] = merged['payout'].fillna(0) / 100
    elif bet_type == 'umatan':
        top2 = pred_df[pred_df['rank_pred'] == 2][['race_id', 'horse_number']].copy()
        top2.columns = ['race_id', 'h2']
        filtered = filtered.copy()
        filtered['h1'] = filtered['horse_number']
        filtered = filtered.merge(top2, on='race_id', how='left')
        
        merged = filtered.merge(bt_returns.rename(columns={'horse_1': 'win1', 'horse_2': 'win2'}),
                                on='race_id', how='left')
        merged['is_hit'] = (merged['h1'] == merged['win1']) & (merged['h2'] == merged['win2']) & (~merged['payout'].isna())
        merged['return'] = np.where(merged['is_hit'], merged['payout'] / 100, 0)
        merged = merged.drop_duplicates(subset=['race_id'])
    elif bet_type == 'sanrenpuku':
        top2 = pred_df[pred_df['rank_pred'] == 2][['race_id', 'horse_number']].copy()
        top2.columns = ['race_id', 'h2']
        top3 = pred_df[pred_df['rank_pred'] == 3][['race_id', 'horse_number']].copy()
        top3.columns = ['race_id', 'h3']
        filtered = filtered.merge(top2, on='race_id', how='left').merge(top3, on='race_id', how='left')
        
        bt_returns_tmp = bt_returns.copy()
        bt_returns_tmp['h_sorted'] = bt_returns_tmp.apply(
            lambda x: tuple(sorted([x['horse_1'], x['horse_2'], x['horse_3']])), axis=1)
        
        filtered['h_sorted'] = filtered.apply(
            lambda x: tuple(sorted([x['horse_number'], x['h2'], x['h3']])) if pd.notna(x['h3']) else (0, 0, 0), axis=1)
        
        merged = filtered.merge(bt_returns_tmp[['race_id', 'h_sorted', 'payout']], 
                                on=['race_id', 'h_sorted'], how='left')
        merged['is_hit'] = ~merged['payout'].isna()
        merged['return'] = merged['payout'].fillna(0) / 100
    else:
        return None
    
    total = len(merged)
    hits = merged['is_hit'].sum()
    hr = hits / total * 100 if total > 0 else 0
    total_return = merged['return'].sum()
    roi = total_return / total * 100 if total > 0 else 0
    avg_return = merged[merged['is_hit']]['return'].mean() if hits > 0 else 0
    
    return {
        'condition': condition_name,
        'bet_type': bet_type,
        'total': total,
        'hits': hits,
        'hit_rate': hr,
        'total_return': total_return,
        'roi': roi,
        'avg_return': avg_return,
        'data': merged,
    }


def analyze_specific_races(merged_df, condition_name, n_samples=10):
    """具体的なレースの詳細分析"""
    hits = merged_df[merged_df['is_hit']]
    misses = merged_df[~merged_df['is_hit']]
    
    print(f"\n■ {condition_name} - 高配当的中 Top{n_samples}:")
    if len(hits) > 0:
        top_hits = hits.nlargest(min(n_samples, len(hits)), 'return')
        for i, (_, row) in enumerate(top_hits.iterrows(), 1):
            pop = int(row['popularity']) if pd.notna(row.get('popularity')) else '-'
            odds = row['win_odds'] if pd.notna(row.get('win_odds')) else '-'
            print(f"  {i:>2}. 払戻{row['return']:.1f}倍 | {pop}番人気({odds:.1f}倍) | race_id={row['race_id']}")
    else:
        print("  的中なし")
    
    print(f"\n■ {condition_name} - 外れレースの特徴:")
    if len(misses) > 0:
        avg_odds = misses['win_odds'].mean() if 'win_odds' in misses.columns else 0
        avg_pop = misses['popularity'].mean() if 'popularity' in misses.columns else 0
        avg_finish = misses['finish_position'].mean() if 'finish_position' in misses.columns else 0
        print(f"  外れ件数: {len(misses):,}")
        print(f"  平均オッズ: {avg_odds:.2f}倍")
        print(f"  平均人気: {avg_pop:.2f}位")
        print(f"  平均着順: {avg_finish:.2f}着")
        
        # 外れた時の着順分布
        if 'finish_position' in misses.columns:
            finish_dist = misses['finish_position'].value_counts().sort_index().head(5)
            print(f"  着順分布: ", end="")
            for pos, count in finish_dist.items():
                print(f"{int(pos)}着:{count}件 ", end="")
            print()


def main():
    print("=" * 80)
    print("6年間Walk-forward検証 + 特定条件深掘り + 具体レース分析")
    print("=" * 80)
    
    races, pedigrees, corners, race_details, horses, returns = load_data()
    
    # 6年間Walk-forward期間
    years = [2020, 2021, 2022, 2023, 2024, 2025]
    
    # 検証する条件
    conditions = [
        ('tansho', 1, 5, '単勝 1-5倍'),
        ('tansho', 5, 10, '単勝 5-10倍'),
        ('tansho', 10, 20, '単勝 10-20倍'),
        ('tansho', 20, 50, '単勝 20-50倍'),
        ('tansho', 50, 200, '単勝 50倍以上'),
        ('fukusho', 1, 5, '複勝 1-5倍'),
        ('fukusho', 5, 10, '複勝 5-10倍'),
        ('fukusho', 10, 20, '複勝 10-20倍'),
        ('fukusho', 20, 50, '複勝 20-50倍'),
        ('umaren', 1, 5, '馬連 1-5倍'),
        ('umaren', 5, 10, '馬連 5-10倍'),
        ('umaren', 10, 20, '馬連 10-20倍'),
        ('umaren', 20, 50, '馬連 20-50倍'),
        ('umatan', 1, 5, '馬単 1-5倍'),
        ('umatan', 5, 10, '馬単 5-10倍'),
        ('umatan', 10, 20, '馬単 10-20倍'),
        ('umatan', 20, 50, '馬単 20-50倍'),
        ('sanrenpuku', 1, 5, '三連複 1-5倍'),
        ('sanrenpuku', 5, 10, '三連複 5-10倍'),
        ('sanrenpuku', 10, 20, '三連複 10-20倍'),
        ('sanrenpuku', 20, 50, '三連複 20-50倍'),
    ]
    
    all_results = []
    
    for year in years:
        print(f"\n{'#'*80}")
        print(f"# {year}年 分析")
        print(f"{'#'*80}")
        
        train_end = f'{year-1}-12-31'
        test_start = f'{year}-01-01'
        test_end = f'{year}-12-31'
        
        train = races[races['race_date'] <= train_end].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] <= test_end)].copy()
        
        if len(test) == 0:
            print(f"  {year}年のテストデータなし")
            continue
        
        print(f"  学習: {len(train):,}件 / テスト: {len(test):,}件")
        
        # 特徴量生成
        engine = LeakFreeFeatureEngineerV33()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        test_f = engine.transform(test)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        
        val_start = pd.to_datetime(train_end) - pd.DateOffset(years=1)
        train_sub = train_f[train_f['race_date'] < val_start]
        valid_sub = train_f[train_f['race_date'] >= val_start]
        
        if len(train_sub) < 1000:
            print(f"  学習データ不足")
            continue
        
        model = train_model(train_sub, valid_sub, feature_cols)
        
        X_test = test_f[feature_cols].fillna(0)
        preds = model.predict(X_test)
        
        # 予測データ作成
        pred_df = test_f[['race_id', 'horse_number', 'win_odds', 'popularity', 'finish_position']].copy()
        pred_df['score'] = preds
        pred_df['rank_pred'] = pred_df.groupby('race_id')['score'].rank(ascending=False, method='first')
        
        print(f"\n■ {year}年 条件別ROI:")
        print(f"{'条件':>16} | {'件数':>5} | {'的中':>4} | {'的中率':>6} | {'ROI':>7} | {'平均払戻':>7}")
        print("-" * 65)
        
        year_results = []
        
        for bet_type, odds_min, odds_max, cond_name in conditions:
            result = analyze_condition(pred_df, returns, bet_type, odds_min, odds_max, cond_name)
            if result and result['total'] >= 10:
                mark = "★" if result['roi'] >= 100 else ""
                print(f"{cond_name:>16} | {result['total']:>5} | {result['hits']:>4} | {result['hit_rate']:>5.2f}% | {result['roi']:>6.1f}%{mark} | {result['avg_return']:>6.2f}倍")
                
                all_results.append({
                    'year': year,
                    'condition': cond_name,
                    'bet_type': bet_type,
                    'total': result['total'],
                    'hits': result['hits'],
                    'hit_rate': result['hit_rate'],
                    'roi': result['roi'],
                    'avg_return': result['avg_return'],
                })
                
                year_results.append(result)
        
        # 有望条件の具体レース分析（最新年のみ）
        if year == 2025:
            print(f"\n{'='*80}")
            print(f"【2025年 具体レース分析】")
            print(f"{'='*80}")
            
            for result in year_results:
                if result['roi'] >= 90:  # ROI90%以上の条件を深掘り
                    analyze_specific_races(result['data'], result['condition'])
    
    # 6年間サマリー
    print("\n" + "=" * 80)
    print("【6年間Walk-forward検証サマリー】")
    print("=" * 80)
    
    df = pd.DataFrame(all_results)
    
    summary = df.groupby('condition').agg({
        'roi': ['mean', 'std', 'min', 'max'],
        'hit_rate': 'mean',
        'total': 'sum',
        'hits': 'sum',
    }).round(2)
    
    summary.columns = ['平均ROI', 'σ', '最低ROI', '最高ROI', '平均的中率', '総件数', '総的中']
    summary = summary.sort_values('平均ROI', ascending=False)
    
    print(f"\n{'条件':>16} | {'平均ROI':>7} | {'σ':>5} | {'最低':>6} | {'最高':>6} | {'総件数':>6} | {'評価'}")
    print("-" * 75)
    
    for cond, row in summary.iterrows():
        eval_ = ""
        if row['平均ROI'] >= 100:
            eval_ = "★有望"
        elif row['平均ROI'] >= 90 and row['σ'] < 20:
            eval_ = "安定"
        elif row['最低ROI'] >= 70:
            eval_ = "堅実"
        elif row['最低ROI'] < 50:
            eval_ = "変動大"
        
        print(f"{cond:>16} | {row['平均ROI']:>6.1f}% | {row['σ']:>4.1f} | {row['最低ROI']:>5.1f}% | {row['最高ROI']:>5.1f}% | {int(row['総件数']):>6,} | {eval_}")
    
    # 年別ROI推移
    print("\n" + "=" * 80)
    print("【年別ROI推移（有望条件）】")
    print("=" * 80)
    
    promising_conditions = summary[summary['平均ROI'] >= 80].index.tolist()
    
    for cond in promising_conditions[:10]:  # 上位10条件
        cond_data = df[df['condition'] == cond]
        print(f"\n■ {cond}:")
        print("  年 | ROI | 件数 | 的中 | 的中率")
        print("  " + "-" * 40)
        for _, row in cond_data.iterrows():
            mark = "★" if row['roi'] >= 100 else ""
            print(f"  {row['year']} | {row['roi']:>5.1f}%{mark} | {row['total']:>4} | {row['hits']:>4} | {row['hit_rate']:>5.2f}%")
    
    # 深い推論
    print("\n" + "=" * 80)
    print("【深い推論: 6年間検証からの洞察】")
    print("=" * 80)
    
    # 安定して高ROIの条件を特定
    stable_high = summary[(summary['平均ROI'] >= 90) & (summary['σ'] < 25)].index.tolist()
    high_variance = summary[summary['σ'] >= 30].index.tolist()
    consistent_profit = summary[summary['最低ROI'] >= 70].index.tolist()
    
    print(f"""
■ 発見1: 安定して高ROIの条件（平均≥90%, σ<25）
{chr(10).join(['  - ' + c for c in stable_high]) if stable_high else '  なし'}

■ 発見2: 変動が大きい条件（σ≥30）
{chr(10).join(['  - ' + c for c in high_variance]) if high_variance else '  なし'}

■ 発見3: 堅実な条件（最低ROI≥70%）
{chr(10).join(['  - ' + c for c in consistent_profit]) if consistent_profit else '  なし'}

■ 推論: 最適なポートフォリオ戦略
  1. メイン: 安定高ROI条件から選択
  2. リスクヘッジ: 堅実条件で損失を抑制
  3. 変動大条件は少額 or 見送り

■ 注意点
  - 6年間で一貫してROI100%超の条件は稀
  - 年次変動を考慮した資金管理が重要
  - 条件組み合わせでリスク分散を検討
""")
    
    # CSV保存
    output_path = project_root / 'outputs/analysis/walkforward_6years_detailed.csv'
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"\n結果保存: {output_path}")
    
    print("\n" + "=" * 80)
    print("分析完了")
    print("=" * 80)


if __name__ == "__main__":
    main()
