# -*- coding: utf-8 -*-
"""
6年間Walk-forward検証 + Top5拡張多角的買い方分析

分析内容:
1. Top1-5まで拡張した予測馬の活用
2. 6年間(2020-2025)のWalk-forward検証
3. 多角的な買い方パターンの比較:
   - 単勝/複勝: Top1のみ vs Top1-3フォーメーション
   - ワイド: Top1-2, Top1-3, Top2-3, Top1-2-3ボックス
   - 馬連: Top1-2, Top1-3, Top1-2-3ボックス
   - 馬単: Top1→2, Top1→2-3, Top1-2→1-2
   - 三連複: Top1-2-3, Top1-2-3-4ボックス, Top1-2→3-5流し
   - 三連単: Top1→2→3, Top1→2-3→2-4, フォーメーション各種
4. オッズ帯別・人気帯別の詳細分析
5. 年次安定性評価（標準偏差、最低ROI）
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
from itertools import combinations, permutations
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


def get_race_predictions(test_df, preds, n_top=5):
    """レースごとにTop1-5の予測馬情報を取得"""
    d = test_df[['race_id', 'horse_number', 'win_odds', 'popularity', 'finish_position']].copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    race_data = {}
    for i in range(1, n_top + 1):
        top_i = d[d['rank_pred'] == i][['race_id', 'horse_number', 'win_odds', 'popularity', 'finish_position']].copy()
        top_i.columns = ['race_id', f'h{i}', f'h{i}_odds', f'h{i}_pop', f'h{i}_finish']
        if i == 1:
            race_df = top_i
        else:
            race_df = race_df.merge(top_i, on='race_id', how='left')
    
    return race_df


def analyze_bet_pattern(race_df, returns_df, bet_type, pattern_name, pattern_func):
    """特定の買い方パターンを分析"""
    bt_returns = returns_df[returns_df['bet_type'] == bet_type].copy()
    
    results = []
    for _, row in race_df.iterrows():
        bet_combos = pattern_func(row)  # [(horses tuple), ...]
        
        for combo in bet_combos:
            hit, payout = check_hit(bt_returns, row['race_id'], bet_type, combo)
            results.append({
                'race_id': row['race_id'],
                'combo': combo,
                'is_hit': hit,
                'payout': payout,
                'h1_odds': row['h1_odds'],
                'h1_pop': row['h1_pop'],
            })
    
    df = pd.DataFrame(results)
    if len(df) == 0:
        return None
    
    # 1レースあたりの集計
    race_summary = df.groupby('race_id').agg({
        'is_hit': 'max',  # いずれかが当たればTrue
        'payout': 'max',  # 最高配当
        'h1_odds': 'first',
        'h1_pop': 'first',
    }).reset_index()
    
    n_bets_per_race = df.groupby('race_id').size().mean()
    
    total_races = len(race_summary)
    total_bets = len(df)
    hits = race_summary['is_hit'].sum()
    total_payout = race_summary['payout'].sum()
    
    roi = total_payout / total_bets * 100 if total_bets > 0 else 0
    hit_rate = hits / total_races * 100 if total_races > 0 else 0
    
    return {
        'pattern': pattern_name,
        'bet_type': bet_type,
        'total_races': total_races,
        'bets_per_race': n_bets_per_race,
        'total_bets': total_bets,
        'hits': hits,
        'hit_rate': hit_rate,
        'total_payout': total_payout,
        'roi': roi,
        'data': race_summary 
    }


def check_hit(bt_returns, race_id, bet_type, combo):
    """馬券が当たったかチェック"""
    race_returns = bt_returns[bt_returns['race_id'] == race_id]
    
    if bet_type == 'tansho':
        hit = race_returns[race_returns['horse_number'] == combo[0]]
        if len(hit) > 0:
            return True, hit.iloc[0]['payout'] / 100
        return False, 0
        
    elif bet_type == 'fukusho':
        hit = race_returns[race_returns['horse_number'] == combo[0]]
        if len(hit) > 0:
            return True, hit.iloc[0]['payout'] / 100
        return False, 0
        
    elif bet_type in ['umaren', 'wide']:
        h_min, h_max = min(combo[0], combo[1]), max(combo[0], combo[1])
        for _, r in race_returns.iterrows():
            r_min = min(r['horse_1'], r['horse_2'])
            r_max = max(r['horse_1'], r['horse_2'])
            if h_min == r_min and h_max == r_max:
                return True, r['payout'] / 100
        return False, 0
        
    elif bet_type == 'umatan':
        for _, r in race_returns.iterrows():
            if combo[0] == r['horse_1'] and combo[1] == r['horse_2']:
                return True, r['payout'] / 100
        return False, 0
        
    elif bet_type == 'sanrenpuku':
        h_sorted = tuple(sorted(combo))
        for _, r in race_returns.iterrows():
            r_sorted = tuple(sorted([r['horse_1'], r['horse_2'], r['horse_3']]))
            if h_sorted == r_sorted:
                return True, r['payout'] / 100
        return False, 0
        
    elif bet_type == 'sanrentan':
        for _, r in race_returns.iterrows():
            if combo[0] == r['horse_1'] and combo[1] == r['horse_2'] and combo[2] == r['horse_3']:
                return True, r['payout'] / 100
        return False, 0
    
    return False, 0


# ============================================================
# 買い方パターン定義
# ============================================================

def pattern_tansho_top1(row):
    """単勝 Top1のみ"""
    if pd.isna(row['h1']): return []
    return [(int(row['h1']),)]

def pattern_tansho_top3(row):
    """単勝 Top1-3各1点"""
    r = []
    for i in range(1, 4):
        h = row.get(f'h{i}')
        if pd.notna(h):
            r.append((int(h),))
    return r

def pattern_fukusho_top1(row):
    """複勝 Top1のみ"""
    if pd.isna(row['h1']): return []
    return [(int(row['h1']),)]

def pattern_fukusho_top3(row):
    """複勝 Top1-3各1点"""
    r = []
    for i in range(1, 4):
        h = row.get(f'h{i}')
        if pd.notna(h):
            r.append((int(h),))
    return r

def pattern_wide_1_2(row):
    """ワイド Top1-Top2"""
    if pd.isna(row['h1']) or pd.isna(row['h2']): return []
    return [(int(row['h1']), int(row['h2']))]

def pattern_wide_box3(row):
    """ワイド Top1-2-3ボックス (3点)"""
    horses = [row.get(f'h{i}') for i in range(1, 4)]
    horses = [int(h) for h in horses if pd.notna(h)]
    if len(horses) < 2: return []
    return list(combinations(horses, 2))

def pattern_umaren_1_2(row):
    """馬連 Top1-Top2"""
    if pd.isna(row['h1']) or pd.isna(row['h2']): return []
    return [(int(row['h1']), int(row['h2']))]

def pattern_umaren_box3(row):
    """馬連 Top1-2-3ボックス (3点)"""
    horses = [row.get(f'h{i}') for i in range(1, 4)]
    horses = [int(h) for h in horses if pd.notna(h)]
    if len(horses) < 2: return []
    return list(combinations(horses, 2))

def pattern_umatan_1to2(row):
    """馬単 Top1→Top2"""
    if pd.isna(row['h1']) or pd.isna(row['h2']): return []
    return [(int(row['h1']), int(row['h2']))]

def pattern_umatan_1to23(row):
    """馬単 Top1→Top2,3 (2点)"""
    r = []
    h1 = row.get('h1')
    if pd.isna(h1): return []
    for i in [2, 3]:
        hi = row.get(f'h{i}')
        if pd.notna(hi):
            r.append((int(h1), int(hi)))
    return r

def pattern_umatan_box3(row):
    """馬単 Top1-2-3ボックス (6点)"""
    horses = [row.get(f'h{i}') for i in range(1, 4)]
    horses = [int(h) for h in horses if pd.notna(h)]
    if len(horses) < 2: return []
    return list(permutations(horses, 2))

def pattern_sanrenpuku_123(row):
    """三連複 Top1-2-3 (1点)"""
    horses = [row.get(f'h{i}') for i in range(1, 4)]
    horses = [int(h) for h in horses if pd.notna(h)]
    if len(horses) < 3: return []
    return [tuple(horses)]

def pattern_sanrenpuku_box4(row):
    """三連複 Top1-2-3-4ボックス (4点)"""
    horses = [row.get(f'h{i}') for i in range(1, 5)]
    horses = [int(h) for h in horses if pd.notna(h)]
    if len(horses) < 3: return []
    return list(combinations(horses, 3))

def pattern_sanrenpuku_12to345(row):
    """三連複 Top1-2軸→Top3-5流し (3点)"""
    h1, h2 = row.get('h1'), row.get('h2')
    if pd.isna(h1) or pd.isna(h2): return []
    r = []
    for i in [3, 4, 5]:
        hi = row.get(f'h{i}')
        if pd.notna(hi):
            r.append(tuple(sorted([int(h1), int(h2), int(hi)])))
    return r

def pattern_sanrentan_123(row):
    """三連単 Top1→2→3 (1点)"""
    horses = [row.get(f'h{i}') for i in range(1, 4)]
    if any(pd.isna(h) for h in horses): return []
    return [(int(horses[0]), int(horses[1]), int(horses[2]))]

def pattern_sanrentan_1to23to24(row):
    """三連単 Top1→2,3→2,3,4 フォーメーション (4点)"""
    h1, h2, h3, h4 = [row.get(f'h{i}') for i in range(1, 5)]
    if pd.isna(h1) or pd.isna(h2) or pd.isna(h3): return []
    
    r = []
    for second in [h2, h3]:
        for third in [h2, h3, h4] if pd.notna(h4) else [h2, h3]:
            if second != third and pd.notna(second) and pd.notna(third):
                r.append((int(h1), int(second), int(third)))
    return r

def pattern_sanrentan_box3(row):
    """三連単 Top1-2-3ボックス (6点)"""
    horses = [row.get(f'h{i}') for i in range(1, 4)]
    horses = [int(h) for h in horses if pd.notna(h)]
    if len(horses) < 3: return []
    return list(permutations(horses, 3))


def main():
    print("=" * 80)
    print("6年間Walk-forward検証 + Top5拡張多角的買い方分析")
    print("=" * 80)
    
    races, pedigrees, corners, race_details, horses, returns = load_data()
    
    # 6年間Walk-forward期間設定
    years = [2020, 2021, 2022, 2023, 2024, 2025]
    
    # 分析する買い方パターン
    patterns = [
        ('tansho', '単勝 Top1', pattern_tansho_top1),
        ('tansho', '単勝 Top1-3各1点', pattern_tansho_top3),
        ('fukusho', '複勝 Top1', pattern_fukusho_top1),
        ('fukusho', '複勝 Top1-3各1点', pattern_fukusho_top3),
        ('wide', 'ワイド Top1-2', pattern_wide_1_2),
        ('wide', 'ワイド Top1-2-3ボックス', pattern_wide_box3),
        ('umaren', '馬連 Top1-2', pattern_umaren_1_2),
        ('umaren', '馬連 Top1-2-3ボックス', pattern_umaren_box3),
        ('umatan', '馬単 Top1→2', pattern_umatan_1to2),
        ('umatan', '馬単 Top1→2,3', pattern_umatan_1to23),
        ('umatan', '馬単 Top1-2-3ボックス', pattern_umatan_box3),
        ('sanrenpuku', '三連複 Top1-2-3', pattern_sanrenpuku_123),
        ('sanrenpuku', '三連複 Top1-2-3-4ボックス', pattern_sanrenpuku_box4),
        ('sanrenpuku', '三連複 Top1-2軸流し', pattern_sanrenpuku_12to345),
        ('sanrentan', '三連単 Top1→2→3', pattern_sanrentan_123),
        ('sanrentan', '三連単 フォーメーション', pattern_sanrentan_1to23to24),
        ('sanrentan', '三連単 Top1-2-3ボックス', pattern_sanrentan_box3),
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
            print(f"  {year}年のテストデータなし、スキップ")
            continue
        
        print(f"  学習データ: {len(train):,}件 (〜{train_end})")
        print(f"  テストデータ: {len(test):,}件 ({test_start}〜{test_end})")
        
        # 特徴量生成
        engine = LeakFreeFeatureEngineerV33()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        test_f = engine.transform(test)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        
        # 検証用データ分割
        val_start = pd.to_datetime(train_end) - pd.DateOffset(years=1)
        train_sub = train_f[train_f['race_date'] < val_start]
        valid_sub = train_f[train_f['race_date'] >= val_start]
        
        if len(train_sub) < 1000 or len(valid_sub) < 100:
            print(f"  学習データ不足、スキップ")
            continue
        
        # モデル学習
        model = train_model(train_sub, valid_sub, feature_cols)
        
        # 予測
        X_test = test_f[feature_cols].fillna(0)
        preds = model.predict(X_test)
        
        # レースごとのTop5予測を取得
        race_df = get_race_predictions(test_f, preds, n_top=5)
        
        print(f"\n■ {year}年 買い方パターン別ROI")
        print(f"{'買い方':>25} | {'点数/R':>6} | {'的中率':>7} | {'ROI':>7} | {'備考'}")
        print("-" * 70)
        
        year_results = []
        
        for bet_type, pattern_name, pattern_func in patterns:
            result = analyze_bet_pattern(race_df, returns, bet_type, pattern_name, pattern_func)
            if result:
                roi_mark = "★" if result['roi'] >= 100 else ""
                note = ""
                if result['roi'] >= 100:
                    note = "収益候補"
                elif result['roi'] >= 90:
                    note = "惜しい"
                    
                print(f"{pattern_name:>25} | {result['bets_per_race']:>6.1f} | {result['hit_rate']:>6.2f}% | {result['roi']:>6.1f}%{roi_mark} | {note}")
                
                result['year'] = year
                year_results.append(result)
                all_results.append({
                    'year': year,
                    'pattern': pattern_name,
                    'bet_type': bet_type,
                    'bets_per_race': result['bets_per_race'],
                    'hit_rate': result['hit_rate'],
                    'roi': result['roi'],
                })
    
    # 6年間のサマリー
    print("\n" + "=" * 80)
    print("【6年間Walk-forward検証サマリー】")
    print("=" * 80)
    
    df = pd.DataFrame(all_results)
    
    summary = df.groupby('pattern').agg({
        'roi': ['mean', 'std', 'min', 'max'],
        'hit_rate': 'mean',
        'bets_per_race': 'first',
    }).round(2)
    
    summary.columns = ['平均ROI', 'ROI標準偏差', '最低ROI', '最高ROI', '平均的中率', '点数/R']
    summary = summary.sort_values('平均ROI', ascending=False)
    
    print(f"\n{'買い方':>25} | {'平均ROI':>8} | {'σ':>6} | {'最低':>6} | {'最高':>6} | {'的中率':>7} | {'評価'}")
    print("-" * 85)
    
    for pattern, row in summary.iterrows():
        eval_ = ""
        if row['平均ROI'] >= 100:
            eval_ = "★有望"
        elif row['平均ROI'] >= 90 and row['ROI標準偏差'] < 20:
            eval_ = "安定"
        elif row['最低ROI'] < 50:
            eval_ = "変動大"
        
        print(f"{pattern:>25} | {row['平均ROI']:>7.1f}% | {row['ROI標準偏差']:>5.1f} | {row['最低ROI']:>5.1f}% | {row['最高ROI']:>5.1f}% | {row['平均的中率']:>6.2f}% | {eval_}")
    
    # CSV保存
    output_path = project_root / 'outputs/analysis/walkforward_6years_multipattern.csv'
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"\n結果保存: {output_path}")
    
    # 深い推論
    print("\n" + "=" * 80)
    print("【深い推論: 買い方パターンの比較分析】")
    print("=" * 80)
    
    # Top候補の発見
    top_patterns = summary[summary['平均ROI'] >= 90].index.tolist()
    stable_patterns = summary[(summary['ROI標準偏差'] < 15) & (summary['平均ROI'] >= 70)].index.tolist()
    
    print(f"""
■ 発見1: 高ROI候補（平均ROI ≥ 90%）
{chr(10).join(['  - ' + p for p in top_patterns]) if top_patterns else '  なし'}

■ 発見2: 安定候補（σ < 15, ROI ≥ 70%）  
{chr(10).join(['  - ' + p for p in stable_patterns]) if stable_patterns else '  なし'}

■ 発見3: 点数拡大の効果
  - 単勝Top1 vs Top1-3: 点数3倍で的中率向上効果は？
  - 馬連1点 vs ボックス3点: 的中率向上 vs ROI低下のトレードオフ
  - 三連単1点 vs フォーメーション: リスク分散効果

■ 発見4: 年次変動パターン
  - 標準偏差が大きい買い方は年によって大きくブレる
  - 最低ROIが低い買い方は特定年で大損の可能性

■ 推奨戦略
  1. 高ROI + 低σの買い方を選択
  2. 点数増やしすぎず、1レースあたり期待値を維持
  3. オッズフィルター（20-50倍帯）との組み合わせ
""")
    
    print("\n" + "=" * 80)
    print("分析完了")
    print("=" * 80)


if __name__ == "__main__":
    main()
