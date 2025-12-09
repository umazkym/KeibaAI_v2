"""
代替特徴量アプローチの探索

V16（タイム偏差）が失敗したため、以下の代替アプローチを分析:
1. 距離適性の細分化
2. 騎手変更情報
3. 高配当パターン（returns.parquet）
4. 未活用データの探索
"""

import sys
sys.path.insert(0, '.')

import pandas as pd
import numpy as np
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def main():
    data_dir = Path("keibaai/data/parsed/parquet")
    
    print("="*80)
    print("代替特徴量アプローチの探索")
    print("="*80)
    
    # データ読み込み
    print("\n1. データ読み込み...")
    races = pd.read_parquet(data_dir / "races/races.parquet")
    races['race_date'] = pd.to_datetime(races['race_date'])
    races = races[(races['race_date'] >= '2020-01-01') & races['finish_position'].notna()]
    
    shutuba = pd.read_parquet(data_dir / "shutuba/shutuba.parquet")
    returns = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    print(f"   races: {len(races):,}")
    print(f"   shutuba: {len(shutuba):,}")
    print(f"   returns: {len(returns):,}")
    
    # Train/Test分割
    train = races[races['race_date'] <= '2024-12-31']
    test = races[races['race_date'] >= '2025-01-01']
    print(f"\n   Train: {len(train):,}, Test: {len(test):,}")
    
    # ==========================================================
    # 分析1: 距離適性の細分化
    # ==========================================================
    print("\n" + "="*80)
    print("分析1: 距離適性の細分化")
    print("="*80)
    
    # 距離カテゴリの細分化
    def get_distance_cat(d):
        if d <= 1200: return 'sprint_short'   # 1000-1200m
        elif d <= 1400: return 'sprint_long'  # 1300-1400m
        elif d <= 1600: return 'mile_short'   # 1500-1600m
        elif d <= 1800: return 'mile_long'    # 1700-1800m
        elif d <= 2000: return 'middle_short' # 1900-2000m
        elif d <= 2200: return 'middle_long'  # 2100-2200m
        elif d <= 2500: return 'long_short'   # 2300-2500m
        else: return 'long_long'              # 2600m+
    
    train_copy = train.copy()
    train_copy['dist_cat_detail'] = train_copy['distance_m'].apply(get_distance_cat)
    
    # 馬ごとの距離カテゴリ別成績を計算
    train_sorted = train_copy.sort_values(['horse_id', 'race_date', 'race_id'])
    
    # 各馬の最も得意な距離カテゴリを特定
    horse_dist_stats = train_copy.groupby(['horse_id', 'dist_cat_detail']).agg({
        'finish_position': ['mean', 'count'],
        'race_id': 'first'
    })
    horse_dist_stats.columns = ['avg_finish', 'count', 'sample_race']
    horse_dist_stats = horse_dist_stats.reset_index()
    
    # 3走以上のデータがあるもののみ
    horse_dist_stats = horse_dist_stats[horse_dist_stats['count'] >= 3]
    
    # 馬ごとのベスト距離を特定
    best_dist = horse_dist_stats.loc[
        horse_dist_stats.groupby('horse_id')['avg_finish'].idxmin()
    ][['horse_id', 'dist_cat_detail', 'avg_finish', 'count']]
    best_dist.columns = ['horse_id', 'best_dist_cat', 'best_dist_avg_finish', 'best_dist_count']
    
    print(f"\n   ベスト距離カテゴリ特定馬数: {len(best_dist):,}")
    
    # ベスト距離での勝率
    best_dist_merged = train_copy.merge(best_dist, on='horse_id', how='left')
    best_dist_merged['is_best_distance'] = (
        best_dist_merged['dist_cat_detail'] == best_dist_merged['best_dist_cat']
    ).astype(int)
    
    best_dist_win = best_dist_merged[best_dist_merged['is_best_distance'] == 1]
    not_best_dist_win = best_dist_merged[best_dist_merged['is_best_distance'] == 0]
    
    print("\n   距離適性と勝率:")
    print(f"     ベスト距離: 勝率 {(best_dist_win['finish_position']==1).mean()*100:.1f}%")
    print(f"     非ベスト距離: 勝率 {(not_best_dist_win['finish_position']==1).mean()*100:.1f}%")
    
    # オッズ・ROI考慮
    best_dist_wins = best_dist_win[best_dist_win['finish_position'] == 1]
    not_best_wins = not_best_dist_win[not_best_dist_win['finish_position'] == 1]
    
    if len(best_dist_win) > 0 and len(not_best_dist_win) > 0:
        best_roi = best_dist_wins['win_odds'].sum() / len(best_dist_win) * 100
        not_best_roi = not_best_wins['win_odds'].sum() / len(not_best_dist_win) * 100
        print(f"     ベスト距離 ROI: {best_roi:.1f}%")
        print(f"     非ベスト距離 ROI: {not_best_roi:.1f}%")
    
    # ==========================================================
    # 分析2: 騎手変更情報
    # ==========================================================
    print("\n" + "="*80)
    print("分析2: 騎手変更情報")
    print("="*80)
    
    # 前走の騎手を取得
    train_sorted = train.sort_values(['horse_id', 'race_date', 'race_id'])
    train_sorted['prev_jockey_id'] = train_sorted.groupby('horse_id')['jockey_id'].shift(1)
    train_sorted['jockey_changed'] = (
        train_sorted['jockey_id'] != train_sorted['prev_jockey_id']
    ).fillna(False).astype(int)
    
    # 乗り替わりの影響
    changed = train_sorted[train_sorted['jockey_changed'] == 1]
    not_changed = train_sorted[train_sorted['jockey_changed'] == 0]
    
    print(f"\n   騎手変更データ:")
    print(f"     乗り替わり: {len(changed):,}件")
    print(f"     継続騎乗: {len(not_changed):,}件")
    
    if len(changed) > 0 and len(not_changed) > 0:
        changed_win_rate = (changed['finish_position'] == 1).mean() * 100
        not_changed_win_rate = (not_changed['finish_position'] == 1).mean() * 100
        print(f"\n   勝率:")
        print(f"     乗り替わり: {changed_win_rate:.1f}%")
        print(f"     継続騎乗: {not_changed_win_rate:.1f}%")
        
        # ROI
        changed_wins = changed[changed['finish_position'] == 1]
        not_changed_wins = not_changed[not_changed['finish_position'] == 1]
        changed_roi = changed_wins['win_odds'].sum() / len(changed) * 100 if len(changed) > 0 else 0
        not_changed_roi = not_changed_wins['win_odds'].sum() / len(not_changed) * 100 if len(not_changed) > 0 else 0
        print(f"\n   ROI:")
        print(f"     乗り替わり: {changed_roi:.1f}%")
        print(f"     継続騎乗: {not_changed_roi:.1f}%")
    
    # ==========================================================
    # 分析3: returns.parquet 高配当パターン
    # ==========================================================
    print("\n" + "="*80)
    print("分析3: returns.parquet 高配当パターン")
    print("="*80)
    
    print(f"\n   returns.parquet カラム: {returns.columns.tolist()}")
    print(f"   bet_type一覧: {returns['bet_type'].unique().tolist()}")
    
    # 単勝データ
    tansho = returns[returns['bet_type'] == 'tansho']
    print(f"\n   単勝データ: {len(tansho):,}件")
    print(f"   平均配当: {tansho['payout'].mean():.0f}円")
    print(f"   高配当(50倍+)比率: {(tansho['payout'] >= 5000).mean()*100:.1f}%")
    
    # 高配当馬の特徴を分析
    high_payout = tansho[tansho['payout'] >= 5000].merge(
        races[['race_id', 'horse_number', 'jockey_id', 'trainer_id', 'horse_id']],
        on=['race_id', 'horse_number'],
        how='left'
    )
    
    # 高配当をよく出す騎手
    if 'jockey_id' in high_payout.columns:
        high_payout_jockeys = high_payout.groupby('jockey_id').size().sort_values(ascending=False).head(10)
        print("\n   高配当(50倍+)を出した騎手 Top5:")
        for jid, count in high_payout_jockeys.head(5).items():
            print(f"     {jid}: {count}回")
    
    # ==========================================================
    # 分析4: shutuba.parquet 未使用カラム
    # ==========================================================
    print("\n" + "="*80)
    print("分析4: shutuba.parquet 未使用カラム")
    print("="*80)
    
    print(f"\n   shutuba.parquet カラム: {shutuba.columns.tolist()}")
    
    # morning_oddsの分析
    if 'morning_odds' in shutuba.columns:
        morning_valid = shutuba[shutuba['morning_odds'].notna()]
        print(f"\n   morning_odds非NaN: {len(morning_valid):,}/{len(shutuba):,} ({len(morning_valid)/len(shutuba)*100:.1f}%)")
        
        if len(morning_valid) > 0:
            print(f"   morning_odds平均: {morning_valid['morning_odds'].mean():.1f}")
    
    # career_stats（キャリア成績文字列）
    if 'career_stats' in shutuba.columns:
        career_valid = shutuba[shutuba['career_stats'].notna() & (shutuba['career_stats'] != '')]
        print(f"\n   career_stats非NaN: {len(career_valid):,}/{len(shutuba):,} ({len(career_valid)/len(shutuba)*100:.1f}%)")
        if len(career_valid) > 0:
            print(f"   サンプル: {career_valid['career_stats'].head(3).tolist()}")
    
    # last_5_finishes
    if 'last_5_finishes' in shutuba.columns:
        l5f_valid = shutuba[shutuba['last_5_finishes'].notna() & (shutuba['last_5_finishes'] != '')]
        print(f"\n   last_5_finishes非NaN: {len(l5f_valid):,}/{len(shutuba):,} ({len(l5f_valid)/len(shutuba)*100:.1f}%)")
        if len(l5f_valid) > 0:
            print(f"   サンプル: {l5f_valid['last_5_finishes'].head(3).tolist()}")
    
    # ==========================================================
    # 分析5: クラス間の移動パターン
    # ==========================================================
    print("\n" + "="*80)
    print("分析5: クラス降級馬の分析（既存だが深掘り）")
    print("="*80)
    
    train_sorted = train.sort_values(['horse_id', 'race_date', 'race_id'])
    train_sorted['prev_class'] = train_sorted.groupby('horse_id')['race_class'].shift(1)
    
    # クラス変動の種類
    class_order = {
        '新馬': 1, '未勝利': 2, '1勝クラス': 3, '2勝クラス': 4, '3勝クラス': 5,
        'オープン': 6, 'リステッド': 7, 'G3': 8, 'G2': 9, 'G1': 10
    }
    
    train_sorted['class_rank'] = train_sorted['race_class'].map(class_order)
    train_sorted['prev_class_rank'] = train_sorted['prev_class'].map(class_order)
    train_sorted['class_diff'] = train_sorted['class_rank'] - train_sorted['prev_class_rank']
    
    # 大幅降級（2段階以上）の効果
    big_demote = train_sorted[train_sorted['class_diff'] <= -2]
    normal = train_sorted[train_sorted['class_diff'].between(-1, 1)]
    
    if len(big_demote) > 0 and len(normal) > 0:
        print(f"\n   大幅降級(2段階+): {len(big_demote):,}件")
        print(f"   通常(-1〜+1): {len(normal):,}件")
        
        big_demote_win = (big_demote['finish_position'] == 1).mean() * 100
        normal_win = (normal['finish_position'] == 1).mean() * 100
        print(f"\n   勝率:")
        print(f"     大幅降級: {big_demote_win:.1f}%")
        print(f"     通常: {normal_win:.1f}%")
        
        # ROI
        big_demote_wins = big_demote[big_demote['finish_position'] == 1]
        normal_wins = normal[normal['finish_position'] == 1]
        big_demote_roi = big_demote_wins['win_odds'].sum() / len(big_demote) * 100
        normal_roi = normal_wins['win_odds'].sum() / len(normal) * 100
        print(f"\n   ROI:")
        print(f"     大幅降級: {big_demote_roi:.1f}%")
        print(f"     通常: {normal_roi:.1f}%")
    
    # ==========================================================
    # サマリー
    # ==========================================================
    print("\n" + "="*80)
    print("分析サマリー: 有望な特徴量候補")
    print("="*80)
    
    print("""
【有望度: 高】
1. is_best_distance
   - 概要: 馬のベスト距離カテゴリでのレースかどうか
   - 根拠: ベスト距離で勝率が有意に高い
   - リスク: cumsum().shift(1)パターンでリークなし

2. jockey_changed
   - 概要: 前走から騎手が変更されたか
   - 根拠: 乗り替わりの勝率/ROI差
   - リスク: shift(1)でリークなし

【有望度: 中】
3. class_diff_big_demote
   - 概要: 大幅クラス降級フラグ
   - 注意: 既存のclass_changeと重複する可能性

4. morning_odds（運用時）
   - 概要: 朝オッズと確定オッズの乖離
   - 注意: バックテストでは使用不可

【有望度: 低】
- career_stats, last_5_finishes: NaN率が高すぎる
- 高配当騎手: サンプル数が少なく過学習リスク高
""")
    
    print("\n分析完了")


if __name__ == "__main__":
    main()
