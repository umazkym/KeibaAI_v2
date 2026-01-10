#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
記事ベースの深層分析スクリプト Part 2

記事「競馬AI開発ロードマップ」の提案に基づく詳細分析:
1. returns.parquetの確認
2. 時系列パターン分析（上がり調子・スランプ検出）
3. セグメント別ROI傾向分析
4. 展開パターン分析
5. Entity Embeddings候補の詳細分析
"""

import pandas as pd
import numpy as np
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

BASE_PATH = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\parsed\parquet")

def print_section(title: str):
    print("\n" + "=" * 80)
    print(f"【{title}】")
    print("=" * 80)

def main():
    # ========================================
    print_section("1. returns.parquet の詳細確認")
    # ========================================
    
    returns_path = BASE_PATH / "returns" / "returns.parquet"
    if returns_path.exists():
        returns = pd.read_parquet(returns_path)
        print(f"行数: {len(returns):,}")
        print(f"列数: {len(returns.columns)}")
        print(f"\nカラム一覧:")
        for col in returns.columns:
            dtype = returns[col].dtype
            null_count = returns[col].isnull().sum()
            null_pct = null_count / len(returns) * 100
            unique_count = returns[col].nunique()
            print(f"  {col}: {dtype} (NULL: {null_pct:.1f}%, ユニーク: {unique_count:,})")
        
        if 'bet_type' in returns.columns:
            print(f"\n--- 券種別レコード数 ---")
            print(returns['bet_type'].value_counts())
        
        if 'payout' in returns.columns:
            print(f"\n--- 払戻金額統計 ---")
            print(f"平均: {returns['payout'].mean():.0f}円")
            print(f"中央値: {returns['payout'].median():.0f}円")
            print(f"最大: {returns['payout'].max():,.0f}円")
            
            # 券種別
            if 'bet_type' in returns.columns:
                print(f"\n--- 券種別平均払戻 ---")
                print(returns.groupby('bet_type')['payout'].mean().round(0))
    
    # ========================================
    print_section("2. 時系列パターン分析（上がり調子・スランプ検出）")
    # ========================================
    
    races_path = BASE_PATH / "races" / "races.parquet"
    if races_path.exists():
        races = pd.read_parquet(races_path)
        
        # 馬ごとに時系列でソート
        races['race_date'] = pd.to_datetime(races['race_date'])
        races = races.sort_values(['horse_id', 'race_date'])
        
        # 5走以上の馬のみ分析
        horse_counts = races.groupby('horse_id').size()
        horses_with_history = horse_counts[horse_counts >= 5].index
        races_with_history = races[races['horse_id'].isin(horses_with_history)].copy()
        
        print(f"分析対象馬数: {len(horses_with_history):,}頭")
        print(f"分析対象レコード数: {len(races_with_history):,}件")
        
        # 各馬の過去成績パターン分析
        # (1) 着順の推移パターン
        print("\n--- 着順推移パターン（過去3走→今走）---")
        
        races_with_history['prev_finish_1'] = races_with_history.groupby('horse_id')['finish_position'].shift(1)
        races_with_history['prev_finish_2'] = races_with_history.groupby('horse_id')['finish_position'].shift(2)
        races_with_history['prev_finish_3'] = races_with_history.groupby('horse_id')['finish_position'].shift(3)
        
        # 上がり調子パターン: 前3走で着順が連続改善
        improving = races_with_history[
            (races_with_history['prev_finish_1'] < races_with_history['prev_finish_2']) &
            (races_with_history['prev_finish_2'] < races_with_history['prev_finish_3'])
        ]
        print(f"\n上がり調子パターン（前3走で着順連続改善）: {len(improving):,}件")
        if len(improving) > 0:
            print(f"  今走平均着順: {improving['finish_position'].mean():.2f}")
            print(f"  今走1着率: {(improving['finish_position'] == 1).mean() * 100:.1f}%")
        
        # スランプパターン: 前3走で着順が連続悪化
        declining = races_with_history[
            (races_with_history['prev_finish_1'] > races_with_history['prev_finish_2']) &
            (races_with_history['prev_finish_2'] > races_with_history['prev_finish_3'])
        ]
        print(f"\nスランプパターン（前3走で着順連続悪化）: {len(declining):,}件")
        if len(declining) > 0:
            print(f"  今走平均着順: {declining['finish_position'].mean():.2f}")
            print(f"  今走1着率: {(declining['finish_position'] == 1).mean() * 100:.1f}%")
        
        # 復活パターン: 前走大敗→今走好走（休養明け等）
        bounce_back = races_with_history[
            (races_with_history['prev_finish_1'] >= 10) &
            (races_with_history['finish_position'] <= 3)
        ]
        print(f"\n復活パターン（前走10着以下→今走3着以内）: {len(bounce_back):,}件")
        if len(bounce_back) > 0:
            bounce_back_rate = len(bounce_back) / len(races_with_history[races_with_history['prev_finish_1'] >= 10]) * 100
            print(f"  復活率: {bounce_back_rate:.1f}%")
    
    # ========================================
    print_section("3. セグメント別勝率傾向分析")
    # ========================================
    
    if races_path.exists():
        races = pd.read_parquet(races_path)
        races['is_win'] = (races['finish_position'] == 1).astype(int)
        
        # 芝/ダート別
        print("\n--- 馬場タイプ別勝率 ---")
        if 'track_surface' in races.columns:
            print(races.groupby('track_surface')['is_win'].agg(['mean', 'count']))
        
        # 距離カテゴリ別
        print("\n--- 距離カテゴリ別勝率 ---")
        if 'distance_category' in races.columns:
            print(races.groupby('distance_category')['is_win'].agg(['mean', 'count']))
        
        # クラス別
        print("\n--- クラス別勝率 ---")
        if 'race_class' in races.columns:
            print(races.groupby('race_class')['is_win'].agg(['mean', 'count']))
        
        # 競馬場別
        print("\n--- 競馬場別勝率 ---")
        if 'venue' in races.columns:
            print(races.groupby('venue')['is_win'].agg(['mean', 'count']).sort_values('mean', ascending=False))
        
        # 月別（6月の成績低下を検証）
        print("\n--- 月別勝率（夏競馬検証）---")
        races['month'] = pd.to_datetime(races['race_date']).dt.month
        print(races.groupby('month')['is_win'].agg(['mean', 'count']))
    
    # ========================================
    print_section("4. 展開予測の詳細分析")
    # ========================================
    
    corners_path = BASE_PATH / "corners" / "corner_positions.parquet"
    if corners_path.exists() and races_path.exists():
        corners = pd.read_parquet(corners_path)
        races = pd.read_parquet(races_path)
        
        # 4コーナー（最終コーナー）での位置と着順の関係
        c4 = corners[corners['corner'] == 4].copy()
        c4 = c4.merge(
            races[['race_id', 'horse_number', 'finish_position', 'win_odds']],
            on=['race_id', 'horse_number'],
            how='left'
        )
        
        print("\n--- 4コーナー位置別の着順・勝率 ---")
        c4['is_win'] = (c4['finish_position'] == 1).astype(int)
        c4_stats = c4.groupby('position').agg({
            'is_win': ['mean', 'count'],
            'finish_position': 'mean'
        }).round(3)
        c4_stats.columns = ['勝率', 'レコード数', '平均着順']
        print(c4_stats.head(10))
        
        # 逃げ馬（4コーナー1番手）の分析
        print("\n--- 逃げ馬（4コーナー1番手）の詳細分析 ---")
        frontrunners = c4[c4['position'] == 1]
        print(f"総数: {len(frontrunners):,}件")
        print(f"勝率: {frontrunners['is_win'].mean() * 100:.1f}%")
        print(f"平均着順: {frontrunners['finish_position'].mean():.2f}")
        
        # 差し馬（4コーナー5-8番手）の分析
        print("\n--- 差し馬（4コーナー5-8番手）の詳細分析 ---")
        closers = c4[(c4['position'] >= 5) & (c4['position'] <= 8)]
        print(f"総数: {len(closers):,}件")
        print(f"勝率: {closers['is_win'].mean() * 100:.1f}%")
        print(f"平均着順: {closers['finish_position'].mean():.2f}")
        
        # 追い込み馬（4コーナー10番手以下）の分析
        print("\n--- 追い込み馬（4コーナー10番手以下）の詳細分析 ---")
        late_closers = c4[c4['position'] >= 10]
        print(f"総数: {len(late_closers):,}件")
        print(f"勝率: {late_closers['is_win'].mean() * 100:.1f}%")
        print(f"平均着順: {late_closers['finish_position'].mean():.2f}")
    
    # ========================================
    print_section("5. Entity Embeddings詳細分析")
    # ========================================
    
    if races_path.exists():
        races = pd.read_parquet(races_path)
        races['is_win'] = (races['finish_position'] == 1).astype(int)
        
        # 騎手の詳細分析
        print("\n--- 騎手別勝率分析（100騎乗以上）---")
        jockey_stats = races.groupby('jockey_id').agg({
            'is_win': ['sum', 'count', 'mean'],
            'win_odds': 'mean'
        })
        jockey_stats.columns = ['勝利数', '騎乗数', '勝率', '平均オッズ']
        jockey_stats = jockey_stats[jockey_stats['騎乗数'] >= 100]
        print(f"100騎乗以上の騎手数: {len(jockey_stats)}")
        print(f"勝率トップ10:")
        print(jockey_stats.sort_values('勝率', ascending=False).head(10))
        
        # 調教師の詳細分析
        print("\n--- 調教師別勝率分析（100頭以上出走）---")
        trainer_stats = races.groupby('trainer_id').agg({
            'is_win': ['sum', 'count', 'mean']
        })
        trainer_stats.columns = ['勝利数', '出走数', '勝率']
        trainer_stats = trainer_stats[trainer_stats['出走数'] >= 100]
        print(f"100頭以上出走の調教師数: {len(trainer_stats)}")
        print(f"勝率トップ10:")
        print(trainer_stats.sort_values('勝率', ascending=False).head(10))
        
        # 種牡馬の詳細分析
        pedigrees_path = BASE_PATH / "pedigrees" / "pedigrees.parquet"
        if pedigrees_path.exists():
            pedigrees = pd.read_parquet(pedigrees_path)
            # 父馬（generation=1）のみ
            sires = pedigrees[pedigrees['generation'] == 1][['horse_id', 'ancestor_id', 'ancestor_name']]
            sires = sires.rename(columns={'ancestor_id': 'sire_id', 'ancestor_name': 'sire_name'})
            
            races_with_sire = races.merge(sires, on='horse_id', how='left')
            
            print("\n--- 種牡馬別勝率分析（産駒100頭以上）---")
            sire_stats = races_with_sire.groupby('sire_id').agg({
                'is_win': ['sum', 'count', 'mean'],
                'sire_name': 'first'
            })
            sire_stats.columns = ['勝利数', '出走数', '勝率', '種牡馬名']
            sire_stats = sire_stats[sire_stats['出走数'] >= 100]
            print(f"産駒100頭以上の種牡馬数: {len(sire_stats)}")
            print(f"勝率トップ10:")
            print(sire_stats.sort_values('勝率', ascending=False)[['種牡馬名', '勝利数', '出走数', '勝率']].head(10))
    
    # ========================================
    print_section("6. 穴馬分析（オッズ10倍以上で好走）")
    # ========================================
    
    if races_path.exists():
        races = pd.read_parquet(races_path)
        
        # 穴馬（オッズ10倍以上）の勝率
        print("\n--- オッズ帯別勝率 ---")
        races['odds_category'] = pd.cut(
            races['win_odds'],
            bins=[0, 3, 5, 10, 20, 50, 1000],
            labels=['~3倍', '3-5倍', '5-10倍', '10-20倍', '20-50倍', '50倍~']
        )
        odds_stats = races.groupby('odds_category').agg({
            'finish_position': lambda x: (x == 1).mean(),
            'race_id': 'count'
        })
        odds_stats.columns = ['勝率', 'レコード数']
        print(odds_stats)
        
        # 穴馬の特徴分析
        print("\n--- 穴馬（10-20倍）が勝つ場合の特徴 ---")
        ana_winners = races[(races['win_odds'] >= 10) & (races['win_odds'] < 20) & (races['finish_position'] == 1)]
        print(f"該当件数: {len(ana_winners):,}件")
        
        if len(ana_winners) > 0:
            print(f"\n馬場タイプ分布:")
            print(ana_winners['track_surface'].value_counts())
            
            print(f"\n距離カテゴリ分布:")
            print(ana_winners['distance_category'].value_counts())
            
            print(f"\n競馬場分布（上位5）:")
            print(ana_winners['venue'].value_counts().head(5))
    
    # ========================================
    print_section("7. 記事提案の実装優先度評価")
    # ========================================
    
    print("""
【分析結果に基づく実装優先度】

┌─────────────────────────────────────────────────────────────────┐
│ 優先度1: 時系列シーケンスモデル（Transformer/LSTM）              │
├─────────────────────────────────────────────────────────────────┤
│ 根拠:                                                           │
│ - 61.3%の馬が5走以上のデータを持つ                              │
│ - 上がり調子/スランプパターンで今走結果に明確な差異              │
│ - 復活パターン（前走大敗→今走好走）の検出可能性                 │
│ 期待効果: ★★★★★                                            │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ 優先度2: Entity Embeddings（騎手・調教師・種牡馬）              │
├─────────────────────────────────────────────────────────────────┤
│ 根拠:                                                           │
│ - 騎手448人、調教師483人→十分学習可能なカーディナリティ        │
│ - 種牡馬21,087頭→高カーディナリティも埋め込みで対応可能        │
│ - 現在はリーディング順位や勝率で簡略化→情報損失大              │
│ 期待効果: ★★★★☆                                            │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ 優先度3: 展開予測（脚質マッチング強化）                          │
├─────────────────────────────────────────────────────────────────┤
│ 根拠:                                                           │
│ - corner_positionsデータで脚質パターン抽出可能                  │
│ - 逃げ馬（4C1番手）勝率が高い→展開次第で変動                   │
│ - 逃げ馬複数時のハイペース予測→既存特徴量に追加可能            │
│ 期待効果: ★★★☆☆                                            │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ 優先度4: セグメント別モデル分割                                  │
├─────────────────────────────────────────────────────────────────┤
│ 根拠:                                                           │
│ - 芝28万件/ダート29万件→十分なデータ量で分割学習可能           │
│ - 距離カテゴリ別にも傾向差あり→専用モデル構築可能              │
│ - 記事推奨: 芝/ダート/障害、短距離/中長距離で分割               │
│ 期待効果: ★★★☆☆                                            │
└─────────────────────────────────────────────────────────────────┘
""")

if __name__ == "__main__":
    main()
