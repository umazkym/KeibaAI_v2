#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
新モデル考案のためのデータ探索スクリプト

記事「競馬AI開発ロードマップ」の提案に基づき、以下を分析:
1. 各Parquetファイルのスキーマと基本統計
2. Entity Embeddings候補（騎手・調教師・血統）のカーディナリティ
3. 時系列パターン分析の基礎データ
4. セグメント別傾向分析
5. 展開予測の基礎データ
"""

import pandas as pd
import numpy as np
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# パス設定
BASE_PATH = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\parsed\parquet")

def print_section(title: str):
    print("\n" + "=" * 80)
    print(f"【{title}】")
    print("=" * 80)

def analyze_parquet_schema(df: pd.DataFrame, name: str):
    """Parquetファイルのスキーマと基本統計を出力"""
    print(f"\n--- {name} ---")
    print(f"行数: {len(df):,}")
    print(f"列数: {len(df.columns)}")
    print(f"\nカラム一覧:")
    for col in df.columns:
        dtype = df[col].dtype
        null_count = df[col].isnull().sum()
        null_pct = null_count / len(df) * 100 if len(df) > 0 else 0
        unique_count = df[col].nunique()
        print(f"  {col}: {dtype} (NULL: {null_pct:.1f}%, ユニーク: {unique_count:,})")

def main():
    print_section("1. 主要Parquetファイルの基本情報")
    
    # races.parquet
    races_path = BASE_PATH / "races" / "races.parquet"
    if races_path.exists():
        races = pd.read_parquet(races_path)
        analyze_parquet_schema(races, "races.parquet")
        
        # 年別分布
        if 'race_date' in races.columns:
            races['year'] = pd.to_datetime(races['race_date']).dt.year
            print(f"\n年別レコード数:")
            print(races['year'].value_counts().sort_index())
    
    # shutuba.parquet
    shutuba_path = BASE_PATH / "shutuba" / "shutuba.parquet"
    if shutuba_path.exists():
        shutuba = pd.read_parquet(shutuba_path)
        analyze_parquet_schema(shutuba, "shutuba.parquet")
    
    # horses.parquet
    horses_path = BASE_PATH / "horses" / "horses.parquet"
    if horses_path.exists():
        horses = pd.read_parquet(horses_path)
        analyze_parquet_schema(horses, "horses.parquet")
    
    # pedigrees.parquet
    pedigrees_path = BASE_PATH / "pedigrees" / "pedigrees.parquet"
    if pedigrees_path.exists():
        pedigrees = pd.read_parquet(pedigrees_path)
        analyze_parquet_schema(pedigrees, "pedigrees.parquet")
    
    # corner_positions.parquet
    corners_path = BASE_PATH / "corners" / "corner_positions.parquet"
    if corners_path.exists():
        corners = pd.read_parquet(corners_path)
        analyze_parquet_schema(corners, "corner_positions.parquet")
    
    # ========================================
    print_section("2. Entity Embeddings候補の分析")
    # ========================================
    
    if races_path.exists():
        races = pd.read_parquet(races_path)
        
        print("\n--- 騎手 (jockey_id) ---")
        if 'jockey_id' in races.columns:
            jockey_counts = races['jockey_id'].value_counts()
            print(f"総騎手数: {races['jockey_id'].nunique():,}")
            print(f"騎乗回数分布:")
            print(f"  最多: {jockey_counts.max():,}回")
            print(f"  中央値: {jockey_counts.median():.0f}回")
            print(f"  10回以上の騎手: {(jockey_counts >= 10).sum():,}人")
            print(f"  100回以上の騎手: {(jockey_counts >= 100).sum():,}人")
            print(f"  1000回以上の騎手: {(jockey_counts >= 1000).sum():,}人")
        
        print("\n--- 調教師 (trainer_id / trainer_name) ---")
        trainer_col = 'trainer_id' if 'trainer_id' in races.columns else 'trainer_name'
        if trainer_col in races.columns:
            trainer_counts = races[trainer_col].value_counts()
            print(f"総調教師数: {races[trainer_col].nunique():,}")
            print(f"管理馬出走回数分布:")
            print(f"  最多: {trainer_counts.max():,}回")
            print(f"  中央値: {trainer_counts.median():.0f}回")
            print(f"  100回以上: {(trainer_counts >= 100).sum():,}人")
        
        print("\n--- 種牡馬 (sire_id / 血統情報) ---")
        if pedigrees_path.exists():
            pedigrees = pd.read_parquet(pedigrees_path)
            # sire_id相当のカラムを探す
            if 'generation' in pedigrees.columns:
                sires = pedigrees[pedigrees['generation'] == 1]
                if 'ancestor_id' in sires.columns:
                    sire_counts = sires['ancestor_id'].value_counts()
                    print(f"総種牡馬数: {sires['ancestor_id'].nunique():,}")
                    print(f"産駒数分布:")
                    print(f"  最多: {sire_counts.max():,}頭")
                    print(f"  10頭以上の産駒を持つ種牡馬: {(sire_counts >= 10).sum():,}")
                    print(f"  100頭以上の産駒を持つ種牡馬: {(sire_counts >= 100).sum():,}")
    
    # ========================================
    print_section("3. セグメント別分析（芝/ダート、距離、クラス）")
    # ========================================
    
    if races_path.exists():
        races = pd.read_parquet(races_path)
        
        # 芝/ダート分布
        if 'track_surface' in races.columns:
            print("\n--- 馬場タイプ別分布 ---")
            print(races['track_surface'].value_counts())
        
        # 距離分布
        if 'distance_m' in races.columns:
            print("\n--- 距離分布 ---")
            races['distance_category'] = pd.cut(
                races['distance_m'],
                bins=[0, 1400, 1800, 2200, 2800, 10000],
                labels=['短距離(~1400m)', 'マイル(1400-1800m)', '中距離(1800-2200m)', '長距離(2200-2800m)', '超長距離(2800m~)']
            )
            print(races['distance_category'].value_counts())
        
        # クラス分布
        if 'race_class' in races.columns:
            print("\n--- クラス分布 ---")
            print(races['race_class'].value_counts().head(15))
        
        # 着順分布
        if 'finish_position' in races.columns:
            print("\n--- 着順分布（上位10）---")
            print(races['finish_position'].value_counts().sort_index().head(10))
    
    # ========================================
    print_section("4. 時系列パターン分析の基礎データ")
    # ========================================
    
    if races_path.exists():
        races = pd.read_parquet(races_path)
        
        # 馬ごとの出走回数
        if 'horse_id' in races.columns:
            horse_race_counts = races.groupby('horse_id').size()
            print("\n--- 馬ごとの出走回数分布 ---")
            print(f"総馬数: {races['horse_id'].nunique():,}")
            print(f"最多出走: {horse_race_counts.max()}回")
            print(f"平均出走: {horse_race_counts.mean():.1f}回")
            print(f"中央値: {horse_race_counts.median():.0f}回")
            print(f"5回以上出走: {(horse_race_counts >= 5).sum():,}頭")
            print(f"10回以上出走: {(horse_race_counts >= 10).sum():,}頭")
            
            # 過去成績の推移分析が可能な馬の割合
            print(f"\n時系列分析に十分なデータ（5走以上）を持つ馬: {(horse_race_counts >= 5).sum() / len(horse_race_counts) * 100:.1f}%")
    
    # ========================================
    print_section("5. 展開予測の基礎データ（コーナー通過順）")
    # ========================================
    
    if corners_path.exists():
        corners = pd.read_parquet(corners_path)
        
        print("\n--- corner_positions.parquet ---")
        print(f"総レコード数: {len(corners):,}")
        
        if 'corner' in corners.columns:
            print(f"\nコーナー別レコード数:")
            print(corners['corner'].value_counts().sort_index())
        
        if 'position' in corners.columns:
            print(f"\n通過順位分布（上位5）:")
            print(corners['position'].value_counts().sort_index().head(5))
        
        if 'gap_from_leader' in corners.columns:
            print(f"\n先頭からの馬身差統計:")
            print(f"  平均: {corners['gap_from_leader'].mean():.2f}馬身")
            print(f"  中央値: {corners['gap_from_leader'].median():.2f}馬身")
            print(f"  最大: {corners['gap_from_leader'].max():.2f}馬身")
    
    # ========================================
    print_section("6. オッズデータの確認")
    # ========================================
    
    odds_base = BASE_PATH / "odds"
    for bet_type in ['tansho_fukusho', 'umaren', 'umatan', 'wide', 'sanrenpuku', 'sanrentan']:
        bet_path = odds_base / bet_type
        if bet_path.exists():
            files = list(bet_path.glob("*.parquet"))
            if files:
                sample = pd.read_parquet(files[0])
                print(f"\n--- {bet_type} ---")
                print(f"ファイル数: {len(files)}")
                print(f"サンプルカラム: {list(sample.columns)[:5]}...")
    
    # ========================================
    print_section("7. 記事提案の実装可能性評価")
    # ========================================
    
    print("""
【Entity Embeddings】
- 騎手: ✅ 実装可能（十分なカーディナリティと出走データあり）
- 調教師: ✅ 実装可能
- 種牡馬: ✅ 実装可能（pedigrees.parquetで血統情報あり）

【Learning to Rank】
- ✅ 既にV4.4でLambdaRank実装済み
- 追加改善: finish_positionを使った相対ラベル設計

【時系列モデル (Transformer/LSTM)】
- 前提条件: 馬ごとに十分な過去走データが必要
- 分析結果を基に実装可能性を判断

【GNN (展開予測)】
- corner_positions.parquetで通過順・馬身差データあり
- 対戦履歴グラフの構築は可能

【オッズ分離】
- ✅ 既に実装済み（sample_weightのみ使用）
""")

if __name__ == "__main__":
    main()
