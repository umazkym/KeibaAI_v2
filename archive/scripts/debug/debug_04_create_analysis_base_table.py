#!/usr/bin/env python3
"""
デバッグスクリプト04: 分析用ベーステーブルの作成

既存の1,000件データ（races, shutuba, horses, pedigrees）を結合し、
特徴量生成に必要な全情報を統合したベーステーブルを作成します。

目的:
- 小規模データでパイプライン全体を検証
- 特徴量エンジン → モデル学習 → 評価のサイクルを確立
- 問題点の早期発見
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def load_parquet_files():
    """既存のParquetファイルをロード"""
    print("=" * 80)
    print("📁 Parquetファイルのロード")
    print("=" * 80)

    base_dir = Path("keibaai/data/parsed/parquet")

    # 1. レース結果
    races_path = base_dir / "races" / "races.parquet"
    df_races = pd.read_parquet(races_path)
    print(f"\n✅ races.parquet: {len(df_races):,}行 × {len(df_races.columns)}列")
    print(f"   カラム: {', '.join(df_races.columns.tolist()[:10])}...")

    # 2. 出馬表
    shutuba_path = base_dir / "shutuba" / "shutuba.parquet"
    df_shutuba = pd.read_parquet(shutuba_path)
    print(f"\n✅ shutuba.parquet: {len(df_shutuba):,}行 × {len(df_shutuba.columns)}列")
    print(f"   カラム: {', '.join(df_shutuba.columns.tolist()[:10])}...")

    # 3. 馬プロフィール
    horses_path = base_dir / "horses" / "horses.parquet"
    df_horses = pd.read_parquet(horses_path)
    print(f"\n✅ horses.parquet: {len(df_horses):,}行 × {len(df_horses.columns)}列")
    print(f"   カラム: {', '.join(df_horses.columns.tolist())}")

    # 4. 血統
    pedigrees_path = base_dir / "pedigrees" / "pedigrees.parquet"
    df_pedigrees = pd.read_parquet(pedigrees_path)
    print(f"\n✅ pedigrees.parquet: {len(df_pedigrees):,}行 × {len(df_pedigrees.columns)}列")
    print(f"   カラム: {', '.join(df_pedigrees.columns.tolist())}")

    return df_races, df_shutuba, df_horses, df_pedigrees


def analyze_join_keys(df_races, df_shutuba, df_horses, df_pedigrees):
    """結合キーの一致状況を分析"""
    print("\n" + "=" * 80)
    print("🔗 結合キーの一致状況分析")
    print("=" * 80)

    # race_id + horse_id の組み合わせ
    races_keys = set(zip(df_races['race_id'], df_races['horse_id']))
    shutuba_keys = set(zip(df_shutuba['race_id'], df_shutuba['horse_id']))

    print(f"\nraces.parquet の (race_id, horse_id) ペア数: {len(races_keys):,}")
    print(f"shutuba.parquet の (race_id, horse_id) ペア数: {len(shutuba_keys):,}")

    common_keys = races_keys & shutuba_keys
    only_races = races_keys - shutuba_keys
    only_shutuba = shutuba_keys - races_keys

    print(f"\n共通ペア数: {len(common_keys):,}")
    print(f"races のみ: {len(only_races):,}")
    print(f"shutuba のみ: {len(only_shutuba):,}")

    # 馬ID
    races_horse_ids = set(df_races['horse_id'].unique())
    horses_horse_ids = set(df_horses['horse_id'].unique())
    pedigrees_horse_ids = set(df_pedigrees['horse_id'].unique())

    print(f"\n馬ID数:")
    print(f"  races.parquet: {len(races_horse_ids):,}")
    print(f"  horses.parquet: {len(horses_horse_ids):,}")
    print(f"  pedigrees.parquet: {len(pedigrees_horse_ids):,}")

    common_horses_all = races_horse_ids & horses_horse_ids & pedigrees_horse_ids
    print(f"\n  3つ全てに存在: {len(common_horses_all):,}")

    missing_in_horses = races_horse_ids - horses_horse_ids
    missing_in_pedigrees = races_horse_ids - pedigrees_horse_ids

    if missing_in_horses:
        print(f"\n⚠️ races には存在するが horses にない馬: {len(missing_in_horses):,}頭")
        print(f"   サンプル: {list(missing_in_horses)[:5]}")

    if missing_in_pedigrees:
        print(f"\n⚠️ races には存在するが pedigrees にない馬: {len(missing_in_pedigrees):,}頭")
        print(f"   サンプル: {list(missing_in_pedigrees)[:5]}")


def create_base_table(df_races, df_shutuba, df_horses, df_pedigrees):
    """分析用ベーステーブルを作成"""
    print("\n" + "=" * 80)
    print("🔨 分析用ベーステーブルの作成")
    print("=" * 80)

    # ステップ1: races を基準にする
    print("\n【ステップ1】races.parquet を基準テーブルとして使用")
    df_base = df_races.copy()
    print(f"  初期行数: {len(df_base):,}")

    # ステップ2: shutuba から追加情報を取得
    print("\n【ステップ2】shutuba.parquet から morning_odds などを結合")

    # shutuba から必要なカラムのみ選択
    shutuba_cols = ['race_id', 'horse_id', 'morning_odds', 'morning_popularity']
    # これらのカラムが実際に存在するか確認
    available_shutuba_cols = ['race_id', 'horse_id']
    for col in ['morning_odds', 'morning_popularity']:
        if col in df_shutuba.columns:
            available_shutuba_cols.append(col)

    df_shutuba_subset = df_shutuba[available_shutuba_cols].copy()

    # 結合前の行数
    before_merge = len(df_base)

    # left join
    df_base = df_base.merge(
        df_shutuba_subset,
        on=['race_id', 'horse_id'],
        how='left',
        suffixes=('', '_shutuba')
    )

    print(f"  結合後行数: {len(df_base):,} (変化: {len(df_base) - before_merge})")
    print(f"  追加カラム: {', '.join([c for c in available_shutuba_cols if c not in ['race_id', 'horse_id']])}")

    # ステップ3: horses から血統情報を取得
    print("\n【ステップ3】horses.parquet から血統情報（sire_id, dam_id等）を結合")

    # horses から必要なカラムを選択
    horses_cols = ['horse_id']
    for col in ['sire_id', 'sire_name', 'dam_id', 'dam_name',
                'damsire_id', 'damsire_name', 'birth_date',
                'breeder_name', 'producing_area', 'coat_color']:
        if col in df_horses.columns:
            horses_cols.append(col)

    df_horses_subset = df_horses[horses_cols].copy()

    before_merge = len(df_base)
    df_base = df_base.merge(
        df_horses_subset,
        on='horse_id',
        how='left',
        suffixes=('', '_horses')
    )

    print(f"  結合後行数: {len(df_base):,} (変化: {len(df_base) - before_merge})")
    print(f"  追加カラム: {', '.join([c for c in horses_cols if c != 'horse_id'])}")

    # ステップ4: pedigrees から祖先情報を追加（generation=1のみ: 父母）
    print("\n【ステップ4】pedigrees.parquet から父母情報を結合")

    # generation=1（父母）のみを取得
    df_pedigrees_gen1 = df_pedigrees[df_pedigrees['generation'] == 1].copy()

    # ピボットして横持ちに変換
    # （本来は父・母を分けるべきだが、簡易的に最初の2祖先を取得）
    pedigree_info = {}
    for horse_id, group in df_pedigrees_gen1.groupby('horse_id'):
        ancestors = group[['ancestor_id', 'ancestor_name']].values
        pedigree_info[horse_id] = {
            'pedigree_ancestor_1_id': ancestors[0][0] if len(ancestors) > 0 else None,
            'pedigree_ancestor_1_name': ancestors[0][1] if len(ancestors) > 0 else None,
            'pedigree_ancestor_2_id': ancestors[1][0] if len(ancestors) > 1 else None,
            'pedigree_ancestor_2_name': ancestors[1][1] if len(ancestors) > 1 else None,
        }

    df_pedigree_pivot = pd.DataFrame.from_dict(pedigree_info, orient='index')
    df_pedigree_pivot.reset_index(inplace=True)
    df_pedigree_pivot.rename(columns={'index': 'horse_id'}, inplace=True)

    before_merge = len(df_base)
    df_base = df_base.merge(
        df_pedigree_pivot,
        on='horse_id',
        how='left'
    )

    print(f"  結合後行数: {len(df_base):,} (変化: {len(df_base) - before_merge})")
    print(f"  追加カラム: pedigree_ancestor_1_id/name, pedigree_ancestor_2_id/name")

    return df_base


def analyze_base_table(df_base):
    """ベーステーブルの品質分析"""
    print("\n" + "=" * 80)
    print("📊 ベーステーブルのデータ品質分析")
    print("=" * 80)

    print(f"\n基本情報:")
    print(f"  総行数: {len(df_base):,}")
    print(f"  総列数: {len(df_base.columns)}")
    print(f"  メモリ使用量: {df_base.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")

    print(f"\n全カラム一覧 ({len(df_base.columns)}個):")
    for i, col in enumerate(df_base.columns, 1):
        dtype = df_base[col].dtype
        null_count = df_base[col].isna().sum()
        null_pct = (null_count / len(df_base)) * 100
        print(f"  {i:2}. {col:30} | {str(dtype):15} | 欠損: {null_count:5} ({null_pct:5.2f}%)")

    # 重要カラムの統計
    print(f"\n重要カラムのサンプル値:")
    important_cols = ['race_id', 'horse_id', 'finish_position', 'win_odds',
                      'sire_id', 'dam_id', 'pedigree_ancestor_1_id']

    for col in important_cols:
        if col in df_base.columns:
            non_null = df_base[col].notna().sum()
            sample_val = df_base[col].dropna().iloc[0] if non_null > 0 else 'N/A'
            print(f"  {col:30}: 非欠損={non_null:5}件 | サンプル値={sample_val}")

    # ユニーク数
    print(f"\nユニーク数:")
    print(f"  ユニークレース数: {df_base['race_id'].nunique():,}")
    print(f"  ユニーク馬数: {df_base['horse_id'].nunique():,}")
    if 'jockey_id' in df_base.columns:
        print(f"  ユニーク騎手数: {df_base['jockey_id'].nunique():,}")
    if 'trainer_id' in df_base.columns:
        print(f"  ユニーク調教師数: {df_base['trainer_id'].nunique():,}")


def save_base_table(df_base):
    """ベーステーブルを保存"""
    print("\n" + "=" * 80)
    print("💾 ベーステーブルの保存")
    print("=" * 80)

    output_dir = Path("keibaai/data/parsed/parquet/analysis")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "base_table.parquet"
    df_base.to_parquet(output_path, index=False)

    file_size = output_path.stat().st_size / 1024 / 1024

    print(f"\n✅ 保存完了:")
    print(f"   パス: {output_path}")
    print(f"   サイズ: {file_size:.2f} MB")
    print(f"   行数: {len(df_base):,}")
    print(f"   列数: {len(df_base.columns)}")

    # CSVでもサンプルを保存（デバッグ用）
    csv_path = output_dir / "base_table_sample.csv"
    df_base.head(100).to_csv(csv_path, index=False, encoding='utf-8-sig')
    print(f"\n   サンプルCSV: {csv_path} (先頭100行)")


def check_feature_engine_compatibility(df_base):
    """FeatureEngineとの互換性チェック"""
    print("\n" + "=" * 80)
    print("🔧 FeatureEngine 互換性チェック")
    print("=" * 80)

    # features.yaml で期待されるカラム
    required_cols = {
        '基本': ['race_id', 'horse_id', 'race_date', 'age', 'sex', 'basis_weight'],
        '成績': ['finish_position', 'finish_time_seconds', 'last_3f_time'],
        '人': ['jockey_id', 'trainer_id'],
        '馬体': ['horse_weight', 'horse_weight_change'],
        '市場': ['win_odds', 'popularity'],
        '血統': ['sire_id', 'dam_id'],
    }

    print("\n必要カラムの存在確認:")
    all_ok = True

    for category, cols in required_cols.items():
        missing = [col for col in cols if col not in df_base.columns]
        if missing:
            print(f"  ❌ {category}: 不足 → {', '.join(missing)}")
            all_ok = False
        else:
            print(f"  ✅ {category}: OK")

    if all_ok:
        print("\n🎉 FeatureEngine で利用可能なベーステーブルです！")
    else:
        print("\n⚠️ 一部のカラムが不足していますが、代替手段で対応可能です")


def main():
    """メイン処理"""
    print("=" * 80)
    print("🔍 KeibaAI_v2 分析用ベーステーブル作成")
    print(f"⏰ 実行時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    # ステップ1: ファイルロード
    df_races, df_shutuba, df_horses, df_pedigrees = load_parquet_files()

    # ステップ2: 結合キー分析
    analyze_join_keys(df_races, df_shutuba, df_horses, df_pedigrees)

    # ステップ3: ベーステーブル作成
    df_base = create_base_table(df_races, df_shutuba, df_horses, df_pedigrees)

    # ステップ4: 品質分析
    analyze_base_table(df_base)

    # ステップ5: FeatureEngine互換性チェック
    check_feature_engine_compatibility(df_base)

    # ステップ6: 保存
    save_base_table(df_base)

    print("\n" + "=" * 80)
    print("📋 次のステップ")
    print("=" * 80)
    print("""
💡 ベーステーブル作成完了！次は以下を実行します:

1. FeatureEngine でこのテーブルから特徴量を生成
   → python keibaai/src/features/generate_features.py

2. 生成された特徴量の確認
   → debug_05_check_features.py を実行

3. モデル学習・評価
   → python keibaai/src/models/train_mu_model.py

4. 問題なければ全20,157件のパースを実行
    """)

    print("=" * 80)
    print("✅ デバッグスクリプト04完了")
    print("=" * 80)


if __name__ == "__main__":
    main()
