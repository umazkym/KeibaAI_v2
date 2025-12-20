"""
race_class正規化の検証スクリプト

RaceClassNormalizerを使用してrace_classを正規化し、結果を検証する。
"""

import sys
from pathlib import Path

# パス設定
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pandas as pd
from keibaai.src.utils.race_class_normalizer import RaceClassNormalizer

DATA_DIR = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\parsed\parquet")


def main():
    print("=" * 80)
    print("race_class正規化の検証")
    print("=" * 80)
    
    # 正規化ユーティリティの初期化
    normalizer = RaceClassNormalizer()
    print(f"\n設定ファイル: {normalizer.config_path}")
    print(f"正規化クラス数: {len(normalizer._normalized_classes)}")
    
    # データ読み込み
    print("\nデータ読み込み中...")
    races = pd.read_parquet(DATA_DIR / 'races' / 'races.parquet')
    races['race_date'] = pd.to_datetime(races['race_date'])
    races['year'] = races['race_date'].dt.year
    
    # 平地レースのみ
    flat_races = races[~races['race_name'].str.contains('障害', na=False)]
    print(f"平地レース数: {len(flat_races):,}")
    
    # ===============================
    # 1. 正規化前の状態
    # ===============================
    print("\n" + "=" * 80)
    print("1. 正規化前のrace_class分布")
    print("=" * 80)
    
    # None/NaNを含む分布
    print("\n【全体】")
    print(f"  NaN/None: {flat_races['race_class'].isna().sum():,}")
    for cls, cnt in flat_races['race_class'].value_counts().items():
        print(f"  {cls}: {cnt:,} ({cnt/len(flat_races)*100:.1f}%)")
    
    # ===============================
    # 2. 正規化実行
    # ===============================
    print("\n" + "=" * 80)
    print("2. 正規化実行")
    print("=" * 80)
    
    print("\n正規化中...")
    flat_races = normalizer.normalize_dataframe(flat_races)
    print("完了")
    
    # ===============================
    # 3. 正規化後の状態
    # ===============================
    print("\n" + "=" * 80)
    print("3. 正規化後の分布")
    print("=" * 80)
    
    print("\n【race_class_normalized】")
    null_count = flat_races['race_class_normalized'].isna().sum()
    print(f"  NaN/None: {null_count:,} ({null_count/len(flat_races)*100:.1f}%)")
    
    for cls, cnt in flat_races['race_class_normalized'].value_counts().items():
        ordinal = normalizer.get_ordinal(cls)
        print(f"  {cls} (Lv.{ordinal}): {cnt:,} ({cnt/len(flat_races)*100:.1f}%)")
    
    # ===============================
    # 4. 年別の分布確認
    # ===============================
    print("\n" + "=" * 80)
    print("4. 年別の正規化クラス分布")
    print("=" * 80)
    
    # 主要クラスのみ
    major_classes = ['新馬', '未勝利', '1勝クラス', '2勝クラス', '3勝クラス', 'オープン', 'G1']
    
    print("\n年別分布（%）:")
    crosstab = pd.crosstab(
        flat_races['year'], 
        flat_races['race_class_normalized'], 
        normalize='index'
    ) * 100
    
    available_classes = [c for c in major_classes if c in crosstab.columns]
    print(crosstab[available_classes].round(1).to_string())
    
    # ===============================
    # 5. 2014-2019 vs 2020-2025 比較
    # ===============================
    print("\n" + "=" * 80)
    print("5. 期間比較（正規化後）")
    print("=" * 80)
    
    old = flat_races[flat_races['year'] <= 2019]
    new = flat_races[flat_races['year'] >= 2020]
    
    print("\n【クラス分布比較】")
    print(f"{'クラス':<12} {'2014-2019':>10} {'2020-2025':>10} {'差分':>8}")
    print("-" * 45)
    
    for cls in major_classes:
        old_pct = (old['race_class_normalized'] == cls).mean() * 100
        new_pct = (new['race_class_normalized'] == cls).mean() * 100
        diff = new_pct - old_pct
        flag = "⚠️" if abs(diff) > 3 else ""
        print(f"{cls:<12} {old_pct:>9.1f}% {new_pct:>9.1f}% {diff:>+7.1f}% {flag}")
    
    # ===============================
    # 6. 検証結果
    # ===============================
    print("\n" + "=" * 80)
    print("6. 検証結果")
    print("=" * 80)
    
    # 正規化成功率
    success_rate = (1 - null_count / len(flat_races)) * 100
    print(f"\n正規化成功率: {success_rate:.1f}%")
    
    # 期間間の分布均一性
    max_diff = 0
    for cls in major_classes:
        old_pct = (old['race_class_normalized'] == cls).mean() * 100
        new_pct = (new['race_class_normalized'] == cls).mean() * 100
        max_diff = max(max_diff, abs(new_pct - old_pct))
    
    print(f"期間間の最大差: {max_diff:.1f}%")
    
    if success_rate > 99 and max_diff < 5:
        print("\n✅ 正規化成功: データが適切に統一されました")
    else:
        print("\n⚠️ 要確認: 一部のデータが正規化できていません")
    
    # ===============================
    # 7. 正規化できなかったレコードの詳細
    # ===============================
    if null_count > 0:
        print("\n" + "=" * 80)
        print("7. 正規化できなかったレコード")
        print("=" * 80)
        
        failed = flat_races[flat_races['race_class_normalized'].isna()]
        print(f"\n件数: {len(failed):,}")
        
        print("\nサンプル（10件）:")
        for _, row in failed[['race_name', 'race_class', 'year']].drop_duplicates().head(10).iterrows():
            print(f"  [{row['year']}] race_class='{row['race_class']}', race_name='{row['race_name']}'")
    
    print("\n" + "=" * 80)
    print("検証完了")
    print("=" * 80)


if __name__ == '__main__':
    main()
