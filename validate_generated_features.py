"""
生成された特徴量の内容を検証するスクリプト

検証項目:
1. オッズ系カラムが除外されているか (win_odds, popularity, morning_odds, morning_popularity)
2. 生成された特徴量の数とカラム名
3. 欠損率の確認
4. サンプルデータの表示
"""

import sys
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq

def validate_features():
    """
    特徴量ファイルを検証
    """
    print("=" * 80)
    print("🔍 生成された特徴量の検証")
    print("=" * 80)
    print()

    # 特徴量ディレクトリのパス
    features_dir = Path('keibaai/data/features/parquet')

    if not features_dir.exists():
        print(f"❌ エラー: {features_dir} が存在しません")
        print()
        print("💡 ヒント: まず特徴量生成を実行してください")
        print("   python keibaai/src/features/generate_features.py --start_date 2020-01-01 --end_date 2020-12-31")
        return

    # パーティション化されたParquetファイルを読み込み
    try:
        print(f"📂 読み込み中: {features_dir}")
        print()

        # PyArrow Datasetとして読み込み（パーティション対応）
        dataset = pq.ParquetDataset(features_dir, use_legacy_dataset=False)
        table = dataset.read()
        df = table.to_pandas()

        print(f"✅ 読み込み成功: {len(df):,} 行")
        print()

    except Exception as e:
        print(f"❌ 読み込みエラー: {e}")
        return

    # --- 検証1: オッズ系カラムが除外されているか ---
    print("=" * 80)
    print("📋 検証1: オッズ系カラムが除外されているか")
    print("=" * 80)
    print()

    odds_columns = ['win_odds', 'popularity', 'morning_odds', 'morning_popularity']
    found_odds = [col for col in odds_columns if col in df.columns]

    if found_odds:
        print(f"❌ 警告: 以下のオッズ系カラムが含まれています（学習時は使用禁止）:")
        for col in found_odds:
            print(f"   - {col}")
        print()
        print("💡 対策: features.yaml の exclude_patterns を確認してください")
    else:
        print("✅ 正常: オッズ系カラムは除外されています")
    print()

    # --- 検証2: 生成された特徴量の一覧 ---
    print("=" * 80)
    print("📋 検証2: 生成された特徴量")
    print("=" * 80)
    print()

    print(f"📊 総カラム数: {len(df.columns)}")
    print()

    # カラム名をカテゴリ別に分類
    race_cols = [c for c in df.columns if c.startswith(('distance', 'track', 'weather', 'venue', 'race_'))]
    horse_cols = [c for c in df.columns if c.startswith(('horse_', 'age', 'sex', 'weight'))]
    jockey_cols = [c for c in df.columns if 'jockey' in c.lower()]
    trainer_cols = [c for c in df.columns if 'trainer' in c.lower()]
    sire_cols = [c for c in df.columns if 'sire' in c.lower() or 'dam' in c.lower()]
    other_cols = [c for c in df.columns if c not in race_cols + horse_cols + jockey_cols + trainer_cols + sire_cols]

    print(f"レース系特徴量:     {len(race_cols)} 個")
    print(f"馬系特徴量:         {len(horse_cols)} 個")
    print(f"騎手系特徴量:       {len(jockey_cols)} 個")
    print(f"調教師系特徴量:     {len(trainer_cols)} 個")
    print(f"血統系特徴量:       {len(sire_cols)} 個")
    print(f"その他:             {len(other_cols)} 個")
    print()

    # サンプル表示
    print("🔍 レース系特徴量 (例):")
    for col in race_cols[:5]:
        print(f"   - {col}")
    if len(race_cols) > 5:
        print(f"   ... 他 {len(race_cols) - 5} 個")
    print()

    print("🔍 馬系特徴量 (例):")
    for col in horse_cols[:5]:
        print(f"   - {col}")
    if len(horse_cols) > 5:
        print(f"   ... 他 {len(horse_cols) - 5} 個")
    print()

    print("🔍 血統系特徴量 (例):")
    for col in sire_cols[:5]:
        print(f"   - {col}")
    if len(sire_cols) > 5:
        print(f"   ... 他 {len(sire_cols) - 5} 個")
    print()

    # --- 検証3: 欠損率の確認 ---
    print("=" * 80)
    print("📋 検証3: 欠損率の確認（上位10カラム）")
    print("=" * 80)
    print()

    missing_stats = []
    for col in df.columns:
        missing_count = df[col].isna().sum()
        missing_pct = (missing_count / len(df)) * 100
        if missing_pct > 0:
            missing_stats.append({
                'column': col,
                'missing_count': missing_count,
                'missing_pct': missing_pct
            })

    if missing_stats:
        missing_df = pd.DataFrame(missing_stats).sort_values('missing_pct', ascending=False)
        print(missing_df.head(10).to_string(index=False))
        print()

        high_missing = missing_df[missing_df['missing_pct'] > 50]
        if len(high_missing) > 0:
            print(f"⚠️  警告: {len(high_missing)} 個のカラムが50%以上欠損しています")
            print()
    else:
        print("✅ 欠損なし: すべてのカラムに値が入っています")
        print()

    # --- 検証4: サンプルデータの表示 ---
    print("=" * 80)
    print("📋 検証4: サンプルデータ（先頭3行）")
    print("=" * 80)
    print()

    # race_id, horse_number, horse_name + いくつかの特徴量を表示
    display_cols = ['race_id', 'horse_number']
    if 'horse_name' in df.columns:
        display_cols.append('horse_name')

    # 数値系の特徴量を数個追加
    numeric_features = [c for c in df.columns if c not in display_cols and df[c].dtype in ['int64', 'float64', 'Int64', 'Float64']]
    display_cols.extend(numeric_features[:5])

    print(df[display_cols].head(3).to_string(index=False))
    print()

    # --- 検証5: データ型の確認 ---
    print("=" * 80)
    print("📋 検証5: データ型の分布")
    print("=" * 80)
    print()

    dtype_counts = df.dtypes.value_counts()
    for dtype, count in dtype_counts.items():
        print(f"{str(dtype):20} : {count:3} カラム")
    print()

    # --- 最終判定 ---
    print("=" * 80)
    print("✅ 検証完了")
    print("=" * 80)
    print()

    if found_odds:
        print("⚠️  警告: オッズ系カラムが含まれています")
        print("   → features.yaml の設定を確認してください")
    else:
        print("✅ 特徴量は正しく生成されています")
        print()
        print("📝 次のステップ:")
        print("   1. 欠損カラムの警告を確認")
        print("   2. モデル学習を実行: python keibaai/src/models/train_mu_model.py")

if __name__ == '__main__':
    validate_features()
