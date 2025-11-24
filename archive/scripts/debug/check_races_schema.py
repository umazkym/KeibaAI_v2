"""
races.parquetのスキーマを確認するスクリプト

特徴量生成で警告が出た以下のカラムが存在するか確認:
- passing_order_1, passing_order_4
- venue
- distance_category
- track_surface
- damsire_id
"""

from pathlib import Path
import pandas as pd

def check_schema():
    print("=" * 80)
    print("🔍 races.parquet スキーマ確認")
    print("=" * 80)
    print()

    races_path = Path('keibaai/data/parsed/parquet/races/races.parquet')

    if not races_path.exists():
        print(f"❌ エラー: {races_path} が存在しません")
        return

    # Parquetファイルを読み込み
    df = pd.read_parquet(races_path)

    print(f"📊 races.parquet: {len(df):,} 行 × {len(df.columns)} 列")
    print()

    # 全カラム名を表示
    print("📋 全カラム一覧:")
    print("-" * 80)
    for i, col in enumerate(df.columns, 1):
        dtype = str(df[col].dtype)
        missing_pct = (df[col].isna().sum() / len(df)) * 100
        print(f"{i:2}. {col:25} ({dtype:10}) 欠損: {missing_pct:5.1f}%")
    print()

    # 特定カラムの存在確認
    print("=" * 80)
    print("🔍 特徴量生成で必要なカラムの存在確認")
    print("=" * 80)
    print()

    required_cols = {
        'passing_order_1': 'コーナー通過順位1',
        'passing_order_2': 'コーナー通過順位2',
        'passing_order_3': 'コーナー通過順位3',
        'passing_order_4': 'コーナー通過順位4',
        'venue': '競馬場',
        'distance_category': '距離カテゴリ',
        'track_surface': '馬場種別',
        'distance_m': '距離(m)',
        'weather': '天候',
        'track_condition': '馬場状態',
    }

    for col, desc in required_cols.items():
        if col in df.columns:
            missing_pct = (df[col].isna().sum() / len(df)) * 100
            print(f"✅ {col:25} ({desc:20}) - 欠損: {missing_pct:5.1f}%")
        else:
            print(f"❌ {col:25} ({desc:20}) - **存在しない**")
    print()

    # サンプルデータを表示
    print("=" * 80)
    print("🔍 サンプルデータ（先頭3行）")
    print("=" * 80)
    print()

    sample_cols = ['race_id', 'distance_m', 'track_surface', 'venue',
                   'passing_order_1', 'passing_order_2', 'passing_order_3', 'passing_order_4']
    available_cols = [c for c in sample_cols if c in df.columns]

    if available_cols:
        print(df[available_cols].head(3).to_string(index=False))
    else:
        print("⚠️  表示可能なカラムがありません")
    print()

    # distance_categoryの存在確認
    print("=" * 80)
    print("📋 distance_category について")
    print("=" * 80)
    print()

    if 'distance_category' in df.columns:
        print("✅ distance_category は存在します")
        print(f"   値の分布: {df['distance_category'].value_counts().to_dict()}")
    else:
        print("❌ distance_category は存在しません")
        print()
        print("💡 対策:")
        print("   distance_category は distance_m から派生特徴量として生成する必要があります")
        print("   例:")
        print("     - 短距離: 1000-1400m")
        print("     - マイル: 1401-1800m")
        print("     - 中距離: 1801-2200m")
        print("     - 長距離: 2201m-")
        print()
        print("   features.yaml または feature_engine.py で生成ロジックを確認してください")

    print()

if __name__ == '__main__':
    check_schema()
