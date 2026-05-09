"""
特徴量生成で警告が出たカラムの欠損原因を診断し、対策を提示

特徴量生成時の警告:
  KeyError: "['passing_order_1', 'passing_order_4'] not in index"
  KeyError: "['venue'] not in index"
  KeyError: "['distance_category'] not in index"
  KeyError: "['track_surface'] not in index"
  KeyError: "['damsire_id'] not in index"
"""

from pathlib import Path
import pandas as pd
import yaml

def diagnose_missing_columns():
    print("=" * 80)
    print("🔧 特徴量生成の警告診断ツール")
    print("=" * 80)
    print()

    # --- パート1: 警告が出たカラムのリスト ---
    print("📋 警告が出たカラム:")
    print("-" * 80)

    missing_cols = {
        'passing_order_1': {
            'source': 'races.parquet',
            'type': 'レース結果から抽出すべき',
            'fix': 'results_parser.pyで passing_order を分割'
        },
        'passing_order_4': {
            'source': 'races.parquet',
            'type': 'レース結果から抽出すべき',
            'fix': 'results_parser.pyで passing_order を分割'
        },
        'venue': {
            'source': 'races.parquet',
            'type': 'レース結果から抽出すべき',
            'fix': 'results_parser.pyでレース基本情報を抽出'
        },
        'distance_category': {
            'source': '派生特徴量',
            'type': 'distance_mから生成すべき',
            'fix': 'feature_engine.pyで distance_m → distance_category変換'
        },
        'track_surface': {
            'source': 'races.parquet',
            'type': 'レース結果から抽出すべき',
            'fix': 'results_parser.pyでレース基本情報を抽出'
        },
        'damsire_id': {
            'source': '血統データ',
            'type': 'pedigrees.parquetから計算すべき',
            'fix': 'feature_engine.pyでマージ時に母父を計算'
        }
    }

    for col, info in missing_cols.items():
        print(f"  - {col:20} ({info['source']})")
    print()

    # --- パート2: races.parquetの現状確認 ---
    print("=" * 80)
    print("📊 パート1: races.parquet の現状確認")
    print("=" * 80)
    print()

    races_path = Path('keibaai/data/parsed/parquet/races/races.parquet')

    if not races_path.exists():
        print(f"❌ {races_path} が存在しません")
        print("   → まずパース処理を実行してください")
        return

    races_df = pd.read_parquet(races_path)
    print(f"✅ races.parquet: {len(races_df):,} 行 × {len(races_df.columns)} 列")
    print()

    # 必要なカラムの存在確認
    race_required = ['passing_order_1', 'passing_order_4', 'venue', 'track_surface', 'distance_m']

    print("🔍 必要カラムの存在確認:")
    print("-" * 80)
    for col in race_required:
        if col in races_df.columns:
            missing_pct = (races_df[col].isna().sum() / len(races_df)) * 100
            print(f"  ✅ {col:20} 存在 (欠損: {missing_pct:5.1f}%)")
        else:
            print(f"  ❌ {col:20} **存在しない**")
    print()

    # passing_orderカラムの存在確認
    if 'passing_order' in races_df.columns:
        print("💡 passing_order カラムが存在します:")
        sample_value = races_df['passing_order'].iloc[0]
        print(f"   サンプル値: {sample_value}")
        print(f"   データ型: {races_df['passing_order'].dtype}")
        print()
        print("   → passing_order_1, passing_order_2... への分割が必要です")
        print("   → results_parser.py の parse_passing_order() で実装済みかを確認")
    print()

    # --- パート3: features.yamlの設定確認 ---
    print("=" * 80)
    print("📊 パート2: features.yaml の設定確認")
    print("=" * 80)
    print()

    features_yaml_path = Path('keibaai/configs/features.yaml')

    if not features_yaml_path.exists():
        print(f"❌ {features_yaml_path} が存在しません")
    else:
        with open(features_yaml_path, 'r', encoding='utf-8') as f:
            features_config = yaml.safe_load(f)

        # past_performanceの設定確認
        if 'feature_recipes' in features_config:
            past_perf = features_config['feature_recipes'].get('past_performance', {})
            if past_perf.get('enabled'):
                columns = past_perf.get('columns', [])
                print(f"✅ past_performance.columns に {len(columns)} カラム指定:")
                for col in columns:
                    print(f"     - {col}")
                print()

                missing_in_config = [c for c in columns if c not in races_df.columns and not c.startswith('passing_order')]
                if missing_in_config:
                    print(f"   ⚠️  以下のカラムは races.parquet に存在しません:")
                    for col in missing_in_config:
                        print(f"       - {col}")

            # interaction_featuresの確認
            interaction = features_config['feature_recipes'].get('interaction_features', {})
            if interaction.get('enabled'):
                interactions = interaction.get('interactions', [])
                print()
                print(f"✅ interaction_features に {len(interactions)} 個の交互作用:")
                for inter in interactions:
                    context_col = inter.get('context_column')
                    if context_col == 'venue':
                        if 'venue' in races_df.columns:
                            print(f"     ✅ {inter['name']:20} (venue 使用可能)")
                        else:
                            print(f"     ❌ {inter['name']:20} (venue **存在しない**)")
                    elif context_col == 'distance_category':
                        print(f"     ⚠️  {inter['name']:20} (distance_category は派生特徴量)")
                    elif context_col == 'track_surface':
                        if 'track_surface' in races_df.columns:
                            print(f"     ✅ {inter['name']:20} (track_surface 使用可能)")
                        else:
                            print(f"     ❌ {inter['name']:20} (track_surface **存在しない**)")
        print()

    # --- パート4: 血統データの確認 ---
    print("=" * 80)
    print("📊 パート3: 血統データ (damsire_id) の確認")
    print("=" * 80)
    print()

    pedigrees_path = Path('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
    horses_path = Path('keibaai/data/parsed/parquet/horses/horses.parquet')

    if pedigrees_path.exists():
        pedigrees_df = pd.read_parquet(pedigrees_path)
        print(f"✅ pedigrees.parquet: {len(pedigrees_df):,} 行")
        print(f"   カラム: {', '.join(pedigrees_df.columns)}")
        print()

    if horses_path.exists():
        horses_df = pd.read_parquet(horses_path)
        print(f"✅ horses.parquet: {len(horses_df):,} 行")
        print(f"   カラム: {', '.join(horses_df.columns)}")
        print()

        if 'sire_id' in horses_df.columns:
            sire_missing = (horses_df['sire_id'].isna().sum() / len(horses_df)) * 100
            print(f"   ✅ sire_id 存在 (欠損: {sire_missing:.1f}%)")
        else:
            print(f"   ❌ sire_id **存在しない**")

        if 'dam_id' in horses_df.columns:
            dam_missing = (horses_df['dam_id'].isna().sum() / len(horses_df)) * 100
            print(f"   ✅ dam_id  存在 (欠損: {dam_missing:.1f}%)")
        else:
            print(f"   ❌ dam_id  **存在しない**")

        if 'damsire_id' in horses_df.columns:
            damsire_missing = (horses_df['damsire_id'].isna().sum() / len(horses_df)) * 100
            print(f"   ✅ damsire_id 存在 (欠損: {damsire_missing:.1f}%)")
        else:
            print(f"   ❌ damsire_id **存在しない**")
            print()
            print("   💡 damsire_id の生成方法:")
            print("      1. pedigrees.parquet から dam_id の父を検索")
            print("      2. feature_engine.py のマージ処理で追加")

    print()

    # --- パート5: 対策の提示 ---
    print("=" * 80)
    print("📝 対策サマリー")
    print("=" * 80)
    print()

    fixes_needed = []

    # 1. passing_order分割
    if 'passing_order' in races_df.columns and 'passing_order_1' not in races_df.columns:
        fixes_needed.append({
            'priority': 1,
            'title': 'passing_order の分割',
            'file': 'keibaai/src/modules/parsers/results_parser.py',
            'action': 'parse_passing_order() 関数で passing_order を passing_order_1~4 に分割',
            'code': """
# results_parser.py の parse_result_row() 内で:
passing_str = columns[10].get_text(strip=True)  # "1-1-1-1"
if '-' in passing_str:
    orders = passing_str.split('-')
    for i, order in enumerate(orders, 1):
        row_data[f'passing_order_{i}'] = parse_int_or_none(order)
"""
        })

    # 2. venue, track_surface抽出
    if 'venue' not in races_df.columns or 'track_surface' not in races_df.columns:
        fixes_needed.append({
            'priority': 2,
            'title': 'venue, track_surface の抽出',
            'file': 'keibaai/src/modules/parsers/results_parser.py',
            'action': 'extract_race_metadata() でレース基本情報を抽出（実装済みの可能性あり）',
            'note': 'CLAUDE.md によると、最近のパーサー改善で実装済みのはず。再パースが必要かも。'
        })

    # 3. distance_category生成
    fixes_needed.append({
        'priority': 3,
        'title': 'distance_category の生成',
        'file': 'keibaai/src/modules/features/feature_engine.py',
        'action': 'distance_m から distance_category を派生特徴量として生成',
        'code': """
# feature_engine.py 内で:
def create_distance_category(df):
    conditions = [
        df['distance_m'] <= 1400,
        (df['distance_m'] > 1400) & (df['distance_m'] <= 1800),
        (df['distance_m'] > 1800) & (df['distance_m'] <= 2200),
        df['distance_m'] > 2200
    ]
    choices = ['短距離', 'マイル', '中距離', '長距離']
    df['distance_category'] = pd.Series(pd.NA, dtype='object')
    df['distance_category'] = pd.Series(
        [choices[i] for i in range(len(choices)) for _ in range(len(df))
         if conditions[i].iloc[_]], dtype='category'
    )
    return df
"""
    })

    # 4. damsire_id生成
    if 'damsire_id' not in (horses_df.columns if horses_path.exists() and 'horses_df' in locals() else []):
        fixes_needed.append({
            'priority': 4,
            'title': 'damsire_id の生成',
            'file': 'keibaai/src/modules/features/feature_engine.py',
            'action': 'pedigrees.parquet から母父IDを計算してマージ',
            'code': """
# feature_engine.py 内で:
def add_damsire_id(df, pedigrees_df):
    # dam_id を持つレコードのみ処理
    dam_pedigrees = pedigrees_df.rename(columns={'horse_id': 'dam_id', 'ancestor_id': 'damsire_id'})
    # 母の父（2世代目の父側）を取得
    # これは pedigree_parser の構造に依存するため、実際のデータ構造を確認して実装
    df = df.merge(dam_pedigrees[['dam_id', 'damsire_id']], on='dam_id', how='left')
    return df
"""
        })

    # 対策を表示
    for i, fix in enumerate(fixes_needed, 1):
        print(f"[対策 {fix['priority']}] {fix['title']}")
        print(f"  ファイル: {fix['file']}")
        print(f"  内容: {fix['action']}")
        if 'code' in fix:
            print(f"  実装例:")
            for line in fix['code'].strip().split('\n'):
                print(f"    {line}")
        if 'note' in fix:
            print(f"  📝 Note: {fix['note']}")
        print()

    print("=" * 80)
    print("✅ 診断完了")
    print("=" * 80)
    print()
    print("📌 推奨する作業順序:")
    print("  1. races.parquet の再確認 (check_races_schema.py を実行)")
    print("  2. 欠損カラムがパーサーで実装済みか確認")
    print("  3. 実装済みなら再パース、未実装ならパーサー修正")
    print("  4. distance_category など派生特徴量は feature_engine.py で追加")
    print("  5. 修正後、再度特徴量生成を実行")
    print()

if __name__ == '__main__':
    diagnose_missing_columns()
