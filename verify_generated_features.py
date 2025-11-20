#!/usr/bin/env python3
"""
生成された特徴量を検証するスクリプト
"""
import pandas as pd
import yaml
from pathlib import Path


def verify_features():
    """生成された特徴量を検証"""
    print(f"\n{'='*80}")
    print("特徴量生成結果の検証")
    print(f"{'='*80}\n")

    # パスを設定
    features_dir = Path(__file__).parent / "keibaai" / "data" / "features" / "parquet"
    feature_names_file = features_dir / "feature_names.yaml"

    # 特徴量リストを読み込み
    if feature_names_file.exists():
        with open(feature_names_file, 'r', encoding='utf-8') as f:
            feature_info = yaml.safe_load(f)

        print(f"✓ 特徴量リストファイル: {feature_names_file}")

        # YAMLの構造を確認
        if isinstance(feature_info, dict):
            # 辞書の場合
            if 'feature_names' in feature_info:
                feature_names = feature_info['feature_names']
            elif 'features' in feature_info:
                feature_names = feature_info['features']
            else:
                # キーを表示
                print(f"  YAMLキー: {list(feature_info.keys())}")
                feature_names = list(feature_info.keys())
        elif isinstance(feature_info, list):
            # リストの場合
            feature_names = feature_info
        else:
            print(f"  ⚠️  予期しないYAML構造: {type(feature_info)}")
            print(f"  内容: {feature_info}")
            return False

        print(f"✓ 生成された特徴量数: {len(feature_names)}")
        print(f"\n{'─'*80}")
        print("特徴量カテゴリ:")
        print(f"{'─'*80}\n")

        # 特徴量をカテゴリ別に分類
        categories = {
            'horse_past': [],
            'jockey': [],
            'trainer': [],
            'venue': [],
            'distance': [],
            'surface': [],
            'prev_': [],
            'changed_': [],
            'days_since': [],
            'others': []
        }

        for feat in feature_names:
            categorized = False
            for category, features in categories.items():
                if category in feat:
                    features.append(feat)
                    categorized = True
                    break
            if not categorized:
                categories['others'].append(feat)

        # カテゴリ別にカウント
        print(f"{'カテゴリ':<20s} {'特徴量数':>10s}")
        print(f"{'─'*32}")

        category_names = {
            'horse_past': '馬の過去成績',
            'jockey': '騎手統計',
            'trainer': '調教師統計',
            'venue': '競馬場別',
            'distance': '距離別',
            'surface': 'コース別',
            'prev_': '前回レース',
            'changed_': '変更フラグ',
            'days_since': '経過日数',
            'others': 'その他'
        }

        total_features = 0
        for key, features in categories.items():
            if features:
                count = len(features)
                total_features += count
                print(f"{category_names[key]:<20s} {count:>10d}")

        print(f"{'─'*32}")
        print(f"{'合計':<20s} {total_features:>10d}")

        # 主要な特徴量を表示
        print(f"\n{'─'*80}")
        print("主要な特徴量（最初の20個）:")
        print(f"{'─'*80}\n")

        for i, feat in enumerate(feature_names[:20], 1):
            print(f"  {i:2d}. {feat}")

        if len(feature_names) > 20:
            print(f"\n  ... 他 {len(feature_names) - 20} 個")

    else:
        print(f"❌ 特徴量リストファイルが見つかりません: {feature_names_file}")
        return

    # 実際のParquetファイルを読み込んで検証
    print(f"\n{'─'*80}")
    print("実際のデータを検証:")
    print(f"{'─'*80}\n")

    try:
        # パーティション化されたParquetを読み込み
        df = pd.read_parquet(features_dir)

        print(f"✓ データ読み込み成功")
        print(f"✓ 総行数: {len(df):,}")
        print(f"✓ 総カラム数: {len(df.columns)}")

        # データ型の確認
        print(f"\n{'─'*80}")
        print("データ型の分布:")
        print(f"{'─'*80}\n")

        dtype_counts = df.dtypes.value_counts()
        for dtype, count in dtype_counts.items():
            print(f"  {str(dtype):<20s}: {count:>5d} カラム")

        # 欠損値の確認
        print(f"\n{'─'*80}")
        print("欠損値の状況（上位10カラム）:")
        print(f"{'─'*80}\n")

        missing_counts = df.isnull().sum()
        missing_pct = (missing_counts / len(df) * 100).round(2)
        missing_summary = pd.DataFrame({
            'カラム': missing_counts.index,
            '欠損数': missing_counts.values,
            '欠損率(%)': missing_pct.values
        })
        missing_summary = missing_summary[missing_summary['欠損数'] > 0].sort_values('欠損率(%)', ascending=False)

        if len(missing_summary) > 0:
            print(missing_summary.head(10).to_string(index=False))
            print(f"\n  欠損値を含むカラム: {len(missing_summary)} / {len(df.columns)}")
        else:
            print("  ✓ 欠損値はありません")

        # サンプルデータを表示
        print(f"\n{'─'*80}")
        print("サンプルデータ（最初の3行、主要カラムのみ）:")
        print(f"{'─'*80}\n")

        key_columns = ['race_id', 'horse_id', 'jockey_id', 'trainer_id',
                      'horse_past_races', 'jockey_win_rate', 'trainer_win_rate']
        available_columns = [col for col in key_columns if col in df.columns]

        if available_columns:
            print(df[available_columns].head(3).to_string(index=False))
        else:
            print(df.head(3).to_string(index=False))

        # 成功メッセージ
        print(f"\n{'='*80}")
        print("✓ 特徴量生成が正常に完了しています")
        print(f"{'='*80}\n")

        return True

    except Exception as e:
        print(f"❌ データ読み込み中にエラー: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = verify_features()
    exit(0 if success else 1)
