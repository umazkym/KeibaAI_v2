#!/usr/bin/env python3
"""
Parquetファイルのnull型カラムを修正するスクリプト

問題: null型のカラムがArrowNotImplementedErrorを引き起こす
解決: null型カラムを削除または適切な型に変換
"""
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
from pathlib import Path
import shutil
from datetime import datetime


def backup_file(file_path: Path) -> Path:
    """ファイルをバックアップ"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = file_path.with_suffix(f".parquet.backup_{timestamp}")
    shutil.copy2(file_path, backup_path)
    print(f"✓ バックアップ作成: {backup_path.name}")
    return backup_path


def fix_races_parquet(parquet_path: Path):
    """races.parquetのnull型カラムを修正"""
    print(f"\n{'='*80}")
    print(f"修正対象: {parquet_path.name}")
    print(f"{'='*80}\n")

    # バックアップ作成
    backup_file(parquet_path)

    # 全カラムを読み込み（null型カラム以外）
    parquet_file = pq.ParquetFile(parquet_path)
    schema = parquet_file.schema_arrow

    # null型カラムを特定
    null_columns = []
    valid_columns = []

    for field in schema:
        if str(field.type) == "null":
            null_columns.append(field.name)
        else:
            valid_columns.append(field.name)

    print(f"null型カラム検出: {null_columns}")
    print(f"有効なカラム数: {len(valid_columns)}")

    # 有効なカラムのみ読み込み
    table = pq.read_table(parquet_path, columns=valid_columns)
    df = table.to_pandas()

    print(f"✓ データ読み込み完了: {len(df):,}行")

    # 重要なカラムを追加（trainer_idは別途対応が必要）
    if 'trainer_id' in null_columns:
        print("\n⚠️  trainer_id はnull型でした")
        print("   → horses.parquet から trainer_id を取得して補完します")

        # horses.parquetを読み込み
        horses_path = parquet_path.parent.parent / "horses" / "horses.parquet"
        if horses_path.exists():
            horses_df = pd.read_parquet(horses_path)

            # trainer_idのマッピングを作成（horse_id → trainer_id）
            horse_trainer_map = horses_df.set_index('horse_id')['trainer_id'].to_dict()

            # races.parquetにtrainer_idを補完
            df['trainer_id'] = df['horse_id'].map(horse_trainer_map)

            # 補完状況を確認
            filled_count = df['trainer_id'].notna().sum()
            total_count = len(df)
            fill_rate = (filled_count / total_count * 100)

            print(f"   ✓ trainer_id 補完完了: {filled_count:,} / {total_count:,} ({fill_rate:.2f}%)")
        else:
            print(f"   ❌ horses.parquetが見つかりません: {horses_path}")
            df['trainer_id'] = pd.Series(dtype='string')

    # 削除されたカラムの報告
    removed_columns = [col for col in null_columns if col not in ['trainer_id']]
    if removed_columns:
        print(f"\n削除されたカラム: {removed_columns}")

    # 保存
    df.to_parquet(parquet_path, engine='pyarrow', compression='snappy', index=False)
    print(f"\n✓ 修正完了: {parquet_path}")
    print(f"✓ 最終カラム数: {len(df.columns)}")


def fix_shutuba_parquet(parquet_path: Path):
    """shutuba.parquetのnull型カラムを修正"""
    print(f"\n{'='*80}")
    print(f"修正対象: {parquet_path.name}")
    print(f"{'='*80}\n")

    # バックアップ作成
    backup_file(parquet_path)

    # 全カラムを読み込み（null型カラム以外）
    parquet_file = pq.ParquetFile(parquet_path)
    schema = parquet_file.schema_arrow

    # null型カラムを特定
    null_columns = []
    valid_columns = []

    for field in schema:
        if str(field.type) == "null":
            null_columns.append(field.name)
        else:
            valid_columns.append(field.name)

    print(f"null型カラム検出: {null_columns}")
    print(f"有効なカラム数: {len(valid_columns)}")

    # 有効なカラムのみ読み込み
    table = pq.read_table(parquet_path, columns=valid_columns)
    df = table.to_pandas()

    print(f"✓ データ読み込み完了: {len(df):,}行")

    # 重要なカラムを空データで追加
    if 'morning_odds' in null_columns:
        print("\n⚠️  morning_odds はnull型でした")
        print("   → double型の空カラムとして追加します")
        df['morning_odds'] = pd.Series(dtype='float64')

    if 'morning_popularity' in null_columns:
        print("\n⚠️  morning_popularity はnull型でした")
        print("   → Int64型の空カラムとして追加します")
        df['morning_popularity'] = pd.Series(dtype='Int64')

    # 削除されたカラムの報告
    kept_columns = ['morning_odds', 'morning_popularity']
    removed_columns = [col for col in null_columns if col not in kept_columns]
    if removed_columns:
        print(f"\n削除されたカラム: {removed_columns}")

    # 保存
    df.to_parquet(parquet_path, engine='pyarrow', compression='snappy', index=False)
    print(f"\n✓ 修正完了: {parquet_path}")
    print(f"✓ 最終カラム数: {len(df.columns)}")


def verify_fix(parquet_path: Path):
    """修正後のファイルを検証"""
    print(f"\n{'─'*80}")
    print(f"検証: {parquet_path.name}")
    print(f"{'─'*80}\n")

    parquet_file = pq.ParquetFile(parquet_path)
    schema = parquet_file.schema_arrow

    null_columns = [field.name for field in schema if str(field.type) == "null"]

    if null_columns:
        print(f"❌ まだnull型カラムが残っています: {null_columns}")
        return False
    else:
        print(f"✓ null型カラムは存在しません")
        print(f"✓ 総カラム数: {len(schema)}")
        print(f"✓ 総行数: {parquet_file.metadata.num_rows:,}")

        # データ読み込みテスト
        try:
            df = pd.read_parquet(parquet_path)
            print(f"✓ データ読み込みテスト成功: {len(df):,}行")
            return True
        except Exception as e:
            print(f"❌ データ読み込みエラー: {e}")
            return False


def main():
    """メイン処理"""
    print(f"\n{'='*80}")
    print("Parquet Null型カラム修正スクリプト")
    print(f"{'='*80}\n")

    base_path = Path(__file__).parent / "keibaai" / "data" / "parsed" / "parquet"

    # 修正対象ファイル
    races_path = base_path / "races" / "races.parquet"
    shutuba_path = base_path / "shutuba" / "shutuba.parquet"

    results = {}

    # races.parquetを修正
    if races_path.exists():
        try:
            fix_races_parquet(races_path)
            results['races'] = verify_fix(races_path)
        except Exception as e:
            print(f"\n❌ races.parquet修正中にエラー: {e}")
            import traceback
            traceback.print_exc()
            results['races'] = False
    else:
        print(f"⚠️  ファイルが見つかりません: {races_path}")

    # shutuba.parquetを修正
    if shutuba_path.exists():
        try:
            fix_shutuba_parquet(shutuba_path)
            results['shutuba'] = verify_fix(shutuba_path)
        except Exception as e:
            print(f"\n❌ shutuba.parquet修正中にエラー: {e}")
            import traceback
            traceback.print_exc()
            results['shutuba'] = False
    else:
        print(f"⚠️  ファイルが見つかりません: {shutuba_path}")

    # 最終サマリー
    print(f"\n\n{'='*80}")
    print("修正完了サマリー")
    print(f"{'='*80}\n")

    all_success = True
    for name, success in results.items():
        status = "✓ 成功" if success else "❌ 失敗"
        print(f"{name}.parquet: {status}")
        if not success:
            all_success = False

    if all_success:
        print(f"\n✓ 全てのファイルが正常に修正されました")
        print(f"\n次のステップ:")
        print(f"  python keibaai/src/features/generate_features.py --start_date 2023-01-01 --end_date 2023-12-31")
    else:
        print(f"\n❌ 一部のファイルで問題が発生しました")
        print(f"バックアップファイル（*.backup_*）を確認してください")


if __name__ == "__main__":
    main()
