"""
shutuba.parquetのrace_dateがNoneのエントリを、races.parquetから正しい日付で更新する
"""
import logging
import pandas as pd
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def main():
    # データパス
    shutuba_path = Path('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')
    races_path = Path('keibaai/data/parsed/parquet/races/races.parquet')

    # データロード
    logging.info(f"shutuba.parquetをロード: {shutuba_path}")
    shutuba = pd.read_parquet(shutuba_path)
    logging.info(f"  総行数: {len(shutuba):,}行")

    logging.info(f"\nraces.parquetをロード: {races_path}")
    races = pd.read_parquet(races_path)
    races['race_date'] = pd.to_datetime(races['race_date'])
    logging.info(f"  総行数: {len(races):,}行")

    # race_dateがNoneのエントリを確認
    null_date_mask = shutuba['race_date'].isna()
    null_count = null_date_mask.sum()
    logging.info(f"\nrace_dateがNullのエントリ: {null_count:,}行")

    if null_count == 0:
        logging.info("全てのrace_dateが設定されています。処理完了。")
        return

    # races.parquetから race_id → race_date のマッピングを作成
    race_date_map = races[['race_id', 'race_date']].drop_duplicates('race_id').set_index('race_id')['race_date'].to_dict()
    logging.info(f"\nrace_date_map作成: {len(race_date_map):,}件のrace_id")

    # Nullのrace_dateを更新
    def fill_race_date(row):
        if pd.isna(row['race_date']) and row['race_id'] in race_date_map:
            return race_date_map[row['race_id']]
        return row['race_date']

    logging.info("\nrace_dateを更新中...")
    shutuba['race_date'] = shutuba.apply(fill_race_date, axis=1)

    # 更新後の確認
    updated_null_count = shutuba['race_date'].isna().sum()
    fixed_count = null_count - updated_null_count

    logging.info(f"\n✅ 更新完了:")
    logging.info(f"   修正されたエントリ: {fixed_count:,}行")
    logging.info(f"   残りのNull: {updated_null_count:,}行")

    # race_date列を統一した型に変換（datetime64）
    logging.info("\nrace_date列をdatetime型に統一中...")
    shutuba['race_date'] = pd.to_datetime(shutuba['race_date'])
    logging.info("✅ 型変換完了")

    # 保存
    logging.info(f"\n保存中: {shutuba_path}")
    shutuba.to_parquet(shutuba_path, index=False)
    logging.info("✅ 保存完了")

    # 確認: 2020-03-28のデータ
    logging.info("\n確認: 2020年3月の日付別エントリ数")
    shutuba['race_date'] = pd.to_datetime(shutuba['race_date'])
    march_data = shutuba[(shutuba['race_date'] >= '2020-03-01') & (shutuba['race_date'] <= '2020-03-31')]
    dates = sorted(march_data['race_date'].dt.date.unique())

    for date in dates:
        count = len(march_data[march_data['race_date'].dt.date == date])
        print(f"  {date}: {count}エントリ")

if __name__ == '__main__':
    main()
