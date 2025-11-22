"""
shutuba.parquetのrace_dateに含まれる全角文字を半角に変換する
"""
import logging
import pandas as pd
from pathlib import Path
import unicodedata

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def zenkaku_to_hankaku(text):
    """全角英数字を半角に変換"""
    if pd.isna(text):
        return text
    if isinstance(text, str):
        # NFKCで正規化（全角→半角）
        return unicodedata.normalize('NFKC', text)
    return text

def main():
    shutuba_path = Path('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')

    logging.info(f"shutuba.parquetをロード: {shutuba_path}")
    shutuba = pd.read_parquet(shutuba_path)
    logging.info(f"  総行数: {len(shutuba):,}行")

    # race_dateの型を確認
    logging.info(f"\nrace_date列の型: {shutuba['race_date'].dtype}")

    # サンプルを確認
    logging.info("\nrace_dateのサンプル（最初の10件）:")
    for i, val in enumerate(shutuba['race_date'].head(10)):
        logging.info(f"  [{i}] {repr(val)} (type: {type(val).__name__})")

    # 文字列型のrace_dateを確認
    if shutuba['race_date'].dtype == 'object':
        string_dates = shutuba[shutuba['race_date'].apply(lambda x: isinstance(x, str))]
        logging.info(f"\n文字列型のrace_date: {len(string_dates):,}行")

        if len(string_dates) > 0:
            logging.info("文字列型race_dateのサンプル:")
            for val in string_dates['race_date'].unique()[:20]:
                logging.info(f"  {repr(val)}")

            # 全角文字を含むものを検出
            fullwidth_dates = string_dates[string_dates['race_date'].str.contains(r'[０-９]', regex=True, na=False)]
            logging.info(f"\n全角数字を含むrace_date: {len(fullwidth_dates):,}行")

            if len(fullwidth_dates) > 0:
                logging.info("全角数字を含むrace_dateのサンプル:")
                for val in fullwidth_dates['race_date'].unique()[:20]:
                    logging.info(f"  {repr(val)}")

    # 全てのrace_dateを半角に変換してからdatetimeに変換
    logging.info("\n全角→半角変換を実行中...")
    shutuba['race_date'] = shutuba['race_date'].apply(zenkaku_to_hankaku)
    logging.info("✅ 変換完了")

    # datetime型に統一
    logging.info("\nrace_date列をdatetime型に変換中...")
    try:
        shutuba['race_date'] = pd.to_datetime(shutuba['race_date'], errors='coerce')
        logging.info("✅ datetime変換完了")
    except Exception as e:
        logging.error(f"datetime変換エラー: {e}")
        return

    # 変換後のNullチェック
    null_count = shutuba['race_date'].isna().sum()
    logging.info(f"\n変換後のNull: {null_count:,}行")

    if null_count > 0:
        logging.warning(f"⚠️  {null_count}行のrace_dateがNullになりました")
        null_race_ids = shutuba[shutuba['race_date'].isna()]['race_id'].unique()
        logging.warning(f"Null race_idの例: {null_race_ids[:10].tolist()}")

    # 保存
    logging.info(f"\n保存中: {shutuba_path}")
    shutuba.to_parquet(shutuba_path, index=False)
    logging.info("✅ 保存完了")

    # 確認
    logging.info("\n最終確認:")
    logging.info(f"  race_date型: {shutuba['race_date'].dtype}")
    logging.info(f"  Null数: {shutuba['race_date'].isna().sum():,}行")
    logging.info(f"  最小日付: {shutuba['race_date'].min()}")
    logging.info(f"  最大日付: {shutuba['race_date'].max()}")

if __name__ == '__main__':
    main()
