"""
欠損しているshutuba HTMLファイルを再パースしてshutuba.parquetに追加する
"""
import logging
import pandas as pd
from pathlib import Path
import sys

# プロジェクトルートをsys.pathに追加
project_root = Path(__file__).resolve().parent / 'keibaai'
sys.path.append(str(project_root))

from src.modules.parsers import shutuba_parser

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def main():
    # 欠損しているrace_id（2020-03-28, 29, 31）
    missing_race_ids = [
        # 2020-03-28: 中山第3回1日目
        '202006030101', '202006030102', '202006030103', '202006030104',
        '202006030105', '202006030106', '202006030107', '202006030108',
        '202006030109', '202006030110', '202006030111', '202006030112',
        # 2020-03-28: 中京第1回7日目
        '202007010701', '202007010702', '202007010703', '202007010704',
        '202007010705', '202007010706', '202007010707', '202007010708',
        '202007010709', '202007010710', '202007010711', '202007010712',
        # 2020-03-28: 阪神第2回1日目
        '202009020101', '202009020102', '202009020103', '202009020104',
        '202009020105', '202009020106', '202009020107', '202009020108',
        '202009020109', '202009020110', '202009020111', '202009020112',

        # 2020-03-29: 中山第3回2日目
        '202006030201', '202006030202', '202006030203', '202006030204',
        '202006030205', '202006030206', '202006030207', '202006030208',
        '202006030209', '202006030210', '202006030211', '202006030212',
        # 2020-03-29: 中京第1回8日目
        '202007010801', '202007010802', '202007010803', '202007010804',
        '202007010805', '202007010806', '202007010807', '202007010808',
        '202007010809', '202007010810', '202007010811', '202007010812',
        # 2020-03-29: 阪神第2回2日目
        '202009020201', '202009020202', '202009020203', '202009020204',
        '202009020205', '202009020206', '202009020207', '202009020208',
        '202009020209', '202009020210', '202009020211', '202009020212',

        # 2020-03-31: 中山第3回4日目（火曜日開催）
        '202006030401', '202006030402', '202006030403', '202006030404',
        '202006030405', '202006030406', '202006030407', '202006030408',
        '202006030409', '202006030410', '202006030411', '202006030412',
    ]

    html_dir = Path('keibaai/data/raw/html/shutuba')
    shutuba_parquet_path = Path('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')

    # 既存のshutuba.parquetをロード
    if shutuba_parquet_path.exists():
        existing_df = pd.read_parquet(shutuba_parquet_path)
        logging.info(f"既存shutuba.parquet: {len(existing_df)}行")
    else:
        existing_df = pd.DataFrame()
        logging.info("既存shutuba.parquetが見つかりません。新規作成します。")

    # 欠損race_idをパース
    parsed_dfs = []
    success_count = 0
    fail_count = 0

    for race_id in missing_race_ids:
        html_file = html_dir / f"{race_id}.bin"

        if not html_file.exists():
            logging.warning(f"❌ HTMLファイルが見つかりません: {html_file}")
            fail_count += 1
            continue

        try:
            df = shutuba_parser.parse_shutuba_html(str(html_file), race_id=race_id)
            if df is not None and not df.empty:
                parsed_dfs.append(df)
                success_count += 1
                logging.info(f"✅ パース成功: {race_id} ({len(df)}行)")
            else:
                logging.warning(f"⚠️  空データ: {race_id}")
                fail_count += 1
        except Exception as e:
            logging.error(f"❌ パース失敗: {race_id} - {e}")
            fail_count += 1

    # 新しいデータを結合
    if parsed_dfs:
        new_df = pd.concat(parsed_dfs, ignore_index=True)
        logging.info(f"\n新規パースデータ: {len(new_df)}行")

        # 既存データと結合
        if not existing_df.empty:
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            # 重複削除（念のため）
            combined_df = combined_df.drop_duplicates(subset=['race_id', 'horse_id'], keep='last')
        else:
            combined_df = new_df

        # 保存
        shutuba_parquet_path.parent.mkdir(parents=True, exist_ok=True)
        combined_df.to_parquet(shutuba_parquet_path, index=False)
        logging.info(f"\n✅ 保存完了: {shutuba_parquet_path}")
        logging.info(f"   総行数: {len(combined_df):,}行")
        logging.info(f"   成功: {success_count}/{len(missing_race_ids)}")
        logging.info(f"   失敗: {fail_count}/{len(missing_race_ids)}")
    else:
        logging.error("新規パースデータがありません。")

if __name__ == '__main__':
    main()
