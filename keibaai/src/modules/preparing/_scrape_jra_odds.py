import json
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
import random

# from omegaconf import DictConfig

from src import pipeline_core
from src.utils import data_utils

log = logging.getLogger(__name__)

def scrape_and_save_jra_odds(race_id: str, cfg: dict, db_conn):
    """
    単一レースのJRAオッズを取得（ダミー）し、JSONとして保存する。
    
    Args:
        race_id (str): レースID
        cfg (dict): 設定オブジェクト
        db_conn: SQLiteコネクション
    """
    log.info(f"JRAオッズのダミー生成を開始: {race_id}")

    # --- 1. ダミーのオッズJSONデータを生成 ---
    jst = timezone(timedelta(hours=9))
    dummy_data = {
        "race_id": race_id,
        "snapshot_time": datetime.now(jst).isoformat(),
        "source": "jra_official_dummy",
        "market": {
            "win": {str(i): {"odds": round(random.uniform(1.5, 100.0), 1), "popularity": i} for i in range(1, 19)},
            "place": {str(i): {"odds": round(random.uniform(1.1, 20.0), 1), "popularity": i} for i in range(1, 19)},
        },
        "raw_html": "<html><body>dummy content</body></html>",
        "data_version": "v_dummy_20250101"
    }
    dummy_data_bytes = json.dumps(dummy_data, ensure_ascii=False, indent=2).encode('utf-8')

    # --- 2. ファイルパスを構築し、データを保存 ---
    output_dir = Path(cfg['default']['raw_data_path']) / "json" / "jra_odds"
    
    # construct_filename を data_utils から呼び出す
    file_name = data_utils.construct_filename(
        base_name=race_id,
        identifier="snapshot",
        data=dummy_data_bytes,
        extension="json"
    )
    file_path = output_dir / file_name

    # atomic_write を pipeline_core から呼び出す
    pipeline_core.atomic_write(str(file_path), dummy_data_bytes)
    log.info(f"ダミーオッズデータを保存しました: {file_path}")

    # --- 3. メタデータをDBに保存 ---
    dummy_url = f"https://www.jra.go.jp/dummy/odds/{race_id}"
    
    # save_fetch_metadata を data_utils から呼び出す
    data_utils.save_fetch_metadata(
        db_conn=db_conn,
        url=dummy_url,
        file_path=str(file_path),
        data=dummy_data_bytes,
        http_status=200,
        fetch_method='dummy_generator',
        error_message=None
    )
    log.info(f"メタデータをデータベースに保存しました: {dummy_url}")

    return file_path
