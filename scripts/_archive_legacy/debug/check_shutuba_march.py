# check_shutuba_null_dates.py として保存
import pandas as pd
from pathlib import Path

shutuba_path = Path('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')
shutuba = pd.read_parquet(shutuba_path)

# race_dateがNullのデータを確認
null_dates = shutuba[shutuba['race_date'].isna()]
print(f"race_dateがNullのエントリ: {len(null_dates)}行")

# 再パースしたrace_idを確認
target_race_ids = ['202006030101', '202006030102', '202007010701', '202009020101']
for race_id in target_race_ids:
    data = shutuba[shutuba['race_id'] == race_id]
    if len(data) > 0:
        sample_date = data['race_date'].iloc[0]
        print(f"{race_id}: {len(data)}エントリ, race_date={sample_date}")
    else:
        print(f"{race_id}: データなし")