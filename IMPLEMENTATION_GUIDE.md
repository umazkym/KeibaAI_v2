# 実装ガイド - データパイプライン統合方法

**作成日**: 2025-11-16
**対象**: KeibaAI_v2 スクレイピング＆パースパイプライン
**目的**: debug_full_pipeline_by_date.pyで検証したパーサーを本番パイプラインに統合

---

## 📊 検証結果サマリー

### ✅ output_finalの品質評価: **完璧**

| 指標 | 結果 | 評価 |
|------|------|------|
| race_results.csv - distance_m欠損率 | 0.00% | ✅ 合格 |
| race_results.csv - track_surface欠損率 | 0.00% | ✅ 合格 |
| shutuba.csv - horse_id欠損率 | 0.00% | ✅ 合格 |
| データ整合性（race_results vs shutuba） | 完全一致 | ✅ 合格 |
| 血統データ（5世代） | 1,181行 | ✅ 正常 |
| 過去成績データ | 469走 | ✅ 正常 |

**結論**: デバッグスクリプトで使用したパーサーは、**本番品質**を達成しています。

---

## 🎯 推奨実装方法

### 方法1: 正式パーサーの動作確認（最推奨）⭐⭐⭐

既存の正式パイプライン（`keibaai/src/run_parsing_pipeline_local.py`）が、デバッグスクリプトと同じ品質を達成できるか確認します。

#### ステップ1: Python環境の確認

```bash
# Dockerコンテナを使用する場合
docker-compose up -d
docker-compose exec app bash

# または仮想環境を使用する場合
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt  # requirements.txtを作成する必要あり
```

#### ステップ2: 正式パイプラインの実行

```bash
cd /home/user/KeibaAI_v2
python keibaai/src/run_parsing_pipeline_local.py
```

**期待される出力**:
```
data/parsed/parquet/races/races.parquet
data/parsed/parquet/shutuba/shutuba.parquet
data/parsed/parquet/horses/horses.parquet
data/parsed/parquet/pedigrees/pedigrees.parquet
```

#### ステップ3: 出力品質の検証

```python
import pandas as pd

# レース結果の検証
df_races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
print(f"総レース数: {len(df_races)}")
print(f"distance_m欠損率: {df_races['distance_m'].isna().sum() / len(df_races) * 100:.2f}%")
print(f"track_surface欠損率: {df_races['track_surface'].isna().sum() / len(df_races) * 100:.2f}%")

# 出馬表の検証
df_shutuba = pd.read_parquet('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')
print(f"総出走頭数: {len(df_shutuba)}")
print(f"horse_id欠損率: {df_shutuba['horse_id'].isna().sum() / len(df_shutuba) * 100:.2f}%")
```

**判定基準**:
- ✅ 欠損率 0% → 正式パイプラインをそのまま使用
- ⚠️ 欠損率 > 0% → 方法2（パーサー修正）を実施

---

### 方法2: debug_scraping_and_parsing.pyのロジックを正式パーサーに統合

もし正式パーサーの品質が不十分な場合、デバッグスクリプトの改良点を統合します。

#### 統合対象の改良点

1. **extract_race_metadata_enhanced()の強化**
   - 4段階フォールバック実装
   - 障害レース対応
   - 地方競馬対応

2. **parse_result_row_enhanced()の改良**
   - より堅牢なHTMLパース
   - 欠損値処理の改善

#### 実装例

**ファイル**: `keibaai/src/modules/parsers/results_parser.py`

```python
def extract_race_metadata_enhanced(soup: BeautifulSoup) -> Dict:
    """
    拡張されたレースメタデータ抽出（4段階フォールバック）

    改良点:
    - レベル1: data_intro（最も詳細）
    - レベル2: diary_snap_cut（代替）
    - レベル3: racedata（最小限）
    - レベル4: デフォルト値
    """
    metadata = {
        'distance_m': None,
        'track_surface': None,
        'weather': None,
        'track_condition': None,
        # ... その他のフィールド
    }

    # レベル1: data_intro
    race_data_intro = soup.find('div', class_='data_intro')
    if race_data_intro:
        # パース処理
        return metadata

    # レベル2: diary_snap_cut（フォールバック）
    diary_snap = soup.find('div', class_='diary_snap_cut')
    if diary_snap:
        # パース処理
        return metadata

    # レベル3: racedata（最小限フォールバック）
    race_data_dl = soup.find('dl', class_='racedata')
    if race_data_dl:
        # パース処理
        return metadata

    # レベル4: ログ警告
    logging.warning("レースメタデータの抽出に失敗")
    return metadata
```

---

### 方法3: CSV→Parquet変換ユーティリティの作成

debug_full_pipeline_by_date.pyの出力を、既存パイプラインと互換性のあるParquet形式に変換します。

#### 実装

**ファイル**: `keibaai/src/utils/csv_to_parquet_converter.py`

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
デバッグスクリプトのCSV出力をParquet形式に変換

使用方法:
    python keibaai/src/utils/csv_to_parquet_converter.py \
        --input output_final \
        --output keibaai/data/parsed/parquet
"""

import argparse
import pandas as pd
from pathlib import Path

def convert_race_results(csv_path: Path, parquet_path: Path):
    """race_results.csv → races.parquet"""
    df = pd.read_csv(csv_path, encoding='utf-8-sig')

    # データ型の調整
    int_cols = ['distance_m', 'head_count', 'finish_position', 'age',
                'popularity', 'horse_weight', 'horse_weight_change']
    for col in int_cols:
        if col in df.columns:
            df[col] = df[col].astype('Int64')  # 欠損対応の整数型

    # Parquet保存
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(parquet_path, index=False, engine='pyarrow')
    print(f"✓ {parquet_path}: {len(df)}行")

def convert_shutuba(csv_path: Path, parquet_path: Path):
    """shutuba.csv → shutuba.parquet"""
    df = pd.read_csv(csv_path, encoding='utf-8-sig')

    int_cols = ['bracket_number', 'horse_number', 'age',
                'horse_weight', 'horse_weight_change']
    for col in int_cols:
        if col in df.columns:
            df[col] = df[col].astype('Int64')

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(parquet_path, index=False, engine='pyarrow')
    print(f"✓ {parquet_path}: {len(df)}行")

def convert_horses(csv_path: Path, parquet_path: Path):
    """horses.csv → pedigrees.parquet"""
    df = pd.read_csv(csv_path, encoding='utf-8-sig')

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(parquet_path, index=False, engine='pyarrow')
    print(f"✓ {parquet_path}: {len(df)}行")

def convert_horses_performance(csv_path: Path, parquet_path: Path):
    """horses_performance.csv → horses_performance.parquet"""
    df = pd.read_csv(csv_path, encoding='utf-8-sig')

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(parquet_path, index=False, engine='pyarrow')
    print(f"✓ {parquet_path}: {len(df)}行")

def main():
    parser = argparse.ArgumentParser(description='CSV→Parquet変換')
    parser.add_argument('--input', required=True, help='CSVディレクトリ')
    parser.add_argument('--output', required=True, help='Parquet出力ディレクトリ')
    args = parser.parse_args()

    input_dir = Path(args.input)
    output_dir = Path(args.output)

    print(f"変換開始: {input_dir} → {output_dir}")

    # race_results.csv → races.parquet
    convert_race_results(
        input_dir / 'race_results.csv',
        output_dir / 'races' / 'races.parquet'
    )

    # shutuba.csv → shutuba.parquet
    convert_shutuba(
        input_dir / 'shutuba.csv',
        output_dir / 'shutuba' / 'shutuba.parquet'
    )

    # horses.csv → pedigrees.parquet
    convert_horses(
        input_dir / 'horses.csv',
        output_dir / 'pedigrees' / 'pedigrees.parquet'
    )

    # horses_performance.csv → horses_performance.parquet
    convert_horses_performance(
        input_dir / 'horses_performance.csv',
        output_dir / 'horses_performance' / 'horses_performance.parquet'
    )

    print("変換完了")

if __name__ == '__main__':
    main()
```

**使用方法**:
```bash
python keibaai/src/utils/csv_to_parquet_converter.py \
    --input output_final \
    --output keibaai/data/parsed/parquet
```

---

### 方法4: 日付ベース統合パイプラインの作成（理想形）

スクレイピングからパースまでを日付ベースで一括実行できる統合パイプラインを作成します。

#### 実装

**ファイル**: `keibaai/src/run_daily_pipeline.py`

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日付ベース統合パイプライン

機能:
1. 指定日付のレース一覧を取得（スクレイピング）
2. レース結果、出馬表、馬情報をスクレイピング
3. パース（正式パーサーを使用）
4. Parquet形式で保存
5. データ品質検証

使用方法:
    # スクレイピング＋パース
    python keibaai/src/run_daily_pipeline.py --date 2023-10-09

    # パースのみ（既存HTMLファイルを使用）
    python keibaai/src/run_daily_pipeline.py --date 2023-10-09 --parse-only

    # スクレイピングのみ
    python keibaai/src/run_daily_pipeline.py --date 2023-10-09 --scrape-only
"""

import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import List
import sys

# プロジェクトルートをパスに追加
sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.modules.preparing._scrape_html import (
    scrape_kaisai_date,
    scrape_race_id_list,
    fetch_html_robust_get
)
from src.modules.parsers import (
    results_parser,
    shutuba_parser,
    horse_info_parser,
    pedigree_parser
)
from src import pipeline_core
import pandas as pd

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def scrape_by_date(target_date: str, config: dict) -> List[str]:
    """指定日付のレースをスクレイピング"""
    logging.info(f"[スクレイピング] 対象日付: {target_date}")

    # 1. 開催日リストを取得
    kaisai_dates = scrape_kaisai_date(target_date[:4])  # 年指定

    # 2. race_idリストを取得
    race_ids = []
    for date in kaisai_dates:
        if date.startswith(target_date.replace('-', '')):
            race_ids_for_date = scrape_race_id_list(date)
            race_ids.extend(race_ids_for_date)

    logging.info(f"  検出されたレース: {len(race_ids)}件")

    # 3. レース結果をスクレイピング
    # （省略: 実装は debug_full_pipeline_by_date.py を参考）

    return race_ids

def parse_all(race_ids: List[str], config: dict):
    """全データをパース"""
    logging.info(f"[パース] {len(race_ids)}件のレースを処理")

    # 1. レース結果のパース
    races_df_list = []
    for race_id in race_ids:
        file_path = Path(config['raw_data_path']) / 'html' / 'race' / f"{race_id}.bin"
        if file_path.exists():
            df = results_parser.parse_results_html(str(file_path), race_id)
            if df is not None and not df.empty:
                races_df_list.append(df)

    if races_df_list:
        races_df = pd.concat(races_df_list, ignore_index=True)
        output_path = Path(config['parsed_data_path']) / 'parquet' / 'races' / 'races.parquet'
        output_path.parent.mkdir(parents=True, exist_ok=True)
        races_df.to_parquet(output_path, index=False)
        logging.info(f"  レース結果: {len(races_df)}行 → {output_path}")

    # 2. 出馬表のパース
    # （省略）

    # 3. 馬情報のパース
    # （省略）

def validate_output(config: dict):
    """データ品質検証"""
    logging.info("[検証] データ品質チェック")

    races_path = Path(config['parsed_data_path']) / 'parquet' / 'races' / 'races.parquet'
    if races_path.exists():
        df = pd.read_parquet(races_path)

        missing_distance = df['distance_m'].isna().sum()
        missing_surface = df['track_surface'].isna().sum()

        logging.info(f"  レース結果: {len(df)}行")
        logging.info(f"    distance_m欠損: {missing_distance}行 ({100*missing_distance/len(df):.2f}%)")
        logging.info(f"    track_surface欠損: {missing_surface}行 ({100*missing_surface/len(df):.2f}%)")

        if missing_distance == 0 and missing_surface == 0:
            logging.info("  ✅ データ品質: 合格")
        else:
            logging.warning("  ⚠️ データ品質: 要改善")

def main():
    parser = argparse.ArgumentParser(description='日付ベース統合パイプライン')
    parser.add_argument('--date', required=True, help='対象日付 (YYYY-MM-DD)')
    parser.add_argument('--scrape-only', action='store_true', help='スクレイピングのみ')
    parser.add_argument('--parse-only', action='store_true', help='パースのみ')
    args = parser.parse_args()

    setup_logging()

    # 設定読み込み
    config = {
        'raw_data_path': 'keibaai/data/raw',
        'parsed_data_path': 'keibaai/data/parsed'
    }

    # スクレイピング
    if not args.parse_only:
        race_ids = scrape_by_date(args.date, config)
    else:
        # 既存ファイルからrace_idを取得
        race_ids = []  # 実装省略

    # パース
    if not args.scrape_only:
        parse_all(race_ids, config)

    # 検証
    if not args.scrape_only:
        validate_output(config)

    logging.info("[完了] すべての処理が完了しました")

if __name__ == '__main__':
    main()
```

---

## 📝 実装優先順位

### 優先度 HIGH ⭐⭐⭐（必須）

1. **方法1を実行**
   - 既存の正式パイプラインを実行
   - 出力品質を検証
   - 品質が十分なら、そのまま採用

2. **Python環境の整備**
   - Docker環境のセットアップ、または
   - 仮想環境の作成とrequirements.txt整備

### 優先度 MEDIUM ⭐⭐（推奨）

3. **方法3を実装**
   - CSV→Parquet変換ユーティリティ
   - デバッグ出力を本番形式に変換

4. **方法4を実装**
   - 日付ベース統合パイプライン
   - 運用を大幅に効率化

### 優先度 LOW ⭐（任意）

5. **方法2を実装**
   - 正式パーサーの改良
   - ただし、方法1で品質が十分なら不要

---

## 🔄 運用フロー（方法4採用時）

### 日次運用

```bash
# 前日のレースデータを取得・パース
python keibaai/src/run_daily_pipeline.py --date $(date -d yesterday +%Y-%m-%d)
```

### 週次バッチ

```bash
# 1週間分のデータを一括処理
for i in {1..7}; do
    date=$(date -d "$i days ago" +%Y-%m-%d)
    python keibaai/src/run_daily_pipeline.py --date $date
done
```

### 品質監視

```bash
# データ品質レポートを生成
python keibaai/src/utils/validate_parsed_data.py > quality_report.txt
```

---

## 🎯 最終推奨事項

### 今すぐ実行すべきこと

1. **Python環境の確認**
   ```bash
   # debug_full_pipeline_by_date.pyを実行した環境を確認
   which python
   python --version
   ```

2. **正式パイプラインの実行**
   ```bash
   python keibaai/src/run_parsing_pipeline_local.py
   ```

3. **出力品質の検証**
   - distance_m, track_surfaceの欠損率をチェック
   - 0%なら正式パイプライン採用
   - >0%なら方法2で改良

### 長期的な改善

1. **統合パイプライン（方法4）の実装**
   - スクレイピング〜パースを一括化
   - 日付ベースで実行可能に

2. **自動品質監視の導入**
   - 欠損率を自動チェック
   - アラート機能を追加

3. **デバッグスクリプトの整理**
   - 有用なスクリプトをkeibaai/scripts/debug/に移動
   - 不要なスクリプトを削除

---

## 📚 参考資料

- **CLAUDE.md**: プロジェクト全体の構造とガイドライン
- **PIPELINE_VERIFICATION_REPORT.md**: 詳細な検証結果
- **schema.md**: データスキーマ定義
- **DEBUG_REPORT.md**: パーサー改良の履歴

---

**作成者**: Claude (AI Assistant)
**更新日**: 2025-11-16
