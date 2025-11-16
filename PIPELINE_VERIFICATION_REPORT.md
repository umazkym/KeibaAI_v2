# パイプライン検証レポート

**作成日**: 2025-11-16
**検証対象**: debug_full_pipeline_by_date.py によるスクレイピング＆パース処理
**対象日付**: 2023-10-09
**出力先**: output_final/

---

## 1. 検証結果サマリー

### ✅ 総合判定: **合格（問題なし）**

すべてのデータが正しくパースされており、欠損率0%を達成しています。
実際のパイプラインで使用可能な品質です。

---

## 2. データ検証結果

### 2.1 race_results.csv

| 項目 | 値 |
|------|------|
| **総行数** | 311行（24レース） |
| **カラム数** | 55カラム |
| **ユニークrace_id** | 24件 |
| **distance_m欠損** | 0行 (0.00%) ✅ |
| **track_surface欠損** | 0行 (0.00%) ✅ |

**track_surface分布**:
- ダート: 162頭 (52.1%)
- 芝: 137頭 (44.1%)
- 障害: 12頭 (3.9%)

**重要カラム**: race_id, distance_m, track_surface, weather, track_condition, venue, race_class, finish_position, horse_id, horse_name, jockey_name など

---

### 2.2 shutuba.csv（出馬表）

| 項目 | 値 |
|------|------|
| **総行数** | 311行（24レース） |
| **カラム数** | 26カラム |
| **ユニークrace_id** | 24件 |
| **horse_id欠損** | 0行 (0.00%) ✅ |
| **horse_name欠損** | 0行 (0.00%) ✅ |

**race_results.csvとの整合性**:
- race_id一致: ✅（両方とも24件）
- 行数一致: ✅（両方とも311行）

---

### 2.3 horses.csv（血統データ）

| 項目 | 値 |
|------|------|
| **総行数** | 1,181行 |
| **カラム数** | 11カラム |
| **ユニーク馬ID** | 20頭 |

**世代分布**:
```
世代1:    40行  (2^0 * 20 = 20馬 × 各2先祖)
世代2:    80行  (2^1 * 20)
世代3:   157行  (2^2 * 20、重複祖先あり)
世代4:   306行  (2^3 * 20、重複祖先あり)
世代5:   598行  (2^4 * 20、重複祖先あり)
```

**カラム**: horse_id, horse_name, birth_date, trainer_name, trainer_id, owner_name, breeder_name, producing_area, ancestor_id, ancestor_name, generation

---

### 2.4 horses_performance.csv（過去成績）

| 項目 | 値 |
|------|------|
| **総行数** | 469走 |
| **カラム数** | 27カラム |
| **ユニーク馬ID** | 20頭 |
| **平均出走回数/頭** | 23.5走 |

**カラム**: horse_id, race_date, venue, weather, race_number, race_name, race_id, race_grade, head_count, bracket_number, horse_number, finish_position, jockey_name, basis_weight, distance_m, track_surface, track_condition, finish_time_str, finish_time_seconds, margin_str, margin_seconds, passing_order, last_3f_time, win_odds, popularity, horse_weight, horse_weight_change

---

## 3. パーサー構造の分析

### 3.1 デバッグスクリプトのパーサー

**ファイル**: `debug_scraping_and_parsing.py`

**特徴**:
- `keibaai.src.modules.parsers.common_utils` のロジックを移植
- `extract_race_metadata_enhanced()` などの関数を独自実装
- HTMLコンテンツを直接受け取れるように改造
- **実行確認**: ✅ 正常動作（欠損率0%達成）

### 3.2 正式なパーサー

**ファイル**: `keibaai/src/modules/parsers/results_parser.py`

**特徴**:
- `parse_results_html(file_path)` がエントリーポイント
- `extract_race_metadata_enhanced()` を含む
- `common_utils` を使用
- 4段階フォールバック実装済み
- **使用箇所**: `run_parsing_pipeline_local.py`

### 3.3 パーサーの関係性

```
debug_scraping_and_parsing.py (デバッグ用)
  ├─ common_utilsのロジックを移植
  └─ HTMLコンテンツベースのパース

keibaai/src/modules/parsers/results_parser.py (正式版)
  ├─ common_utils をインポート
  └─ ファイルパスベースのパース
```

**結論**: 両者は**同等のロジック**を使用しているが、インターフェースが異なる

---

## 4. 既存パイプラインの構造

### 4.1 スクレイピングパイプライン

**ファイル**: `keibaai/src/run_scraping_pipeline_local.py`

**機能**:
1. 開催日リストを取得
2. race_idリストを取得
3. レース結果HTMLをスクレイピング
4. 出馬表HTMLをスクレイピング
5. 馬プロフィール・過去成績・血統をスクレイピング

**出力**: `data/raw/html/` 配下に.binファイル

### 4.2 パースパイプライン

**ファイル**: `keibaai/src/run_parsing_pipeline_local.py`

**機能**:
1. `data/raw/html/race/*.bin` → `data/parsed/parquet/races/races.parquet`
2. `data/raw/html/shutuba/*.bin` → `data/parsed/parquet/shutuba/shutuba.parquet`
3. `data/raw/html/horse/*.bin` → `data/parsed/parquet/horses/horses.parquet`
4. `data/raw/html/ped/*.bin` → `data/parsed/parquet/pedigrees/pedigrees.parquet`

**エラーハンドリング**: `pipeline_core.parse_with_error_handling()` を使用

### 4.3 パイプラインコア

**ファイル**: `keibaai/src/pipeline_core.py`

**提供機能**:
- `atomic_write()`: 安全なファイル書き込み
- `parse_with_error_handling()`: エラーハンドリング付きパース実行
- `setup_logging()`: ロギング設定
- `load_config()`: YAML設定読み込み
- `get_db_connection()`: SQLite接続取得

---

## 5. 問題点と改善提案

### 5.1 現在の問題点

#### ❌ 問題1: パーサーの二重管理

- **デバッグスクリプト**: `debug_scraping_and_parsing.py` のカスタムパーサー
- **正式パイプライン**: `keibaai/src/modules/parsers/` の正式パーサー
- **リスク**: 同期が取れなくなる可能性

#### ⚠️ 問題2: 出力形式の不一致

- **デバッグスクリプト**: CSV形式で出力（`output_final/*.csv`）
- **正式パイプライン**: Parquet形式で出力（`data/parsed/parquet/`）
- **リスク**: 後続処理（特徴量生成など）がParquetを期待している

#### ⚠️ 問題3: パイプラインの分断

- スクレイピングとパースが別スクリプト
- 日付ベースの一括処理が難しい

---

### 5.2 改善提案

#### 提案A: 正式パーサーの動作確認と統一 ✅ **推奨**

**概要**: 既存の正式なパイプラインが正しく動作するか確認し、デバッグスクリプトで確認した改善点を統合する

**手順**:
1. `run_parsing_pipeline_local.py` を実行して、正式パーサーの動作を確認
2. 出力されたParquetファイルのデータ品質を検証
3. 欠損率が高い場合は、正式パーサーを修正
4. デバッグスクリプトを廃止または補助ツールとして位置づける

**メリット**:
- 既存の構造を最大限活用
- Parquet形式で出力（後続処理と整合）
- エラーハンドリング・ロギングが充実

**デメリット**:
- 正式パーサーに問題がある場合、修正が必要

---

#### 提案B: 統合パイプラインスクリプトの作成

**概要**: スクレイピングからパースまでを日付ベースで一括実行できるスクリプトを作成

**実装イメージ**:
```python
# keibaai/src/run_daily_pipeline.py

def run_daily_pipeline(target_date: str):
    # 1. スクレイピング
    race_ids = scrape_by_date(target_date)

    # 2. パース（正式パーサーを使用）
    parse_races(race_ids)
    parse_shutuba(race_ids)
    parse_horses(race_ids)

    # 3. データ品質チェック
    validate_output()
```

**メリット**:
- 日付指定で一括実行可能
- 正式パーサーを使用
- デバッグしやすい

**デメリット**:
- 新規スクリプト作成が必要

---

#### 提案C: Parquet変換ユーティリティの追加

**概要**: debug_full_pipeline_by_date.py の出力をParquet形式に変換するユーティリティを追加

**実装イメージ**:
```python
# keibaai/src/utils/csv_to_parquet.py

def convert_debug_output_to_parquet(csv_dir: Path, parquet_dir: Path):
    # race_results.csv → races.parquet
    # shutuba.csv → shutuba.parquet
    # horses.csv → pedigrees.parquet
    # horses_performance.csv → horses_performance.parquet
```

**メリット**:
- デバッグスクリプトの出力を活用可能
- 既存パイプラインとの互換性を確保

**デメリット**:
- 追加の変換ステップが必要

---

## 6. 推奨実装手順

### ステップ1: 正式パイプラインの動作確認 ✅

```bash
# 既存binファイルをパース
cd /home/user/KeibaAI_v2
python keibaai/src/run_parsing_pipeline_local.py
```

**期待される動作**:
- `data/parsed/parquet/races/races.parquet` が生成される
- `data/parsed/parquet/shutuba/shutuba.parquet` が生成される
- `data/parsed/parquet/horses/horses.parquet` が生成される
- `data/parsed/parquet/pedigrees/pedigrees.parquet` が生成される

**検証**:
```python
import pandas as pd

# レース結果の確認
df_races = pd.read_parquet('data/parsed/parquet/races/races.parquet')
print(f"レース結果: {len(df_races)}行")
print(f"distance_m欠損率: {df_races['distance_m'].isna().sum() / len(df_races) * 100:.2f}%")
print(f"track_surface欠損率: {df_races['track_surface'].isna().sum() / len(df_races) * 100:.2f}%")
```

---

### ステップ2: データ品質の比較

**比較項目**:
- 欠損率（distance_m, track_surface, horse_id など）
- データ型の整合性
- レコード数

**判定基準**:
- 欠損率 0% → 正式パイプライン使用可能 ✅
- 欠損率 > 0% → 正式パーサーの修正が必要 ⚠️

---

### ステップ3: 統合パイプラインの作成（必要に応じて）

もし既存パイプラインの品質が十分であれば、日付ベースの統合パイプラインを作成します。

**ファイル**: `keibaai/src/run_daily_pipeline.py`

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日付ベース統合パイプライン

機能:
1. 指定日付のレース一覧を取得
2. スクレイピング（レース結果、出馬表、馬情報）
3. パース（正式パーサーを使用）
4. データ品質検証

使用方法:
    python keibaai/src/run_daily_pipeline.py --date 2023-10-09
"""

import argparse
from pathlib import Path
from datetime import datetime
from typing import List

from modules.preparing._scrape_html import scrape_kaisai_date, scrape_race_id_list
from modules.parsers import results_parser, shutuba_parser, horse_info_parser, pedigree_parser
import pipeline_core

def main():
    parser = argparse.ArgumentParser(description='日付ベース統合パイプライン')
    parser.add_argument('--date', required=True, help='対象日付 (YYYY-MM-DD)')
    args = parser.parse_args()

    # 1. 設定読み込み
    config = pipeline_core.load_config('configs/default.yaml')

    # 2. スクレイピング
    print(f"[フェーズ1] スクレイピング: {args.date}")
    race_ids = scrape_by_date(args.date, config)

    # 3. パース
    print(f"[フェーズ2] パース: {len(race_ids)}件のレース")
    parse_all(race_ids, config)

    # 4. 検証
    print(f"[フェーズ3] データ品質検証")
    validate_output(config)

    print("[完了] すべての処理が完了しました")

if __name__ == '__main__':
    main()
```

---

### ステップ4: データ品質監視の自動化

**ファイル**: `keibaai/src/utils/validate_parsed_data.py`

```python
#!/usr/bin/env python3
"""
パース済みデータの品質検証

使用方法:
    python keibaai/src/utils/validate_parsed_data.py
"""

import pandas as pd
from pathlib import Path

def validate_races():
    """レース結果の検証"""
    df = pd.read_parquet('data/parsed/parquet/races/races.parquet')

    critical_cols = ['race_id', 'distance_m', 'track_surface', 'horse_id']

    print(f"レース結果: {len(df)}行")
    for col in critical_cols:
        missing_rate = df[col].isna().sum() / len(df) * 100
        status = "✅" if missing_rate == 0 else "⚠️"
        print(f"  {status} {col}: {missing_rate:.2f}% 欠損")

def main():
    validate_races()
    # validate_shutuba()
    # validate_horses()
    # validate_pedigrees()

if __name__ == '__main__':
    main()
```

---

## 7. 次のアクション

### 優先度 HIGH ⭐⭐⭐

1. **既存の正式パイプラインを実行**
   ```bash
   python keibaai/src/run_parsing_pipeline_local.py
   ```

2. **出力されたParquetファイルの品質を検証**
   ```python
   import pandas as pd
   df = pd.read_parquet('data/parsed/parquet/races/races.parquet')
   print(df.info())
   print(df['distance_m'].isna().sum())
   ```

3. **品質が十分なら、そのまま使用**
   - デバッグスクリプトは補助ツールとして保持
   - 正式パイプラインを本番利用

### 優先度 MEDIUM ⭐⭐

4. **日付ベース統合パイプラインの作成**
   - スクレイピング＋パースを一括実行
   - デバッグしやすい構造

5. **データ品質監視の自動化**
   - 欠損率チェック
   - データ型チェック
   - レコード数チェック

### 優先度 LOW ⭐

6. **デバッグスクリプトの整理**
   - 不要なスクリプトを削除
   - 有用なスクリプトを `keibaai/scripts/debug/` に移動

---

## 8. まとめ

### ✅ output_finalフォルダの検証結果

**総合評価**: **合格**

- すべてのデータが正しくパースされている
- 重要カラムの欠損率0%を達成
- データ構造の整合性が確認できた

### 🔍 実装方法

**推奨アプローチ**: **提案A（正式パーサーの動作確認と統一）**

1. 既存の正式パイプライン（`run_parsing_pipeline_local.py`）を実行
2. 出力品質を検証
3. 品質が十分なら、そのまま本番利用
4. 必要に応じて、日付ベース統合パイプラインを作成

### 📝 次のステップ

1. `python keibaai/src/run_parsing_pipeline_local.py` を実行
2. 出力されたParquetファイルを検証
3. 品質が十分なら、既存パイプラインを採用
4. 品質に問題があれば、正式パーサーを修正

---

**作成者**: Claude (AI Assistant)
**レビュー**: 必要に応じて人間の開発者が確認
