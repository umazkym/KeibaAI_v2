# KeibaAI_v2 ログアナライザー使用ガイド

## 概要

このツールは、KeibaAI_v2のスクレイピングとパースパイプラインの実行ログを分析し、詳細な統計情報とレポートを生成します。

## ツールの種類

### 1. 基本版: `analyze_keiba_logs.py`

シンプルで高速なログ分析ツール。基本的な統計情報を提供します。

**特徴:**
- 軽量で高速
- 基本的な統計情報
- エラーと警告の抽出
- JSON/テキスト形式でレポート保存

### 2. 拡張版: `analyze_keiba_logs_advanced.py`

より詳細な分析とデータ品質レポートを提供します。

**特徴:**
- 詳細な成功/失敗統計
- HTTPエラー分類
- データ品質問題の特定
- パフォーマンス指標
- 欠損フィールドの追跡

## 使い方

### 基本的な使用方法

#### 1. 今日のログを分析

```bash
# 基本版
python analyze_keiba_logs.py

# 拡張版
python analyze_keiba_logs_advanced.py
```

デフォルトでは、今日の日付のログディレクトリ (`keibaai/data/logs/YYYY/MM/DD`) を分析します。

#### 2. 特定の日付のログを分析

```bash
# 基本版
python analyze_keiba_logs.py keibaai/data/logs/2025/11/18

# 拡張版
python analyze_keiba_logs_advanced.py keibaai/data/logs/2025/11/18
```

### パイプライン実行後の分析フロー

```bash
# 1. スクレイピングパイプライン実行
python keibaai/src/run_scraping_pipeline_with_args.py --from 2020-01-01 --to 2025-10-31

# 2. パースパイプライン実行
python keibaai/src/run_parsing_pipeline_local.py

# 3. ログ分析（基本版）
python analyze_keiba_logs.py

# または、拡張版で詳細分析
python analyze_keiba_logs_advanced.py
```

## 出力ファイル

分析結果は `keibaai/data/logs/analysis/` ディレクトリに保存されます。

### 基本版の出力

- `log_analysis_YYYYMMDD_HHMMSS.txt` - テキスト形式のサマリーレポート
- `log_analysis_YYYYMMDD_HHMMSS.json` - JSON形式の詳細データ

### 拡張版の出力

- `advanced_log_analysis_YYYYMMDD_HHMMSS.txt` - 詳細テキストレポート
- `advanced_log_analysis_YYYYMMDD_HHMMSS.json` - JSON形式の詳細統計

## レポートの見方

### 総合統計

```
■ 総合統計
  • 総ログエントリ数: 12,345
```

パイプライン実行中に記録された全ログメッセージの数。

### ログレベル別集計

```
■ ログレベル別統計
┌─────────────┬──────────┬─────────┐
│ レベル      │ 件数     │ 割合    │
├─────────────┼──────────┼─────────┤
│ INFO        │   10,234 │  82.89% │
│ WARNING     │    1,543 │  12.50% │
│ ERROR       │      568 │   4.60% │
└─────────────┴──────────┴─────────┘
```

- **INFO**: 正常な処理ログ
- **WARNING**: 注意が必要だが処理は継続
- **ERROR**: エラーが発生（処理スキップの可能性）
- **CRITICAL**: 重大なエラー（システム停止の可能性）

### スクレイピング統計

```
■ スクレイピング詳細統計
┌──────────────┬──────────┬──────────┬──────────┬──────────┐
│ データ種別   │ 成功     │ 失敗     │ スキップ │ 合計     │
├──────────────┼──────────┼──────────┼──────────┼──────────┤
│ races        │    5,234 │       12 │      145 │    5,391 │
│ shutuba      │    5,189 │       18 │      184 │    5,391 │
│ horses       │   23,456 │      234 │    1,234 │   24,924 │
│ pedigrees    │   22,987 │      145 │      876 │   24,008 │
└──────────────┴──────────┴──────────┴──────────┴──────────┘
```

- **成功**: 正常にスクレイピングできた件数
- **失敗**: エラーで取得できなかった件数
- **スキップ**: 既に存在するためスキップした件数

### パース統計

```
■ パース詳細統計
┌──────────────┬──────────┬──────────┬──────────┬──────────┐
│ データ種別   │ 成功     │ 失敗     │ 成功率   │ 合計     │
├──────────────┼──────────┼──────────┼──────────┼──────────┤
│ races        │    5,221 │       13 │  99.75% │    5,234 │
│ shutuba      │    5,171 │       18 │  99.65% │    5,189 │
│ horses       │   23,222 │      234 │  99.00% │   23,456 │
│ pedigrees    │   22,842 │      145 │  99.37% │   22,987 │
└──────────────┴──────────┴──────────┴──────────┴──────────┘
```

- **成功率**: パース成功率（高いほど良い）
- 95%以下の場合、HTMLフォーマット変更やパーサーのバグの可能性

### 欠損フィールド

```
  欠損フィールド (上位15件):
    • distance_m                       :    234 件
    • track_surface                    :    187 件
    • weather                          :     92 件
    • venue                            :     45 件
```

パース時にHTMLから抽出できなかったフィールド。データ品質の指標となります。

### パフォーマンス統計

```
■ パフォーマンス統計
  • scraping          :   2.45 時間 (  8,820.0 秒)
  • parsing           :   0.82 時間 (  2,952.0 秒)
  • total_pipeline    :   3.27 時間 ( 11,772.0 秒)

  平均スクレイピング速度: 8.32 items/秒
  平均パース速度: 15.47 items/秒
```

パイプライン全体の実行時間とスループット。

### エラー詳細

```
■ エラー詳細 (最新20件)
  1. [14:23:45] HTTP 404 error when scraping race 202301050101
  2. [14:25:12] Parse error: distance_m not found in race data
  ...
```

発生したエラーの詳細。問題の診断に使用します。

## よくある問題と対処法

### 1. ログファイルが見つからない

**症状:**
```
⚠️  ログファイルが見つかりませんでした
```

**原因:**
- パイプラインがまだ実行されていない
- ログディレクトリのパスが間違っている

**対処法:**
```bash
# ログディレクトリを確認
ls -la keibaai/data/logs/2025/11/18/

# パイプラインを実行
python keibaai/src/run_scraping_pipeline_with_args.py --from 2025-11-18 --to 2025-11-18
```

### 2. HTTPエラーが多い

**症状:**
```
  HTTPエラー統計:
    • HTTP 400: 234 件
    • HTTP 403: 12 件
```

**原因:**
- スクレイピング間隔が短すぎる
- IPアドレスがブロックされている
- レースIDが存在しない

**対処法:**
- `configs/scraping.yaml` でsleep間隔を調整
- 60秒待機してから再実行
- レースID範囲を確認

### 3. パース失敗率が高い

**症状:**
```
│ races        │    4,221 │    1,013 │  80.65% │    5,234 │
```

**原因:**
- HTMLフォーマットが変更された
- パーサーのバグ
- 壊れたHTMLファイル

**対処法:**
```bash
# パーサーをデバッグモードで実行
python debug_scraping_and_parsing.py

# 失敗したファイルを確認
python validate_parsed_data.py
```

### 4. 欠損フィールドが多い

**症状:**
```
  欠損フィールド (上位15件):
    • distance_m                       :  2,234 件
```

**原因:**
- HTMLの構造が予期しない形式
- 古い形式のレースデータ
- 地方競馬など異なるフォーマット

**対処法:**
- `modules/parsers/results_parser.py` でフォールバック処理を追加
- `DEBUG_REPORT.md` を参照して既知の問題を確認

## JSONデータの活用

出力されたJSONファイルは、さらなる分析やダッシュボード作成に使用できます。

### Python での読み込み例

```python
import json
from pathlib import Path

# JSONファイルを読み込み
with open('keibaai/data/logs/analysis/log_analysis_20251118_120000.json', 'r') as f:
    data = json.load(f)

# 統計情報を取得
total_entries = data['metadata']['total_entries']
error_count = len(data['errors'])
warning_count = len(data['warnings'])

# スクレイピング成功率を計算
races_stats = data['scraping_stats']['races']
success_rate = races_stats['scraped'] / (races_stats['scraped'] + races_stats['failed']) * 100

print(f"Total entries: {total_entries:,}")
print(f"Errors: {error_count:,}")
print(f"Race scraping success rate: {success_rate:.2f}%")
```

### データ可視化例

```python
import pandas as pd
import matplotlib.pyplot as plt

# HTTP エラーの可視化
http_errors = data['scraping_stats']['http_errors']
df = pd.DataFrame(list(http_errors.items()), columns=['Status Code', 'Count'])

df.plot(kind='bar', x='Status Code', y='Count', title='HTTP Error Distribution')
plt.savefig('http_errors.png')
```

## 高度な使い方

### 1. 複数日のログを一括分析

```bash
#!/bin/bash
# analyze_logs_range.sh

for day in {01..30}; do
    log_dir="keibaai/data/logs/2025/11/${day}"
    if [ -d "$log_dir" ]; then
        echo "Analyzing $log_dir"
        python analyze_keiba_logs_advanced.py "$log_dir"
    fi
done
```

### 2. 異常検知

```python
# detect_anomalies.py
import json
import sys

def detect_anomalies(json_path):
    with open(json_path, 'r') as f:
        data = json.load(f)

    anomalies = []

    # エラー率が5%以上
    total = data['metadata']['total_entries']
    error_count = data['error_count']
    if error_count / total > 0.05:
        anomalies.append(f"High error rate: {error_count/total*100:.2f}%")

    # パース成功率が95%未満
    for dtype in ['races', 'shutuba', 'horses', 'pedigrees']:
        stats = data['parsing_stats'][dtype]
        total = stats['parsed'] + stats['failed']
        if total > 0:
            success_rate = stats['parsed'] / total
            if success_rate < 0.95:
                anomalies.append(f"Low {dtype} parse rate: {success_rate*100:.2f}%")

    return anomalies

if __name__ == "__main__":
    anomalies = detect_anomalies(sys.argv[1])
    if anomalies:
        print("⚠️  Anomalies detected:")
        for a in anomalies:
            print(f"  - {a}")
    else:
        print("✅ No anomalies detected")
```

### 3. 定期的なモニタリング

```bash
# cron job example: 毎日深夜3時に分析
# 0 3 * * * /home/user/KeibaAI_v2/analyze_logs_daily.sh

#!/bin/bash
# analyze_logs_daily.sh

TODAY=$(date +%Y/%m/%d)
LOG_DIR="keibaai/data/logs/$TODAY"

cd /home/user/KeibaAI_v2

if [ -d "$LOG_DIR" ]; then
    python analyze_keiba_logs_advanced.py "$LOG_DIR"

    # 異常検知
    LATEST_JSON=$(ls -t keibaai/data/logs/analysis/*.json | head -1)
    python detect_anomalies.py "$LATEST_JSON"
else
    echo "No logs found for $TODAY"
fi
```

## トラブルシューティング

### メモリ不足エラー

大量のログファイルを処理する際にメモリ不足になる場合：

```python
# メモリ効率的な処理（ストリーミング処理）
# TODO: 必要に応じて実装
```

### 文字エンコーディングエラー

```python
# ログファイルのエンコーディングを確認
file keibaai/data/logs/2025/11/18/*.log

# エンコーディングを指定して読み込み
with open(log_file, 'r', encoding='utf-8', errors='replace') as f:
    # ...
```

## 参考資料

- **CLAUDE.md**: プロジェクト全体のガイド
- **schema.md**: データスキーマ定義
- **DEBUG_REPORT.md**: パーサー改善レポート
- **PROGRESS.md**: データ品質の進捗

## サポート

問題が発生した場合：

1. エラーメッセージを確認
2. `DEBUG_REPORT.md` で既知の問題を確認
3. GitHubのIssueを検索
4. 新しいIssueを作成（エラーログを添付）

---

**最終更新**: 2025-11-18
