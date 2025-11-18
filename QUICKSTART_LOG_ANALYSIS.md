# ログ分析クイックスタートガイド

## 🚀 3ステップで始める

### 1. パイプラインを実行

```bash
# スクレイピングパイプライン実行
python keibaai/src/run_scraping_pipeline_with_args.py --from 2020-01-01 --to 2025-10-31

# パースパイプライン実行
python keibaai/src/run_parsing_pipeline_local.py
```

### 2. ログを分析

```bash
# 基本版（高速、シンプル）
python analyze_keiba_logs.py

# または拡張版（詳細、推奨）
python analyze_keiba_logs_advanced.py
```

### 3. レポートを確認

```bash
# 生成されたレポートを確認
ls -lh keibaai/data/logs/analysis/

# テキストレポートを表示
cat keibaai/data/logs/analysis/advanced_log_analysis_*.txt | less
```

## 📊 出力例

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

■ パフォーマンス統計
  • scraping            :   2.45 時間 (  8,820.0 秒)
  • parsing             :   0.82 時間 (  2,952.0 秒)
```

## 🔍 特定の日付を分析

```bash
# 2025年11月18日のログを分析
python analyze_keiba_logs_advanced.py keibaai/data/logs/2025/11/18

# 別の日付
python analyze_keiba_logs_advanced.py keibaai/data/logs/2025/10/15
```

## 📈 JSONデータを活用

```python
import json

# JSONレポートを読み込み
with open('keibaai/data/logs/analysis/advanced_log_analysis_20251118_100144.json', 'r') as f:
    data = json.load(f)

# 統計情報を取得
print(f"Total entries: {data['metadata']['total_entries']:,}")
print(f"Errors: {data['error_count']:,}")
print(f"Warnings: {data['warning_count']:,}")

# スクレイピング成功率
races = data['scraping_stats']['races']
total = races['scraped'] + races['failed'] + races['skipped']
success_rate = races['scraped'] / total * 100 if total > 0 else 0
print(f"Race scraping success rate: {success_rate:.2f}%")
```

## 🛠️ トラブルシューティング

### ログファイルが見つからない

```bash
# ログディレクトリを確認
ls -la keibaai/data/logs/

# 最新のログを探す
find keibaai/data/logs -type f -name "*.log" -mtime -1
```

### エラーが多い場合

```bash
# エラーのみを抽出
grep ERROR keibaai/data/logs/2025/11/18/*.log

# 警告も含めて確認
grep -E "ERROR|WARNING" keibaai/data/logs/2025/11/18/*.log
```

## 📚 詳細ドキュメント

より詳しい情報は `LOG_ANALYZER_README.md` を参照してください。

---

**問題がある場合**: GitHubのIssueを作成するか、ログファイルとエラーメッセージを添えて報告してください。
