# スクレイピングガイド - 5年分のデータ取得

**作成日**: 2025-11-16
**対象**: KeibaAI_v2 データスクレイピング
**目的**: 2020-2024年の5年分データを効率的にスクレイピング

---

## ✅ 実装されている最適化機能

### 1. **既存ファイルの自動スキップ** ⭐⭐⭐

すべてのスクレイピング関数に`skip=True`（デフォルト）が実装されています：

```python
scrape_html_race(race_id_list, skip=True)      # 既存のレース結果をスキップ
scrape_html_shutuba(race_id_list, skip=True)   # 既存の出馬表をスキップ
scrape_html_horse(horse_id_list, skip=True, cache_ttl_days=7)  # 7日間キャッシュ
scrape_html_ped(horse_id_list, skip=True)      # 既存の血統データをスキップ
```

**効果**:
- 既にデータがあるレースは**スキップ**されます
- スクレイピング時間を**大幅に短縮**
- サーバー負荷を**最小化**

### 2. **Anti-ban対策**

```python
MIN_SLEEP_SECONDS = 2.5      # 最小待機時間
MAX_SLEEP_SECONDS = 5.0      # 最大待機時間
HTTP_400_SLEEP_SECONDS = 60  # IP BAN検出時は60秒待機
```

- ランダムsleep（2.5-5秒）
- HTTP 400エラー検出 → 60秒pause
- ランダムUser-Agent
- Retry with exponential backoff

### 3. **馬データのキャッシュ戦略**

```python
cache_ttl_days = 7  # 馬データは7日間キャッシュ
```

馬のプロフィールや過去成績は変更が少ないため、7日間はキャッシュを使用します。

---

## 📊 所要時間の試算

### ケース1: 既存データあり（2020-2024）→ 新規データのみ取得

```
既存: 2020.1-2024.10（スキップ）
新規: 2024.11-2024.12のみスクレイピング

- レース数: 約60日 × 10レース = 600レース
- 時間: 600レース × 4秒 = 40分
- 新規馬データ: 約500頭 × 3件 × 4秒 = 100分
- 合計: 約2.5時間
```

### ケース2: データなし → 5年分を全スクレイピング

```
- レース数: 約18,000レース
- 時間: 18,000レース × 4秒 = 20時間
- 馬データ: 約50,000頭 × 3件 × 4秒 = 167時間
- 合計: 約187時間（7.8日）

※ただし、既存データがある場合は自動的にスキップされます
```

### ケース3: 段階的スクレイピング（推奨）

```
年ごとに分割して実行:
- 2020年: 約1.5日
- 2021年: 約1.5日
- 2022年: 約1.5日
- 2023年: 約1.5日
- 2024年: 約1.5日

メリット:
- 途中でエラーが発生しても、完了した年のデータは保持される
- 各年ごとにデータ品質を検証できる
- サーバー負荷を分散できる
```

---

## 🚀 使用方法

### 新規作成: `run_scraping_pipeline_with_args.py`

コマンドライン引数で日付範囲を指定できるスクリプトを作成しました。

**ファイル**: `keibaai/src/run_scraping_pipeline_with_args.py`

### 基本的な使用方法

#### 1. 特定期間をスクレイピング（既存データはスキップ）

```bash
python keibaai/src/run_scraping_pipeline_with_args.py \
    --from 2024-01-01 \
    --to 2024-12-31
```

#### 2. 5年分を一括スクレイピング（既存データはスキップ）

```bash
python keibaai/src/run_scraping_pipeline_with_args.py \
    --from 2020-01-01 \
    --to 2024-12-31
```

**注意**: 既にデータがある場合、自動的にスキップされるため、実際の所要時間は**大幅に短縮**されます。

#### 3. 既存データを強制上書き（再スクレイピング）

```bash
python keibaai/src/run_scraping_pipeline_with_args.py \
    --from 2024-01-01 \
    --to 2024-01-31 \
    --no-skip
```

#### 4. 年ごとに段階的スクレイピング（推奨）⭐⭐⭐

```bash
# 2020年
python keibaai/src/run_scraping_pipeline_with_args.py \
    --from 2020-01-01 --to 2020-12-31

# 2021年
python keibaai/src/run_scraping_pipeline_with_args.py \
    --from 2021-01-01 --to 2021-12-31

# 2022年
python keibaai/src/run_scraping_pipeline_with_args.py \
    --from 2022-01-01 --to 2022-12-31

# 2023年
python keibaai/src/run_scraping_pipeline_with_args.py \
    --from 2023-01-01 --to 2023-12-31

# 2024年
python keibaai/src/run_scraping_pipeline_with_args.py \
    --from 2024-01-01 --to 2024-12-31
```

**メリット**:
- エラー発生時の影響が限定的
- 進捗状況を把握しやすい
- 各年ごとにデータ品質を検証可能

---

## 📝 実行前の確認事項

### 1. Python環境の確認

```bash
python --version  # Python 3.10+ 推奨
pip list | grep -E "pandas|requests|beautifulsoup4|selenium"
```

必要なパッケージ:
- pandas
- requests
- beautifulsoup4
- selenium
- webdriver-manager
- pyyaml

### 2. ディスク容量の確認

```bash
df -h keibaai/data/

# 必要容量の目安:
# - 5年分のHTMLデータ: 約10-20GB
# - パース済みParquetデータ: 約2-5GB
# - 合計: 約15-25GB
```

### 3. 既存データの確認

```bash
# 既存のレースデータ数を確認
find keibaai/data/raw/html/race -name "*.bin" -o -name "*.html" | wc -l

# 既存の馬データ数を確認
find keibaai/data/raw/html/horse -name "*.bin" -o -name "*.html" | wc -l

# 既存の血統データ数を確認
find keibaai/data/raw/html/ped -name "*.bin" -o -name "*.html" | wc -l
```

---

## 🔍 進捗状況の監視

### ログファイルの確認

```bash
# リアルタイムでログを監視
tail -f keibaai/data/logs/$(date +%Y)/ $(date +%m)/$(date +%d)/scraping.log

# ログの要約を表示
grep -E "取得しました|エラー|完了" keibaai/data/logs/*/scraping.log
```

### スクレイピング速度の確認

```bash
# 取得したファイル数をカウント
watch -n 60 'find keibaai/data/raw/html/race -name "*.bin" | wc -l'
```

---

## ⚠️ トラブルシューティング

### 問題1: HTTP 400エラー（IP BAN）

```
エラーメッセージ:
HTTP 400 Error. IP BANの可能性: https://db.netkeiba.com/race/...
```

**対処法**:
1. スクリプトを**一時停止**（60秒自動待機）
2. 待機後、自動的に再開されます
3. 頻繁に発生する場合は、`MIN_SLEEP_SECONDS`を増やす

### 問題2: Selenium TimeoutException

```
エラーメッセージ:
TimeoutException: レースリスト取得エラー
```

**対処法**:
1. ChromeDriverのバージョンを確認
2. `SELENIUM_WAIT_TIMEOUT`を増やす（デフォルト30秒）
3. ヘッドレスモードを無効化して目視確認

### 問題3: ディスク容量不足

```
エラーメッセージ:
OSError: [Errno 28] No space left on device
```

**対処法**:
1. 不要なファイルを削除
2. 日付範囲を分割して実行
3. 外部ストレージを使用

---

## 🎯 推奨実行スケジュール

### パターンA: 段階的スクレイピング（最推奨）⭐⭐⭐

```bash
# Day 1: 2020年
nohup python keibaai/src/run_scraping_pipeline_with_args.py \
    --from 2020-01-01 --to 2020-12-31 > scrape_2020.log 2>&1 &

# Day 2: 2021年
nohup python keibaai/src/run_scraping_pipeline_with_args.py \
    --from 2021-01-01 --to 2021-12-31 > scrape_2021.log 2>&1 &

# ... (以下同様)
```

**メリット**:
- 安全（エラー時の影響が限定的）
- 進捗把握が容易
- 各年ごとに検証可能

### パターンB: 一括スクレイピング（リスク高）

```bash
# 5年分を一括実行（約7.8日間）
nohup python keibaai/src/run_scraping_pipeline_with_args.py \
    --from 2020-01-01 --to 2024-12-31 > scrape_all.log 2>&1 &
```

**注意**:
- 途中でエラーが発生すると、完了済みのデータも失われる可能性
- 長時間実行のため、サーバー再起動などのリスクあり

### パターンC: 月次スクレイピング（超安全）

```bash
# 各月ごとに実行（60回に分割）
for year in {2020..2024}; do
    for month in {01..12}; do
        python keibaai/src/run_scraping_pipeline_with_args.py \
            --from ${year}-${month}-01 \
            --to ${year}-${month}-31
        sleep 60  # 月ごとに1分休憩
    done
done
```

---

## 📊 スクレイピング後の検証

### データ品質チェック

```bash
# パースを実行
python keibaai/src/run_parsing_pipeline_local.py

# Parquetファイルの検証
python verify_output_final.py  # 作成済みのスクリプトを使用
```

### 欠損データの確認

```python
import pandas as pd

# レース結果の検証
df_races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
print(f"総レース数: {len(df_races)}")
print(f"distance_m欠損率: {df_races['distance_m'].isna().sum() / len(df_races) * 100:.2f}%")
print(f"track_surface欠損率: {df_races['track_surface'].isna().sum() / len(df_races) * 100:.2f}%")
```

---

## 🔧 設定のカスタマイズ

### sleep時間の調整

`keibaai/src/modules/preparing/_scrape_html.py`を編集：

```python
# より慎重にスクレイピング（IP BAN対策を強化）
MIN_SLEEP_SECONDS = 5.0   # デフォルト: 2.5
MAX_SLEEP_SECONDS = 10.0  # デフォルト: 5.0
```

### キャッシュ期間の調整

```python
# 馬データのキャッシュを30日間に延長
scrape_html_horse(horse_id_list, skip=True, cache_ttl_days=30)
```

---

## 💡 ベストプラクティス

### 1. **深夜帯に実行** ⭐

```bash
# crontabで深夜3時に実行（サーバー負荷が低い時間帯）
0 3 * * * cd /path/to/KeibaAI_v2 && python keibaai/src/run_scraping_pipeline_with_args.py --from 2024-01-01 --to 2024-12-31
```

CLAUDE.mdにも記載されています：
> **Deep Night Batch (03:00)** でスクレイピング

### 2. **段階的に実行** ⭐⭐

年ごと、または月ごとに分割して実行することで、エラーの影響を最小化します。

### 3. **定期的にバックアップ** ⭐

```bash
# データをバックアップ
tar -czf keibaai_data_backup_$(date +%Y%m%d).tar.gz keibaai/data/raw/html/
```

### 4. **ログを保存** ⭐

すべての実行結果をログに記録し、後で検証できるようにします。

---

## 📚 関連ドキュメント

- **CLAUDE.md**: プロジェクト全体のガイド
- **IMPLEMENTATION_GUIDE.md**: パイプライン実装方法
- **PIPELINE_VERIFICATION_REPORT.md**: データ品質検証レポート

---

## ❓ FAQ

### Q1: 既に5年分のデータがある場合、どうすればいい？

A: `--skip`オプション（デフォルト）で実行すれば、既存データは自動的にスキップされます。新規データのみがスクレイピングされます。

### Q2: 特定のレースだけ再スクレイピングしたい

A: `--no-skip`オプションで、該当期間を再スクレイピングできます。

### Q3: スクレイピング中にエラーが発生したら？

A: 既に取得済みのデータは保持されます。エラー箇所から再実行してください。

### Q4: IP BANされたらどうなる？

A: スクリプトがHTTP 400を検出すると、自動的に60秒待機します。その後、再開されます。

---

**作成者**: Claude (AI Assistant)
**更新日**: 2025-11-16
