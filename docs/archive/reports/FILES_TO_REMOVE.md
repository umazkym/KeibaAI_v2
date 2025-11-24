# KeibaAI_v2 不要ファイルリスト

**作成日**: 2025-11-16
**目的**: GitHubリポジトリおよびローカルから削除すべき不要なファイルを明記

---

## 📊 現状サマリー

- **Git追跡中のファイル総数**: 203個
- **Git追跡中のPythonファイル**: 86個
- **不要と思われるファイル**: **約80-100個**
- **削除可能な容量**: **約20-25 MB**

---

## 🔴 確実に削除すべきファイル（GitHubとローカル両方から）

### 1. データファイル（keibaai/data/配下）- 合計53個

#### ❌ Parquetファイル（特徴量データ）- 6個
```
keibaai/data/features/parquet/year=2023/month=5/01ae3ea71ebf4b268f9c885d66de6ca2-0.parquet
keibaai/data/features/parquet/year=2023/month=5/049d0348fad442c99c47e73d63df73d8-0.parquet
keibaai/data/features/parquet/year=2023/month=5/203879e3a9404fb1834ecad73511b0a2-0.parquet
keibaai/data/features/parquet/year=2023/month=5/207741414ecf4dc689eba1bb29b5ecf5-0.parquet
keibaai/data/features/parquet/year=2023/month=5/29fe37a458f5435b9abf1f595ce8de1f-0.parquet
keibaai/data/features/parquet/year=2023/month=5/2b3df8bad8a14a2bb5345eb201a9be8e-0.parquet
```
**理由**: 生成可能なデータ。再生成すべき。

#### ❌ ログファイル - 6個
```
keibaai/data/logs/2025/11/11/parsing.log
keibaai/data/logs/2025/11/11/pipeline.log
keibaai/data/logs/2025/11/12/parsing.log
keibaai/data/logs/2025/11/12/pipeline.log
keibaai/data/logs/2025/11/13/parsing.log
keibaai/data/logs/2025/11/13/pipeline.log
keibaai/data/logs/2025/11/14/parsing.log
keibaai/data/logs/2025/11/14/pipeline.log
```
**理由**: 一時的なログ。保持不要。**サイズ**: 不明（通常数KB-数MB）

#### ❌ データベースファイル - 1個（18MB）
```
keibaai/data/metadata/db.sqlite3
```
**理由**: **18MB**の大容量。生成可能。スキーマのみをSQLダンプで保存すべき。
**重要**: これが最も大きなファイル

#### ❌ モデルファイル - 3個（175KB）
```
keibaai/data/models/mu_model_v1/feature_names.json
keibaai/data/models/mu_model_v1/ranker.pkl
keibaai/data/models/mu_model_v1/regressor.pkl
```
**理由**: 訓練済みモデル。再訓練可能。バージョン管理不要。

#### ❌ JRAオッズJSONファイル - 約30個
```
keibaai/data/raw/json/jra_odds/.tmp_od1g091u2023010501_snapshot_20251111T102126+09
keibaai/data/raw/json/jra_odds/2023010501_snapshot_20251111T102201+0900_sha256=49bfe8d5.json
keibaai/data/raw/json/jra_odds/2023010501_snapshot_20251111T102255+0900_sha256=c5730716.json
keibaai/data/raw/json/jra_odds/2023010501_snapshot_20251111T102324+0900_sha256=ec3bfcf5.json
keibaai/data/raw/json/jra_odds/2023010501_snapshot_20251111T102358+0900_sha256=a421c741.json
keibaai/data/raw/json/jra_odds/2023010501_snapshot_20251111T102452+0900_sha256=da11d854.json
keibaai/data/raw/json/jra_odds/2023010502_snapshot_20251111T102255+0900_sha256=828fcde1.json
keibaai/data/raw/json/jra_odds/2023010502_snapshot_20251111T102324+0900_sha256=f66fd434.json
keibaai/data/raw/json/jra_odds/2023010502_snapshot_20251111T102358+0900_sha256=06a9edd1.json
keibaai/data/raw/json/jra_odds/2023010502_snapshot_20251111T102452+0900_sha256=a5c112d3.json
... （その他約20個）
```
**理由**: 一時的なスナップショットデータ。生データは保持不要。

---

### 2. デバッグ・分析スクリプト（ルートディレクトリ）- 約26個

#### ❌ デバッグスクリプト（debug_*.py）- 7個
```
debug_full_pipeline_by_date.py           (21KB)
debug_full_pipeline_comprehensive.py     (23KB)
debug_missing_distance.py                (5.1KB)
debug_parse_from_bins.py                 (6.9KB)
debug_regex_live.py                      (4KB)
debug_scraping_and_parsing.py            (25KB)
```
**理由**: 一時的なデバッグ目的。開発完了後は不要。

#### ❌ 分析スクリプト（analyze_*.py）- 4個
```
analyze_multiple_bins.py                 (13KB)
analyze_output_20231009.py               (3.1KB)
analyze_output_final.py                  (14KB)
analyze_output_simple.py                 (14KB)
```
**理由**: 特定日時のデータ分析。再利用性低い。

#### ❌ テストスクリプト（test_*.py）- 3個
```
test_distance_parser.py                  (5.5KB)
test_distance_parser_v2.py               (3.9KB)
test_regex_patterns.py                   (5.4KB)
```
**理由**: ルートに配置すべきでない。keibaai/tests/に統合すべき。

#### ❌ 検証スクリプト（verify_*, validate_*, check_*, inspect_*）- 9個
```
verify_output_final.py                   (7.2KB)
validate_new_parquet.py                  (6.8KB)
validate_parquet.py                      (7.1KB)
validate_parsed_data.py                  (8.7KB)
check_bin_files.py                       (2.4KB)
check_features_data.py                   (7.1KB)
check_parsed_data.py                     (9.6KB)
inspect_parquet_data.py                  (2.6KB)
inspect_pedigrees.py                     (1.9KB)
```
**理由**: 一部は有用だが、ルートに配置すべきでない。keibaai/scripts/に移動すべき。

#### ❌ その他スクリプト - 1個
```
apply_distance_parser_fix.sh             (1.1KB)
```
**理由**: 一時的な修正用シェルスクリプト。修正完了後は不要。

---

### 3. 生成データファイル（CSV/HTML/TXT）- 3個

```
debug_scraped_data.csv                   (144KB)
debug_race_list.html                     (45KB)
distance_analysis.txt                    (672バイト)
```
**理由**: デバッグ時に生成された一時ファイル。保持不要。

---

### 4. 出力ディレクトリ - 2ディレクトリ（合計1MB）

#### ❌ output_final/ - 4個（364KB）
```
output_final/horses.csv                  (162KB)
output_final/horses_performance.csv      (56KB)
output_final/race_results.csv            (102KB)
output_final/shutuba.csv                 (39KB)
```
**理由**: 一時的な検証用出力。生成可能。

#### ❌ test/test_output/ - 4個（推定100-200KB）
```
test/test_output/horses.csv
test/test_output/horses_performance.csv
test/test_output/races.csv
test/test_output/shutuba.csv
```
**理由**: テスト実行時の出力。.gitignoreで除外すべき。

#### ⚠️ test/*.bin - 5個（推定500KB）
```
test/202001010101.bin
test/2009100502_profile.bin
test/2009100502_perf.bin
test/2009100502.bin
test/202001010102.bin
```
**判断保留**: テスト用のサンプルHTMLファイル。
- **保持する場合**: テストフィクスチャとして有用（ただし、keibaai/tests/fixtures/に移動）
- **削除する場合**: .binファイルは大きい可能性（要確認）

---

### 5. 重複・冗長なREADMEファイル（ルート）- 16個

```
ANALYSIS_REPORT_LATEST.md
CRITICAL_FIXES_20251116.md
DEBUG_FULL_PIPELINE_README.md
DEBUG_MISSING_DISTANCE_README.md
DEBUG_PARSE_FROM_BINS_README.md
DEBUG_REPORT.md
DEBUG_SCRAPING_ANALYSIS_REPORT.md
DISTANCE_PARSER_FIX.md
FINAL_SUMMARY.md
IMPLEMENTATION_GUIDE.md
MULTIPLE_BINS_README.md
OUTPUT_FINAL_ANALYSIS_REPORT.md
OUTPUT_VERIFICATION_GUIDE.md
PARSER_TEST_README.md
PIPELINE_VERIFICATION_REPORT.md
SCRAPING_GUIDE.md
SPECIFICATION_UPDATE_PROPOSAL.md
```

**理由**:
- 開発過程で作成された一時的なレポート
- 情報が重複している
- CLAUDE.mdに統合済みの内容が多い

**推奨**:
- 重要な情報は CLAUDE.md に統合済み
- 歴史的記録として残したい場合は、`docs/archive/` ディレクトリを作成して移動
- 完全に削除しても問題ない

---

## 🟡 保持すべきファイル（削除しないこと）

### ✅ 必須ドキュメント - 4個
```
CLAUDE.md                                # プロジェクトの完全ガイド
PROGRESS.md                              # データ品質追跡
schema.md                                # データスキーマ定義
指示.md                                  # 日本語仕様書
AI競馬予測・最適投資システム — 完全仕様書（詳細実装版）.txt
```

### ✅ Git設定ファイル
```
.gitignore
.gitignore.recommended
```

### ✅ プロジェクトコア（keibaai/配下）
```
keibaai/src/             # ソースコード
keibaai/configs/         # YAML設定ファイル
keibaai/tests/           # テストコード（正規の場所）
keibaai/scripts/         # ユーティリティスクリプト
keibaai/README.md
keibaai/requirements.txt
```

### ✅ YAMLファイル（設定のみ）
```
keibaai/data/features/feature_names.yaml
keibaai/data/features/parquet/feature_names.yaml
```
**理由**: 設定ファイルとして有用。データファイルではない。

---

## 📋 削除手順（推奨）

### ステップ1: GitHubからの削除（履歴からも完全削除）

以下のファイル/ディレクトリをGit履歴から完全削除：

```bash
# 1. 大容量ファイルを優先的に削除
git rm -r keibaai/data/metadata/db.sqlite3
git rm -r keibaai/data/features/parquet/year=2023/
git rm -r keibaai/data/models/mu_model_v1/
git rm -r keibaai/data/logs/
git rm -r keibaai/data/raw/json/jra_odds/

# 2. 出力ディレクトリ
git rm -r output_final/
git rm -r test/test_output/

# 3. デバッグスクリプト（ルート）
git rm debug_*.py
git rm analyze_*.py
git rm test_*.py
git rm verify_*.py validate_*.py check_*.py inspect_*.py
git rm apply_distance_parser_fix.sh

# 4. 生成データファイル
git rm debug_scraped_data.csv debug_race_list.html distance_analysis.txt

# 5. 冗長なREADMEファイル（選択的に削除）
# 完全削除する場合:
git rm ANALYSIS_REPORT_LATEST.md CRITICAL_FIXES_20251116.md \
       DEBUG_FULL_PIPELINE_README.md DEBUG_MISSING_DISTANCE_README.md \
       DEBUG_PARSE_FROM_BINS_README.md DEBUG_REPORT.md \
       DEBUG_SCRAPING_ANALYSIS_REPORT.md DISTANCE_PARSER_FIX.md \
       FINAL_SUMMARY.md IMPLEMENTATION_GUIDE.md \
       MULTIPLE_BINS_README.md OUTPUT_FINAL_ANALYSIS_REPORT.md \
       OUTPUT_VERIFICATION_GUIDE.md PARSER_TEST_README.md \
       PIPELINE_VERIFICATION_REPORT.md SCRAPING_GUIDE.md \
       SPECIFICATION_UPDATE_PROPOSAL.md

# アーカイブする場合:
mkdir -p docs/archive
git mv ANALYSIS_REPORT_LATEST.md docs/archive/
git mv CRITICAL_FIXES_20251116.md docs/archive/
# ... 以下同様

# 6. コミットとプッシュ
git commit -m "chore: Remove unnecessary debug files, generated data, and temporary outputs

- Remove 18MB database file (regeneratable)
- Remove parquet features and model files (regeneratable)
- Remove debug/analysis scripts from root
- Remove temporary output directories
- Archive historical documentation to docs/archive/

Total size reduction: ~20-25 MB
Files removed: ~80-100 files"

git push -u origin claude/setup-gitignore-01H7kx8HDmjfCVWHdQURUi7W
```

### ステップ2: Git履歴からの完全削除（オプション）

**注意**: 以下は履歴改変になるため、慎重に実行してください。

```bash
# BFG Repo-Cleanerを使用（推奨）
# https://rtyley.github.io/bfg-repo-cleaner/

# 1. BFGのダウンロード
wget https://repo1.maven.org/maven2/com/madgag/bfg/1.14.0/bfg-1.14.0.jar

# 2. 大容量ファイルの削除
java -jar bfg-1.14.0.jar --delete-files db.sqlite3
java -jar bfg-1.14.0.jar --delete-folders "{data,output_final,test/test_output}"

# 3. 履歴のクリーンアップ
git reflog expire --expire=now --all
git gc --prune=now --aggressive

# 4. 強制プッシュ
git push origin --force --all
```

**または git filter-branch を使用:**

```bash
# metadata/db.sqlite3を履歴から完全削除
git filter-branch --force --index-filter \
  'git rm --cached --ignore-unmatch keibaai/data/metadata/db.sqlite3' \
  --prune-empty --tag-name-filter cat -- --all

# dataディレクトリ全体を履歴から削除
git filter-branch --force --index-filter \
  'git rm -r --cached --ignore-unmatch keibaai/data/features/parquet keibaai/data/logs keibaai/data/models keibaai/data/raw/json' \
  --prune-empty --tag-name-filter cat -- --all

# クリーンアップ
git reflog expire --expire=now --all
git gc --prune=now --aggressive

# 強制プッシュ
git push origin --force --all
```

### ステップ3: ローカルからの削除

GitHubから削除後、ローカルでも不要なファイルを削除：

```bash
# 既にgit rmしたファイルはローカルからも削除される
# 追加で.gitignoreに記載されているファイルを削除したい場合:

# output_final/が残っている場合
rm -rf output_final/

# test/test_output/が残っている場合
rm -rf test/test_output/

# その他の一時ファイル
rm -f debug_scraped_data.csv debug_race_list.html distance_analysis.txt
```

---

## 🎯 削除後の期待される効果

| 項目 | Before | After | 削減量 |
|------|--------|-------|--------|
| **Git追跡ファイル数** | 203個 | 100-120個 | **80-100個削減** |
| **リポジトリサイズ** | 約25-30 MB | 5-10 MB | **20 MB削減** |
| **ルートディレクトリ** | 雑多 | クリーン | **可読性向上** |
| **data/ディレクトリ** | 53ファイル追跡 | 2-3ファイルのみ | **50ファイル削減** |

---

## ⚠️ 重要な注意事項

### 1. バックアップの作成

削除前に必ずバックアップを取ってください：

```bash
# リポジトリ全体のバックアップ
cd /home/user
tar -czf KeibaAI_v2_backup_$(date +%Y%m%d).tar.gz KeibaAI_v2/

# 特定のファイルのバックアップ
mkdir -p KeibaAI_v2_backup/data
cp -r KeibaAI_v2/keibaai/data/metadata/db.sqlite3 KeibaAI_v2_backup/data/
cp -r KeibaAI_v2/keibaai/data/models/ KeibaAI_v2_backup/data/
```

### 2. データベースのスキーマ保存

db.sqlite3を削除する前に、スキーマをエクスポート：

```bash
sqlite3 keibaai/data/metadata/db.sqlite3 .schema > keibaai/data/metadata/schema.sql
git add keibaai/data/metadata/schema.sql
git commit -m "docs: Export database schema before removing db.sqlite3"
```

### 3. .gitkeepファイルの配置

削除後、ディレクトリ構造を保持：

```bash
# 必要なディレクトリに.gitkeepを配置
touch keibaai/data/metadata/.gitkeep
touch keibaai/data/models/.gitkeep
touch keibaai/data/features/parquet/.gitkeep
touch keibaai/data/logs/.gitkeep
touch keibaai/data/raw/json/jra_odds/.gitkeep

git add keibaai/data/**/.gitkeep
git commit -m "chore: Add .gitkeep files to preserve directory structure"
```

### 4. チーム共有の場合

もしチームで開発している場合、履歴改変は全メンバーに影響します：

```bash
# 他のメンバーは以下を実行する必要あり:
git fetch origin
git reset --hard origin/main  # またはブランチ名
git pull origin main --rebase
```

---

## 📊 削除優先度リスト

### 🔴 最優先（即座に削除推奨）

1. **keibaai/data/metadata/db.sqlite3** (18MB)
2. **keibaai/data/features/parquet/** (Parquetファイル)
3. **keibaai/data/models/mu_model_v1/** (モデルファイル)
4. **output_final/** (364KB)
5. **debug_scraped_data.csv** (144KB)

**削減効果**: 約19-20 MB

### 🟡 中優先（整理推奨）

6. **keibaai/data/logs/** (ログファイル)
7. **keibaai/data/raw/json/jra_odds/** (JSONファイル)
8. **test/test_output/** (テスト出力)
9. **ルートのdebug_*.py、analyze_*.py** (デバッグスクリプト)

**削減効果**: 約2-3 MB

### 🟢 低優先（任意）

10. **冗長なREADME/REPORTファイル** (アーカイブまたは削除)
11. **test/*.bin** (テストフィクスチャ、移動を検討)

**削減効果**: 約1-2 MB

---

## ✅ 最終確認チェックリスト

削除前に以下を確認してください：

- [ ] バックアップを作成した
- [ ] db.sqlite3のスキーマをエクスポートした
- [ ] .gitignoreが正しく設定されている
- [ ] .gitkeepファイルを配置した
- [ ] 削除するファイルリストを再確認した
- [ ] チームメンバー（いる場合）に通知した
- [ ] ブランチで作業している（mainブランチでの直接作業を避ける）

---

**このファイルは削除ガイドとして保存してください。実際の削除はこの内容を確認後、慎重に実行してください。**
