# 🔍 デバッグ・リカバリー実行ガイド

**作成日**: 2025-11-18
**目的**: スクレイピングエラー・パースエラーの原因特定とリカバリー

---

## 📋 目次

1. [概要](#概要)
2. [実行手順](#実行手順)
3. [各スクリプトの詳細](#各スクリプトの詳細)
4. [トラブルシューティング](#トラブルシューティング)
5. [リカバリー手順](#リカバリー手順)

---

## 📌 概要

### 作成されたデバッグツール

| スクリプト | 目的 | 出力 |
|-----------|------|------|
| `debug_html_files_analysis.py` | HTMLファイルの統計分析 | ファイルサイズ、年別分布、異常ファイル検出 |
| `debug_parse_errors_analysis.py` | パースエラーの詳細分析 | エラータイプ、失敗ファイル、エラーメッセージ |
| `recovery_failed_scraping.py` | 失敗ファイルのリカバリー | 失敗ファイルリスト、削除・再取得対象 |

---

## 🚀 実行手順

### ステップ1: HTMLファイルの統計分析

まず、HTMLファイルの状態を確認します。

```powershell
# PowerShellで実行
cd C:\Users\zk-ht\Keiba\Keiba_AI_v2

python debug_html_files_analysis.py
```

**出力内容**:
- 年別のファイル数とサイズ統計
- 空ファイルの検出
- 異常に小さいファイル（<1KB）の検出
- 2025年と正常ファイルのサンプル内容比較

**出力ファイル**:
- `keibaai/data/logs/analysis/html_analysis_YYYYMMDD_HHMMSS.json`

**確認ポイント**:
- ✅ 2025年のファイル数
- ✅ 空ファイルの数
- ✅ ファイルサイズの分布

---

### ステップ2: パースエラーの詳細分析

次に、実際にパーサーを実行してエラーを特定します。

#### 2-1. 2025年のファイルを分析

```powershell
# 2025年のレース結果ファイルのみ分析（最大100件）
python debug_parse_errors_analysis.py --year 2025 --max-files 100 --race
```

**出力内容**:
- パース成功/失敗の件数
- エラータイプ別の集計
- エラーが発生したファイルの詳細
- ファイルの内容プレビュー

**出力ファイル**:
- `keibaai/data/logs/analysis/parse_errors_2025_YYYYMMDD_HHMMSS.json`

#### 2-2. 正常な年（2023年など）も確認

```powershell
# 比較のため、正常な2023年も分析
python debug_parse_errors_analysis.py --year 2023 --max-files 50 --race
```

**確認ポイント**:
- ✅ 2025年と2023年のエラー率の違い
- ✅ エラーメッセージの内容
- ✅ ファイル内容の違い

---

### ステップ3: 結果の提示

**以下の出力を提示してください**：

1. **HTMLファイル分析の結果**:
   ```powershell
   # ターミナルの出力全体をコピー
   python debug_html_files_analysis.py
   ```

2. **2025年のパースエラー分析**:
   ```powershell
   # ターミナルの出力全体をコピー
   python debug_parse_errors_analysis.py --year 2025 --max-files 50 --race
   ```

3. **JSONファイルの一部**:
   ```powershell
   # JSONファイルの最初の50行を確認
   Get-Content keibaai\data\logs\analysis\parse_errors_2025_*.json -Head 50
   ```

---

## 📖 各スクリプトの詳細

### 1. `debug_html_files_analysis.py`

#### 機能
- HTMLファイルの統計情報を収集
- 年別のファイル数とサイズ分析
- 空ファイル・異常ファイルの検出
- サンプルファイルの内容表示

#### 使用方法
```bash
python debug_html_files_analysis.py
```

#### オプションなし
すべてのHTMLファイルを自動分析します。

#### 出力例
```
📊 ファイル統計（年別）
================================================================================

■ RACE ファイル:
年         ファイル数       平均サイズ         最小         最大
--------------------------------------------------------------------------------
2020           5,234       45.2 KB      12.3 KB      89.5 KB
2023           6,543       46.8 KB      13.1 KB      91.2 KB
2025           2,727        0.5 KB       0 B        1.2 KB  ← 異常
--------------------------------------------------------------------------------
```

---

### 2. `debug_parse_errors_analysis.py`

#### 機能
- 実際にパーサーを実行してエラーを検出
- エラータイプの分類
- エラーファイルの詳細情報収集
- ファイル内容のプレビュー

#### 使用方法
```bash
# 基本的な使用方法
python debug_parse_errors_analysis.py

# 特定の年のみ分析
python debug_parse_errors_analysis.py --year 2025

# 最大分析件数を指定
python debug_parse_errors_analysis.py --year 2025 --max-files 50

# レース結果のみ分析
python debug_parse_errors_analysis.py --year 2025 --race

# 出馬表のみ分析
python debug_parse_errors_analysis.py --year 2025 --shutuba
```

#### 出力例
```
📄 レース結果ファイルのパース分析
================================================================================
🔍 2025年のファイルに絞り込み: 2,727件
⚠️  ファイル数が多いため、最初の50件のみ分析します
📊 分析対象: 50件

  [1/50] 202501010201... ❌ empty_dataframe
  [2/50] 202501010202... ❌ AttributeError
  [3/50] 202501010203... ❌ empty_dataframe
  ...

--------------------------------------------------------------------------------
✅ 成功: 0件
❌ 失敗: 50件

📊 エラーサマリー
================================================================================
■ エラータイプ別集計:
  エラータイプ                        件数
  ------------------------------------------
  empty_dataframe                      35
  AttributeError                       10
  IndexError                            5
```

---

### 3. `recovery_failed_scraping.py`

#### 機能
- 失敗したファイルの検出とリスト化
- 再取得対象のrace_id/horse_idを抽出
- 失敗ファイルの削除（DRY RUN対応）

#### 使用方法
```bash
# 失敗ファイルを検出してリストを生成
python recovery_failed_scraping.py --detect

# 削除のDRY RUN（実際には削除しない）
python recovery_failed_scraping.py --detect --delete --dry-run

# 実際に削除（注意！）
python recovery_failed_scraping.py --detect --delete

# 最小ファイルサイズを指定（デフォルト: 1024 bytes）
python recovery_failed_scraping.py --detect --min-size 2048
```

#### 出力例
```
🔍 失敗ファイルの検出中...
================================================================================

📁 raceディレクトリをチェック中...
   空ファイル: 1,523件
   小さすぎるファイル（<1024B）: 1,204件

📁 shutubaディレクトリをチェック中...
   空ファイル: 1,523件
   小さすぎるファイル（<1024B）: 1,204件

================================================================================
📊 検出結果:
   空ファイル: 3,046件
   小さすぎるファイル: 2,408件
   合計: 5,454件

📝 リカバリーリストを生成中...
💾 リカバリーリストを保存: keibaai/data/logs/recovery/recovery_list_20251118_120000.json
   レースID: 2,727件
   馬ID: 0件
```

---

## 🔧 トラブルシューティング

### エラー1: `ModuleNotFoundError: No module named 'src'`

**原因**: プロジェクトルートのパス設定が間違っている

**解決策**:
```powershell
# プロジェクトルートで実行していることを確認
cd C:\Users\zk-ht\Keiba\Keiba_AI_v2
python debug_html_files_analysis.py
```

---

### エラー2: `FileNotFoundError: [Errno 2] No such file or directory`

**原因**: データディレクトリが見つからない

**解決策**:
```powershell
# データディレクトリの存在確認
Test-Path keibaai\data\raw\html\race

# 存在しない場合は、データパスを確認
ls keibaai\data
```

---

### エラー3: 文字化けが発生する

**原因**: Windowsのコンソールエンコーディング

**解決策**:
```powershell
# PowerShellのエンコーディングをUTF-8に設定
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$PSDefaultParameterValues['*:Encoding'] = 'utf8'

# 再度実行
python debug_html_files_analysis.py
```

---

## 🔄 リカバリー手順

### フェーズ1: 原因特定（現在のフェーズ）

1. ✅ HTMLファイル分析の実行
2. ✅ パースエラー分析の実行
3. ⏳ **結果をClaude（AI）に提示**
4. ⏳ 原因の確定

---

### フェーズ2: 失敗ファイルのクリーンアップ（原因確定後）

```powershell
# 1. 失敗ファイルを検出
python recovery_failed_scraping.py --detect

# 2. 削除のDRY RUN（確認）
python recovery_failed_scraping.py --detect --delete --dry-run

# 3. 問題なければ実際に削除
python recovery_failed_scraping.py --detect --delete
```

---

### フェーズ3: 再スクレイピング（24-48時間待機後）

```powershell
# リカバリーリストから再取得対象を確認
Get-Content keibaai\data\logs\recovery\recovery_list_*.json

# 修正版スクレイピングスクリプトで再実行（後日作成）
# python run_scraping_recovery.py --recovery-list keibaai\data\logs\recovery\recovery_list_*.json
```

---

## 📊 期待される分析結果

### シナリオ1: 2025年のファイルが空または不完全

**症状**:
- 2025年のファイルサイズが極端に小さい（<1KB）
- パース時に`empty_dataframe`エラー

**原因**:
- IP BANにより、スクレイピングが途中で停止
- 空または不完全なHTMLファイルが保存された

**対策**:
1. 2025年のファイルをすべて削除
2. スリープ時間を延長（5-15秒）
3. 24-48時間待機後、再スクレイピング

---

### シナリオ2: HTMLフォーマットの変更

**症状**:
- 特定の年・月のファイルのみエラー
- `AttributeError`や`IndexError`が多発

**原因**:
- netkeibaのHTML構造が変更された
- パーサーのセレクタがミスマッチ

**対策**:
1. サンプルHTMLの内容を確認
2. パーサーのセレクタを修正
3. フォールバック処理を追加

---

### シナリオ3: 日付範囲の指定ミス

**症状**:
- 未来の日付（2025年）のファイルが大量に存在
- すべてエラー

**原因**:
- スクレイピング時に終了日を誤指定（2025-10-31）
- 存在しないレースのURLにアクセス

**対策**:
1. 未来の日付のファイルを削除
2. 日付バリデーション機能を追加
3. 正しい日付範囲で再実行

---

## ✅ チェックリスト

実行前に確認：

- [ ] プロジェクトルートで実行している
- [ ] PowerShellのエンコーディングをUTF-8に設定
- [ ] `keibaai/data`ディレクトリが存在する
- [ ] 十分なディスク容量がある

実行後に提示する情報：

- [ ] `debug_html_files_analysis.py`の出力全体
- [ ] `debug_parse_errors_analysis.py --year 2025`の出力
- [ ] JSONファイルの最初の50行
- [ ] 気づいた点・疑問点

---

## 📞 次のステップ

**現在**: デバッグスクリプトの実行と結果の提示

**次回**:
1. 提示された結果をもとに原因を確定
2. 適切な修正・リカバリー手順を策定
3. スクレイピング設定の改善
4. 再スクレイピングの実行

---

**作成者**: Claude
**最終更新**: 2025-11-18
