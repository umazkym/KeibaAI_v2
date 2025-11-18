# スクレイピング問題デバッグガイド

**作成日**: 2025-11-18
**対象**: KeibaAI_v2 スクレイピングエラーの調査と修正

---

## 📋 検出された問題

### 1. **HTTP 400 エラー (CRITICAL)**

```
HTTP 400 Error. IP BANの可能性: https://race.netkeiba.com/top/calendar.html?year=2020&month=1
```

**症状**:
- カレンダーURL取得時に初回リクエストで即座に400エラー
- その後60秒間の待機が発生

**考えられる原因**:
1. ❌ **IP BAN** - 可能性は低い（初回リクエストで発生するため）
2. ✅ **リクエスト形式の問題** - URLパラメータやヘッダーが不適切
3. ✅ **短すぎるリクエスト間隔** - 前回の実行からの時間が短い
4. ✅ **netkeibaサイトの仕様変更** - URLやパラメータ形式が変更された可能性

**調査ステップ**:
```bash
# ステップ1: URLとリクエストの確認
python debug_scraping_issues.py
```

このスクリプトは以下を実行します：
- テスト1: URL定数の確認
- テスト2: カレンダーURLへのシンプルなリクエスト
- テスト3: 待機時間を設けた複数リクエスト

---

### 2. **2,727件のパースエラー**

```
パースエラー (results_parser): ...race/2025*.bin
```

**症状**:
- 特に2025年のレース結果ファイルでパースエラーが多発
- 17,430件のレース結果のうち、2,727件（約13.5%）が失敗

**考えられる原因**:
1. ✅ **未実施レース** - 2025年の未来のレースをスクレイピングしている
2. ✅ **HTML構造の変更** - netkeibaのページ構造が変更された
3. ✅ **404エラーページ** - 存在しないレースIDを取得している
4. ❌ **エンコーディング問題** - 可能性は低い（他の年は成功している）

**調査ステップ**:
```bash
# ステップ2: パースエラーの詳細分析
python debug_parse_errors.py
```

このスクリプトは以下を実行します：
- 2025年のレースファイルを詳細分析
- HTML構造の確認
- エラーパターンの分類
- 全年度のランダムサンプリング

---

### 3. **日付抽出の失敗 (411件の警告)**

```
HTMLから日付を抽出できませんでした (race_id: 202310010201)。Noneを返します。
```

**症状**:
- 202310010201など、2023年10月のレースで日付抽出失敗
- 411件の警告

**考えられる原因**:
1. ✅ **HTML構造のバリエーション** - 異なるHTMLフォーマット
2. ✅ **地方競馬のレース** - 中央競馬と異なる構造
3. ✅ **特殊なレース** - 障害競走など特殊なレース形式

**対応**:
- パーサーの fallback パターンを追加（CLAUDE.mdに記載あり）
- 地方競馬用の専用パーサーを実装

---

### 4. **日付範囲のハードコード問題**

**場所**: `keibaai/src/run_scraping_pipeline_local.py:71-72`

```python
# ❌ 現在の実装（ハードコード）
from_date = "2023-01-01"
to_date = "2023-01-31"
```

**問題**:
- ユーザーが `2020-01-01 ～ 2025-10-31` を指定しても無視される
- コマンドライン引数が実装されていない

**解決策**:
新しいスクリプト `run_scraping_with_config.py` を使用
```bash
python run_scraping_with_config.py --from-date 2020-01-01 --to-date 2025-10-31
```

---

### 5. **ログファイルの文字化け**

**症状**:
```
繧ｹ繧ｯ繝ｬ繧､繝斐Φ繧ｰ繝代う繝励Λ繧､繝ｳ・・bin蠖｢蠑擾ｼ峨ｒ髢句ｧ・
```

**原因**:
- ログファイルはUTF-8で書き込まれている
- Windowsのコンソールがデフォルトで**Shift-JIS**または**CP932**を使用
- `type`コマンドで表示する際にエンコーディングが一致していない

**対応**:
1. PowerShellでUTF-8として読み込む:
```powershell
Get-Content keibaai\data\logs\2025\11\18\scraping.log -Encoding UTF8
```

2. または、デバッグスクリプトのログを確認:
```bash
# 新しいログは正しく表示される
cat debug_scraping.log
```

---

## 🔧 デバッグ実行手順

### **ステップ1: 基本的な動作確認**

```bash
# デバッグスクリプトを実行
python debug_scraping_issues.py
```

**期待される出力**:
- ✅ URL定数が正しく表示される
- ✅ カレンダーURLからHTMLが取得できる（ステータス200）
- ✅ または、HTTP 400エラーの詳細が表示される

**次のアクション**:
- HTTP 200が返れば → ステップ2へ
- HTTP 400が返れば → 以下を確認
  - 前回のスクレイピングからの経過時間（24時間以上空ける）
  - netkeibaサイトが正常かブラウザで確認
  - リクエスト間隔を増やす（`MIN_SLEEP_SECONDS = 5.0` など）

---

### **ステップ2: パースエラーの詳細分析**

```bash
# パースエラーを詳細分析
python debug_parse_errors.py
```

**期待される出力**:
- 2025年のファイル分析結果
- エラーパターンの分類
  - "404 エラーページ" → 存在しないレース
  - "レース情報が見つかりません" → 未実施レース
  - "該当するデータがありません" → 中止されたレース

**次のアクション**:
- 未実施レースが多い → 日付範囲を調整（未来のレースを除外）
- HTML構造が異なる → パーサーの改善が必要

---

### **ステップ3: 段階的なスクレイピング実行**

```bash
# フェーズ0a（開催日とレースID取得）のみ実行
python run_scraping_with_config.py \
  --from-date 2023-10-01 \
  --to-date 2023-10-31 \
  --phase 0a
```

**確認ポイント**:
- HTTP 400エラーが発生しないか
- 開催日が正しく取得できるか
- レースIDが適切に取得できるか

**成功したら次へ**:
```bash
# フェーズ0b（レース結果と出馬表）を実行
python run_scraping_with_config.py \
  --from-date 2023-10-01 \
  --to-date 2023-10-31 \
  --phase 0b
```

**確認ポイント**:
- .binファイルが正しく保存されるか
- ファイルサイズが適切か（0バイトでないか）

---

### **ステップ4: パース実行**

```bash
# パースパイプラインを実行
python keibaai/src/run_parsing_pipeline_local.py
```

**確認ポイント**:
- パースエラーの数と割合
- 2025年のファイルでエラーが多い → 未来のレースをスキップする処理を追加

---

## 🚀 改善提案

### **1. リクエスト間隔の調整**

現在の設定（`_scrape_html.py:39-40`）:
```python
MIN_SLEEP_SECONDS = 2.5
MAX_SLEEP_SECONDS = 5.0
```

**推奨設定**:
```python
MIN_SLEEP_SECONDS = 5.0   # 5秒に増加
MAX_SLEEP_SECONDS = 10.0  # 10秒に増加
```

**メリット**:
- IP BANのリスクを大幅に削減
- サーバー負荷の軽減
- より安定したスクレイピング

**デメリット**:
- 実行時間が約2倍に増加
- しかし、再実行のコストを考えると許容範囲

---

### **2. 既存ファイルのスキップ**

現在の実装では、既存ファイルをチェックしていない可能性があります。

**改善案**:
```python
def scrape_html_race(race_ids: List[str], skip_existing: bool = True):
    for race_id in race_ids:
        file_path = Path(f"data/raw/html/race/{race_id}.bin")

        if skip_existing and file_path.exists():
            logger.debug(f"スキップ（既存）: {race_id}")
            continue

        # スクレイピング実行
        ...
```

**メリット**:
- 中断からの再開が容易
- 不要なリクエストを削減
- 実行時間の短縮

---

### **3. 未実施レースの検出とスキップ**

パース時に未実施レースを検出してスキップする:

```python
def parse_race_results(file_path: Path):
    # HTML読み込み
    soup = BeautifulSoup(html_text, "html.parser")

    # 未実施レースのチェック
    if "レース情報が見つかりません" in html_text:
        logger.info(f"未実施レース: {file_path.name}")
        return pd.DataFrame()  # 空のDataFrameを返す（エラーではない）

    # 通常のパース処理
    ...
```

**メリット**:
- エラーログが減少（真のエラーだけが記録される）
- パース成功率の向上

---

### **4. 日付範囲の自動調整**

未来の日付を自動的に除外:

```python
def scrape_kaisai_date(from_: str, to_: str):
    from datetime import datetime

    # 未来の日付を除外
    today = datetime.now().date()
    to_date = min(pd.to_datetime(to_).date(), today)

    logger.info(f"調整後の終了日: {to_date}")
    ...
```

**メリット**:
- ユーザーが未来の日付を指定してもエラーにならない
- 不要なリクエストを削減

---

### **5. エラーログの改善**

現在のログレベル:
```python
logger.critical(f"HTTP 400 Error. IP BANの可能性: {url}")
```

**改善案**:
```python
logger.error(f"HTTP 400 Error: {url}")
logger.error(f"レスポンス内容: {response.text[:500]}")
logger.info("60秒間待機します...")
```

**メリット**:
- CRITICALレベルは本当に深刻な問題だけに使用
- デバッグに役立つ詳細情報を記録
- ユーザーに状況を明確に伝える

---

## 📊 期待される改善効果

### **Before (現在)**

| 指標 | 値 |
|-----|-----|
| HTTP 400エラー | 1回（初回で発生） |
| パースエラー率 | 13.5% (2,727/20,157) |
| 警告（日付抽出失敗） | 411件 |
| ログ文字化け | あり |

### **After (改善後)**

| 指標 | 目標値 |
|-----|--------|
| HTTP 400エラー | 0回（またはリトライで成功） |
| パースエラー率 | 3%以下（未実施レースを除外） |
| 警告（日付抽出失敗） | 50件以下（fallback実装） |
| ログ文字化け | なし（UTF-8で表示） |

---

## 🔄 次回実行時の推奨手順

### **安全な実行計画**

```bash
# 1. 小さな期間でテスト（1ヶ月）
python run_scraping_with_config.py \
  --from-date 2023-10-01 \
  --to-date 2023-10-31 \
  --phase all

# 2. パース実行
python keibaai/src/run_parsing_pipeline_local.py

# 3. 結果を検証
python debug_parse_errors.py

# 4. 成功したら期間を拡大（3ヶ月）
python run_scraping_with_config.py \
  --from-date 2023-10-01 \
  --to-date 2023-12-31 \
  --phase all

# 5. 最終的に全期間（ただし未来の日付を除外）
python run_scraping_with_config.py \
  --from-date 2020-01-01 \
  --to-date 2025-11-18 \  # 今日の日付まで
  --phase all
```

### **リクエスト間隔の調整**

```bash
# _scrape_html.py を編集
# MIN_SLEEP_SECONDS = 5.0
# MAX_SLEEP_SECONDS = 10.0
```

### **24時間ルール**

- スクレイピングは**24時間に1回**まで
- 複数回実行する場合は、少なくとも**24時間以上**間隔を空ける
- 緊急時でも**最低12時間**は空ける

---

## 📝 実行ログの確認方法

### **UTF-8でログを表示（PowerShell）**

```powershell
# 最新のログファイル
Get-Content keibaai\data\logs\2025\11\18\scraping.log -Encoding UTF8 | Select-Object -First 30
```

### **デバッグログの確認**

```bash
# デバッグスクリプトのログ
cat debug_scraping.log
cat debug_parse_errors.log
```

### **ログ分析ツール**

```bash
# 既存の分析ツールを使用
python analyze_keiba_logs_fixed.py keibaai/data/logs/2025/11/18
```

---

## ✅ チェックリスト

### **デバッグ実行前**

- [ ] 前回のスクレイピングから24時間以上経過している
- [ ] netkeibaサイトがブラウザで正常にアクセスできる
- [ ] リクエスト間隔を適切に設定している（5-10秒）
- [ ] 日付範囲が妥当（未来の日付を含まない）

### **デバッグ実行中**

- [ ] `debug_scraping_issues.py` を実行
- [ ] HTTP 200が返ることを確認
- [ ] `debug_parse_errors.py` を実行
- [ ] パースエラーのパターンを分析

### **本実行前**

- [ ] テスト期間（1ヶ月）で成功している
- [ ] パースエラー率が許容範囲（5%以下）
- [ ] 既存ファイルスキップが有効
- [ ] ログが正しく記録されている

### **本実行中**

- [ ] 定期的にログを確認（HTTP 400が発生していないか）
- [ ] ディスク容量を監視
- [ ] 中断時のためにrace_id_list.csvを保存

### **本実行後**

- [ ] パース実行
- [ ] エラー率を確認
- [ ] データ品質を検証（validate_parquet.py）
- [ ] PROGRESS.mdを更新

---

## 🆘 トラブルシューティング

### **Q1: HTTP 400エラーが続く場合**

**A1**: 以下を試してください：
1. 24時間以上待つ
2. リクエスト間隔を15-20秒に増やす
3. ブラウザでURLを確認（手動でアクセスできるか）
4. netkeibaのrobots.txtを確認
5. 必要に応じてVPNを使用（慎重に）

### **Q2: パースエラーが50%以上の場合**

**A2**: 以下を確認：
1. HTMLファイルが正しく保存されているか（0バイトでないか）
2. エンコーディングが正しいか
3. netkeibaのHTML構造が変わっていないか
4. パーサーのfallbackパターンが十分か

### **Q3: Seleniumがエラーになる場合**

**A3**: ChromeDriverの確認：
```bash
# ChromeDriverの更新
pip install --upgrade webdriver-manager
```

### **Q4: メモリ不足エラー**

**A4**: バッチサイズを小さくする：
```bash
# 月ごとに分割して実行
for month in {1..12}; do
  python run_scraping_with_config.py \
    --from-date 2023-$(printf "%02d" $month)-01 \
    --to-date 2023-$(printf "%02d" $month)-31
done
```

---

## 📚 参考ドキュメント

- `CLAUDE.md` - プロジェクト全体の理解
- `schema.md` - データスキーマ
- `PROGRESS.md` - データ品質の進捗
- `DEBUG_REPORT.md` - パーサー改善履歴

---

**作成者**: Claude Code
**バージョン**: 1.0
**最終更新**: 2025-11-18
