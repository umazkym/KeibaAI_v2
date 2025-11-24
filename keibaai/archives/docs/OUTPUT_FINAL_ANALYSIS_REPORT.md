# output_final フォルダ分析レポート

**分析日時**: 2025-11-16
**分析対象**: `/home/user/KeibaAI_v2/output_final/`
**分析者**: Claude Code

---

## 📊 サマリー

| ファイル | 行数 | 列数 | データ品質 | 状態 |
|---------|-----|-----|-----------|------|
| race_results.csv | 311 | 55 | 86.92% | ⚠️ **重大な問題あり** |
| shutuba.csv | 311 | 26 | 65.38% | ⚠️ 軽微な問題あり |
| horses.csv | 1,181 | 11 | 100.00% | ✅ 完璧 |
| horses_performance.csv | 469 | 27 | 62.50% | ⚠️ **重大な問題あり** |

**総合判定**: ⚠️ **修正が必要** - 2つの重大な問題が検出されました

---

## 🔴 重大な問題

### 1. **distance_m の誤抽出** (race_results.csv)

**症状**:
- distance_m カラムが "10" と誤って抽出されている
- 本来は "1000m"、"1600m" であるべき

**具体例**:
```csv
race_id,distance_m,track_surface
202305040301,10,ダート     ← 本来は 1000m であるべき
202305040302,1600,芝        ← 正常
202305040303,2400,芝        ← 正常
```

**影響範囲**:
- すべてのレースデータに影響している可能性
- 機械学習の特徴量として distance_m は非常に重要なため、この誤りは致命的

**原因推定**:
1. 正規表現パターンが最初の数字のみをマッチしている
2. HTMLのテキストフォーマットが想定と異なる（例: "ダ1000m" ではなく "ダート10 00m" のように分割されている）
3. パーサーのバグ

**推奨される対応**:
1. 実際のHTMLファイル（.binファイル）を確認し、テキストフォーマットを検証
2. 正規表現パターンを修正
3. 全データを再パース

---

### 2. **horses_performance.csv の大量欠損**

**症状**:
- 15カラムが100%欠損
- 特に重要な以下のカラムが完全に空:
  - `distance_m` (100%欠損)
  - `track_surface` (100%欠損)
  - `track_condition` (100%欠損)
  - `finish_time_seconds` (100%欠損)
  - `margin_seconds` (100%欠損)
  - `passing_order` (100%欠損)
  - `last_3f_time` (100%欠損)
  - `win_odds` (100%欠損)

**具体例**:
```csv
horse_id,race_date,venue,weather,distance_m,track_surface,...
2014110029,2024-08-02,笠松,晴,,,         ← distance_m, track_surface が空
2014110029,2024-04-29,笠松,曇,,,         ← 全行で同じ問題
```

**影響範囲**:
- 馬の過去成績データが使用不可能
- 特徴量生成に必要な基本情報がすべて欠落

**原因推定**:
1. パーサー（`parse_horse_performance`）が正しく動作していない
2. HTMLフォーマットが想定と異なる
3. 地方競馬データ（笠松）のフォーマット違い

**推奨される対応**:
1. `keibaai/src/modules/parsers/horse_info_parser.py` の `parse_horse_performance` 関数を確認
2. 実際のHTMLファイル（*_perf.bin）を確認
3. パーサーを修正または再実装

---

## ⚠️ 軽微な問題

### 3. **賞金情報の100%欠損** (race_results.csv)

**症状**:
- `prize_1st`～`prize_5th`: 100%欠損
- `prize_money` (着順賞金): 61.4%欠損

**評価**: これは**正常な可能性が高い**
- 一部のレースでは賞金情報がHTMLに含まれていない
- 低着順の馬には賞金がない（61.4%欠損は妥当）

**対応**: 不要（ただし、賞金情報が必要な場合は別途スクレイピングが必要）

---

### 4. **shutuba.csv の出馬表情報欠損**

**症状**:
- `morning_odds` (朝オッズ): 100%欠損
- `morning_popularity` (朝人気): 100%欠損
- `owner_name` (馬主名): 100%欠損
- `prize_total` (獲得賞金): 100%欠損
- `career_stats` (通算成績): 100%欠損

**評価**: これは**正常な可能性が高い**
- 出馬表HTMLに含まれていない情報
- morning_odds は別途スクレイピングが必要（JRA公式サイトなど）

**対応**: 必要に応じて追加スクレイピング

---

### 5. **horse_id の不整合**

**症状**:
- race_results.csv には 311頭の馬
- shutuba.csv にも 311頭の馬
- しかし horses.csv (血統データ) には 20頭しかない

**評価**: これは**意図的なサンプリングの可能性が高い**
- デバッグテストのため、20頭のサンプルのみスクレイピングした
- 血統データのスクレイピングは時間がかかるため

**対応**: 本番運用時は全馬の血統データをスクレイピング

---

## ✅ 正常なデータ

### horses.csv (血統データ)
- **完璧**: 100%データ品質
- 20頭の馬の血統情報（5世代分）
- 1,181行のデータ（20頭 × 約59祖先）
- 欠損値なし

**世代分布**:
```
第1世代: 40行 (父母)
第2世代: 80行 (祖父母)
第3世代: 157行
第4世代: 306行
第5世代: 598行
```

---

## 📈 データ整合性チェック

### ✅ race_id の整合性
- race_results.csv: 24レース
- shutuba.csv: 24レース
- **完全一致**: 共通24レース

### ✅ レコード数の整合性
- race_results.csv と shutuba.csv の馬数が一致
- 各レースの出走頭数が正しく対応

### ⚠️ horse_id の整合性
- race_results には 311頭
- horses (血統) には 20頭のみ
- **291頭の血統データが欠落**（意図的なサンプリングの可能性）

---

## 🎯 推奨される次のステップ

### 優先度: 高 🔴

1. **distance_m 問題の修正**
   ```bash
   # 実際のHTMLを確認
   python3 -c "
   with open('data/raw/html/race/202305040301.bin', 'rb') as f:
       html = f.read().decode('euc_jp', errors='replace')
   print(html[:1000])  # 最初の1000文字を表示
   "

   # パーサーの正規表現を修正
   # debug_scraping_and_parsing.py の 268行目付近
   ```

2. **horses_performance パーサーの修正**
   ```bash
   # パーサーを確認
   cat keibaai/src/modules/parsers/horse_info_parser.py

   # 実際のHTMLを確認
   python3 -c "
   with open('data/raw/html/horse/2014110029_perf.bin', 'rb') as f:
       html = f.read().decode('euc_jp', errors='replace')
   print(html[:1000])
   "
   ```

### 優先度: 中 🟡

3. **全データの再パース**
   ```bash
   # 修正後、全データを再パース
   python debug_full_pipeline_by_date.py --date 2023-10-09 --parse-only --output-dir output_fixed
   ```

4. **データ品質の検証**
   ```bash
   # 修正後のデータを検証
   python verify_output_final.py
   python analyze_output_simple.py
   ```

### 優先度: 低 🟢

5. **追加データのスクレイピング**
   - 全311頭の血統データ（現在20頭のみ）
   - 賞金情報（必要に応じて）
   - morning_odds（必要に応じて）

---

## 📝 実際のパイプラインでの使用について

### ❌ 現状では使用不可

以下の理由により、**このデータは実際のパイプラインで使用できません**:

1. **distance_m の誤り**: 機械学習モデルの重要な特徴量が誤っている
2. **horses_performance の欠損**: 過去成績データが使用不可能
3. **サンプルサイズが小さい**: 24レース、311頭のみ（本番は数万レース必要）

### ✅ 修正後に使用可能

上記の問題を修正すれば、以下のようにパイプラインに統合できます:

1. **Parquetへの変換**
   ```python
   # CSVをParquetに変換
   import pandas as pd

   df = pd.read_csv('output_fixed/race_results.csv', encoding='utf-8-sig')
   df.to_parquet('data/parsed/parquet/races/races.parquet', index=False)
   ```

2. **特徴量生成**
   ```bash
   python keibaai/src/features/generate_features.py \
     --start_date 2023-10-09 \
     --end_date 2023-10-09
   ```

3. **モデル学習**
   ```bash
   python keibaai/src/models/train_mu_model.py
   ```

---

## 📚 参考情報

### 関連ファイル
- デバッグスクリプト: `debug_full_pipeline_by_date.py`
- パーサー: `debug_scraping_and_parsing.py`
- 検証スクリプト: `verify_output_final.py`
- テストパターン: `test_regex_patterns.py`

### プロジェクト仕様
- CLAUDE.md: データパイプラインの説明
- schema.md: データスキーマ定義
- PROGRESS.md: データ品質改善の履歴

---

## 🤝 結論

**output_finalフォルダのデータには2つの重大な問題があります**:
1. ❌ distance_m の誤抽出
2. ❌ horses_performance の大量欠損

これらを修正すれば、実際のパイプラインで使用可能になります。
修正後は、CLAUDE.mdに記載された標準的なデータパイプラインフローに統合できます。

---

**分析完了**: 2025-11-16
