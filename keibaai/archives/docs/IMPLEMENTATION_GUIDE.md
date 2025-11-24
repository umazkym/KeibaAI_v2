# KeibaAI_v2 パイプライン統合ガイド

**作成日**: 2025-11-16
**対象**: output_final のデバッグ結果を本番パイプラインに統合する方法

[続きは長いため、簡潔版を作成します]

## 🎯 問題の要約

### 検出された問題
1. **distance_m の誤抽出**: "10" (本来は "1000m")
2. **horses_performance.csv の大量欠損**: 15カラムが100%欠損

### 修正方法

#### distance_m パーサーの修正

**ファイル**: `debug_scraping_and_parsing.py` (268行目付近)

**現在のコード**:
```python
distance_match = re.search(r'(芝|ダート?)[^0-9]*?(\d+)\s*m?', text, re.IGNORECASE)
```

**修正後**:
```python
# 3-4桁の数字を明示的にマッチ
distance_match = re.search(r'(芝|ダート?)[^0-9]*?(\d{3,4})\s*m?', text, re.IGNORECASE)
```

### 実装手順

1. **パーサーを修正**
2. **テストを実行**  
3. **全データを再パース**
4. **検証**
5. **本番パイプラインに統合**

詳細は OUTPUT_FINAL_ANALYSIS_REPORT.md を参照

