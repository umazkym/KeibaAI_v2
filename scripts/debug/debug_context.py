#!/usr/bin/env python3
"""展開注釈デバッグ: RaceContextAnalyzerを直接テスト"""
import sys
sys.path.insert(0, '.')

print("=== Test 1: Import ===")
try:
    from keibaai.analysis.race_context_analyzer import RaceContextAnalyzer, batch_annotate_history
    print("  ✅ Import成功")
except ImportError as e:
    print(f"  ❌ Import失敗: {e}")
    sys.exit(1)

print("\n=== Test 2: Parquet race_id format ===")
analyzer = RaceContextAnalyzer()
analyzer._load_parquet()
sample_ids = analyzer._df['race_id'].head(5).tolist()
print(f"  parquet race_id samples: {sample_ids}")
print(f"  parquet race_id dtype: {analyzer._df['race_id'].dtype}")

# Scraped race_id from HTML
target_id = '202609010111'
print(f"\n  scraper race_id: '{target_id}'")
matches = analyzer._df[analyzer._df['race_id'] == target_id]
print(f"  parquetでのマッチ数: {len(matches)}")

if len(matches) == 0:
    # 部分マッチを試す
    print(f"  部分マッチ(starts_with):")
    partial = analyzer._df[analyzer._df['race_id'].str.startswith(target_id[:8])]
    print(f"    {target_id[:8]}* → {len(partial)} rows")
    if len(partial) > 0:
        print(f"    サンプル: {partial['race_id'].unique()[:5].tolist()}")

print("\n=== Test 3: batch_annotateテスト ===")
# 模擬history
test_history = [
    {
        'race_id': target_id,
        '4C': '3',
        '着順': '1',
        'タイム指数': '55.0',
        'T指数': '55.0',
    }
]
result = batch_annotate_history(analyzer, test_history)
print(f"  展開注釈: '{result[0].get('展開注釈', 'MISSING')}'")
print(f"  展開補正: '{result[0].get('展開補正', 'MISSING')}'")
print(f"  展開ラベル: '{result[0].get('展開ラベル', 'MISSING')}'")
print(f"  ペース: '{result[0].get('ペース', 'MISSING')}'")

# 実際のrace_idで試す
print("\n=== Test 4: 既知のrace_idでテスト ===")
known_id = sample_ids[0] if sample_ids else ''
print(f"  既知のrace_id: '{known_id}'")
known_runners = analyzer.get_race_runners(str(known_id))
print(f"  runners: {len(known_runners)}")
if len(known_runners) > 0:
    ctx = analyzer.analyze_race(str(known_id))
    print(f"  pace_type: {ctx.get('pace_type')}")
    print(f"  pace_index_avg: {ctx.get('pace_index_avg')}")
    print(f"  num_runners: {ctx.get('num_runners')}")
