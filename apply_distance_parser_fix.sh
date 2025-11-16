#!/bin/bash
# 距離パーサーの修正を適用するスクリプト

set -e

echo "==================================="
echo "距離パーサー修正スクリプト"
echo "==================================="
echo ""

# バックアップ
echo "[1] バックアップを作成..."
cp debug_scraping_and_parsing.py debug_scraping_and_parsing.py.backup
echo "    ✓ debug_scraping_and_parsing.py.backup を作成"

# 修正を適用
echo ""
echo "[2] パターンを修正..."
echo "    修正前: (芝|ダート?)"
echo "    修正後: (芝|ダー?ト?)"

# sed で一括置換
sed -i 's/(芝|ダート?)/(芝|ダー?ト?)/g' debug_scraping_and_parsing.py

# 修正を確認
echo ""
echo "[3] 修正結果を確認..."
grep -n "芝|ダー?ト?" debug_scraping_and_parsing.py | head -5

echo ""
echo "✓ 修正完了！"
echo ""
echo "次のステップ:"
echo "  1. テストを実行: python test_distance_parser_v2.py"
echo "  2. 再パース: python debug_full_pipeline_by_date.py --date 2023-10-09 --parse-only --output-dir output_fixed"
echo "  3. 検証: python analyze_output_simple.py"

