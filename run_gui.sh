#!/bin/bash

# KeibaAI_v2 GUI Dashboard 起動スクリプト
# 使い方: ./run_gui.sh

echo "🐴 KeibaAI_v2 GUI Dashboard を起動します..."
echo ""

# プロジェクトルートに移動
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# 依存関係のチェック
echo "📦 依存関係をチェック中..."
if ! command -v streamlit &> /dev/null; then
    echo "❌ Streamlitがインストールされていません"
    echo "以下のコマンドでインストールしてください:"
    echo "  pip install -r keibaai/gui/requirements.txt"
    exit 1
fi

echo "✅ Streamlitが見つかりました"
echo ""

# Streamlitアプリを起動
echo "🚀 ダッシュボードを起動しています..."
echo "ブラウザで http://localhost:8501 にアクセスしてください"
echo ""
echo "終了するには Ctrl+C を押してください"
echo ""

streamlit run keibaai/gui/app.py \
    --server.port 8501 \
    --server.address localhost \
    --browser.gatherUsageStats false
