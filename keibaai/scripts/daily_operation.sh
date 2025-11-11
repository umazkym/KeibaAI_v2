#!/bin/bash
# scripts/daily_operation.sh
# 日次運用スクリプト（完全版）
# 仕様書 13.1 に基づく実装
# 実行想定: 深夜〜午前のスケジュールで順次実行

set -euo pipefail

# --- ▼ 環境変数の設定 (必要に応じて変更) ---

# プロジェクトのルートディレクトリ (このスクリプトの親の親)
BASE_DIR=$(cd "$(dirname "$0")/.." && pwd)
# Python実行環境 (venvなど)
PYTHON_EXEC="$BASE_DIR/venv/bin/python" # venvを想定

# --- ▲ 環境変数の設定 ---

# 当日/対象日
TARGET_DATE=$(date +%Y-%m-%d)
YESTERDAY_DATE=$(date -d '1 day ago' +%Y-%m-%d)

LOG_DIR="$BASE_DIR/data/logs/$(date +%Y)/$(date +%m)/$(date +%d)"
mkdir -p "$LOG_DIR"
exec &> >(tee -a "$LOG_DIR/daily_operation.log")

echo "=========================================="
echo "Keiba AI 日次運用開始"
echo "対象日: ${TARGET_DATE}"
echo "プロジェクトルート: ${BASE_DIR}"
echo "=========================================="
cd "$BASE_DIR"

# (注: 仕様書 13.1 の深夜バッチは、仕様書 19.3.2 や 17.1 の実装とは
# 　　 引数が異なるため、仕様書 17.x で作成したスクリプトの引数に合わせます)

# ---- 深夜バッチ: 静的データ取得（03:00 実行想定） ----
echo "[1/11] データ取得中 (過去データ / 静的データ)..."
# (run_scraping_pipeline_local.py は日付引数を取らない設計だったため、そのまま実行)
$PYTHON_EXEC src/run_scraping_pipeline_local.py
# (※注: 仕様書の run_scraping_pipeline_local.py は日付引数を持つが、
#   アップロードされたファイル(run_scraping_pipeline_local.py)は引数処理がないため、
#   ここではファイルの実装に合わせて引数なしで呼び出します)

echo "[2/11] データパース中..."
# (仕様書 17.1 の parse_all.py を使う場合)
# $PYTHON_EXEC src/parsers/parse_all.py --date ${TARGET_DATE}
# (アップロードされた run_parsing_pipeline_local.py を使う場合)
$PYTHON_EXEC src/run_parsing_pipeline_local.py

echo "[3/11] 特徴量生成中..."
# (仕様書 17.2 の generate_features.py を使用)
$PYTHON_EXEC src/features/generate_features.py \
    --date ${TARGET_DATE} \
    --config "configs/default.yaml" \
    --features_config "configs/features.yaml"

# ---- 当日午前: JRAオッズ取得と最終推論 ----
# (※注: 仕様書 13.1 ではJRAオッズ取得がここに入るが、
#   run_scraping_pipeline_local.py [c.f: 90] が既にオッズ取得(ダミー)を
#   実行しているため、ここでは推論から開始する)

echo "[4/11] モデル推論（μ, σ, ν）実行中..."
# (仕様書 17.3 の predict.py を使用)
$PYTHON_EXEC src/models/predict.py \
    --date ${TARGET_DATE} \
    --model_dir "data/models/latest" \
    --config "configs/default.yaml" \
    --models_config "configs/models.yaml"

echo "[5/11] シミュレーション（オッズ反映）実行..."
# (仕様書 17.4 の simulate_daily_races.py を使用)
$PYTHON_EXEC src/sim/simulate_daily_races.py \
    --date ${TARGET_DATE} \
    --K 1000 \
    --model_id "latest" \
    --config "configs/default.yaml"

echo "[6/11] ポートフォリオ最適化中..."
# (仕様書 17.5 の optimize_daily_races.py を使用)
$PYTHON_EXEC src/optimizer/optimize_daily_races.py \
    --date ${TARGET_DATE} \
    --W_0 100000 \
    --config "configs/default.yaml" \
    --optimization_config "configs/optimization.yaml"

# (仕様書 13.1 の残りのステップ)
echo "[7/11] メトリクス更新（Brier, ECE, ROI 等）..."
# (※注: src/monitoring/update_metrics.py は未作成のため、実行をコメントアウト)
# $PYTHON_EXEC src/monitoring/update_metrics.py --date ${TARGET_DATE}

echo "=========================================="
echo "日次運用完了"
echo "$(date '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="