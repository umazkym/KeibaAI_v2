#!/bin/bash
# クリーンな特徴量で学習期間全体を再生成

echo "=========================================="
echo "クリーンな特徴量の再生成"
echo "=========================================="

# 2020-2023年の特徴量を再生成（ターゲット変数除外版）
echo "1. 2020年を再生成中..."
python keibaai/src/features/generate_features.py --start_date 2020-01-01 --end_date 2020-12-31

echo "2. 2021年を再生成中..."
python keibaai/src/features/generate_features.py --start_date 2021-01-01 --end_date 2021-12-31

echo "3. 2022年を再生成中..."
python keibaai/src/features/generate_features.py --start_date 2022-01-01 --end_date 2022-12-31

echo "4. 2023年を再生成中..."
python keibaai/src/features/generate_features.py --start_date 2023-01-01 --end_date 2023-12-31

echo ""
echo "=========================================="
echo "特徴量再生成完了"
echo "=========================================="
echo ""
echo "次のステップ:"
echo "python keibaai/src/models/train_mu_model.py --start_date 2020-01-01 --end_date 2023-12-31"
