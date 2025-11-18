# データ品質チェック・モデル分析ガイド

**最終更新**: 2025-11-18

このガイドでは、KeibaAI_v2プロジェクトにおける包括的なデータ品質チェックとモデル分析システムの使い方を説明します。

---

## 📋 目次

1. [概要](#概要)
2. [システム構成](#システム構成)
3. [基本的な使い方](#基本的な使い方)
4. [各モジュールの詳細](#各モジュールの詳細)
5. [自動化とスケジューリング](#自動化とスケジューリング)
6. [トラブルシューティング](#トラブルシューティング)

---

## 概要

### 実装された機能

✅ **包括的なバリデーションパイプライン**
- スクレイピングデータの品質チェック（404率、エンコーディングエラー）
- パースデータの整合性検証（スキーマ、型、論理チェック）
- 特徴量のデータリーク検出と欠損率チェック
- 自動レポート生成（JSON + Markdown）

✅ **リアルタイムモニタリングシステム**
- メトリクス収集（成功率、欠損率、実行時間）
- 異常検知（閾値ベース + 統計的検出）
- アラート機能（重要度別）
- ダッシュボード用データ生成

✅ **モデル分析・デバッグツール**
- 詳細な評価指標（Brier Score, ECE, ROI, Top-K Accuracy）
- セグメント別性能分析（距離別、馬場別、クラス別）
- エラー分析（予測失敗レースの特定）
- 特徴量重要度分析
- 具体的な改善提案の自動生成

✅ **統合実行スクリプト**
- ワンコマンドで全チェックを実行
- cron対応（定期自動実行）
- レポート自動生成

---

## システム構成

### ファイル構成

```
keibaai/
├── src/
│   └── modules/
│       ├── validation/
│       │   ├── __init__.py
│       │   └── validation_pipeline.py          # バリデーションパイプライン
│       └── monitoring/
│           ├── monitoring_local.py             # リアルタイムモニタリング
│           └── model_analyzer.py               # モデル分析ツール
├── scripts/
│   └── run_quality_checks.py                   # 統合実行スクリプト
└── docs/
    └── QUALITY_CHECK_GUIDE.md                  # このファイル
```

### データフロー

```
┌─────────────────────────────────────────────────────────────┐
│                  run_quality_checks.py                       │
│                  (統合実行スクリプト)                         │
└──────────────┬──────────────────────────────────────────────┘
               │
    ┌──────────┴──────────┬──────────────────┬─────────────────┐
    │                     │                  │                 │
    ▼                     ▼                  ▼                 ▼
┌────────────┐    ┌────────────┐    ┌────────────┐    ┌────────────┐
│ Validation │    │ Monitoring │    │   Model    │    │  Report    │
│  Pipeline  │    │   System   │    │  Analyzer  │    │ Generator  │
└─────┬──────┘    └─────┬──────┘    └─────┬──────┘    └─────┬──────┘
      │                 │                  │                 │
      ▼                 ▼                  ▼                 ▼
 ┌─────────┐      ┌─────────┐      ┌─────────┐      ┌─────────┐
 │   JSON  │      │   JSON  │      │   JSON  │      │Markdown │
 │Markdown │      │ Metrics │      │Markdown │      │ Summary │
 └─────────┘      └─────────┘      └─────────┘      └─────────┘
```

---

## 基本的な使い方

### 1. 全チェックを実行

最もシンプルな使い方：

```bash
python keibaai/scripts/run_quality_checks.py --config keibaai/configs/default.yaml
```

これにより以下が自動的に実行されます：
- スクレイピングデータの検証
- パースデータの検証
- 特徴量データの検証
- モニタリングメトリクスの収集
- レポート生成（JSON + Markdown）

**出力先**: `keibaai/data/quality_reports/YYYY/MM/DD/`

### 2. バリデーションのみ実行

```bash
python keibaai/scripts/run_quality_checks.py \
  --config keibaai/configs/default.yaml \
  --validation-only
```

### 3. モニタリングのみ実行

```bash
python keibaai/scripts/run_quality_checks.py \
  --config keibaai/configs/default.yaml \
  --monitoring-only
```

### 4. モデル分析のみ実行

モデルの性能を詳細に分析する場合：

```bash
python keibaai/scripts/run_quality_checks.py \
  --config keibaai/configs/default.yaml \
  --model-analysis-only \
  --model-dir keibaai/data/models/latest \
  --predictions keibaai/data/predictions/2023-10-01.parquet \
  --actuals keibaai/data/parsed/parquet/races/races.parquet
```

**必須引数**:
- `--model-dir`: モデルディレクトリのパス
- `--predictions`: 予測結果のParquetファイル
- `--actuals`: 実績データのParquetファイル

### 5. 日付範囲を指定して実行

特定期間のデータのみをチェック：

```bash
python keibaai/scripts/run_quality_checks.py \
  --config keibaai/configs/default.yaml \
  --start-date 2023-10-01 \
  --end-date 2023-10-31
```

### 6. 出力先を指定

```bash
python keibaai/scripts/run_quality_checks.py \
  --config keibaai/configs/default.yaml \
  --output-dir /path/to/custom/reports
```

---

## 各モジュールの詳細

### 1. ValidationPipeline

**場所**: `keibaai/src/modules/validation/validation_pipeline.py`

**機能**:
- スクレイピングデータの品質チェック
  - ファイル数とサイズの統計
  - 404エラー率の検出（1KB未満のファイル）
  - エンコーディングエラーの検出

- パースデータの整合性検証
  - スキーマチェック（必須カラムの存在確認）
  - 型チェック（数値カラムの型検証）
  - 論理整合性チェック（着順 <= 出走頭数など）
  - 欠損率の計算と警告

- 特徴量データの検証
  - データリークチェック（win_odds, place_odds等の禁止カラム検出）
  - 欠損率チェック
  - 無限値・NaNの検出

**使い方（Pythonコード）**:

```python
from pathlib import Path
from keibaai.src.modules.validation.validation_pipeline import ValidationPipeline

data_path = Path('keibaai/data')
config = {}  # 設定辞書

pipeline = ValidationPipeline(data_path, config)
summary = pipeline.validate_all(start_date='2023-10-01', end_date='2023-10-31')

# レポート保存
pipeline.save_report(Path('validation_report.json'), format='json')
pipeline.save_report(Path('validation_report.md'), format='markdown')
```

**出力例**:

```json
{
  "timestamp": "2023-11-18T15:30:00",
  "total_checks": 25,
  "passed": 20,
  "warnings": 4,
  "failed": 1,
  "results": [
    {
      "check_name": "scraping_race",
      "status": "pass",
      "message": "race のスクレイピングデータは正常",
      "details": {
        "file_count": 278098,
        "total_size_mb": 1250.5,
        "error_rate": 0.005
      }
    }
  ]
}
```

### 2. MonitoringSystem

**場所**: `keibaai/src/modules/monitoring/monitoring_local.py`

**機能**:
- リアルタイムメトリクス収集
  - スクレイピング成功率
  - パース成功率
  - 特徴量欠損率
  - モデル相関係数
  - 実行時間

- 異常検知
  - 閾値ベースの検知（設定可能）
  - 統計的異常検知（3σ法）
  - トレンド分析

- アラート機能
  - 重要度別アラート（info, warning, error, critical）
  - アラート履歴の保存

**使い方（Pythonコード）**:

```python
from pathlib import Path
from keibaai.src.modules.monitoring.monitoring_local import MonitoringSystem

data_path = Path('keibaai/data')
config = {}

monitoring = MonitoringSystem(data_path, config)

# メトリクスの記録
monitoring.record_metric('scraping_success_rate', 0.98, metadata={'category': 'race'})

# スクレイピングメトリクスの追跡
monitoring.track_scraping_metrics('2023-10-01', '2023-10-31')

# パースメトリクスの追跡
monitoring.track_parsing_metrics()

# メトリクスの保存
monitoring.save_metrics(Path('monitoring_metrics.json'))

# サマリの取得
summary = monitoring.get_summary()
print(summary)
```

**アラートの例**:

```json
{
  "timestamp": "2023-11-18T15:30:00",
  "severity": "warning",
  "message": "スクレイピング成功率が低下しています",
  "metric_name": "scraping_success_rate",
  "current_value": 0.92,
  "threshold": 0.95,
  "details": {}
}
```

### 3. ModelAnalyzer

**場所**: `keibaai/src/modules/monitoring/model_analyzer.py`

**機能**:
- 全体的な評価指標
  - スピアマン順位相関係数
  - Brier Score（確率キャリブレーション）
  - Expected Calibration Error (ECE)
  - Top-K Accuracy（K=1,3,5）
  - RMSE（タイム予測）
  - ROI（模擬投資収益率）

- セグメント別性能分析
  - 距離別（スプリント、マイル、中距離、長距離、超長距離）
  - 馬場別（芝、ダート）
  - クラス別（新馬、未勝利、1勝クラスなど）
  - 競馬場別
  - 天候別

- エラー分析
  - 予測精度の低いレースを特定
  - エラーの共通パターンを分析
  - 距離別・馬場別のエラー分布

- 特徴量重要度分析
  - 特徴量重要度の可視化
  - 上位10件の重要特徴量

- 改善提案の自動生成
  - 性能評価に基づく具体的な提案
  - セグメント別の改善ポイント
  - データ拡充の提案

**使い方（Pythonコード）**:

```python
from pathlib import Path
import pandas as pd
from keibaai.src.modules.monitoring.model_analyzer import ModelAnalyzer

model_path = Path('keibaai/data/models/latest')
data_path = Path('keibaai/data')

# データの読み込み
predictions_df = pd.read_parquet('predictions.parquet')
actuals_df = pd.read_parquet('actuals.parquet')

# 分析の実行
analyzer = ModelAnalyzer(model_path, data_path)
report = analyzer.analyze(predictions_df, actuals_df)

# レポート保存
analyzer.save_report(report, Path('model_analysis.json'))
```

**レポート例**:

```json
{
  "timestamp": "2023-11-18T15:30:00",
  "model_path": "keibaai/data/models/latest",
  "overall_metrics": {
    "spearman_mean": -0.52,
    "spearman_median": -0.55,
    "brier_score": 0.18,
    "ece": 0.08,
    "top_1_accuracy": 0.32,
    "top_3_accuracy": 0.68,
    "top_5_accuracy": 0.85,
    "roi": 5.2
  },
  "segment_analysis": {
    "distance": {
      "sprint": {"count": 5000, "spearman_mean": -0.48},
      "mile": {"count": 7000, "spearman_mean": -0.55},
      "intermediate": {"count": 6000, "spearman_mean": -0.50}
    }
  },
  "error_analysis": {
    "worst_races": [
      {
        "race_id": "202305040301",
        "correlation": 0.1,
        "distance_m": 1600,
        "track_surface": "芝"
      }
    ]
  },
  "recommendations": [
    "⚠ sprint セグメントで性能が低い。このセグメント向けの特徴量追加を検討してください。",
    "✓ 予測精度は良好（相関=-0.52）。"
  ]
}
```

---

## 自動化とスケジューリング

### cronジョブの設定例

#### 1. 毎日深夜3時に全チェックを実行

```bash
# crontab -e で編集
0 3 * * * cd /path/to/KeibaAI_v2 && python keibaai/scripts/run_quality_checks.py --config keibaai/configs/default.yaml
```

#### 2. 週次でモデル分析を実行（日曜日 4時）

```bash
0 4 * * 0 cd /path/to/KeibaAI_v2 && python keibaai/scripts/run_quality_checks.py --config keibaai/configs/default.yaml --model-analysis-only --model-dir keibaai/data/models/latest --predictions keibaai/data/predictions/latest.parquet --actuals keibaai/data/parsed/parquet/races/races.parquet
```

#### 3. エラー検知とアラート通知（Slack連携の場合）

`keibaai/scripts/alert_to_slack.sh` を作成：

```bash
#!/bin/bash

# 品質チェック実行
python keibaai/scripts/run_quality_checks.py --config keibaai/configs/default.yaml

# 失敗した場合（終了コード1）にSlack通知
if [ $? -ne 0 ]; then
  curl -X POST -H 'Content-type: application/json' \
    --data '{"text":"⚠ KeibaAI データ品質チェックで問題が検出されました"}' \
    https://hooks.slack.com/services/YOUR/WEBHOOK/URL
fi
```

crontabに追加：

```bash
0 3 * * * /path/to/KeibaAI_v2/keibaai/scripts/alert_to_slack.sh
```

---

## トラブルシューティング

### Q1: ImportError が発生する

**症状**:
```
ImportError: cannot import name 'ValidationPipeline' from 'keibaai.src.modules.validation'
```

**解決策**:
```bash
# プロジェクトルートからPYTHONPATHを設定
export PYTHONPATH=/path/to/KeibaAI_v2:$PYTHONPATH
python keibaai/scripts/run_quality_checks.py --config keibaai/configs/default.yaml
```

### Q2: データパスが見つからない

**症状**:
```
FileNotFoundError: [Errno 2] No such file or directory: 'keibaai/data/raw/html'
```

**解決策**:
- `keibaai/configs/default.yaml` の `data_path` が正しいか確認
- スクレイピングを実行してデータを生成

```bash
# スクレイピング実行
python keibaai/src/run_scraping_pipeline_local.py
```

### Q3: モデル分析でエラーが発生

**症状**:
```
KeyError: 'predicted_score'
```

**解決策**:
- 予測結果のParquetファイルに `predicted_score` カラムが存在するか確認
- 予測の実行:

```bash
python keibaai/src/models/predict.py \
  --date 2023-10-01 \
  --model_dir keibaai/data/models/latest
```

### Q4: メモリ不足エラー

**症状**:
```
MemoryError
```

**解決策**:
- 日付範囲を絞る:

```bash
python keibaai/scripts/run_quality_checks.py \
  --config keibaai/configs/default.yaml \
  --start-date 2023-10-01 \
  --end-date 2023-10-31
```

- または、バリデーションのみ実行:

```bash
python keibaai/scripts/run_quality_checks.py \
  --config keibaai/configs/default.yaml \
  --validation-only
```

---

## まとめ

このシステムにより、以下が自動化されます：

✅ **データ品質の継続的な監視**
- スクレイピングエラーの早期検出
- パースエラーの自動分析
- データリークの防止

✅ **モデル性能の詳細分析**
- セグメント別の弱点を特定
- 改善提案の自動生成
- ROIの追跡

✅ **異常検知とアラート**
- 品質低下の即時検知
- 重要度別のアラート
- トレンド分析

✅ **包括的なレポート生成**
- JSON + Markdown形式
- 統合レポート
- ダッシュボード用データ

---

**質問・改善提案**:
このガイドに関する質問や改善提案は、GitHubのIssueで受け付けています。
