# CLAUDE.md - KeibaAI_v2 AI開発ガイドライン

**最終更新日**: 2025-12-20  
**プロジェクト**: KeibaAI_v2 - 競馬AI予測＆最適投資システム  
**目的**: AIアシスタントがこのコードベースで作業するためのガイド

---

## 🚨 重要ルール（必ず守ること）

> [!IMPORTANT]
> **ディレクトリ配置ルール**:
> - `keibaai/src/`: Pythonパッケージ（import用モジュール・クラス）のみ
> - `scripts/`: CLI実行スクリプト（エントリーポイント）
> - `docs/system/`: プロジェクトドキュメント一式
> - `keibaai/models/{version}/`: モデルバージョン別フォルダ

> [!IMPORTANT]
> **モデルバージョニングルール**:
> - モデル構築ごとに `keibaai/models/{version_name}/` フォルダを作成
> - 必須ファイル: `config.yaml`, `report.md`, `features.txt`
> - 推奨ファイル: `multi_period_results.csv`, `development_log.md`
> - 詳細は [docs/system/15_モデル命名規則とベストプラクティス.md](docs/system/15_モデル命名規則とベストプラクティス.md) を参照

> [!CAUTION]
> **ルートディレクトリに配置してよいファイル**:
> - `CLAUDE.md` - このファイル（AI開発ガイドライン）
> - `README.md` - GitHubリポジトリ説明
> - `requirements.txt`, `pyproject.toml` - 依存関係定義
> - `.gitignore`, `.env` - Git/環境設定
>
> **ルートに配置してはいけないファイル（発見次第移動/削除）**:
> - `*.log` → `keibaai/data/logs/` または削除
> - `*_list.txt` → `keibaai/data/metadata/` または削除
> - 実行スクリプト → `scripts/` 配下へ移動

---

## 📋 目次

1. [プロジェクト概要](#プロジェクト概要)
2. [アーキテクチャとディレクトリ構造](#アーキテクチャとディレクトリ構造)
3. [データパイプライン](#データパイプライン)
4. [主要モジュール](#主要モジュール)
5. [開発ワークフロー](#開発ワークフロー)
6. [設定管理](#設定管理)
7. [テスト基盤](#テスト基盤)
8. [よくあるタスク](#よくあるタスク)
9. [コード規約](#コード規約)
10. [注意事項](#注意事項)

---

## 🎯 プロジェクト概要

### KeibaAI_v2 とは？

**競馬AI予測＆最適投資システム**:
- netkeiba.com / JRA公式から競馬データをスクレイピング
- HTMLを構造化データ（Parquet形式）にパース
- レース履歴・血統・成績データから特徴量を生成
- LightGBMで確率モデル（μ, σ, ν）を訓練
- モンテカルロシミュレーションで勝率を推定
- Fractional Kelly Criterionでポートフォリオ最適化
- パフォーマンス指標（Brier score, ECE, ROI）を追跡

### 主要統計

| 項目 | 数値 |
|-----|------|
| コード行数 | 約5,025行 |
| レースレコード数 | 約278,098件 |
| 血統レコード数 | 約1,377,361件（5世代） |
| テストカバレッジ | ユニット・統合・回帰テスト完備 |
| クラウドコスト | 0円（完全ローカル） |

### 技術スタック

| カテゴリ | 技術 |
|---------|------|
| 言語 | Python 3.10+ (型ヒント付き) |
| データ処理 | pandas, pyarrow (Parquet), NumPy |
| 機械学習 | LightGBM, scikit-learn |
| スクレイピング | requests, BeautifulSoup4, Selenium |
| ストレージ | Parquetファイル, SQLiteメタデータ |
| テスト | pytest |
| 設定 | YAMLファイル |

---

## 🏗️ アーキテクチャとディレクトリ構造

### システムアーキテクチャ

```
┌─────────────────────────────────────────────────────────────┐
│                    KeibaAI_v2 System                         │
├─────────────────────────────────────────────────────────────┤
│  [スクレイピング] → [パース] → [特徴量] → [モデル]           │
│       ↓              ↓           ↓          ↓               │
│    HTML.bin      Parquet    Feature.pq  LightGBM            │
│                                             ↓               │
│                              [シミュレーション] → [最適化]   │
│                                     ↓              ↓        │
│                                  勝率推定      Kelly配分    │
└─────────────────────────────────────────────────────────────┘
```

### ディレクトリ構造

```
KeibaAI_v2/
├── keibaai/                          # メインPythonパッケージ
│   ├── src/                          # ソースコード（import用モジュール）
│   │   ├── features/                 # 特徴量モジュール
│   │   │   ├── feature_engine.py        # FeatureEngineクラス
│   │   │   ├── advanced_features.py     # 高度な特徴量
│   │   │   └── leak_free_feature_engineer_v*.py  # リークフリー版
│   │   ├── models/                  # モデルモジュール
│   │   │   ├── model_train.py           # MuEstimatorクラス
│   │   │   ├── sigma_estimator.py       # SigmaEstimatorクラス
│   │   │   └── nu_estimator.py          # NuEstimatorクラス
│   │   ├── parsers/                 # HTMLパーサー
│   │   ├── preparing/               # スクレイピングモジュール
│   │   ├── sim/                     # シミュレーションモジュール
│   │   ├── optimizer/               # 最適化モジュール
│   │   └── dashboard/               # Streamlitダッシュボード
│   ├── configs/                     # YAML設定ファイル
│   ├── data/                        # ローカルデータ（.gitignore）
│   │   ├── raw/html/                # 生HTMLファイル（.bin形式）
│   │   ├── parsed/parquet/          # パース済みParquetファイル
│   │   ├── features/parquet/        # 特徴量データ
│   │   └── logs/                    # アプリケーションログ
│   ├── models/                      # モデルバージョン別フォルダ ★
│   │   ├── v15_legacy/              # V15レガシーモデル
│   │   ├── v26_restored/            # V2.6復元版
│   │   └── {version}/               # 新バージョン
│   └── tests/                       # テストスイート
├── scripts/                         # 実行スクリプト（CLIツール）★
│   ├── pipelines/                   # データパイプラインスクリプト
│   ├── training/                    # モデル訓練・予測 ★
│   │   ├── train_mu_model.py            # μモデル訓練
│   │   ├── train_mu_v2_model.py         # μv2.0モデル訓練
│   │   ├── test_multi_period_v26.py     # 複数期間テスト
│   │   └── experimental/                # 実験的スクリプト ★
│   ├── optimization/                # ポートフォリオ最適化
│   ├── simulation/                  # シミュレーション
│   ├── debug/                       # デバッグ・検証ツール
│   └── temp/                        # 一時スクリプト
├── docs/                            # ドキュメント
│   └── system/                      # システムドキュメント（29ファイル）
├── CLAUDE.md                        # このファイル
└── README.md                        # ユーザー向けREADME
```

### ファイル配置の判断フロー

```
新しいファイルを作成する
    ↓
[Q1] これは実行スクリプト？（python xxx.py で直接実行）
    YES → scripts/{category}/xxx.py
    NO  → keibaai/src/{category}/xxx.py（モジュール・クラス定義）
    
[Q2] 実験的スクリプト？
    YES → scripts/training/experimental/xxx.py
    NO  → scripts/{category}/xxx.py
```

---

## 🔄 データパイプライン

### エンドツーエンドパイプライン

| フェーズ | 時刻 | 処理 | 入力 | 出力 |
|---------|------|------|------|------|
| 0 | 03:00 | スクレイピング | netkeiba.com | `data/raw/html/*.bin` |
| 1 | 04:00 | パース | HTML.bin | `data/parsed/parquet/*.parquet` |
| 2 | 04:30 | 特徴量生成 | Parquet | `data/features/parquet/` |
| 3 | 週次 | モデル訓練 | Features | `models/{version}/model.pkl` |
| 4 | 10:00 | 予測 | Features + Model | Predictions |
| 5 | レース前 | シミュレーション | Predictions | 勝率推定 |
| 6 | レース前 | 最適化 | 勝率 + オッズ | 配分金額 |

### 主要コマンド

```bash
# スクレイピング
python scripts/pipelines/run_scraping_resumable.py --from-date 2024-01-01

# パース
python scripts/pipelines/run_parsing_resumable.py

# 特徴量生成
python scripts/pipelines/generate_features.py

# モデル訓練
python scripts/training/train_mu_v2_model.py

# 複数期間テスト
python scripts/training/test_multi_period_v26.py
```

---

## 📦 主要モジュール

### 1. FeatureEngine（特徴量生成）

**ファイル**: `keibaai/src/features/feature_engine.py`

```python
from keibaai.src.features.feature_engine import FeatureEngine

fe = FeatureEngine(config_path='keibaai/configs/features.yaml')
df = fe.generate_features(shutuba_df, results_history_df, horse_profiles_df)
feature_cols = fe.get_feature_columns()
```

### 2. LeakFreeFeatureEngineerV15（リークフリー特徴量）

**ファイル**: `keibaai/src/features/leak_free_feature_engineer_v15.py`

- V15推奨モデル用の66特徴量を生成
- リーク防止策: `expanding().mean().shift(1)`
- 主要特徴量: `horse_c4_gap_avg`, `post_style_conflict`, `race_front_runner_count`

### 3. MuEstimator（μモデル）

**ファイル**: `keibaai/src/models/model_train.py`

```python
from keibaai.src.models.model_train import MuEstimator

estimator = MuEstimator(
    ranker_params={'objective': 'lambdarank', 'n_estimators': 1000},
    regressor_params={'objective': 'regression'}
)
estimator.fit(X_train, y_train, group=groups)
mu_pred = estimator.predict(X_test)
```

### 4. RaceSimulator（モンテカルロシミュレーション）

**ファイル**: `keibaai/src/sim/simulator.py`

```python
from keibaai.src.sim.simulator import RaceSimulator

simulator = RaceSimulator(K=1000)
results = simulator.simulate(mu_array, sigma_array, nu)
win_probs = results['win_probs']
```

---

## ⚙️ 設定管理

### YAML設定ファイル

| ファイル | 用途 |
|---------|------|
| `keibaai/configs/default.yaml` | 基本設定（パス、ログ） |
| `keibaai/configs/scraping.yaml` | スクレイピング設定 |
| `keibaai/configs/features.yaml` | 特徴量設定 |
| `keibaai/configs/models.yaml` | モデル設定 |

### モデルバージョン設定

各モデルバージョンは `keibaai/models/{version}/config.yaml` に設定を保存:

```yaml
version: "v26_restored"
created_at: "2025-12-20"

hyperparams:
  objective: "binary"
  learning_rate: 0.03
  max_depth: 3
  num_boost_round: 200

v15_features:
  enabled: true
  min_corner_races: 5
```

---

## 🧪 テスト基盤

### テスト実行

```bash
# 全テスト実行
pytest keibaai/tests/

# ユニットテストのみ
pytest keibaai/tests/unit/

# 特定テスト
pytest keibaai/tests/unit/test_feature_engine.py -v
```

### テスト構造

```
keibaai/tests/
├── unit/           # ユニットテスト
├── integration/    # 統合テスト
└── regression/     # 回帰テスト
```

---

## 📝 コード規約

### 必須ルール

1. **型ヒント**: 全ての関数に型ヒントを付ける
2. **docstring**: 全ての公開関数にdocstringを付ける
3. **エラーハンドリング**: 適切な例外処理を行う
4. **ログ**: `logging` モジュールを使用

### コード例

```python
import logging
from typing import Optional
import pandas as pd

logger = logging.getLogger(__name__)

def process_data(
    df: pd.DataFrame,
    threshold: float = 0.5,
    debug: bool = False
) -> Optional[pd.DataFrame]:
    """
    データを処理する
    
    Args:
        df: 入力データフレーム
        threshold: フィルタリング閾値
        debug: デバッグモード
    
    Returns:
        処理済みデータフレーム、エラー時はNone
    """
    try:
        result = df[df['value'] > threshold]
        logger.info(f"処理完了: {len(result)}行")
        return result
    except Exception as e:
        logger.error(f"処理エラー: {e}")
        return None
```

---

## ⚠️ 注意事項

### データリーク防止

以下の特徴量は学習時に**必ず除外**:

```python
FORBIDDEN_FEATURES = [
    'finish_position',      # 結果
    'finish_time_seconds',  # 結果
    'win_odds',             # 確定オッズ
    'popularity',           # 確定人気
    'last_3f_time',         # レース結果
    'passing_order_*',      # レース結果
]
```

### 時系列分割

```python
# ❌ NG: ランダム分割（データリーク）
from sklearn.model_selection import KFold
kfold = KFold(n_splits=5, shuffle=True)

# ✅ OK: 時系列分割
from sklearn.model_selection import TimeSeriesSplit
tscv = TimeSeriesSplit(n_splits=5)
```

### パス参照

```python
# ❌ NG: 相対パス
config_path = 'configs/default.yaml'

# ✅ OK: プロジェクトルートからの絶対パス
from pathlib import Path
project_root = Path(__file__).resolve().parent.parent.parent
config_path = project_root / 'keibaai' / 'configs' / 'default.yaml'
```

---

## 📚 関連ドキュメント

| ドキュメント | 内容 |
|-------------|------|
| [docs/system/01_システム概要.md](docs/system/01_システム概要.md) | システム全体像 |
| [docs/system/07_機械学習モデル.md](docs/system/07_機械学習モデル.md) | モデル詳細 |
| [docs/system/15_モデル命名規則とベストプラクティス.md](docs/system/15_モデル命名規則とベストプラクティス.md) | バージョニング |
| [docs/system/23_包括的検証レポート_v3.md](docs/system/23_包括的検証レポート_v3.md) | V15検証結果 |
| [docs/system/99_AI開発ガイドライン.md](docs/system/99_AI開発ガイドライン.md) | AI向け詳細ガイド |

---

**Note**: このファイルはAIアシスタント向けのメタドキュメントです。プロジェクトの仕様変更に合わせて適宜更新してください。
