# CLAUDE.md - KeibaAI_v2 AI開発ガイドライン

**最終更新日**: 2026-05-04  
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
> - 本番モデルは `keibaai/models/{version_name}/` に配置
> - 実験モデルは `keibaai/models/_archive/{version_name}/` に配置
> - 本番モデル必須ファイル: `model.pkl`(or `.txt`), `feature_names.json`, `model_info.json`
> - 推奨ファイル: `feature_importance.csv`, `yearly_roi.csv`
> - 命名規則: `v{N}`=Binary, `mu_v{N}_{M}`=μモデル, `sigma_v{N}`=σ, `nu_v{N}`=ν
> - 詳細は `keibaai/models/README.md` を参照

> [!CAUTION]
> **ルートディレクトリに配置してよいファイル**:
> - `CLAUDE.md` - このファイル（AI開発ガイドライン）
> - `README.md` - GitHubリポジトリ説明
> - `requirements.txt`, `pyproject.toml` - 依存関係定義
> - `.gitignore`, `.env` - Git/環境設定
>
> **ルートに配置してよいディレクトリ**:
> - `keibaai/` - メインパッケージ
> - `scripts/` - 実行スクリプト
> - `models/` - 本番デプロイ用モデル（production/production_v2）
> - `docs/` - ドキュメント
> - `outputs/` - 分析出力
> - `results/` - 検証・バックテスト結果
>
> **ルートに配置してはいけないファイル（発見次第移動/削除）**:
> - `*.log` → `keibaai/data/logs/` または削除
> - `*_list.txt`, `*.csv`(IDリスト) → `keibaai/data/metadata/`
> - `*.xlsx` → `outputs/analysis/`
> - 実行スクリプト → `scripts/` 配下へ移動
> - `debug_*.py` → 削除

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
│   │   ├── features/                 # 特徴量モジュール（※README.md参照）
│   │   │   ├── feature_engine.py        # FeatureEngineクラス（旧版）
│   │   │   ├── leak_free_feature_engineer_v15.py  # ★本番V15用
│   │   │   ├── leak_free_feature_engineer_v15_fixed.py  # ★V4.4/V16/V22のベース
│   │   │   ├── leak_free_feature_engineer_v16.py  # V16候補
│   │   │   ├── leak_free_feature_engineer_v22.py  # V22保険
│   │   │   ├── leak_free_feature_engineer_v2〜v14.py  # 依存チェーン（削除不可）
│   │   │   └── roi_features.py          # ROI最適化用特徴量
│   │   ├── models/                  # モデルモジュール
│   │   │   ├── model_train.py           # MuEstimatorクラス
│   │   │   ├── sigma_estimator.py       # SigmaEstimatorクラス
│   │   │   └── nu_estimator.py          # NuEstimatorクラス
│   │   ├── analysis/speed_index/    # SP値（スピード指数）計算エンジン
│   │   │   ├── calculator.py            # SP値計算（基準タイム、馬場指数、斤量補正）
│   │   │   ├── pace_correction.py       # ペース補正エンジン
│   │   │   └── calibration.py           # 係数キャリブレーション
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
│   │   ├── metadata/                # IDリスト等
│   │   └── logs/                    # アプリケーションログ
│   ├── models/                      # モデルバージョン別フォルダ（※README.md参照）
│   │   ├── v15/                     # ★V15 Binaryモデル（本番）
│   │   ├── v16/                     # V16モデル（候補）
│   │   ├── mu_v4_4/                 # ★V4.4 LambdaRank（本番Residual）
│   │   ├── sigma_v2/                # σモデル（不確実性推定）
│   │   ├── nu_v2/                   # νモデル（混沌度推定）
│   │   ├── mu_v2/, mu_baseline/     # 参照用ベースライン
│   │   ├── v26_restored/            # V2.6復元版（設定参照）
│   │   └── _archive/                # 過去の実験モデル（69フォルダ）
│   └── tests/                       # テストスイート
├── models/                          # 本番デプロイ用モデル
│   ├── production/                  # ★V15+V4.4 Ensemble
│   └── production_v2/               # 芝/ダート分離戦略
├── scripts/                         # 実行スクリプト（CLIツール）
│   ├── pipelines/                   # データパイプラインスクリプト
│   ├── training/                    # モデル訓練・予測（※README.md参照）
│   │   ├── train_v15.py                 # ★V15モデル訓練
│   │   ├── predict.py                   # μ/σ/ν推論パイプライン
│   │   └── _archive/                    # 過去の実験スクリプト
│   ├── prediction/                  # 本番予測スクリプト
│   ├── analysis/                    # 分析スクリプト
│   ├── verification/                # 検証スクリプト
│   ├── optimization/                # ポートフォリオ最適化
│   ├── simulation/                  # シミュレーション
│   └── debug/                       # デバッグ・検証ツール
├── docs/                            # ドキュメント
│   ├── system/                      # システムドキュメント
│   └── references/                  # 参考資料（article.txt等）
├── outputs/                         # 分析出力
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
    YES → scripts/training/_archive/xxx.py
    NO  → scripts/{category}/xxx.py

[Q3] 実験的モデル？
    YES → keibaai/models/_archive/{version}/
    NO  → keibaai/models/{version}/
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

# V15 Binary モデル訓練（本番ベース）
python scripts/training/train_v15.py

# V4.4 LambdaRank モデル訓練（Residual Ensemble用）
python scripts/training/train_v4_4.py

# σ/ν モデル訓練（不確実性・混沌度推定）
python scripts/training/train_sigma_v2.py
python scripts/training/train_nu_v2.py

# 推論パイプライン
python scripts/training/predict.py --date 2026-05-04 --model_dir models/production

# Walk-forward 検証
python scripts/training/walkforward_v15.py
```

---

## 📦 主要モジュール

### 1. LeakFreeFeatureEngineerV15（本番特徴量生成）

**ファイル**: `keibaai/src/features/leak_free_feature_engineer_v15.py`

```python
from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15

engine = LeakFreeFeatureEngineerV15()
df = engine.generate_features(results_df)  # 66特徴量を生成
feature_cols = engine.get_feature_columns()
```

> 依存チェーン: v2→v3→...→v14→v15（詳細は `keibaai/src/features/README.md`）

### 2. MuEstimator（μモデル = 期待着順予測）

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

### 3. RaceSimulator（モンテカルロシミュレーション）

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
| [README.md](README.md) | **プロジェクト概要（第三者向け）** |
| [keibaai/models/README.md](keibaai/models/README.md) | **モデル構成・命名規則** |
| [keibaai/src/features/README.md](keibaai/src/features/README.md) | **特徴量エンジニア依存関係** |
| [scripts/training/README.md](scripts/training/README.md) | **訓練スクリプト一覧** |
| [docs/system/01_システム概要.md](docs/system/01_システム概要.md) | システム全体像 |
| [docs/system/07_機械学習モデル.md](docs/system/07_機械学習モデル.md) | モデル詳細 |
| [docs/system/14_モデル改善戦略.md](docs/system/14_モデル改善戦略.md) | モデル改善方針・失敗事例 |
| [docs/system/23_包括的検証レポート_v3.md](docs/system/23_包括的検証レポート_v3.md) | V15検証結果 |
| [docs/system/28_特徴量エンジニアリング検証レポート.md](docs/system/28_特徴量エンジニアリング検証レポート.md) | 特徴量検証・分析方法論 |

---

## 🔬 分析時の心がけ（2026-01-16追加）

特徴量やモデル改善を検証する際は以下を遵守：

> [!IMPORTANT]
> **すぐに結論を出さない**: 複数の視点から検証を重ねる
> 
> **過適合を常に警戒**: 訓練/検証の完全分離、多年度検証、前半/後半比較
> 
> **「なぜ効果があるのか」を考える**: 効果が見られても理由を深堀り
> 
> **本番モデルと同条件でテスト**: V15/V4.4はpopularityを使用していない
> 
> 詳細は [28_特徴量エンジニアリング検証レポート.md](docs/system/28_特徴量エンジニアリング検証レポート.md) を参照

---

**Note**: このファイルはAIアシスタント向けのメタドキュメントです。プロジェクトの仕様変更に合わせて適宜更新してください。

**最終更新日**: 2026-05-04
