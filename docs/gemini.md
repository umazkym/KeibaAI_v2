# gemini.md - KeibaAI_v2 AI Assistant Guidelines

このファイルは、KeibaAI_v2プロジェクトにおいてAIアシスタント（Gemini）がユーザーと対話・開発を行う際の指針を定めたものです。

## 1. 基本原則

*   **言語**: ユーザーとの対話は全て**日本語**で行います。
*   **ドキュメント**: プロジェクトに関するドキュメントや知見は、`docs/` ディレクトリ内に記述・追記します。ルートディレクトリや他の場所に散乱させないでください。
*   **思考プロセス**:
    *   **深く推論する**: 速度よりも質を重視します。表面的な解決策ではなく、根本的な原因や影響範囲を深く考慮してください。時間がかかっても構いません。
    *   **一時的な解決の禁止**: "とりあえず動く" 修正は避け、保守性と拡張性を考慮した実装を行ってください。
    *   **不確実性の排除**: 不明点や自信のない仮定がある場合は、勝手に判断せず、必ずユーザーに質問するか、デバッグコードを作成・実行して事実確認を行ってください。
*   **コンテキスト理解**:
    *   **ディレクトリ構造の把握**: プロジェクトのディレクトリ構造（`CLAUDE.md` 参照）を常に意識し、適切な場所にファイルを配置・編集してください。適当な構造にしてはいけません。

## 2. プロジェクト参照資料

開発や回答を行う際は、以下の既存ドキュメントを優先的に参照してください。

*   **[CLAUDE.md](CLAUDE.md)**: プロジェクトの全体像、アーキテクチャ、主要モジュール、開発ワークフロー、コーディング規約などが網羅されています。まずこれを読んでください。
*   **[docs/system/](docs/system/)**: システムの詳細な仕様書が格納されています。
    *   `01_システム概要.md`
    *   `02_アーキテクチャ.md`
    *   `03_データモデル.md`
    *   ...など

## 3. 行動指針

1.  **タスク開始時**:
    *   `CLAUDE.md` と `task.md` を確認し、現状と目標を明確にします。
    *   必要であれば `docs/` 以下の関連ドキュメントを読み込みます。

2.  **コーディング時**:
    *   `CLAUDE.md` の "Code Conventions & Patterns" を厳守してください。
    *   既存のコードスタイル（型ヒント、docstring、エラーハンドリング）に合わせます。

3.  **行き詰まった時**:
    *   推測で進めず、デバッグスクリプト（`debug_*.py`）を作成して検証するか、ユーザーに相談してください。

## 4. ディレクトリ配置ルール（重要）

**最終更新**: 2025-11-24

### 📋 明確な配置ルール

> **重要**: 以下のルールに厳密に従ってください。例外を作らないことが、長期的な保守性の鍵です。

| ディレクトリ | 用途 | 配置するファイル |
|-------------|------|----------------|
| **`keibaai/src/`** | Pythonパッケージ（import用） | クラス・関数定義、モジュール |
| **`keibaai/src/<category>/`** | ★レガシー実行スクリプト | `generate_features.py`, `daily_allocator.py` など（段階的移行予定） |
| **`scripts/<category>/`** | CLI実行スクリプト（本番） | `python scripts/xxx.py` で実行するスクリプト |
| **`scripts/training/experimental/`** | 実験的スクリプト・過去バージョン | モデル開発の試行錯誤記録、過去の訓練アプローチ |
| **`scripts/temp/`** | 一時作業用スクリプト | すぐに削除される検証用スクリプト |
| **`docs/`** | ドキュメント | システム仕様、レポート、ガイドライン |
| **`archive/`** | アーカイブ | 使用されなくなったスクリプト・ログ |

### 🔍 新規ファイル作成時の判断フロー

```
新しいファイルを作成する
    ↓
Q1: これは実行スクリプト (python xxx.py で直接実行)?
    ├─ NO  → keibaai/src/<category>/xxx.py (モジュール・クラス定義)
    └─ YES → Q2へ
              ↓
         Q2: 本番環境で使用?
              ├─ YES → scripts/<category>/xxx.py
              └─ NO  → Q3へ
                        ↓
                   Q3: 実験的スクリプト? (過去バージョン、試行錯誤)
                        ├─ YES → scripts/training/experimental/xxx.py
                        └─ NO  → scripts/temp/xxx.py
```

### 📝 Experimental Scripts (`scripts/training/experimental/`)

**目的**: モデル開発段階での実験的スクリプト・過去バージョンの保存

**保存対象**:
- 過去の訓練アプローチ
- 実験的な実装
- 異なるアルゴリズムの試行
- Phase X 実装のスナップショット

**重要な原則**:
- **削除しない**: 「後から推論分析できるように」するため、実験的スクリプトは削除せず保存
- **README.md を追加**: experimental/ には必ず説明ドキュメントを配置
- **命名規則**: `train_xxx_phase_d.py`, `train_xxx_experiment.py` などわかりやすい名前

**使用例**:
```bash
# 過去のアプローチを参照
python scripts/training/experimental/train_sigma_nu_phase_d.py --help

# 特定バージョンのモデルを再現
python scripts/training/experimental/train_full_pipeline.py --date 2023-10-01
```

### 📌 レガシースクリプト（注意）

一部のスクリプトが `keibaai/src/` に残存していますが、**新規作成は禁止**:

| ファイル | 理由 | 対処 |
|---------|------|------|
| `keibaai/src/features/generate_features.py` | 使用頻度が高い | 移動はリスクが高いため当面残存 |
| `keibaai/src/optimizer/daily_allocator.py` | レガシー | 必要に応じて段階的移行 |

**新規スクリプトは必ず `scripts/` に配置してください。**

### 🎯 実例

#### ✅ 正しい配置

```
# モジュール (import して使う)
keibaai/src/modules/models/model_train.py  # MuEstimatorクラス
keibaai/src/features/feature_engine.py     # FeatureEngineクラス

# CLI実行スクリプト (本番)
scripts/training/train_mu_v2_model.py
scripts/pipelines/run_scraping_pipeline_local.py
scripts/optimization/optimize_daily_races.py

# 実験的スクリプト
scripts/training/experimental/train_full_pipeline.py
scripts/training/experimental/README.md

# 一時スクリプト
scripts/temp/test_new_feature.py
```

#### ❌ 間違った配置

```
# NG: 実行スクリプトを keibaai/src/ に配置
keibaai/src/models/train_new_model.py  # → scripts/training/ へ

# NG: モジュールを scripts/ に配置
scripts/training/mu_estimator.py  # → keibaai/src/models/ へ

# NG: 実験スクリプトを削除
(削除) train_old_approach.py  # → experimental/ に移動すべき
```

### 🔧 import の書き方

移動したスクリプトでは、適切な import パスを設定:

```python
# scripts/<category>/xxx.py の場合
import sys
from pathlib import Path

# プロジェクトルートをsys.pathに追加
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / 'keibaai'))  # レガシーimport対応

# これで keibaai パッケージをimportできる
from keibaai.src.modules.models.model_train import MuEstimator
```

---
**Note**: このファイルはAIアシスタント自身が参照するためのメタドキュメントです。プロジェクトの仕様変更に合わせて適宜更新してください。
