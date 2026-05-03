# KeibaAI_v2 — 競馬AI予測＆最適投資システム

**最終更新日**: 2026-05-04

---

## このプロジェクトは何か？

日本の競馬（JRA）のレース結果を予測し、**期待値に基づく最適な賭け方**を提案するAIシステムです。

```
netkeiba.com からデータ収集
        ↓
レース結果・成績・血統をParquet形式に変換
        ↓
66種の特徴量を生成（リークフリー）
        ↓
3つの予測モデル（μ, σ, ν）で馬の能力を推定
        ↓
モンテカルロシミュレーション（1000回）で勝率を算出
        ↓
Kelly Criterion で資金配分を最適化
```

### 3つの予測モデルとは？

| モデル | 正式名 | 役割 | 比喩 |
|--------|--------|------|------|
| **μ（ミュー）** | 期待着順モデル | 各馬の「**強さの平均値**」を予測 | 「この馬はだいたい3着前後の実力」 |
| **σ（シグマ）** | 不確実性モデル | 各馬の「**ブレ幅**」を予測 | 「この馬は安定型 or 大穴型」 |
| **ν（ニュー）** | 混沌度モデル | レース全体の「**荒れやすさ**」を予測 | 「このレースは波乱が起きやすい」 |

この3つを組み合わせて、各馬の勝率を確率分布として推定します。

---

## ディレクトリ構成

```
KeibaAI_v2/
│
├── keibaai/                  ← メインPythonパッケージ
│   ├── src/                      ソースコード（importして使うモジュール群）
│   │   ├── features/             特徴量生成エンジン（※README.md参照）
│   │   ├── models/               予測モデルクラス（MuEstimator等）
│   │   ├── analysis/             分析モジュール（SP値計算等）
│   │   ├── parsers/              HTMLパーサー
│   │   ├── preparing/            スクレイピング
│   │   ├── sim/                  モンテカルロシミュレーション
│   │   ├── optimizer/            Kelly Criterion最適化
│   │   └── dashboard/            Streamlitダッシュボード
│   │
│   ├── configs/                  YAML設定ファイル
│   ├── data/                     ローカルデータ（.gitignore対象）
│   ├── models/                   学習済みモデル（バージョン別、※README.md参照）
│   └── tests/                    テストスイート
│
├── models/                   ← 本番デプロイ用モデル
│   ├── production/               現行本番（V15 + V4.4 アンサンブル）
│   └── production_v2/            次期本番候補（芝/ダート分離戦略）
│
├── scripts/                  ← 実行スクリプト（CLIエントリーポイント）
│   ├── pipelines/                データパイプライン（収集→パース→特徴量）
│   ├── training/                 モデル訓練・評価（※README.md参照）
│   ├── prediction/               本番予測
│   ├── analysis/                 データ分析
│   ├── verification/             モデル検証
│   ├── optimization/             ポートフォリオ最適化
│   ├── simulation/               シミュレーション
│   └── debug/                    デバッグツール
│
├── docs/                     ← ドキュメント
│   ├── system/                   システム仕様書（01〜29）
│   └── references/               参考資料
│
├── outputs/                  ← 分析・レポート出力
├── results/                  ← バックテスト・検証結果
│
├── CLAUDE.md                 ← AI開発ガイドライン（開発者向け）
└── README.md                 ← このファイル
```

### なぜ `models/` が2箇所あるのか？

| ディレクトリ | 目的 | 内容 |
|-------------|------|------|
| **`keibaai/models/`** | 開発・実験用 | バージョン別の学習済みモデル（v15, v16等） |
| **`models/`**（ルート） | 本番運用 | デプロイ済みモデル（production/ に最終版を配置） |

開発中に `keibaai/models/v15/` で訓練・検証し、本番確定後に `models/production/` へデプロイする運用です。

---

## 技術スタック

| カテゴリ | 技術 |
|---------|------|
| 言語 | Python 3.10+（型ヒント付き） |
| データ処理 | pandas, PyArrow (Parquet), NumPy |
| 機械学習 | LightGBM, scikit-learn |
| スクレイピング | requests, BeautifulSoup4, Selenium |
| ストレージ | Parquetファイル |
| 設定 | YAML |
| テスト | pytest |

---

## クイックスタート

### 1. データ収集

```bash
python scripts/pipelines/run_scraping_resumable.py --from-date 2024-01-01
```

### 2. パース（HTML → Parquet）

```bash
python scripts/pipelines/run_parsing_resumable.py
```

### 3. 特徴量生成

```bash
python scripts/pipelines/generate_features.py
```

### 4. モデル訓練

```bash
# V15 Binary モデル（本番ベース）
python scripts/training/train_v15.py

# V4.4 LambdaRank モデル（Residual Ensemble用）
python scripts/training/train_v4_4.py

# σ/ν モデル（不確実性・混沌度）
python scripts/training/train_sigma_v2.py
python scripts/training/train_nu_v2.py
```

### 5. 予測

```bash
python scripts/training/predict.py --date 2026-05-04 --model_dir models/production
```

---

## 本番モデル構成

現在の本番モデルは **V15 + V4.4 のアンサンブル** です。

```
V15 Binary (着順予測)     → 基本予測
V4.4 LambdaRank (穴馬検出) → 残差予測 (V15が見逃す情報を補完)
                             ↓
                      加重平均 → 最終μ値
                             ↓
                      σ, ν と合わせてシミュレーション
                             ↓
                      馬券種別の勝率 → Kelly最適化 → 購入金額
```

---

## 関連ドキュメント

| ドキュメント | 対象読者 | 内容 |
|-------------|---------|------|
| [CLAUDE.md](CLAUDE.md) | AI/開発者 | コード規約・開発ガイドライン |
| [docs/system/01_システム概要.md](docs/system/01_システム概要.md) | 全員 | システム全体像 |
| [docs/system/07_機械学習モデル.md](docs/system/07_機械学習モデル.md) | 開発者 | モデル詳細・パラメータ |
| [docs/system/14_モデル改善戦略.md](docs/system/14_モデル改善戦略.md) | 開発者 | 過去の改善履歴・失敗事例 |
| [keibaai/models/README.md](keibaai/models/README.md) | 開発者 | モデルバージョン一覧・命名規則 |
| [keibaai/src/features/README.md](keibaai/src/features/README.md) | 開発者 | 特徴量エンジニア依存関係 |
| [scripts/training/README.md](scripts/training/README.md) | 開発者 | 訓練スクリプト一覧 |
