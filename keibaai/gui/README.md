# KeibaAI_v2 GUI Dashboard

**WebベースのインターフェースでKeibaAI_v2を簡単に操作**

![KeibaAI_v2 Dashboard](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)

---

## 🎯 概要

KeibaAI_v2 GUI Dashboardは、競馬AI予測システムを直感的に操作できるStreamlitベースのWebインターフェースです。
コマンドラインの知識がなくても、ブラウザ上で全ての機能を利用できます。

### 主な機能

- 🏠 **ダッシュボード**: システム全体の状態を一目で確認
- 📊 **データパイプライン**: スクレイピング・パース・特徴量生成をGUIで実行
- 🤖 **モデル学習**: μ, σ, ν モデルの学習をインタラクティブに実行
- 🎲 **シミュレーション**: モンテカルロシミュレーションの設定と実行
- 💰 **ポートフォリオ最適化**: ケリー基準による賭け金最適化
- 📈 **結果分析**: パフォーマンスメトリクスのビジュアル化
- ⚙️ **設定管理**: YAMLファイルをGUIで編集
- 📝 **ログビューア**: リアルタイムログモニタリング

---

## 🚀 クイックスタート

### 1. 依存関係のインストール

```bash
# GUIディレクトリに移動
cd /path/to/KeibaAI_v2/keibaai/gui

# 必要なパッケージをインストール
pip install -r requirements.txt
```

### 2. ダッシュボードの起動

```bash
# プロジェクトルートから実行
cd /path/to/KeibaAI_v2

# Streamlitアプリを起動
streamlit run keibaai/gui/app.py
```

### 3. ブラウザでアクセス

自動的にブラウザが開きます。開かない場合は以下にアクセス:

```
http://localhost:8501
```

---

## 📋 ページ一覧

### 🏠 ダッシュボード
- システムステータスの概要
- データパイプラインの進捗状況
- 最近の活動履歴
- クイックアクション

### 📊 データパイプライン
- **データ取得**: 日付範囲を指定してスクレイピング
- **パース処理**: HTML → Parquet変換
- **特徴量生成**: ML用特徴量の自動生成
- **一括実行**: 全フェーズを連続実行

### 🤖 モデル学習
- **μモデル**: 期待値予測モデルの学習
- **σモデル**: 分散推定モデルの学習
- **νモデル**: 混沌度モデルの学習
- **学習済みモデル**: モデル一覧と管理

### 🎲 シミュレーション
- モンテカルロシミュレーションの実行
- シミュレーション回数の設定
- 結果の可視化
- シミュレーション履歴

### 💰 ポートフォリオ最適化
- フラクショナルケリー基準による最適化
- 初期資金と制約条件の設定
- 最適化結果の表示
- 最適化履歴

### 📈 結果分析
- **モデル評価**: Brier Score, ECE, 精度など
- **シミュレーション分析**: 勝率分布、収束性
- **収益分析**: ROI, 回収率, ドローダウン
- **データ品質**: 欠損値、異常値の確認

### ⚙️ 設定管理
- **基本設定**: データパス、ログレベル
- **スクレイピング設定**: リクエスト間隔、リトライ
- **特徴量設定**: 有効化する特徴量カテゴリ
- **モデル設定**: LightGBMハイパーパラメータ
- **最適化設定**: ケリー係数、EV閾値

### 📝 ログビューア
- **アプリケーションログ**: 全般的なログ
- **スクレイピングログ**: スクレイピング専用ログ
- **モデル学習ログ**: 学習進捗とメトリクス
- **エラーログ**: エラーと警告のまとめ

---

## 🎨 UI/UX の特徴

### インタラクティブな操作
- リアルタイムプログレスバー
- ログのストリーミング表示
- エラーハンドリングとフィードバック

### 視覚的なフィードバック
- メトリクスカード
- カラーコード付きステータス
- グラフとチャートによる可視化

### 使いやすい設計
- サイドバーナビゲーション
- タブによる情報の整理
- エキスパンダーで詳細を隠す
- ツールチップでヘルプ表示

---

## 📁 ディレクトリ構造

```
keibaai/gui/
├── app.py                    # メインアプリケーション
├── components/               # ビューコンポーネント
│   ├── __init__.py
│   ├── dashboard_view.py    # ダッシュボード
│   ├── data_pipeline_view.py # データパイプライン
│   ├── model_training_view.py # モデル学習
│   ├── simulation_view.py   # シミュレーション
│   ├── optimization_view.py # 最適化
│   ├── results_view.py      # 結果分析
│   ├── settings_view.py     # 設定管理
│   └── logs_view.py         # ログビューア
├── requirements.txt         # 依存関係
└── README.md               # このファイル
```

---

## 🔧 カスタマイズ

### テーマの変更

Streamlitのテーマは`.streamlit/config.toml`で変更できます:

```toml
[theme]
primaryColor = "#1f77b4"
backgroundColor = "#ffffff"
secondaryBackgroundColor = "#f0f2f6"
textColor = "#262730"
font = "sans serif"
```

### ポート番号の変更

```bash
streamlit run keibaai/gui/app.py --server.port 8502
```

### ヘッドレスモード（サーバー環境）

```bash
streamlit run keibaai/gui/app.py --server.headless true
```

---

## 🐛 トラブルシューティング

### Q: ダッシュボードが起動しない

**A:** 依存関係を確認してください:
```bash
pip install -r keibaai/gui/requirements.txt
```

### Q: データが表示されない

**A:** データパスが正しく設定されているか確認:
```bash
# configs/default.yaml の data_path を確認
cat keibaai/configs/default.yaml
```

### Q: スクリプト実行時にエラーが出る

**A:** プロジェクトルートから実行されているか確認:
```bash
# 正しい実行方法
cd /path/to/KeibaAI_v2
streamlit run keibaai/gui/app.py
```

### Q: グラフが表示されない

**A:** Plotlyをインストール:
```bash
pip install plotly
```

---

## 📚 詳細ドキュメント

- [CLAUDE.md](../../CLAUDE.md) - 開発者ガイド
- [schema.md](../../schema.md) - データスキーマ
- [指示.md](../../指示.md) - 要件定義（日本語）

---

## 🤝 サポート

問題が発生した場合は、以下を確認してください:

1. **ログファイル**: `keibaai/data/logs/` を確認
2. **設定ファイル**: `keibaai/configs/` のYAMLファイルを確認
3. **GitHubイシュー**: バグ報告や機能リクエスト

---

## 📝 ライセンス

このGUIダッシュボードはKeibaAI_v2プロジェクトの一部です。

---

## 🎉 楽しいKeibaAI_v2ライフを！

ご不明な点がございましたら、お気軽にお問い合わせください。
