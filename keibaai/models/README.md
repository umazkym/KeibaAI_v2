# モデルディレクトリ（keibaai/models/）

**最終更新日**: 2026-05-04

---

## ディレクトリ構成

```
keibaai/models/
├── v15/           ← V15 Binary (本番ベースモデル)
├── v16/           ← V16 Binary (複勝特化候補)
├── mu_v4_4/       ← V4.4 LambdaRank (Residual Ensemble用)
├── mu_v2/         ← μモデル v2.0 (時間予測ベースライン)
├── mu_baseline/   ← μモデル初期ベースライン
├── sigma_v2/      ← σモデル (不確実性推定)
├── nu_v2/         ← νモデル (混沌度推定)
├── v26_restored/  ← v2.6復元版 (設定参照用)
└── _archive/      ← 過去の実験モデル (69フォルダ)
```

## 本番モデルの関係

```
models/production/          ← 本番デプロイ用 (V15 + V4.4 Ensemble)
├── turf_base_model.pkl         V15ベース (芝)
├── dirt_base_model.pkl         V15ベース (ダート)
├── turf_underdog_model.pkl     V4.4 Residual (芝)
├── dirt_underdog_model.pkl     V4.4 Residual (ダート)
├── feature_engineer.pkl        特徴量エンジニア (pickle)
└── config.yaml                 ハイパーパラメータ

models/production_v2/       ← 本番v2 (芝/ダート分離戦略)
├── turf_model.pkl              芝専用モデル
├── dirt_model.pkl              ダート専用モデル
├── feature_engineer.pkl        特徴量エンジニア
└── config.yaml                 ハイパーパラメータ
```

## 各モデルの役割

| フォルダ | 手法 | 目的関数 | 特徴量 | ROI(5年平均) | 状態 |
|---------|------|---------|--------|-------------|------|
| **v15** | LightGBM Binary | AUC | V15 (66個) | 76.8% | ✅ 本番 |
| **v16** | LightGBM Binary | AUC | V16 (72個) | 78.1% | ⚠️ 候補 |
| **mu_v4_4** | LightGBM LambdaRank | NDCG | V16 | 82.6% | ✅ 本番(Residual) |
| **sigma_v2** | LightGBM Regression | RMSE | μ残差ベース | — | ✅ 本番 |
| **nu_v2** | LightGBM Regression | MAE | レース特徴量 | — | ✅ 本番 |
| **mu_v2** | LightGBM Ranker+Reg | RMSE | 時間予測用 | 76.7% | 📚 参照 |
| **mu_baseline** | LightGBM | — | 基本特徴量 | — | 📚 参照 |
| **v26_restored** | — | — | — | — | 📚 設定参照 |

## _archive/ について

過去の実験で生成された69フォルダが格納されています。
これらは本番では使用されていませんが、実験結果の参照用に保持しています。

主な系統:
- `mu_time_v1〜v5`: 時間予測μモデルの初期実験
- `mu_v2_4〜v2_8`: μv2系のROI最適化実験
- `mu_v3_0〜v3_8`: Ranker/Ensemble実験
- `mu_v4_0〜v4_5`: コンテキスト認識実験 (v4_4が本番昇格)
- `mu_v5_0〜v9_0`: 特徴量拡張実験
- `mu_v10_0〜v11_2`: リークフリー完全版実験
- その他: catboost_v1, ensemble_v5, 芝/ダート分離実験 等

## 命名規則

- `v{N}`: Binary分類モデル (1着予測)
- `mu_v{N}_{M}`: μモデル (時間予測) のバージョンN.M
- `sigma_v{N}`: σモデル (不確実性推定)
- `nu_v{N}`: νモデル (混沌度推定)
- `_archive/`: 過去の実験モデル（本番未使用）
